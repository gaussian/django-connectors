"""Run lifecycle.

Every function takes explicit objects and ids plus an optional `actor` — never a
``request``. The service layer is the integration surface hosts are expected to
build on, and a layer that needs an HTTP request cannot be called from a Celery
task, a management command or a test without one being faked.
"""

import logging

from django.db import IntegrityError, transaction
from django.utils import timezone

from django_connectors import locks
from django_connectors.enums import (
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
)
from django_connectors.errors import describe, find_cause, scrub
from django_connectors.exceptions import (
    ConnectorError,
    CredentialsExpired,
    CredentialsRevoked,
)
from django_connectors.landing import runner
from django_connectors.models import Run
from django_connectors.registry import sources

logger = logging.getLogger(__name__)


def enqueue_run(binding, *, trigger=RunTrigger.SCHEDULED, task_reference=""):
    """Queue a Run, collapsing duplicates.

    At most one queued Run exists per Binding: `dedupe_key` carries the binding
    id while queued and is cleared on the transition to running, so a burst of
    webhook deliveries produces one run rather than one per delivery. The
    IntegrityError *is* the coalescing — it means a queued Run already exists.
    """
    try:
        with transaction.atomic():
            return Run.objects.create(
                binding=binding,
                trigger=trigger,
                status=RunStatus.QUEUED,
                dedupe_key=binding.id,
                task_reference=task_reference,
            )
    except IntegrityError:
        return (
            Run.objects.filter(binding=binding, status=RunStatus.QUEUED)
            .order_by("-created_at")
            .first()
        )


def run_binding(binding, *, trigger=RunTrigger.MANUAL, run=None, actor=None):
    """Execute one ingestion for `binding`, recording a Run either way.

    Returns the Run. A Run is always recorded — including when the lease could
    not be taken — so that "nothing happened" is distinguishable from "nothing
    was attempted".
    """
    token = locks.acquire(binding)
    if token is None:
        return _record_skip(
            binding, run, trigger, "another worker holds this binding's lease"
        )

    run = _start(binding, run, trigger)
    try:
        _ensure_runnable(binding)
        source_definition = sources.get(binding.source)
        credentials = _credentials_for(binding.connection)

        _drain_pending_if_needed(binding, source_definition)

        result = runner.execute(
            binding,
            run,
            source_definition=source_definition,
            credentials=credentials,
        )
    except Exception as exc:
        # Classify on the innermost cause, not on what was raised: dlt wraps
        # everything a resource raises in PipelineStepFailed, so matching on the
        # outer type would never see a revoked credential and the Binding would
        # be retried against it forever.
        credentials_error = find_cause(exc, (CredentialsRevoked, CredentialsExpired))
        if credentials_error is not None:
            _block_for_credentials(binding, credentials_error)
        _fail(binding, run, exc)
        return run
    else:
        _succeed(binding, run, result)
        return run
    finally:
        locks.release(binding, token)


def _start(binding, run, trigger):
    now = timezone.now()
    if run is None:
        run = Run.objects.create(binding=binding, trigger=trigger)
    run.status = RunStatus.RUNNING
    run.started_at = now
    run.dlt_pipeline_name = binding.pipeline_name
    # Clearing the dedupe key frees the queue slot: a delivery arriving now
    # should produce a follow-up run rather than being swallowed.
    run.dedupe_key = None
    run.save(update_fields=["status", "started_at", "dlt_pipeline_name", "dedupe_key"])
    return run


def _record_skip(binding, run, trigger, note):
    if run is None:
        run = Run.objects.create(binding=binding, trigger=trigger)
    run.status = RunStatus.SKIPPED
    run.note = note[:255]
    run.finished_at = timezone.now()
    run.dedupe_key = None
    run.save(update_fields=["status", "note", "finished_at", "dedupe_key"])
    return run


def _ensure_runnable(binding):
    if not binding.is_runnable:
        raise ConnectorError(
            f"binding {binding.id} is not runnable (enabled={binding.enabled}, "
            f"status={binding.status})"
        )
    connection = binding.connection
    if not connection.is_usable:
        raise ConnectorError(
            f"connection {connection.id} is {connection.status}, not active"
        )


def _drain_pending_if_needed(binding, source_definition):
    """Load a package left by an interrupted Run, as its own Run.

    Attribution is the point: dlt would otherwise load that package under the
    *next* run's LoadInfo, which would credit rows to a run that never
    extracted them and put the incremental projection window on the wrong ids.
    """
    from django_connectors.landing.destination import build_pipeline

    pipeline = build_pipeline(binding)
    if not runner.has_pending_data(pipeline):
        return None

    recovery = Run.objects.create(
        binding=binding, trigger=RunTrigger.RECOVERY, status=RunStatus.RUNNING
    )
    recovery.started_at = timezone.now()
    try:
        result = runner.drain_pending(binding, recovery)
    except Exception as exc:
        _fail(binding, recovery, exc)
        raise
    _succeed(binding, recovery, result)
    return recovery


def _succeed(binding, run, result):
    now = timezone.now()
    run.status = RunStatus.SUCCEEDED
    run.finished_at = now
    run.dlt_load_ids = result.get("load_ids", [])
    run.metrics = result.get("metrics", {})
    run.dlt_dataset_name = run.dlt_dataset_name or ""
    run.save(
        update_fields=[
            "status",
            "finished_at",
            "dlt_load_ids",
            "metrics",
            "dlt_dataset_name",
        ]
    )

    binding.last_success_at = now
    binding.last_error = ""
    if binding.status in (BindingStatus.PENDING, BindingStatus.BLOCKED):
        binding.status = BindingStatus.ACTIVE

    updated = ["last_success_at", "last_error", "status"]
    snapshot = result.get("schema")
    if snapshot:
        # Projection validation and the mapping UI read this instead of
        # re-running extraction, and Projection activation is gated on it.
        binding.landing_schema = snapshot
        binding.landing_schema_version_hash = runner.schema_fingerprint(snapshot)
        binding.landing_schema_at = now
        updated += [
            "landing_schema",
            "landing_schema_version_hash",
            "landing_schema_at",
        ]

    _apply_index_report(binding, result.get("indexes"))
    binding.save(update_fields=updated)

    _after_success(binding, run)
    return run


def _apply_index_report(binding, report):
    """Downgrade a Binding whose landing indexes could not be provisioned.

    Mutates `binding` in place; the caller saves. ``needs_review`` rather than
    a failed Run because the data landed correctly — what is degraded is the
    cost of the *next* merge, which without an index is linear in table size
    (measured at 179s for 10,000 rows into a 60,000-row table). ``is_runnable``
    includes ``needs_review``, so the Binding keeps syncing while the problem
    is visible.

    Deliberately not cleared by a later run that succeeds. "Needs review" means
    a human decides; a status that healed itself would be one an operator
    learns to ignore. Provisioning is retried on every successful run
    regardless, so the index does appear once the cause is removed.
    """
    failures = (report or {}).get("failed")
    if not failures:
        return binding

    detail = "; ".join(
        f"{failure.get('index') or 'landing index'}: {failure.get('error')}"
        for failure in failures
    )
    binding.status = BindingStatus.NEEDS_REVIEW
    binding.last_error = (
        f"landed successfully, but could not provision landing index(es) — "
        f"merge cost grows with table size until this is fixed. {detail}"
    )
    return binding


def _after_success(binding, run):
    """Re-grade projections against the new schema, then dispatch them.

    Both are best-effort: the ingestion genuinely succeeded, and failing the Run
    because a downstream projection could not be queued would misreport what
    happened — and would make the next attempt re-fetch from the provider for
    no reason. `dispatch_pending_projections` is the safety net for whatever is
    dropped here.
    """
    from django_connectors.services import discovery, projections

    try:
        discovery.refresh_projection_drift(binding)
    except Exception as exc:
        logger.warning(
            "could not refresh projection drift for binding %s: %s",
            binding.id,
            type(exc).__name__,
        )

    try:
        projections.dispatch_after_run(run)
    except Exception as exc:
        logger.warning(
            "could not dispatch projections for run %s: %s", run.id, type(exc).__name__
        )


def _fail(binding, run, exc):
    now = timezone.now()
    error_type, message = describe(exc)
    logger.warning("run %s failed: %s: %s", run.id, error_type, message)

    run.status = RunStatus.FAILED
    run.finished_at = now
    run.error_type = error_type[:255]
    run.error_message = message
    run.save(update_fields=["status", "finished_at", "error_type", "error_message"])

    binding.last_failure_at = now
    binding.last_error = message
    binding.save(update_fields=["last_failure_at", "last_error"])
    return run


def _block_for_credentials(binding, exc):
    """Stop retrying a revoked credential.

    Retrying burns provider quota and invites rate limiting, and no amount of
    retrying fixes revocation.
    """
    connection = binding.connection
    connection.status = ConnectionStatus.REVOKED
    connection.save(update_fields=["status"])
    binding.status = BindingStatus.BLOCKED
    binding.last_error = scrub(exc)
    binding.save(update_fields=["status", "last_error"])


def _credentials_for(connection):
    """Resolve credentials via the configured auth backend, if any."""
    from django_connectors.registry import auth_backends

    if not connection.auth_backend:
        return None
    backend = auth_backends.get(connection.auth_backend)
    return backend.get_credentials(connection)


def reap_stale_runs():
    """Fail Runs whose lease expired, and clear the leases.

    A worker killed mid-run leaves a Run stuck in `running` forever otherwise —
    and because dlt's restart recovery is backed by the *local* working
    directory, an ephemeral worker also loses the in-flight package entirely.
    """
    expired = locks.reap_expired()
    stale = Run.objects.filter(status=RunStatus.RUNNING, binding__lock__isnull=True)
    count = 0
    for run in stale:
        run.status = RunStatus.FAILED
        run.finished_at = timezone.now()
        run.error_type = "LeaseExpired"
        run.error_message = (
            "the worker holding this binding's lease stopped without finishing"
        )
        run.save(update_fields=["status", "finished_at", "error_type", "error_message"])
        count += 1
    return {"leases_reaped": expired, "runs_failed": count}
