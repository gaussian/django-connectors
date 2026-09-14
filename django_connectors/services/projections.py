"""Projection lifecycle services."""

import logging

from django.db import transaction

from django_connectors.enums import (
    BindingStatus,
    ProjectionRunMode,
    ProjectionRunStatus,
    ProjectionStatus,
)
from django_connectors.exceptions import ConfigurationError, ProjectionError
from django_connectors.landing import access, schema
from django_connectors.models import Projection, ProjectionRun
from django_connectors.projections import preview as preview_module
from django_connectors.projections import runner as projection_runner
from django_connectors.projections import validator as validator_module
from django_connectors.projections.targets import get_target
from django_connectors.registry import sources

logger = logging.getLogger(__name__)


def landing_columns_for(projection):
    """Landed columns for a Projection's resource, or None before the first run."""
    if projection.binding.landing_schema_at is None:
        return None
    try:
        return access.landing_columns(projection.binding, projection.resource)
    except Exception as exc:
        logger.debug("landing columns unavailable: %s", type(exc).__name__)
        return None


def merge_key_columns_for(projection):
    """The merge identity the landed resource carries, or None if unknowable."""
    try:
        return schema.merge_key_for(projection.binding, projection.resource)
    except Exception as exc:
        logger.debug("landing merge key unavailable: %s", type(exc).__name__)
        return None


def validate_projection(projection, *, save=True):
    """Validate and, by default, move the Projection to the matching status."""
    source_definition = None
    if projection.binding.source in sources:
        source_definition = sources.get(projection.binding.source)

    result = validator_module.validate_projection(
        projection,
        landing_columns=landing_columns_for(projection),
        source_definition=source_definition,
        merge_key_columns=merge_key_columns_for(projection),
    )
    if save:
        projection.status = validator_module.status_for(result)
        projection.last_error = "; ".join(result.errors)
        projection.save(update_fields=["status", "last_error"])
    return result


def activate_projection(projection):
    """Validate and activate, refusing while the landing schema is unknown.

    Activation is gated on a successful Run having happened, because a mapping
    validated against no schema is a mapping validated against nothing.
    """
    if projection.binding.landing_schema_at is None:
        raise ProjectionError(
            f"binding {projection.binding.id} has not landed anything yet, so "
            f"the mapping cannot be checked against a real schema. Run the "
            f"binding first."
        )
    result = validate_projection(projection).raise_if_invalid()
    projection.status = ProjectionStatus.ACTIVE
    projection.save(update_fields=["status"])
    return result


def update_mapping(
    projection, *, mapping=None, filters=None, acknowledge_identity_change=False
):
    """Change a mapping and bump the version.

    Changing how an identity field is derived orphans everything already
    written under the old identity, and the library cannot clean that up — it
    never learns what the writer wrote. So that particular change has to be
    acknowledged explicitly.
    """
    target = get_target(projection.target)
    if (
        mapping is not None
        and _identity_mapping_changed(projection, target, mapping)
        and not acknowledge_identity_change
    ):
        raise ProjectionError(
            f"this change alters how identity field(s) "
            f"{list(target.identity_fields)} are derived. Records already "
            f"written under the previous identity cannot be found or cleaned "
            f"up by this library. Pass acknowledge_identity_change=True to "
            f"proceed."
        )

    # Atomic with the validation that follows it. A mapping that cannot even
    # compile is expected to land with status INVALID — that is how the editor
    # sees what is wrong — but only if `status` was actually reached. If
    # validation raises instead, the rejected mapping would otherwise stay
    # persisted with a bumped version and status still ACTIVE, which supersedes
    # every in-flight ProjectionRun and makes the sweeper queue runs that all
    # fail the same way.
    with transaction.atomic():
        if mapping is not None:
            projection.mapping = mapping
        if filters is not None:
            projection.filters = filters
        projection.version += 1
        projection.save(update_fields=["mapping", "filters", "version"])
        validate_projection(projection)
    return projection


def _identity_mapping_changed(projection, target, new_mapping):
    return any(
        (projection.mapping or {}).get(name) != (new_mapping or {}).get(name)
        for name in target.identity_fields
    )


def preview_projection(projection, *, limit=None):
    return preview_module.preview_projection(projection, limit=limit)


def sample_resource(projection_or_binding, resource=None, *, limit=None):
    from django_connectors.conf import conf

    binding = getattr(projection_or_binding, "binding", projection_or_binding)
    resource = resource or getattr(projection_or_binding, "resource", None)
    limit = min(limit or conf.SAMPLE_MAX_ROWS, conf.SAMPLE_MAX_ROWS)
    return access.sample_rows(binding, resource, limit=limit)


def queue_projection(projection, *, source_run=None, load_ids=None, mode=None):
    """Record a ProjectionRun over a scope of landed loads, without executing it."""
    mode = mode or ProjectionRunMode.INCREMENTAL
    if load_ids is None:
        load_ids = list(source_run.dlt_load_ids or []) if source_run else []
    return ProjectionRun.objects.create(
        projection=projection,
        source_run=source_run,
        mode=mode,
        projection_version=projection.version,
        load_ids=list(load_ids),
    )


def run_projection(projection, *, source_run=None, load_ids=None, mode=None):
    """Execute a Projection over a scope of landed loads."""
    return projection_runner.execute(
        queue_projection(
            projection, source_run=source_run, load_ids=load_ids, mode=mode
        )
    )


def execute_projection_run(projection_run_id):
    """Execute a queued ProjectionRun by id — the target of a dispatch callable."""
    projection_run = ProjectionRun.objects.select_related(
        "projection__binding__connection"
    ).get(pk=projection_run_id)
    if projection_run.status != ProjectionRunStatus.QUEUED:
        # Delivered twice, or already swept up: the row says what happened.
        return projection_run
    return projection_runner.execute(projection_run)


def dispatcher():
    """The configured ``PROJECTION_DISPATCH`` callable, or None for inline."""
    from django.utils.module_loading import import_string

    from django_connectors.conf import conf

    path = conf.PROJECTION_DISPATCH
    if not path:
        return None
    return import_string(path) if isinstance(path, str) else path


def replay_projection(projection):
    """Re-project every landed row.

    Refused unless the target declares it can replace a scope: replay reasserts
    the whole scope, and only the host can enumerate what it previously wrote
    and therefore decide that is safe.
    """
    target = get_target(projection.target)
    if not target.supports_scope_replace:
        raise ProjectionError(
            f"target {target.key!r} does not declare supports_scope_replace, so "
            f"a full replay cannot be applied safely: this library cannot tell "
            f"which host records a previous mapping produced. Set "
            f"supports_scope_replace=True on the TargetDefinition if the "
            f"writer can replace an owner's records wholesale."
        )
    return run_projection(projection, mode=ProjectionRunMode.FULL)


def dispatch_after_run(run):
    """Queue incremental ProjectionRuns for the resources a Run touched.

    The low-latency path. `dispatch_pending_projections` is the safety net that
    heals whatever this drops. With ``PROJECTION_DISPATCH`` unset the runs
    execute here, inline, while the Binding's lease is held; with it set they
    are recorded as QUEUED and handed to the configured callable.
    """
    if not run.dlt_load_ids:
        return []
    dispatch = dispatcher()
    dispatched = []
    for projection in Projection.objects.filter(
        binding=run.binding, enabled=True, status=ProjectionStatus.ACTIVE
    ):
        projection_run = queue_projection(
            projection, source_run=run, load_ids=run.dlt_load_ids
        )
        if dispatch is None:
            projection_run = projection_runner.execute(projection_run)
        else:
            # Handed to a queue after the row is committed — under
            # ATOMIC_REQUESTS a worker would otherwise look it up before it
            # exists. Outside a transaction this runs at once. It executes
            # after the Binding's lease is released; the sweeper heals a drop.
            transaction.on_commit(lambda run=projection_run: dispatch(run))
        dispatched.append(projection_run)
    return dispatched


def dispatch_pending_projections(*, lookback=None):
    """Sweep every active Projection for landed loads it has not projected.

    Also the safety net for a dispatch queue: a run still QUEUED past
    ``PROJECTION_QUEUE_GRACE`` is one the queue dropped, and is executed here
    before the outstanding loads are computed — so it is not counted as
    outstanding and re-queued a second time.
    """
    from django.utils import timezone

    from django_connectors.conf import conf

    stale = timezone.now() - conf.PROJECTION_QUEUE_GRACE
    dispatched = []
    for projection in Projection.objects.filter(
        enabled=True, status=ProjectionStatus.ACTIVE
    ).select_related("binding"):
        if projection.binding.status == BindingStatus.PURGING:
            continue
        for dropped in ProjectionRun.objects.filter(
            projection=projection,
            status=ProjectionRunStatus.QUEUED,
            created_at__lt=stale,
        ).order_by("created_at"):
            dispatched.append(projection_runner.execute(dropped))
        for projection_run in projection_runner.dispatch_pending(
            projection, lookback=lookback
        ):
            dispatched.append(projection_runner.execute(projection_run))
    return dispatched


def retry_projection_run(projection_run):
    """Re-run a failed ProjectionRun over the same scope.

    A new row rather than a mutation, so the failure stays in the history. The
    whole scope is re-processed because there is no mid-run checkpoint — which
    is exactly why host writers must be idempotent per identity.
    """
    if projection_run.status != ProjectionRunStatus.FAILED:
        raise ConfigurationError(
            f"projection run {projection_run.id} is {projection_run.status}, not failed"
        )
    with transaction.atomic():
        retry = ProjectionRun.objects.create(
            projection=projection_run.projection,
            source_run=projection_run.source_run,
            mode=projection_run.mode,
            projection_version=projection_run.projection.version,
            load_ids=list(projection_run.load_ids or []),
        )
    return projection_runner.execute(retry)
