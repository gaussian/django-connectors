"""Executing a Projection against landed rows.

The contract handed to a host writer, and the reasons for it:

* **Deterministic order.** Rows are ordered by ``(_dlt_load_id, _dlt_id)``, so
  a run and its retry compose identical batches. Without that, "retrying is
  idempotent" is not even testable.
* **One record per identity per batch.** Merge means an identity appears once
  in the landing table, but a full replay across load ids could still surface
  two versions, and a target identity coarser than the merge key can collapse
  two landed records onto one; the newest wins.
* **Delete beats upsert, for the whole run.** A record deleted and re-created
  within one window must not resurrect because the upsert happened to sort
  later — sort order here is a content hash, not time. Within a batch the
  delete is emitted last; across batches the identity is remembered, so an
  upsert in a later batch is suppressed. That is what keeps the host's end
  state independent of ``PROJECTION_BATCH_SIZE``, which is a streaming detail
  and not a semantic unit. It costs one identity tuple held per deleted record
  for the length of the run.
* **A raised writer means the batch was not applied.** There is no mid-run
  checkpoint: the whole ProjectionRun is retried from the start, so writers
  must be idempotent per identity.
* **Under ``on_invalid_record="skip"``, a row that cannot become a record is
  counted and explained, never guessed at.** Three rules make that safe. A row
  whose identity is None is skipped — there is nothing to match it to, so it
  is neither written nor used to delete. A skipped row therefore never
  produces a delete, even when its landed ``_connector_deleted`` is true. And
  a full replay reads every landed row again, so a row skipped for a mapping
  mistake is projected once the mapping is fixed. The count is exact; the
  reasons are capped at ``PROJECTION_MAX_WARNINGS``.
"""

import json
import logging

from django.utils import timezone

from django_connectors.conf import conf
from django_connectors.enums import (
    InvalidRecordPolicy,
    ProjectionRunMode,
    ProjectionRunStatus,
    ProjectionStatus,
)
from django_connectors.errors import describe
from django_connectors.exceptions import CastError, InvalidRecordError, TargetWriteError
from django_connectors.landing import access
from django_connectors.landing.naming import (
    DELETED_COLUMN,
    DLT_ID_COLUMN,
    DLT_LOAD_ID_COLUMN,
)
from django_connectors.models import ProjectionRun
from django_connectors.projections.compiler import compile_mapping
from django_connectors.projections.targets import (
    ProjectedRecord,
    WriterContext,
    WriterResult,
    get_target,
)

logger = logging.getLogger(__name__)

ORDER_COLUMNS = (DLT_LOAD_ID_COLUMN, DLT_ID_COLUMN)


def execute(projection_run):
    """Run one ProjectionRun to completion. Returns it."""
    projection = projection_run.projection

    if projection_run.projection_version < projection.version:
        # A run queued against a superseded mapping would revert rows a newer
        # replay has already corrected.
        return _finish(
            projection_run,
            ProjectionRunStatus.SUPERSEDED,
            note=(
                f"queued at version {projection_run.projection_version}, "
                f"projection is now at {projection.version}"
            ),
        )

    # An atomic claim, not a save: the sweeper rescues stale queued runs and a
    # backed-up queue may deliver the same one meanwhile. Whichever claims the
    # row runs it; the other returns what the row already says.
    started_at = timezone.now()
    claimed = ProjectionRun.objects.filter(
        pk=projection_run.pk, status=ProjectionRunStatus.QUEUED
    ).update(status=ProjectionRunStatus.RUNNING, started_at=started_at)
    if not claimed:
        projection_run.refresh_from_db()
        return projection_run
    projection_run.status = ProjectionRunStatus.RUNNING
    projection_run.started_at = started_at

    try:
        counts = _run(projection_run, projection)
    except Exception as exc:
        error_type, message = describe(exc)
        projection_run.error_type = error_type[:255]
        projection_run.error_message = message
        _finish(projection_run, ProjectionRunStatus.FAILED)

        projection.last_failure_at = timezone.now()
        projection.last_error = message
        projection.save(update_fields=["last_failure_at", "last_error"])
        return projection_run
    except BaseException as exc:
        # Celery's SoftTimeLimitExceeded, a worker's SIGINT and SystemExit are
        # not Exception. Without this the row is left RUNNING forever with no
        # error and no finished_at — there is no reaper for ProjectionRun, and
        # retry_projection_run admits only FAILED, so it can never be answered.
        # The Projection itself is not marked failed: an interrupted worker
        # says nothing about the mapping.
        error_type, message = describe(exc)
        projection_run.error_type = error_type[:255]
        projection_run.error_message = f"interrupted: {message}"
        _finish(projection_run, ProjectionRunStatus.FAILED)
        raise

    projection_run.records_seen = counts["seen"]
    projection_run.records_written = counts["written"]
    projection_run.records_deleted = counts["deleted"]
    projection_run.records_skipped = counts["skipped"]
    projection_run.warnings = counts["warnings"]
    _finish(projection_run, ProjectionRunStatus.SUCCEEDED)

    projection.last_success_at = timezone.now()
    projection.last_error = ""
    projection.save(update_fields=["last_success_at", "last_error"])
    return projection_run


def _run(projection_run, projection):
    binding = projection.binding
    target = get_target(projection.target)
    compiled = compile_mapping(projection.mapping, projection.filters)

    load_ids = _scope_for(projection_run)
    relation = access.binding_relation(binding, projection.resource, load_ids=load_ids)
    rows = access.iter_rows(relation, order_by=ORDER_COLUMNS, binding=binding)

    batch_size = conf.PROJECTION_BATCH_SIZE
    counts = {"seen": 0, "written": 0, "deleted": 0, "skipped": 0, "warnings": []}
    skip_invalid = projection.on_invalid_record == InvalidRecordPolicy.SKIP
    batch = []
    batch_index = 0
    # Run-scoped, not batch-scoped: see the module docstring. Holds identity
    # tuples only, so it grows with the number of deleted records in the run
    # rather than with the run.
    deleted_identities = set()

    for row in rows:
        counts["seen"] += 1
        if not compiled.matches(row):
            continue
        try:
            batch.append(_project(row, compiled, target))
        except (InvalidRecordError, CastError) as exc:
            # Only what one row's data can cause. A MappingValidationError —
            # a column that is not in the landed row — is the mapping's fault,
            # and skipping every row over it would report success on nothing.
            if not skip_invalid:
                raise
            _skip(counts, str(exc), row=row.get(DLT_ID_COLUMN))
            continue
        if len(batch) >= batch_size:
            _write(
                batch,
                projection_run,
                projection,
                target,
                batch_index,
                counts,
                deleted_identities,
            )
            batch = []
            batch_index += 1

    if batch:
        _write(
            batch,
            projection_run,
            projection,
            target,
            batch_index,
            counts,
            deleted_identities,
        )
    return counts


def _scope_for(projection_run):
    """Which load ids this run covers, or None for the whole table."""
    if projection_run.mode == ProjectionRunMode.FULL:
        return None
    return list(projection_run.load_ids or [])


def _project(row, compiled, target):
    deleted = bool(row.get(DELETED_COLUMN))
    # A delete needs its identity and nothing else. Evaluating the other
    # fields too would let an unparseable value in a column the delete never
    # uses block the delete — and leave the host holding a stale record.
    values = compiled.apply(row, only=target.identity_fields if deleted else None)

    # Coercion happens before identity is taken, so the identity a writer joins
    # on is the declared type. Taking it from the raw values instead delivered
    # one field twice in one record with two different types, and let a delete
    # carry a value the target field would have rejected on the upsert path.
    coerced = {}
    for name, value in values.items():
        declared = target.fields.get(name)
        coerced[name] = declared.coerce(value, field_name=name) if declared else value

    identity = {name: coerced.get(name) for name in target.identity_fields}
    missing = sorted(name for name, value in identity.items() if value is None)
    if missing:
        raise InvalidRecordError(
            f"identity field(s) {missing} evaluated to None; the record cannot "
            f"be matched to an existing one"
        )

    if deleted:
        return ProjectedRecord(operation="delete", identity=identity, values={})

    _assert_required(coerced, target)
    return ProjectedRecord(operation="upsert", identity=identity, values=coerced)


def _assert_required(values, target):
    missing = sorted(
        name for name in target.required_fields if values.get(name) is None
    )
    if missing:
        raise InvalidRecordError(
            f"target {target.key!r} requires {missing}, which evaluated to None"
        )


def _write(
    batch, projection_run, projection, target, batch_index, counts, deleted_identities
):
    records = _collapse(batch, target, deleted_identities)
    context = WriterContext(
        owner_content_type_id=projection.binding.connection.owner_content_type_id,
        owner_object_id=projection.binding.connection.owner_object_id,
        identity_scope=target.identity_scope,
        connection_id=projection.binding.connection_id,
        binding_id=projection.binding_id,
        projection_id=projection.id,
        projection_version=projection.version,
        projection_run_id=projection_run.id,
        mode=projection_run.mode,
        batch_index=batch_index,
        load_ids=tuple(projection_run.load_ids or []),
        replace_scope=(
            projection_run.mode == ProjectionRunMode.FULL
            and target.supports_scope_replace
        ),
    )

    try:
        result = target.writer(records, context)
    except Exception as exc:
        raise TargetWriteError(
            f"target {target.key!r} writer failed on batch {batch_index}: {exc}"
        ) from exc

    upserts = sum(1 for r in records if r.operation == "upsert")
    deletes = sum(1 for r in records if r.operation == "delete")
    if isinstance(result, WriterResult):
        counts["written"] += upserts if result.written is None else result.written
        counts["deleted"] += deletes if result.deleted is None else result.deleted
        counts["skipped"] += result.skipped
        for warning in result.warnings:
            _skip(counts, str(warning), count=False)
    else:
        # An int, or nothing: the batch was applied as handed over.
        counts["written"] += upserts
        counts["deleted"] += deletes


def _skip(counts, reason, *, row=None, count=True):
    """Record one skipped row or writer warning, keeping the list bounded."""
    if count:
        counts["skipped"] += 1
    if len(counts["warnings"]) < conf.PROJECTION_MAX_WARNINGS:
        entry = {"reason": reason}
        if row is not None:
            entry["row"] = row
        counts["warnings"].append(entry)


def _collapse(batch, target, deleted_identities=None):
    """One record per identity, deletes winning and emitted last.

    `deleted_identities` is the run's accumulated set of deleted identities and
    is updated here. Deletes win across the whole run, not just this batch —
    otherwise the host's end state depends on where the batch boundary happened
    to fall, which is a tuning knob rather than a statement about the data.
    """
    if deleted_identities is None:
        deleted_identities = set()
    upserts = {}
    deletes = {}
    for record in batch:
        key = _identity_key(record, target)
        if record.operation == "delete":
            deletes[key] = record
            deleted_identities.add(key)
            upserts.pop(key, None)
        elif key not in deleted_identities:
            upserts[key] = record
    return [*upserts.values(), *deletes.values()]


def _identity_key(record, target):
    """A hashable key for one record's identity.

    Validation refuses an object-valued identity field, so in practice this
    only ever sees scalars — but an unforeseen shape reaching here used to
    surface as a bare ``TypeError: unhashable type: 'dict'`` from the middle of
    a run, naming neither the field nor the cause.
    """
    key = []
    for name in target.identity_fields:
        value = record.identity.get(name)
        try:
            hash(value)
        except TypeError:
            value = json.dumps(value, sort_keys=True, default=str)
        key.append(value)
    return tuple(key)


def _finish(projection_run, status, *, note=""):
    projection_run.status = status
    projection_run.finished_at = timezone.now()
    if note:
        projection_run.error_message = note
    projection_run.save(
        update_fields=[
            "status",
            "finished_at",
            "records_seen",
            "records_written",
            "records_deleted",
            "records_skipped",
            "warnings",
            "error_type",
            "error_message",
        ]
    )
    return projection_run


def dispatch_pending(projection, *, lookback=None):
    """Queue a ProjectionRun for every landed load this Projection has not seen.

    Computed as a set difference — the Binding's succeeded load ids minus this
    Projection's succeeded ones — rather than a per-Run "dispatched" flag. The
    flag heals a dropped enqueue but not a ProjectionRun that failed and was
    never retried; the set difference heals both with one mechanism.
    """
    from django_connectors.enums import RunStatus
    from django_connectors.models import Run

    if projection.status != ProjectionStatus.ACTIVE or not projection.enabled:
        return []

    horizon = timezone.now() - (lookback or conf.PROJECTION_SWEEP_LOOKBACK)
    landed = []
    for run in Run.objects.filter(
        binding=projection.binding,
        status=RunStatus.SUCCEEDED,
        created_at__gte=horizon,
    ).order_by("created_at"):
        landed.extend(run.dlt_load_ids or [])

    # Succeeded runs have projected their loads. Queued and running ones hold
    # theirs in flight — but only for as long as that claim is credible: a
    # queued run past the grace period is one the queue dropped (the sweeper
    # executes it before calling this), and a running run past the run
    # timeout is one a killed worker left behind, which no reaper answers.
    # Counting either forever would lose its loads for good.
    now = timezone.now()
    projected = set()
    for previous in ProjectionRun.objects.filter(
        projection=projection,
        status__in=(
            ProjectionRunStatus.SUCCEEDED,
            ProjectionRunStatus.QUEUED,
            ProjectionRunStatus.RUNNING,
        ),
    ):
        if previous.status == ProjectionRunStatus.QUEUED and (
            previous.created_at < now - conf.PROJECTION_QUEUE_GRACE
        ):
            continue
        if previous.status == ProjectionRunStatus.RUNNING and (
            (previous.started_at or previous.created_at)
            < now - conf.DEFAULT_RUN_TIMEOUT
        ):
            continue
        projected.update(previous.load_ids or [])

    outstanding = [load_id for load_id in landed if load_id not in projected]
    if not outstanding:
        return []

    return [
        ProjectionRun.objects.create(
            projection=projection,
            mode=ProjectionRunMode.INCREMENTAL,
            projection_version=projection.version,
            load_ids=outstanding,
        )
    ]
