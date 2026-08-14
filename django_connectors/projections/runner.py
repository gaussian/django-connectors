"""Executing a Projection against landed rows.

The contract handed to a host writer, and the reasons for it:

* **Deterministic order.** Rows are ordered by ``(_dlt_load_id, _dlt_id)``, so
  a run and its retry compose identical batches. Without that, "retrying is
  idempotent" is not even testable.
* **One record per identity per batch.** Merge means an identity appears once
  in the landing table, but a full replay across load ids could still surface
  two versions; the newest wins.
* **Delete beats upsert, and is emitted last.** A record deleted and re-created
  within one window must not resurrect because the upsert happened to sort
  later.
* **A raised writer means the batch was not applied.** There is no mid-run
  checkpoint: the whole ProjectionRun is retried from the start, so writers
  must be idempotent per identity.
"""

import logging

from django.utils import timezone

from django_connectors.conf import conf
from django_connectors.enums import (
    ProjectionRunMode,
    ProjectionRunStatus,
    ProjectionStatus,
)
from django_connectors.errors import describe
from django_connectors.exceptions import ProjectionError, TargetWriteError
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

    projection_run.status = ProjectionRunStatus.RUNNING
    projection_run.started_at = timezone.now()
    projection_run.save(update_fields=["status", "started_at"])

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

    projection_run.records_seen = counts["seen"]
    projection_run.records_written = counts["written"]
    projection_run.records_deleted = counts["deleted"]
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
    counts = {"seen": 0, "written": 0, "deleted": 0}
    batch = []
    batch_index = 0

    for row in rows:
        counts["seen"] += 1
        if not compiled.matches(row):
            continue
        batch.append(_project(row, compiled, target))
        if len(batch) >= batch_size:
            _write(batch, projection_run, projection, target, batch_index, counts)
            batch = []
            batch_index += 1

    if batch:
        _write(batch, projection_run, projection, target, batch_index, counts)
    return counts


def _scope_for(projection_run):
    """Which load ids this run covers, or None for the whole table."""
    if projection_run.mode == ProjectionRunMode.FULL:
        return None
    return list(projection_run.load_ids or [])


def _project(row, compiled, target):
    deleted = bool(row.get(DELETED_COLUMN))
    values = compiled.apply(row)

    identity = {name: values.get(name) for name in target.identity_fields}
    missing = sorted(name for name, value in identity.items() if value is None)
    if missing:
        raise ProjectionError(
            f"identity field(s) {missing} evaluated to None; the record cannot "
            f"be matched to an existing one"
        )

    if deleted:
        return ProjectedRecord(operation="delete", identity=identity, values={})

    coerced = {}
    for name, value in values.items():
        declared = target.fields.get(name)
        coerced[name] = declared.coerce(value, field_name=name) if declared else value
    _assert_required(coerced, target)
    return ProjectedRecord(operation="upsert", identity=identity, values=coerced)


def _assert_required(values, target):
    missing = sorted(
        name for name in target.required_fields if values.get(name) is None
    )
    if missing:
        raise ProjectionError(
            f"target {target.key!r} requires {missing}, which evaluated to None"
        )


def _write(batch, projection_run, projection, target, batch_index, counts):
    records = _collapse(batch, target)
    context = WriterContext(
        owner_content_type_id=projection.binding.connection.owner_content_type_id,
        owner_object_id=projection.binding.connection.owner_object_id,
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
        target.writer(records, context)
    except Exception as exc:
        raise TargetWriteError(
            f"target {target.key!r} writer failed on batch {batch_index}: {exc}"
        ) from exc

    counts["written"] += sum(1 for r in records if r.operation == "upsert")
    counts["deleted"] += sum(1 for r in records if r.operation == "delete")


def _collapse(batch, target):
    """One record per identity, deletes winning and emitted last."""
    upserts = {}
    deletes = {}
    for record in batch:
        key = tuple(record.identity.get(name) for name in target.identity_fields)
        if record.operation == "delete":
            deletes[key] = record
            upserts.pop(key, None)
        elif key not in deletes:
            upserts[key] = record
    return [*upserts.values(), *deletes.values()]


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

    projected = set()
    for previous in ProjectionRun.objects.filter(
        projection=projection, status=ProjectionRunStatus.SUCCEEDED
    ):
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
