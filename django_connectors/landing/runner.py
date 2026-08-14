"""Executing one ingestion Run against the landing database.

The central correctness rule here is that **"pipeline.run() did not raise" does
not mean "the source was reconciled"**. Verified against dlt 1.30: when a
pipeline holds a pending *normalized* load package, ``pipeline.run(new_data)``
loads the old package, returns a clean LoadInfo, and never extracts the new
data at all — dlt's own source comments read "load them and exit", and only a
``logger.warn`` distinguishes the two cases. A runner that treats a
non-exception as success records `succeeded` for data it never fetched, and the
Binding's cursor then advances past records nobody has.

So a Run is only marked succeeded when dlt reports no failed jobs, reports at
least one load id, and those load ids have not already been claimed by an
earlier Run of the same Binding.

The same rule governs the recovery path. "Pending" covers two stages and
``pipeline.load()`` covers only the second, so :func:`drain_pending` normalizes
first and refuses to return at all while anything is still pending — a recovery
Run recorded as succeeded advances ``binding.last_success_at`` and leaves the
Binding `active`, which is a health signal reporting green for a Binding that
has stopped syncing.
"""

import logging

from django_connectors.exceptions import LandingWedgedError, SourceError
from django_connectors.landing.destination import build_pipeline
from django_connectors.landing.instrument import instrument_source

logger = logging.getLogger(__name__)

# Allow-listed metrics. dlt's own `load_info.asdict()` / `trace.asdict()` are
# not stdlib-JSON serializable (pendulum datetimes) and embed absolute worker
# filesystem paths in every job entry, so the trace is never stored wholesale.
_METRIC_KEYS = ("started_at", "finished_at", "rows", "tables")


class PendingPackageError(LandingWedgedError):
    """The pipeline holds an un-loaded package from an earlier Run."""


def has_pending_data(pipeline):
    return bool(pipeline.has_pending_data)


def execute(binding, run, *, source_definition, credentials, allow_pending=False):
    """Run one ingestion for `binding` and return a result dict.

    Assumes the Binding's lease is held. Never call this concurrently for one
    Binding: dlt provides no protection and loses data silently when you do.
    """
    pipeline = build_pipeline(binding)

    if has_pending_data(pipeline) and not allow_pending:
        # Drained by its own Run(trigger="recovery") so that the rows are
        # attributed to the run that actually landed them.
        raise PendingPackageError(
            f"pipeline {binding.pipeline_name} holds a pending load package. "
            f"It must be drained by a recovery run before new data is "
            f"extracted, or dlt will load the old package and report success "
            f"for data it never fetched."
        )

    source = source_definition.build_source(
        binding=binding, credentials=credentials, run=run
    )
    if source is None:
        raise SourceError(f"source {binding.source!r} returned no source")

    selected = list(binding.resources or [])
    if selected:
        missing = sorted(set(selected) - set(source.resources.keys()))
        if missing:
            raise SourceError(
                f"binding selects resources {missing} which source "
                f"{binding.source!r} does not provide; available: "
                f"{sorted(source.resources.keys())}"
            )
        source = source.with_resources(*selected)

    instrument_source(
        source, binding=binding, run=run, source_definition=source_definition
    )

    load_info = pipeline.run(source)

    return _verify(binding, run, pipeline, load_info)


def _verify(binding, run, pipeline, load_info):
    """Turn a LoadInfo into a trustworthy verdict."""
    from django_connectors.models import Run

    if load_info is None:
        # `Pipeline.load()` returns `pip_ex.step_info` when it cleans up an
        # aborted package, and that is None when the step failed before it
        # produced any. Dereferencing it raised `AttributeError: 'NoneType'
        # object has no attribute 'has_failed_jobs'` from inside the runner —
        # an undiagnosable error in place of "nothing landed".
        return {
            "load_ids": [],
            "metrics": _metrics(pipeline),
            "landed": False,
            "schema": schema_snapshot(pipeline, binding),
        }

    if load_info.has_failed_jobs:
        raise LandingWedgedError(
            f"load reported failed jobs for pipeline {binding.pipeline_name}. "
            f"dlt retries a failing job indefinitely, so the pipeline stays "
            f"wedged until the cause is removed — a value too large for its "
            f"destination column is the usual one."
        )

    load_ids = [str(load_id) for load_id in (load_info.loads_ids or [])]
    if not load_ids:
        # Nothing landed. Legitimate when the source had no new records.
        return {
            "load_ids": [],
            "metrics": _metrics(pipeline),
            "landed": False,
            "schema": schema_snapshot(pipeline, binding),
        }

    # Load ids already attributed to an earlier Run mean dlt loaded a package
    # this Run did not extract.
    claimed = set()
    for previous in (
        Run.objects.filter(binding=binding).exclude(pk=run.pk).only("dlt_load_ids")
    ):
        claimed.update(previous.dlt_load_ids or [])
    stolen = sorted(set(load_ids) & claimed)
    if stolen:
        raise LandingWedgedError(
            f"load ids {stolen} were already recorded against an earlier Run of "
            f"this Binding, so this run loaded a pending package rather than "
            f"extracting new data."
        )

    return {
        "load_ids": load_ids,
        "metrics": _metrics(pipeline),
        "landed": True,
        "schema": schema_snapshot(pipeline, binding),
    }


def _metrics(pipeline):
    """A small, JSON-serializable summary of the last run."""
    metrics = {}
    try:
        trace = pipeline.last_trace
        if trace is None:
            return metrics
        normalize_info = getattr(trace, "last_normalize_info", None)
        if normalize_info is not None:
            row_counts = getattr(normalize_info, "row_counts", None)
            if row_counts:
                metrics["row_counts"] = {
                    str(table): int(count) for table, count in dict(row_counts).items()
                }
        for step in getattr(trace, "steps", []) or []:
            name = getattr(step, "step", None)
            if name:
                metrics.setdefault("steps", []).append(str(name))
    except Exception as exc:
        logger.debug("could not collect dlt metrics: %s", type(exc).__name__)
    return metrics


def drain_pending(binding, run):
    """Load a package left behind by an interrupted Run.

    Recorded as its own Run so that its load ids are attributed to the run that
    landed them, which is what the incremental projection window keys on.
    """
    pipeline = build_pipeline(binding)
    if not has_pending_data(pipeline):
        return {"load_ids": [], "metrics": {}, "landed": False}

    # Two stages, because `has_pending_data` covers both and `load()` covers
    # only one. A package left at the *extract* stage — a worker killed between
    # extract and normalize, or a normalize that raised — is invisible to
    # `load()`, so draining with `load()` alone reported "nothing to do" while
    # `has_pending_data` stayed true: every later Run refused to extract, and
    # the Binding wedged forever.
    if pipeline.list_extracted_load_packages():
        pipeline.normalize()

    load_info = None
    if pipeline.list_normalized_load_packages():
        load_info = pipeline.load()

    result = _verify(binding, run, pipeline, load_info)

    if has_pending_data(pipeline):
        # The package survived a full drain, so nothing here will ever shift it
        # and no later Run can extract. Raising keeps the recovery Run off
        # `succeeded`: recorded as success it would advance
        # `binding.last_success_at` and leave the Binding `active`, which is a
        # health signal reporting green for a Binding that has stopped syncing.
        raise PendingPackageError(
            f"pipeline {binding.pipeline_name} still holds pending packages "
            f"after a full drain: extracted="
            f"{pipeline.list_extracted_load_packages()}, normalized="
            f"{pipeline.list_normalized_load_packages()}. No later Run can "
            f"extract until they are removed."
        )
    return result


def schema_snapshot(pipeline, binding):
    """A plain-JSON view of the landed schema.

    Reduced deliberately rather than storing dlt's own ``to_dict()``: that
    carries a version hash and normalizer configuration that change for reasons
    unrelated to the customer's mapping, which would make drift detection fire
    constantly. Only what a mapping can actually depend on is kept.
    """
    tables = {}
    try:
        schema = pipeline.schemas.get(binding.schema_name)
    except Exception:
        schema = None
    if schema is None:
        return {"tables": tables}

    for table_name, table in schema.tables.items():
        if table_name.startswith("_dlt"):
            continue
        tables[table_name] = {
            column_name: {
                "data_type": column.get("data_type"),
                "nullable": column.get("nullable", True),
            }
            for column_name, column in (table.get("columns") or {}).items()
        }
    return {"tables": tables}


def schema_fingerprint(snapshot):
    """A stable hash of a schema snapshot, for cheap drift detection."""
    import hashlib
    import json

    payload = json.dumps(snapshot, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()[:32]
