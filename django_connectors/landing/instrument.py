"""The only path by which a source reaches a pipeline.

Every rule applied here neutralises a behaviour verified against dlt 1.30 that
silently loses or corrupts data:

``add_map`` arity
    dlt decides how to call a map function by counting its signature
    parameters: exactly one means ``f(item)``, **anything else** means
    ``f(item, meta)`` and dlt passes ``meta=None`` into your second parameter.
    So the natural ``def inject(row, binding_id=...)`` writes ``NULL`` binding
    ids into every row — cross-tenant contamination with no error at all.
    ``functools.partial`` is worse: three signature parameters, ``TypeError`` at
    extract time. A one-argument closure is the only safe shape, and the
    assertion below enforces it.

Nested tables
    ``add_map`` stamps root records only, so child tables carry no tenant scope
    and no run filter and are structurally unprojectable. Nested merges also
    inject ``DROP``/``CREATE TABLE`` into the merge SQL; MySQL commits
    implicitly on DDL, and a concurrent reader was measured seeing a fully
    committed landing table at ``COUNT(*) = 0`` of 20,000 rows. So nesting is
    off, with no opt-out, and dicts/lists land as JSON columns instead.

Merge identity
    The compound key ``(_connector_binding_id, *resource_pk)`` guards against a
    second Binding deleting the first's rows — measured removing 10 of 50 when
    keyed on the remote id alone.

Incremental dedup
    dlt defaults the dedup key to the resource's primary key, and with
    ``range_start="closed"`` a record updated at *exactly* the stored cursor
    value is silently dropped. Verified: run 2 emitted an updated row and the
    table still held the old one.

In-package dedup
    A key emitted twice inside one load package is resolved by the merge job's
    ``ROW_NUMBER()``, which orders by ``(SELECT NULL)`` unless a ``dedup_sort``
    column hint exists — so the *oldest* version wins while the cursor has
    already advanced past the newest, and an edit-then-delete pair lands the
    edit. The cursor column is hinted ``dedup_sort: "desc"`` for that reason.
    See :func:`_dedup_sort_column`, including what a cursor-less resource does
    and does not promise.

``hard_delete``
    ``columns={"x": {"hard_delete": True}}`` physically deletes the row on
    merge, which would make deletions unobservable to Projection forever. It is
    never set, and its absence is asserted.
"""

import inspect

from django_connectors.exceptions import SourceError
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    RUN_ID_COLUMN,
    landing_table_name,
)

# Pinned types for the injected columns. `precision` matters: without it dlt
# maps str to MySQL TEXT, which cannot be indexed without a prefix length
# (error 1170); with it the column is varchar(N) and indexes normally.
# `nullable: False` turns the add_map arity bug into a loud
# CannotCoerceNullException at normalize time instead of silent NULL tenancy.
#
# 36, not 32: these hold canonical UUID strings, which include four hyphens.
# MySQL rejects the overflow outright ("Data too long for column"), while sqlite
# accepts any width — so getting this wrong is invisible until production.
UUID_STRING_LENGTH = 36

METADATA_COLUMN_HINTS = {
    BINDING_ID_COLUMN: {
        "data_type": "text",
        "precision": UUID_STRING_LENGTH,
        "nullable": False,
    },
    RUN_ID_COLUMN: {
        "data_type": "text",
        "precision": UUID_STRING_LENGTH,
        "nullable": False,
    },
    DELETED_COLUMN: {"data_type": "bool", "nullable": False},
}


def make_metadata_injector(binding_id, run_id):
    """A one-argument closure stamping tenant and load metadata onto a record.

    Must take exactly one parameter. See the module docstring: any other count
    changes how dlt calls it and silently nulls the binding id.
    """

    def inject(row):
        if not isinstance(row, dict):
            raise SourceError(
                f"Landing records must be dicts, got {type(row).__name__}. "
                f"Arrow/pandas batches cannot be annotated with tenant metadata "
                f"(dlt raises 'object does not support item assignment'), so "
                f"SQL sources must force backend='sqlalchemy'."
            )
        row[BINDING_ID_COLUMN] = binding_id
        row[RUN_ID_COLUMN] = run_id
        # A source that detects deletions sets this itself; default False keeps
        # the landing schema uniform across sources that cannot.
        row.setdefault(DELETED_COLUMN, False)
        return row

    if len(inspect.signature(inject).parameters) != 1:  # pragma: no cover
        raise SourceError(
            "the metadata injector must take exactly one parameter, or dlt will "
            "call it as f(item, meta) and null the binding id on every row"
        )
    return inject


def instrument_source(source, *, binding, run, source_definition=None):
    """Apply every landing invariant to `source`, in place, and return it.

    Note the schema rename: dlt takes the schema name from the source, and a
    cold restore resolves a schema *by name* from ``_dlt_version`` taking the
    newest row. With Bindings sharing a schema name in one dataset, a restore
    was measured handing Binding A the schema of Binding B, and A's own column
    vanished. Because the schema object is replaced, sources must declare hints
    on their resources rather than on a source-level schema.
    """
    from dlt.common.schema import Schema

    if source.schema.name != binding.schema_name:
        source.schema = Schema(binding.schema_name)

    # No opt-out. See the module docstring.
    source.max_table_nesting = 0

    resources = list(source.resources.values())
    if not resources:
        raise SourceError(f"source {binding.source!r} produced no resources")

    for resource in resources:
        _instrument_resource(
            resource,
            binding=binding,
            run=run,
            source_definition=source_definition,
        )
    return source


def _instrument_resource(resource, *, binding, run, source_definition=None):
    table_name = landing_table_name(binding.source, resource.name, binding.landing_key)

    declared_primary_key = _as_tuple(resource._hints.get("primary_key"))
    write_disposition = _normalize_write_disposition(
        resource._hints.get("write_disposition"), resource.name, declared_primary_key
    )

    hints = {
        "table_name": table_name,
        "write_disposition": write_disposition,
        "columns": dict(METADATA_COLUMN_HINTS),
    }
    if write_disposition["disposition"] == "merge":
        # Unconditional: the binding id always leads the merge identity.
        hints["primary_key"] = (BINDING_ID_COLUMN, *declared_primary_key)

    incremental = _build_incremental(source_definition, resource.name, binding)
    if incremental is not None:
        hints["incremental"] = incremental
        dedup_column = _dedup_sort_column(resource, incremental, write_disposition)
        if dedup_column:
            hints["columns"][dedup_column] = {"dedup_sort": "desc"}

    resource.apply_hints(**hints)

    resource.add_map(make_metadata_injector(str(binding.id), str(run.id)))

    _assert_no_hard_delete(resource)


def _normalize_write_disposition(declared, resource_name, primary_key):
    """Name the merge strategy explicitly rather than relying on dlt's default.

    ``upsert`` is unavailable on the sqlalchemy destination — the only route to
    MySQL — and raises at extract time, so ``delete-insert`` is the only usable
    strategy and is stated rather than inherited.

    There is no "unset means merge" fallback, because **there is no unset**:
    ``dlt.resource()`` defaults the hint to ``"append"``, not to ``None``. A
    source that simply omits the disposition therefore appends a fresh copy of
    every re-fetched record on every run — with the merge key correctly
    configured and no error raised. Since an explicit ``"append"`` is
    indistinguishable from the default, a declared ``primary_key`` is used as
    the signal: it means nothing under append disposition, so the combination is
    a mistake rather than a preference, and is refused.
    """
    disposition = declared["disposition"] if isinstance(declared, dict) else declared
    disposition = disposition or "merge"

    if disposition == "merge":
        if not primary_key:
            raise SourceError(
                f"resource {resource_name!r} uses merge disposition but declares "
                f"no primary_key. Merge without a key cannot identify rows to "
                f"replace; declare one, or use append/replace disposition."
            )
        return {"disposition": "merge", "strategy": "delete-insert"}

    if disposition == "append" and primary_key:
        raise SourceError(
            f"resource {resource_name!r} declares primary_key {list(primary_key)} "
            f"with append disposition, which dlt ignores — every run would append "
            f"a second copy of every re-fetched record, silently. dlt defaults "
            f"this hint to 'append', so this is most likely an omission: state "
            f"write_disposition='merge' to keep records reconciled, or drop the "
            f"primary_key if append-only really is intended."
        )
    return {"disposition": disposition}


def _build_incremental(source_definition, resource_name, binding):
    """Construct the resource's Incremental, with the safe settings forced on.

    The library builds this rather than the source, because neither setting can
    be repaired afterwards: assigning to the resource's incremental wrapper
    after the fact is silently ineffective (measured — the boundary record was
    still dropped), and dlt strips the incremental from a *bound* resource's
    signature, so a source that declares its own via a parameter default is
    beyond reach entirely.
    """
    if source_definition is None:
        return None
    kwargs = source_definition.incremental_for(resource_name, binding)
    if not kwargs:
        return None

    from dlt.extract.incremental import Incremental

    forced = dict(kwargs)
    # Empty deduplication key. dlt otherwise defaults it to the resource's
    # primary key, and then drops a record updated at exactly the stored cursor
    # value — the update is lost with no error.
    forced["primary_key"] = ()
    # Let a record with no cursor value through instead of raising
    # IncrementalCursorPathMissing and failing the whole Run. Tombstones carry
    # identity columns only and so have no cursor.
    forced.setdefault("on_cursor_value_missing", "include")
    return Incremental(**forced)


def _dedup_sort_column(resource, incremental, write_disposition):
    """The column dlt should order in-package duplicates by, or None.

    Without this hint, dlt's merge job deduplicates a primary key that appears
    twice inside *one* load package with ``ROW_NUMBER() OVER (... ORDER BY
    (SELECT NULL))`` and keeps whichever row the database happened to emit
    first — normally the oldest. The incremental cursor has meanwhile advanced
    past the newest version, so the source never re-sends it and the stale row
    survives until the record next changes. Measured on sqlite, MySQL and
    PostgreSQL: a batch carrying an edit and then a tombstone for one id landed
    the edit with ``_connector_deleted`` false, and the deletion was lost for
    good — Projection never saw it and the host kept a record the provider had
    deleted.

    Ordering by the cursor descending makes the newest version win, which is
    the same record the cursor claims was consumed.

    Three cases decline the hint rather than risking a worse failure:

    - A cursor that is a JSONPath ("data.updated_at") names no landed column —
      nesting is off — and dlt would raise ``KeyError`` building the merge SQL,
      wedging the pipeline on every load instead of on a duplicate.
    - dlt ignores the hint outside merge disposition, and logs about it.
    - A second ``dedup_sort`` on one table is a ``SchemaCorruptedException``, so
      a source that already nominated a column keeps it.

    A cursor-less resource gets no hint and no guarantee: an in-batch duplicate
    there resolves arbitrarily. There is nothing to order by, and inventing an
    order would be a guess dressed as a rule.
    """
    if write_disposition["disposition"] != "merge":
        return None

    cursor_path = getattr(incremental, "cursor_path", None)
    if not isinstance(cursor_path, str) or not cursor_path.isidentifier():
        return None
    if cursor_path in METADATA_COLUMN_HINTS:
        return None

    declared = resource._hints.get("columns") or {}
    if isinstance(declared, dict) and any(
        isinstance(hint, dict) and hint.get("dedup_sort") for hint in declared.values()
    ):
        return None
    return cursor_path


def _assert_no_hard_delete(resource):
    columns = resource._hints.get("columns") or {}
    offending = sorted(
        name
        for name, hint in columns.items()
        if isinstance(hint, dict) and hint.get("hard_delete")
    )
    if offending:
        raise SourceError(
            f"resource {resource.name!r} sets dlt's hard_delete hint on "
            f"{offending}. That physically removes the row during merge, so "
            f"Projection could never observe the deletion and target records "
            f"would go stale forever. Emit {DELETED_COLUMN}=True instead."
        )


def _as_tuple(value):
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)
