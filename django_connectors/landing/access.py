"""The only module permitted to read the landing database.

Five separate features need landing rows scoped to one Binding — projection
execution, preview, resource sampling, schema discovery and purge — and each one
that built its own query would be an independent opportunity to forget the
tenant filter. So they all come through :func:`binding_relation`, and a test
greps the package to keep it that way.

**No SQL text is constructed anywhere in this package.** Customer mappings and
filters are compiled to a validated Python AST and evaluated in-process. The
values that reach the database are all server-controlled: a derived table name,
the binding id, a load-id scope, and a server-chosen ordering. That removes the
injection surface rather than trying to escape it — and dlt's own Relation
surface could not express the mapping DSL anyway (it has no ``coalesce``,
``concat``, ``json_extract`` or casts), so "compile to SQL" would have meant
hand-rolling an escaper.
"""

import contextlib
import json

from django_connectors.exceptions import LandingError, LandingSchemaError
from django_connectors.landing.destination import build_pipeline
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DLT_LOAD_ID_COLUMN,
    landing_table_name,
)

# dlt's Relation.where supports exactly these; anything else raises.
SUPPORTED_OPERATORS = frozenset({"eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in"})


def binding_dataset(binding, *, pipeline=None):
    """The dlt dataset for `binding`, bound to its own schema.

    Always passes ``schema=``: ``dlt.dataset(destination, dataset)`` with no
    schema silently picks one of however many exist in the dataset.
    """
    pipeline = pipeline or build_pipeline(binding)
    try:
        return pipeline.dataset(schema=binding.schema_name)
    except Exception as exc:
        raise LandingSchemaError(
            f"no landing schema {binding.schema_name!r} yet for binding "
            f"{binding.id}; it is created by the first successful run"
        ) from exc


def binding_relation(binding, resource, *, load_ids=None, pipeline=None):
    """A Relation over `resource`'s landing table, scoped to `binding`.

    `resource` is customer-writable free text, so it is never used to index the
    dataset directly — the physical table name is derived by the server and
    validated. Indexing a dataset would otherwise allow any table in it,
    including dlt's own ``_dlt_loads``.
    """
    dataset = binding_dataset(binding, pipeline=pipeline)
    table_name = landing_table_name(binding.source, resource, binding.landing_key)

    available = set(dataset.schema.tables)
    if table_name not in available:
        raise LandingSchemaError(
            f"resource {resource!r} has no landing table for this binding "
            f"(expected {table_name!r}). Available: "
            f"{sorted(name for name in available if not name.startswith('_dlt'))}"
        )

    relation = dataset[table_name]

    # No `where(BINDING_ID_COLUMN, ...)` here, deliberately. dlt renders a
    # WHERE comparison as a CAST to the column's declared type *including its
    # precision*, and PostgreSQL rejects `CAST(x AS TEXT(36))` outright — "type
    # modifier is not allowed for type text". Verified: the same call succeeds
    # against a column with no precision hint, and the precision is not
    # negotiable because MySQL cannot index TEXT without a prefix length.
    #
    # Tenant isolation does not depend on that filter anyway: the table name is
    # derived server-side from this Binding's landing_key, so the scope is
    # structural. The filter was redundancy, and it is replaced by a stricter
    # form of redundancy in `iter_rows`/`sample_rows` — a foreign row is raised
    # on rather than quietly filtered out, because in a per-Binding table it
    # would mean something has gone badly wrong.

    if load_ids is not None:
        load_ids = [str(load_id) for load_id in load_ids]
        if not load_ids:
            # An empty scope means "nothing landed", not "everything".
            return relation.where(DLT_LOAD_ID_COLUMN, "in", [""])
        relation = relation.where(DLT_LOAD_ID_COLUMN, "in", load_ids)

    return relation


def json_columns(relation):
    """Names of columns dlt typed as JSON.

    Needed because backends disagree about what a JSON column reads back as:
    sqlite hands back the serialized text while MySQL hands back parsed data.
    Left alone, an identical mapping would produce a string on one backend and a
    dict on the other — so the same mapping would work in tests and fail in
    production, or vice versa.
    """
    try:
        schema = dict(relation.columns_schema)
    except Exception:
        return frozenset()
    return frozenset(
        name
        for name, column in schema.items()
        if (column or {}).get("data_type") == "json"
    )


def _verify_tenant(row, expected_binding_id):
    """Raise if a row does not belong to the Binding whose table it came from.

    Replaces the tenant WHERE clause, which PostgreSQL cannot express against a
    precision-hinted column. Raising rather than filtering is the stricter
    choice: in a per-Binding table a foreign row means the naming scheme or the
    metadata injector has failed, and silently dropping it would hide that.
    """
    if expected_binding_id is None:
        return row
    actual = row.get(BINDING_ID_COLUMN)
    if actual is not None and str(actual) != expected_binding_id:
        raise LandingError(
            f"landing row belongs to binding {actual!r} but was read from "
            f"binding {expected_binding_id!r}'s table. Refusing to return it."
        )
    return row


def _decode(row, json_column_names):
    for name in json_column_names:
        value = row.get(name)
        if isinstance(value, str | bytes):
            # Left as-is on failure: a column dlt typed as JSON that does not
            # parse is worth surfacing to the mapping author, not swallowing.
            with contextlib.suppress(TypeError, ValueError):
                row[name] = json.loads(value)
    return row


def iter_rows(relation, *, chunk_size=1000, order_by=None, binding=None):
    """Yield landing rows as dicts, in a deterministic order.

    ``iter_fetch`` yields lists of *tuples*, not dicts, so the column list has
    to be zipped back on. Ordering is server-chosen and always applied: without
    it, batch composition varies between a run and its retry, which would make
    "retry is idempotent" untestable.
    """
    columns = list(relation.columns)
    json_names = json_columns(relation)
    if order_by:
        for column in order_by:
            relation = relation.order_by(column, "asc")
    expected = str(binding.id) if binding is not None else None
    for chunk in relation.iter_fetch(chunk_size):
        for row in chunk:
            decoded = _decode(dict(zip(columns, row, strict=False)), json_names)
            yield _verify_tenant(decoded, expected)


def sample_rows(binding, resource, *, limit, pipeline=None):
    """A bounded sample for the mapping UI. Never unbounded."""
    relation = binding_relation(binding, resource, pipeline=pipeline).limit(limit)
    columns = list(relation.columns)
    json_names = json_columns(relation)
    expected = str(binding.id)
    return [
        _verify_tenant(
            _decode(dict(zip(columns, row, strict=False)), json_names), expected
        )
        for row in relation.fetchall()
    ]


def landing_columns(binding, resource, *, pipeline=None):
    """``{column_name: dlt column schema}`` for a landed resource."""
    relation = binding_relation(binding, resource, pipeline=pipeline)
    return dict(relation.columns_schema)


def drop_landing_tables(binding, *, pipeline=None):
    """Remove every landing table for `binding`.

    Cheap and complete precisely because each Binding owns its tables: purging
    is a DROP rather than a tenant-filtered DELETE that would leave the merge
    key's other tenants interleaved.
    """
    pipeline = pipeline or build_pipeline(binding)
    dropped = []
    try:
        schema = pipeline.schemas.get(binding.schema_name)
    except Exception:
        schema = None
    if schema is None:
        return dropped

    table_names = [name for name in schema.tables if not name.startswith("_dlt")]
    if not table_names:
        return dropped

    with pipeline.sql_client(schema_name=binding.schema_name) as client:
        for table_name in table_names:
            client.drop_tables(table_name)
            dropped.append(table_name)
    return dropped
