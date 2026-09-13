"""Post-load index provisioning for landing tables.

dlt creates **no primary key and no index** on a landing table, and the
sqlalchemy destination's only usable merge strategy is ``delete-insert``, whose
delete step is ``DELETE ... WHERE EXISTS (SELECT 1 FROM staging ...)`` joined on
the merge key. With no index on that key every load scans the whole table.
Measured against MySQL 8.4: loading 200 rows took **0.50s** into a 5,000-row
table, **1.41s** into 20,000 and **3.30s** into 50,000 — and 10,000 rows into a
60,000-row table took **179 seconds**. With the merge index below the same
loads were flat at **0.13-0.17s** across 50,000, 100,000 and 200,000 rows, and
the index survived dlt's own ``ALTER TABLE`` schema evolution.

The indexes cannot be delegated to dlt. ``create_primary_keys=True`` on the
destination is worse than nothing — it hard-fails unless every key column
carries a ``precision`` hint, emits columns in an order different from the
declared one, and, being a real uniqueness constraint, turns a source that
emits a duplicate key inside one batch from a soft dedupe into a permanently
poisoned load package. See :mod:`django_connectors.landing.destination`.

So they are provisioned here, after the load that created the table, and every
rule below exists because a database refused the naive version:

**MySQL needs a prefix length.** dlt maps a ``str`` column with no ``precision``
hint to ``TEXT``, and MySQL refuses to index one: *(1170, "BLOB/TEXT column 'id'
used in key specification without a key length")*. PostgreSQL and sqlite have no
such rule and reject the prefix syntax instead, so the prefix is emitted for
MySQL only. The ``_connector_*`` metadata columns are hinted ``precision=36``
precisely so they are indexable without one.

**MySQL has no ``CREATE INDEX IF NOT EXISTS``.** PostgreSQL and sqlite do, so
"just add IF NOT EXISTS" would work everywhere in development and fail on the
one backend that matters. Existence is therefore checked by reflection before
the statement is issued, which is portable.

**Each index is created in its own statement.** PostgreSQL aborts the whole
transaction on the first failed statement, so batching them would mean one
rejected index silently cancelled the rest.

**Nothing here fails a Run.** The data landed; a missing index makes the *next*
load slow, not this one wrong. A failure is logged and the Binding is moved to
``needs_review`` by the caller, which keeps it running (``is_runnable`` includes
that status) while making the degradation visible.
"""

import hashlib
import logging
from dataclasses import dataclass

from django_connectors.conf import conf
from django_connectors.errors import scrub
from django_connectors.exceptions import LandingSchemaError
from django_connectors.landing.destination import build_pipeline
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DLT_LOAD_ID_COLUMN,
    MAX_IDENTIFIER_LENGTH,
    validate_identifier,
)

logger = logging.getLogger(__name__)

# Prefix length applied to unbounded MySQL TEXT/BLOB key columns. 191 rather
# than something larger because 191 * 4 bytes (utf8mb4) = 764, which fits the
# 767-byte per-column prefix limit of InnoDB's older COMPACT/REDUNDANT row
# formats as well as the 3072 bytes DYNAMIC allows. Merge keys are provider
# identifiers — a 191-character prefix is not a real loss of selectivity.
#
# A resource whose merge key is four or more unbounded text columns exceeds
# InnoDB's 3072-byte total key limit even so. That is reported as a failed
# index rather than silently shortened: a quietly less selective index would
# look provisioned while still scanning.
MYSQL_TEXT_PREFIX_LENGTH = 191

#: Suffixes appended to the landing table name. Short, because the table name
#: may already be close to the 63-character identifier limit.
MERGE_INDEX_SUFFIX = "merge_idx"
LOAD_INDEX_SUFFIX = "load_idx"


@dataclass(frozen=True)
class IndexSpec:
    """One index this library wants on one landing table."""

    name: str
    table: str
    columns: tuple[str, ...]


def index_name(table_name, suffix):
    """A unique, backend-legal index name derived from a landing table name.

    Landing table names may be up to :data:`MAX_IDENTIFIER_LENGTH` characters
    already, so a plain ``f"{table}_{suffix}"`` can overflow. PostgreSQL
    truncates silently at 63, and two landing tables whose names differ only
    past the truncation point would then collide on one index name — index
    names are per-*schema* there, not per-table. The hash keeps the shortened
    form unique.
    """
    name = f"{table_name}_{suffix}"
    if len(name) > MAX_IDENTIFIER_LENGTH:
        digest = hashlib.sha256(name.encode()).hexdigest()[:6]
        keep = MAX_IDENTIFIER_LENGTH - len(suffix) - len(digest) - 2
        name = f"{table_name[:keep].rstrip('_')}_{digest}_{suffix}"
    return validate_identifier(name, kind="landing index name")


def index_specs(table_name, table_schema):
    """The indexes one landing table needs, or an empty list.

    Two, and deliberately in a different column order from each other:

    ``(_connector_binding_id, *resource primary key)``
        exactly the merge predicate. ``instrument_source`` always leads the
        merge identity with the binding id, so this is the join dlt issues on
        every load of a merge resource. Append and replace resources have no
        merge key and get no such index.

    ``(_dlt_load_id, _connector_binding_id)``
        the incremental projection window. Note the order: the original plan
        led with the binding id, but ``access.binding_relation`` deliberately
        emits **no** tenant WHERE clause — PostgreSQL rejects the ``CAST(x AS
        TEXT(36))`` dlt renders for a precision-hinted column — so the only
        predicate is ``_dlt_load_id IN (...)``. An index leading with the
        binding id could not serve it at all. Tenancy is structural here
        anyway: the table belongs to one Binding.
    """
    columns = dict(table_schema.get("columns") or {})
    key_columns = [
        name for name, column in columns.items() if (column or {}).get("primary_key")
    ]
    resource_key = [name for name in key_columns if name != BINDING_ID_COLUMN]

    specs = []
    if BINDING_ID_COLUMN in columns and resource_key:
        specs.append(
            IndexSpec(
                name=index_name(table_name, MERGE_INDEX_SUFFIX),
                table=table_name,
                columns=(BINDING_ID_COLUMN, *resource_key),
            )
        )
    if BINDING_ID_COLUMN in columns and DLT_LOAD_ID_COLUMN in columns:
        specs.append(
            IndexSpec(
                name=index_name(table_name, LOAD_INDEX_SUFFIX),
                table=table_name,
                columns=(DLT_LOAD_ID_COLUMN, BINDING_ID_COLUMN),
            )
        )
    return specs


def ensure_landing_indexes(binding, *, pipeline=None):
    """Create `binding`'s landing indexes if they are missing. Idempotent.

    Called after every successful load rather than only after the first: a
    Binding gains a landing table whenever it starts selecting a new resource,
    and the existence check is one reflection per table against a database the
    load has just finished writing to.

    **Never raises.** Returns
    ``{"created": [...], "existing": [...], "skipped": [...], "failed": [...]}``
    where each failure is ``{"index": name, "error": scrubbed message}``.
    Also usable on its own, to provision a Binding that landed before this
    existed:

        from django_connectors.landing.index import ensure_landing_indexes
        ensure_landing_indexes(binding)
    """
    report = {"created": [], "existing": [], "skipped": [], "failed": []}

    if not conf.PROVISION_LANDING_INDEXES:
        # A host whose landing role has no DDL rights would otherwise put every
        # Binding into needs_review on every run, forever.
        report["skipped"].append("PROVISION_LANDING_INDEXES is False")
        return report

    try:
        pipeline = pipeline or build_pipeline(binding)
        specs = _specs_for_binding(binding, pipeline)
    except Exception as exc:
        _record_failure(report, "", exc, binding)
        return report

    if not specs:
        return report

    try:
        with pipeline.sql_client(schema_name=binding.schema_name) as client:
            _apply(client, specs, report, binding)
    except Exception as exc:
        _record_failure(report, "", exc, binding)
    return report


# --- internals -------------------------------------------------------------


def _specs_for_binding(binding, pipeline):
    try:
        schema = pipeline.schemas.get(binding.schema_name)
    except Exception:
        schema = None
    if schema is None:
        # Nothing has landed for this Binding in this working directory.
        return []

    specs = []
    for table_name, table in schema.tables.items():
        if table_name.startswith("_dlt"):
            continue
        specs.extend(index_specs(table_name, table))
    return specs


def _apply(client, specs, report, binding):
    by_table = {}
    for spec in specs:
        by_table.setdefault(spec.table, []).append(spec)

    for table_name, table_specs in by_table.items():
        try:
            # Reflection goes through SQLAlchemy's own connection handling,
            # which autobegins a transaction dlt does not know about — and
            # dlt's next `execute_sql` then dies on "this connection has
            # already initialized a Transaction()". Holding dlt's transaction
            # around the reflection keeps the two in step.
            with client.begin_transaction():
                reflected = _reflect(client, table_name)
                existing = _existing_index_names(client, table_name)
        except Exception as exc:
            for spec in table_specs:
                _record_failure(report, spec.name, exc, binding)
            continue

        for spec in table_specs:
            if spec.name in existing:
                report["existing"].append(spec.name)
                continue
            try:
                client.execute_sql(_create_index_sql(client, reflected, spec))
            except Exception as exc:
                _record_failure(report, spec.name, exc, binding)
            else:
                report["created"].append(spec.name)
                logger.info(
                    "created landing index %s on %s for binding %s",
                    spec.name,
                    spec.table,
                    binding.id,
                )


def _reflect(client, table_name):
    from sqlalchemy import MetaData, Table

    return Table(
        table_name,
        MetaData(schema=client.dataset_name),
        autoload_with=client.native_connection,
    )


def _existing_index_names(client, table_name):
    from sqlalchemy import inspect as sqlalchemy_inspect

    inspector = sqlalchemy_inspect(client.native_connection)
    return {
        index.get("name")
        for index in inspector.get_indexes(table_name, schema=client.dataset_name)
    }


def _create_index_sql(client, reflected, spec):
    """Compile the ``CREATE INDEX`` for one spec against the live dialect.

    Compiled by SQLAlchemy rather than formatted here, because the three
    backends disagree about where the schema qualifier goes: sqlite wants it on
    the *index* name and an unqualified table, MySQL and PostgreSQL want it on
    the table. Every identifier in it is server-derived — a landing table name
    and this library's own column names — so nothing customer-supplied reaches
    the statement.
    """
    from sqlalchemy import Index
    from sqlalchemy.schema import CreateIndex

    missing = [name for name in spec.columns if name not in reflected.c]
    if missing:
        raise LandingSchemaError(
            f"landing table {spec.table!r} is missing column(s) {missing}, so "
            f"index {spec.name!r} cannot be created."
        )

    columns = [reflected.c[name] for name in spec.columns]
    kwargs = {}
    if client.dialect_name == "mysql":
        lengths = {
            column.name: MYSQL_TEXT_PREFIX_LENGTH
            for column in columns
            if _needs_mysql_prefix(column)
        }
        if lengths:
            kwargs["mysql_length"] = lengths

    index = Index(spec.name, *columns, **kwargs)
    return str(CreateIndex(index).compile(dialect=client.dialect))


def _needs_mysql_prefix(column):
    """Whether MySQL would reject this column in a key without a prefix length.

    Only string and binary types can hit error 1170, and only when the column
    is unbounded (``TEXT``/``BLOB``) or wider than the prefix would be. A
    ``varchar(36)`` metadata column indexes whole.
    """
    from sqlalchemy import types as sqlalchemy_types

    if not isinstance(
        column.type, sqlalchemy_types.String | sqlalchemy_types.LargeBinary
    ):
        return False
    length = getattr(column.type, "length", None)
    return length is None or length > MYSQL_TEXT_PREFIX_LENGTH


def _record_failure(report, name, exc, binding):
    """Log and record. Scrubbed because a DSN can appear in a driver error."""
    detail = scrub(exc)
    report["failed"].append({"index": name, "error": detail})
    logger.warning(
        "could not provision landing index %s for binding %s: %s",
        name or "(unknown)",
        binding.id,
        detail,
    )
