"""Landing identifier construction.

Every landing table name is derived by the server from
``{source}_{resource}_{landing_key}``. None of it is customer-controlled, and
all of it is validated against dlt's own naming convention before use.

dlt's snake_case convention rewrites identifiers in ways that break naive
construction, all verified against dlt 1.30:

* ``__`` collapses to ``_`` — ``rest__events`` becomes ``rest_events``, so
  double-underscore separators are unachievable;
* a trailing ``_`` becomes ``x`` (``rest_events_`` → ``rest_eventsx``);
* a leading digit gains a ``_`` prefix (``9lead`` → ``_9lead``);
* ``-`` becomes ``_`` and uppercase becomes lowercase;
* names over 64 characters get a 6-character hash injected mid-string, which is
  collision-safe but makes the physical name unpredictable.

So a name is never assumed to survive: it is normalized, then asserted
idempotent under normalization. ``_dlt`` is the only prefix dlt reserves, which
is why ``_connector_*`` passes through byte-identical.
"""

from functools import lru_cache

from django_connectors.exceptions import LandingSchemaError

# MySQL's limit for both table and column identifiers, and the point at which
# dlt starts injecting a hash rather than failing.
MAX_IDENTIFIER_LENGTH = 64

# Injected onto every root landing record. `_dlt` is dlt's reserved prefix;
# `_connector_` is ours and survives normalization unchanged.
BINDING_ID_COLUMN = "_connector_binding_id"
RUN_ID_COLUMN = "_connector_run_id"
DELETED_COLUMN = "_connector_deleted"
SOURCE_UPDATED_AT_COLUMN = "_connector_source_updated_at"

METADATA_COLUMNS = (
    BINDING_ID_COLUMN,
    RUN_ID_COLUMN,
    DELETED_COLUMN,
    SOURCE_UPDATED_AT_COLUMN,
)

# dlt's own bookkeeping columns, present on every landing table.
DLT_LOAD_ID_COLUMN = "_dlt_load_id"
DLT_ID_COLUMN = "_dlt_id"


@lru_cache(maxsize=1)
def _naming_convention():
    from dlt.common.normalizers.naming.snake_case import NamingConvention

    return NamingConvention()


def normalize_identifier(name):
    """Apply dlt's snake_case naming convention to a single identifier."""
    return _naming_convention().normalize_identifier(name)


def landing_table_name(source_key, resource, landing_key):
    """The physical landing table for one resource of one Binding.

    Includes ``landing_key`` because every Binding gets its own table: sharing
    one between Bindings loses loads silently, since dlt keys its staging table
    on ``(dataset, table_name)`` alone and each load issues an unconditional
    auto-committed DELETE against it.
    """
    if not landing_key:
        raise LandingSchemaError(
            "Binding has no landing_key; it must be saved before its landing "
            "table name can be derived."
        )

    # Normalize each part first so that joining with single underscores cannot
    # produce a `__` sequence (which would then collapse and change the name).
    parts = [normalize_identifier(str(part)) for part in (source_key, resource)]
    parts.append(landing_key)
    name = "_".join(part.strip("_") or "x" for part in parts)

    return validate_identifier(name, kind="landing table name")


def validate_identifier(name, *, kind="identifier"):
    """Return `name`, or explain exactly why dlt/MySQL would not keep it."""
    normalized = normalize_identifier(name)
    if normalized != name:
        raise LandingSchemaError(
            f"{kind} {name!r} is not stable under dlt's naming convention "
            f"(it would become {normalized!r}). Compose identifiers from "
            f"lowercase letters, digits and single underscores."
        )
    if len(name) > MAX_IDENTIFIER_LENGTH:
        raise LandingSchemaError(
            f"{kind} {name!r} is {len(name)} characters; the limit is "
            f"{MAX_IDENTIFIER_LENGTH}. dlt would silently inject a hash "
            f"mid-string and MySQL would reject it. Shorten the source key or "
            f"the resource name."
        )
    return name


def is_metadata_column(column_name):
    return column_name in METADATA_COLUMNS


def is_internal_column(column_name):
    """Columns the customer never maps: ours and dlt's."""
    return column_name.startswith(("_connector_", "_dlt_"))
