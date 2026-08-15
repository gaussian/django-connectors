"""Reading the landed schema, and detecting drift against it.

The mapping UI needs column names, types and nullability without re-running
extraction, so everything here reads the snapshot written to
``Binding.landing_schema`` after each successful Run rather than touching the
provider.

Drift matters because dlt does not change a column's type when the source's
does. It **adds a sibling** — ``amount__v_text`` next to ``amount`` — and writes
NULL into the original for every row landed after the change. A mapping reading
``amount`` therefore keeps validating and keeps returning NULLs, silently, which
is exactly the failure this module exists to make loud.
"""

from django_connectors.enums import ProjectionStatus
from django_connectors.landing.naming import is_internal_column, landing_table_name

VARIANT_MARKER = "__v_"


def resource_table_map(binding):
    """``{resource: landing table}`` for the resources this Binding selected."""
    return {
        resource: landing_table_name(binding.source, resource, binding.landing_key)
        for resource in (binding.resources or [])
    }


def landing_schema(binding):
    """The landed schema for `binding`, as plain JSON.

    Returns ``{"status": "pending_first_run", "resources": {}}`` before the
    first successful Run rather than raising. A Binding that has not run yet is
    an ordinary state in the setup flow, not an error, and making callers catch
    an exception for it guarantees some caller will not.
    """
    snapshot = binding.landing_schema or {}
    tables = snapshot.get("tables") or {}
    if not tables or binding.landing_schema_at is None:
        return {
            "status": "pending_first_run",
            "resources": {},
            "fingerprint": "",
            "captured_at": None,
        }

    table_to_resource = {
        table: resource for resource, table in resource_table_map(binding).items()
    }
    resources = {}
    for table_name, columns in tables.items():
        resource = table_to_resource.get(
            table_name, _resource_from_table(binding, table_name)
        )
        resources[resource] = {
            "table": table_name,
            "columns": _describe_columns(columns),
        }

    return {
        "status": "ready",
        "resources": resources,
        "fingerprint": binding.landing_schema_version_hash,
        "captured_at": binding.landing_schema_at,
    }


def _resource_from_table(binding, table_name):
    """Recover the resource name from a landing table name.

    Names are built as ``{source}_{resource}_{landing_key}``, so stripping the
    known prefix and suffix leaves the resource. Both ends are server-derived,
    which is what makes this safe to reverse.
    """
    prefix = f"{binding.source}_"
    suffix = f"_{binding.landing_key}"
    name = table_name
    if name.startswith(prefix):
        name = name[len(prefix) :]
    if name.endswith(suffix):
        name = name[: -len(suffix)]
    return name or table_name


def _describe_columns(columns):
    described = {}
    for name, column in (columns or {}).items():
        if is_internal_column(name):
            continue
        described[name] = {
            "data_type": column.get("data_type"),
            "nullable": column.get("nullable", True),
            "variant_of": name.split(VARIANT_MARKER)[0]
            if VARIANT_MARKER in name
            else None,
        }
    return described


def columns_for(binding, resource):
    """``{column: {...}}`` for one resource, or ``{}`` before the first run."""
    schema = landing_schema(binding)
    entry = schema["resources"].get(resource)
    return entry["columns"] if entry else {}


def variant_columns(columns, column_name):
    """Sibling columns dlt created when the source changed a column's type."""
    return sorted(
        name for name in columns if name.startswith(f"{column_name}{VARIANT_MARKER}")
    )


def detect_drift(projection, columns=None):
    """Classify a Projection against the current landed schema.

    Returns ``(status, messages)``. A vanished column makes the mapping
    unexecutable; a variant sibling leaves it executable but quietly wrong, so
    the two are graded differently instead of being collapsed into "broken".
    """
    from django_connectors.exceptions import MappingValidationError
    from django_connectors.projections.compiler import compile_mapping

    if columns is None:
        columns = columns_for(projection.binding, projection.resource)
    if not columns:
        return projection.status, []

    try:
        compiled = compile_mapping(projection.mapping, projection.filters)
    except MappingValidationError as exc:
        return ProjectionStatus.INVALID, [str(exc)]

    messages = []
    status = ProjectionStatus.ACTIVE

    missing = sorted(compiled.source_columns - set(columns))
    if missing:
        status = ProjectionStatus.INVALID
        messages.append(
            f"column(s) {missing} are no longer present in the landed resource"
        )

    for column in sorted(compiled.source_columns & set(columns)):
        variants = variant_columns(columns, column)
        if variants:
            if status != ProjectionStatus.INVALID:
                status = ProjectionStatus.NEEDS_REVIEW
            messages.append(
                f"column {column!r} gained variant(s) {variants}: the source "
                f"changed its type, so rows landed since hold NULL in "
                f"{column!r}. The mapping DSL does not read across variants."
            )

    return status, messages
