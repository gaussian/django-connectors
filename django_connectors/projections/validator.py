"""Validating a Projection before it is allowed to write to the host.

Runs the full checklist: the resource exists, the target is registered, every
required target field is mapped, every referenced column exists and is of a
compatible type, filters parse, and identity is actually obtainable.

The last of those is subtle and is the reason validation is strict rather than
advisory. ``delete-insert`` merge replaces the whole row, so a tombstone —
which carries identity columns only — nulls every other column. A target
identity field sourced from anywhere outside the merge key is therefore ``None``
on the delete path, and the host receives a delete it cannot match to anything.
Refusing that configuration up front makes it unreachable by construction.
"""

from django_connectors.enums import ProjectionStatus
from django_connectors.exceptions import MappingValidationError
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    is_internal_column,
)
from django_connectors.projections.compiler import compile_mapping
from django_connectors.projections.fields import UNMAPPABLE_DLT_TYPES
from django_connectors.projections.targets import get_target


class ValidationResult:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def error(self, message):
        self.errors.append(message)

    def warn(self, message):
        self.warnings.append(message)

    @property
    def ok(self):
        return not self.errors

    def raise_if_invalid(self):
        if self.errors:
            raise MappingValidationError("; ".join(self.errors))
        return self

    def as_dict(self):
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


def validate_projection(projection, *, landing_columns=None, source_definition=None):
    """Check `projection` against its target and the landed schema.

    `landing_columns` is ``{column: dlt column schema}``. When omitted, only the
    checks that do not need the landed schema run — which is what a Projection
    being drafted before the first Run can offer.
    """
    result = ValidationResult()

    try:
        target = get_target(projection.target)
    except Exception as exc:
        result.error(str(exc))
        return result

    try:
        compiled = compile_mapping(projection.mapping, projection.filters)
    except MappingValidationError as exc:
        result.error(str(exc))
        return result

    _check_target_fields(result, projection, target, compiled)

    if landing_columns is None:
        result.warn(
            "the landing schema is not available yet, so source columns and "
            "types were not checked; it is written by the first successful run"
        )
        return result

    _check_source_columns(result, compiled, landing_columns)
    _check_types(result, target, compiled, landing_columns)
    _check_identity_is_obtainable(result, target, compiled, source_definition)
    return result


def _check_target_fields(result, projection, target, compiled):
    unknown = sorted(set(compiled.nodes) - set(target.fields))
    if unknown:
        result.error(
            f"mapping writes fields {unknown} which target "
            f"{projection.target!r} does not declare"
        )

    missing_required = sorted(set(target.required_fields) - set(compiled.nodes))
    if missing_required:
        result.error(
            f"target {projection.target!r} requires {missing_required}, which "
            f"the mapping does not produce"
        )

    missing_identity = sorted(set(target.identity_fields) - set(compiled.nodes))
    if missing_identity:
        result.error(
            f"target {projection.target!r} identifies records by "
            f"{list(target.identity_fields)}; the mapping does not produce "
            f"{missing_identity}"
        )


def _check_source_columns(result, compiled, landing_columns):
    referenced = compiled.source_columns
    unknown = sorted(referenced - set(landing_columns))
    if unknown:
        result.error(
            f"mapping reads columns {unknown} which the landed resource does "
            f"not have; available: "
            f"{sorted(name for name in landing_columns if not is_internal_column(name))}"
        )

    # A dlt variant sibling means the source changed type mid-stream and the
    # original column now holds NULLs for the newer rows.
    for column in sorted(referenced):
        variants = sorted(
            name for name in landing_columns if name.startswith(f"{column}__v_")
        )
        if variants:
            result.warn(
                f"column {column!r} has variant columns {variants}: the source "
                f"changed its type, so rows landed after the change hold NULL "
                f"in {column!r}. The mapping DSL does not read across variants."
            )


def _check_types(result, target, compiled, landing_columns):
    for field_name, node in compiled.nodes.items():
        declared = target.fields.get(field_name)
        if declared is None:
            continue
        for column in sorted(node.source_columns):
            schema = landing_columns.get(column) or {}
            dlt_type = schema.get("data_type")
            if not dlt_type:
                continue
            if dlt_type in UNMAPPABLE_DLT_TYPES:
                result.error(
                    f"{field_name}: column {column!r} is of dlt type "
                    f"{dlt_type!r}, which has no target representation"
                )
                continue
            if (
                declared.compatible_dlt_types
                and dlt_type not in declared.compatible_dlt_types
            ):
                result.warn(
                    f"{field_name}: column {column!r} is {dlt_type!r} but the "
                    f"target field is {declared.type_name!r}; values will be "
                    f"coerced and may fail per row"
                )


def _check_identity_is_obtainable(result, target, compiled, source_definition):
    """Identity must survive a tombstone, or deletes carry a broken key."""
    if source_definition is None or not getattr(
        source_definition, "emits_tombstones", False
    ):
        return

    merge_key_columns = {BINDING_ID_COLUMN, DELETED_COLUMN}
    for field_name in target.identity_fields:
        node = compiled.nodes.get(field_name)
        if node is None:
            continue
        outside = sorted(
            column
            for column in node.source_columns
            if column not in merge_key_columns and is_internal_column(column)
        )
        if outside:
            result.error(
                f"identity field {field_name!r} reads internal column(s) {outside}"
            )


def status_for(result):
    """Map a validation outcome onto a Projection status."""
    if result.errors:
        return ProjectionStatus.INVALID
    if result.warnings:
        return ProjectionStatus.NEEDS_REVIEW
    return ProjectionStatus.ACTIVE
