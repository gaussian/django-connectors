"""Reshaping records at the source, before they land.

The architecture rule is that shaping belongs upstream of landing — in the
customer's SQL view or in the dlt source — never in the projection, which is
one row in, one record out. For a database that rule is easy to follow. For a
spreadsheet there is no upstream: the customer's sheet has one row per case
and one column per stage, and nothing sits between the sheet and this
library to turn it into one row per stage. So this is the upstream.

``unpivot`` turns a wide row into long ones::

    {"columns": ["received_at", "approved_at"], "name_to": "stage",
     "value_to": "at"}

    {"case": "C1", "received_at": "…", "approved_at": "…", "owner": "x"}
      → {"case": "C1", "owner": "x", "stage": "received_at", "at": "…"}
        {"case": "C1", "owner": "x", "stage": "approved_at", "at": "…"}

Every other column is kept on every emitted row (``keep`` narrows that), and a
stage whose value is empty is dropped unless ``drop_empty`` is false, because
a blank cell in a stage column means "did not happen", not "happened at
null". ``columns_matching`` is a glob (``"*_at"``) for sheets where listing
the stages would be brittle.

The landing layer extends the merge key with ``name_to``: one wide row is now
several landed rows, and without the stage in the key they would all merge
into one. That is done in ``landing/instrument.py``, next to the other merge
identity rules, so a source declares *what* to unpivot and never touches keys.
"""

import fnmatch

from django_connectors.exceptions import ConfigurationError

KEYS = frozenset(
    {"columns", "columns_matching", "name_to", "value_to", "keep", "drop_empty"}
)


def validate_unpivot(spec, *, where="unpivot"):
    """Raise ``ConfigurationError`` unless `spec` is a usable unpivot; return it."""
    if not isinstance(spec, dict):
        raise ConfigurationError(f"{where} must be an object")
    unknown = sorted(set(spec) - KEYS)
    if unknown:
        raise ConfigurationError(
            f"{where} has unknown key(s) {unknown}; allowed: {sorted(KEYS)}"
        )

    columns = spec.get("columns")
    pattern = spec.get("columns_matching")
    if not columns and not pattern:
        raise ConfigurationError(
            f"{where} needs 'columns' (a list of column names) or "
            f"'columns_matching' (a glob such as '*_at')"
        )
    if columns is not None and (
        not isinstance(columns, list)
        or not all(isinstance(c, str) and c for c in columns)
    ):
        raise ConfigurationError(f"{where}.columns must be a list of column names")
    if pattern is not None and (not isinstance(pattern, str) or not pattern):
        raise ConfigurationError(f"{where}.columns_matching must be a glob string")

    for key in ("name_to", "value_to"):
        value = spec.get(key)
        if not isinstance(value, str) or not value:
            raise ConfigurationError(
                f"{where}.{key} names the new column and is required"
            )
    if spec["name_to"] == spec["value_to"]:
        raise ConfigurationError(f"{where}: name_to and value_to must differ")

    keep = spec.get("keep")
    if keep is not None and (
        not isinstance(keep, list) or not all(isinstance(c, str) and c for c in keep)
    ):
        raise ConfigurationError(f"{where}.keep must be a list of column names")
    for new in (spec["name_to"], spec["value_to"]):
        if new in (columns or []) or new in (keep or []):
            raise ConfigurationError(
                f"{where}: {new!r} is both a new column and an existing one"
            )
    if "drop_empty" in spec and not isinstance(spec["drop_empty"], bool):
        raise ConfigurationError(f"{where}.drop_empty must be true or false")
    return spec


def unpivot(spec):
    """A one-to-many record map for ``DltResource.add_yield_map``."""
    listed = tuple(spec.get("columns") or ())
    pattern = spec.get("columns_matching")
    name_to, value_to = spec["name_to"], spec["value_to"]
    keep = spec.get("keep")
    drop_empty = spec.get("drop_empty", True)

    def stages_of(row):
        if listed:
            return [c for c in listed if c in row]
        return [c for c in row if fnmatch.fnmatchcase(c, pattern)]

    def reshape(row):
        stages = stages_of(row)
        base = (
            {k: v for k, v in row.items() if k not in stages}
            if keep is None
            else {k: row.get(k) for k in keep}
        )
        for column in stages:
            value = row[column]
            if drop_empty and (
                value is None or (isinstance(value, str) and not value.strip())
            ):
                continue
            yield {**base, name_to: column, value_to: value}

    return reshape


def unpivoted_columns(spec, columns):
    """Which of `columns` `spec` consumes — for checks that need to know."""
    listed = spec.get("columns")
    if listed:
        return [c for c in columns if c in listed]
    return [c for c in columns if fnmatch.fnmatchcase(c, spec["columns_matching"])]
