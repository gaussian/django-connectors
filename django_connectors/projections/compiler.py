"""Compiling a customer mapping into an evaluatable expression tree.

The mapping is customer-authored JSON. It is compiled into small Python objects
and evaluated against a row dict — it is **never** turned into SQL text, and it
never executes customer Python.

That choice removes an entire class of bug rather than defending against it.
Compiling to SQL would mean hand-rolling an identifier escaper, because dlt's
Relation surface cannot express any of what this DSL needs — no ``coalesce``,
``concat``, ``lower``, ``json_extract`` or casts — and a database ``CAST``
cannot report *which* row and field failed, which the preview requires (MySQL
yields NULL plus a session warning). Filtering in Python instead of pushing it
down costs little here: landing tables are per-Binding, so a scan is already
bounded, and no filter column carries an index anyway.

Grammar::

    node   := {"source": <column>, ["json_path": <a.b.c>], ["cast": <cast>],
               ["default": <literal>]}
            | {"constant": <literal>}
            | {"object": {<key>: node, ...}}
            | {"function": <name>, "args": [node, ...], ["cast": <cast>]}

    filter := {"field": <column>, "op": <op>, ["value": <literal>]}
"""

import json

from django_connectors.exceptions import CastError, MappingValidationError
from django_connectors.projections.fields import CASTS

FUNCTIONS = ("coalesce", "concat", "lower", "upper")

FILTER_OPERATORS = (
    "eq",
    "ne",
    "gt",
    "lt",
    "gte",
    "lte",
    "in",
    "not_in",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "is_not_null",
)

# Operators that take no `value`.
UNARY_OPERATORS = frozenset({"is_null", "is_not_null"})

#: How deeply a mapping may nest. `json.loads` happily parses far deeper than
#: the recursive compiler can walk, so without a limit a deep customer payload
#: leaves the compiler with a RecursionError — which is not a
#: MappingValidationError, so it escapes the validator and the API's error
#: handling as a 500 instead of naming the offending field.
MAX_NESTING_DEPTH = 32

MISSING = object()


class Node:
    """A compiled mapping expression."""

    def evaluate(self, row):
        raise NotImplementedError

    @property
    def source_columns(self):
        return frozenset()


class Constant(Node):
    def __init__(self, value):
        self.value = value

    def evaluate(self, row):
        return self.value


class SourceRef(Node):
    def __init__(self, column, *, json_path=(), cast=None, default=MISSING):
        self.column = column
        self.json_path = tuple(json_path)
        self.cast = cast
        self.default = default

    @property
    def source_columns(self):
        return frozenset({self.column})

    def evaluate(self, row):
        if self.column not in row:
            if self.default is not MISSING:
                return self.default
            raise MappingValidationError(
                f"source column {self.column!r} is not present in the landed row"
            )
        value = row[self.column]

        if self.json_path:
            value = _walk_json(value, self.json_path, self.column)

        if value is None and self.default is not MISSING:
            return self.default
        if self.cast is not None and value is not None:
            value = CASTS[self.cast].coerce(value, field_name=self.column)
        return value


class ObjectNode(Node):
    def __init__(self, members):
        self.members = dict(members)

    @property
    def source_columns(self):
        return (
            frozenset().union(*(node.source_columns for node in self.members.values()))
            if self.members
            else frozenset()
        )

    def evaluate(self, row):
        return {key: node.evaluate(row) for key, node in self.members.items()}


class FunctionCall(Node):
    def __init__(self, name, args, *, cast=None):
        self.name = name
        self.args = list(args)
        self.cast = cast

    @property
    def source_columns(self):
        return (
            frozenset().union(*(node.source_columns for node in self.args))
            if self.args
            else frozenset()
        )

    def evaluate(self, row):
        if self.name == "coalesce":
            value = None
            for node in self.args:
                value = node.evaluate(row)
                if value is not None:
                    break
        elif self.name == "concat":
            value = "".join(
                "" if (item := node.evaluate(row)) is None else str(item)
                for node in self.args
            )
        elif self.name in ("lower", "upper"):
            raw = self.args[0].evaluate(row)
            value = None if raw is None else getattr(str(raw), self.name)()
        else:  # pragma: no cover - compile() rejects unknown names
            raise MappingValidationError(f"unknown function {self.name!r}")

        if self.cast is not None and value is not None:
            value = CASTS[self.cast].coerce(value, field_name=self.name)
        return value


class Filter:
    def __init__(self, column, operator, value=None):
        self.column = column
        self.operator = operator
        self.value = value

    def matches(self, row):
        actual = row.get(self.column)
        if self.operator == "is_null":
            return actual is None
        if self.operator == "is_not_null":
            return actual is not None
        if actual is None:
            # SQL three-valued logic: NULL matches nothing except a null check.
            return False

        expected = self.value
        if self.operator == "eq":
            return _loose_equal(actual, expected)
        if self.operator == "ne":
            return not _loose_equal(actual, expected)
        if self.operator == "in":
            return any(_loose_equal(actual, item) for item in expected or ())
        if self.operator == "not_in":
            return not any(_loose_equal(actual, item) for item in expected or ())
        if self.operator == "contains":
            return str(expected) in str(actual)
        if self.operator == "startswith":
            return str(actual).startswith(str(expected))
        if self.operator == "endswith":
            return str(actual).endswith(str(expected))
        try:
            if self.operator == "gt":
                return actual > expected
            if self.operator == "lt":
                return actual < expected
            if self.operator == "gte":
                return actual >= expected
            if self.operator == "lte":
                return actual <= expected
        except TypeError:
            return False
        return False  # pragma: no cover


class CompiledMapping:
    """A whole mapping: one compiled node per target field, plus filters."""

    def __init__(self, nodes, filters):
        self.nodes = dict(nodes)
        self.filters = list(filters)

    @property
    def source_columns(self):
        columns = set()
        for node in self.nodes.values():
            columns |= node.source_columns
        for item in self.filters:
            columns.add(item.column)
        return frozenset(columns)

    def matches(self, row):
        return all(item.matches(row) for item in self.filters)

    def apply(self, row):
        """Evaluate every field. Raises CastError naming the field that failed."""
        values = {}
        for name, node in self.nodes.items():
            try:
                values[name] = node.evaluate(row)
            except CastError as exc:
                raise CastError(f"{name}: {exc}") from exc
        return values


def compile_mapping(mapping, filters=None):
    """Compile customer JSON into a :class:`CompiledMapping`.

    Raises :class:`MappingValidationError` naming the offending field. Only
    structure is checked here; whether the columns exist and the types line up
    is the validator's job, because that needs the landing schema.
    """
    if not isinstance(mapping, dict) or not mapping:
        raise MappingValidationError("mapping must be a non-empty object")

    nodes = {}
    for field_name, spec in mapping.items():
        if not isinstance(field_name, str) or not field_name:
            raise MappingValidationError(f"invalid target field name {field_name!r}")
        try:
            nodes[field_name] = _compile_node(spec)
        except MappingValidationError as exc:
            raise MappingValidationError(f"{field_name}: {exc}") from exc

    return CompiledMapping(nodes, compile_filters(filters))


def _compile_node(spec, depth=0):
    if depth > MAX_NESTING_DEPTH:
        raise MappingValidationError(
            f"mapping nests deeper than {MAX_NESTING_DEPTH} levels"
        )
    if not isinstance(spec, dict):
        raise MappingValidationError(
            'each mapping entry must be an object, e.g. {"source": "column"}'
        )

    forms = [key for key in ("source", "constant", "object", "function") if key in spec]
    if len(forms) != 1:
        raise MappingValidationError(
            f"exactly one of source/constant/object/function is required, got "
            f"{forms or 'none'}"
        )
    form = forms[0]

    cast = spec.get("cast")
    # The type check is not redundant: `cast not in CASTS` hashes `cast`, so a
    # list or dict arriving through the API's bare JSONField raises TypeError
    # rather than MappingValidationError, and a TypeError escapes both the
    # validator and the view as a 500.
    if cast is not None and (not isinstance(cast, str) or cast not in CASTS):
        raise MappingValidationError(
            f"unknown cast {cast!r}; available: {', '.join(sorted(CASTS))}"
        )

    if form == "constant":
        value = spec["constant"]
        _assert_json_literal(value)
        return Constant(value)

    if form == "source":
        column = spec["source"]
        if not isinstance(column, str) or not column:
            raise MappingValidationError(
                f"source must be a column name, got {column!r}"
            )
        json_path = spec.get("json_path")
        path = ()
        if json_path is not None:
            if not isinstance(json_path, str) or not json_path:
                raise MappingValidationError("json_path must be a dotted string")
            path = tuple(part for part in json_path.split(".") if part)
        default = spec.get("default", MISSING)
        if default is not MISSING:
            _assert_json_literal(default)
        return SourceRef(column, json_path=path, cast=cast, default=default)

    if form == "object":
        members = spec["object"]
        if not isinstance(members, dict) or not members:
            raise MappingValidationError("object must be a non-empty mapping")
        return ObjectNode(
            {
                str(key): _compile_node(value, depth + 1)
                for key, value in members.items()
            }
        )

    name = spec["function"]
    if name not in FUNCTIONS:
        raise MappingValidationError(
            f"unknown function {name!r}; available: {', '.join(FUNCTIONS)}"
        )
    args = spec.get("args")
    if not isinstance(args, list) or not args:
        raise MappingValidationError(f"function {name!r} needs a non-empty args list")
    if name in ("lower", "upper") and len(args) != 1:
        raise MappingValidationError(f"function {name!r} takes exactly one argument")
    return FunctionCall(
        name, [_compile_node(arg, depth + 1) for arg in args], cast=cast
    )


def compile_filters(filters):
    if not filters:
        return []
    if not isinstance(filters, list):
        raise MappingValidationError("filters must be a list")

    compiled = []
    for index, spec in enumerate(filters):
        if not isinstance(spec, dict):
            raise MappingValidationError(f"filter {index}: must be an object")
        column = spec.get("field")
        operator = spec.get("op")
        if not isinstance(column, str) or not column:
            raise MappingValidationError(f"filter {index}: 'field' is required")
        if operator not in FILTER_OPERATORS:
            raise MappingValidationError(
                f"filter {index}: unknown op {operator!r}; available: "
                f"{', '.join(FILTER_OPERATORS)}"
            )
        if operator in UNARY_OPERATORS:
            compiled.append(Filter(column, operator))
            continue
        if "value" not in spec:
            raise MappingValidationError(f"filter {index}: 'value' is required")
        value = spec["value"]
        if operator in ("in", "not_in"):
            if not isinstance(value, list):
                raise MappingValidationError(
                    f"filter {index}: {operator} needs a list value"
                )
            for item in value:
                _assert_json_literal(item)
        else:
            _assert_json_literal(value)
        compiled.append(Filter(column, operator, value))
    return compiled


def _assert_json_literal(value, depth=0):
    """Reject anything that is not plain JSON data.

    The mapping arrives as JSON, so this should be unreachable — but the field
    is also writable from Python by host code, and a callable or model instance
    slipping into a "constant" would be evaluated against every row.
    """
    if depth > MAX_NESTING_DEPTH:
        raise MappingValidationError(
            f"literal nests deeper than {MAX_NESTING_DEPTH} levels"
        )
    if isinstance(value, str | int | float | bool | type(None)):
        return
    if isinstance(value, list):
        for item in value:
            _assert_json_literal(item, depth + 1)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise MappingValidationError("object keys must be strings")
            _assert_json_literal(item, depth + 1)
        return
    raise MappingValidationError(
        f"{type(value).__name__} is not a JSON literal; mappings may contain "
        f"only strings, numbers, booleans, null, lists and objects"
    )


def _walk_json(value, path, column):
    """Follow a dotted path into a landed JSON column."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    for part in path:
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list):
            try:
                value = value[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if value is None:
            return None
    return value


def _loose_equal(actual, expected):
    """Compare tolerantly across the type drift landing introduces.

    MySQL has no native boolean, so a landed flag reads back as 0/1 while the
    customer's filter says ``true``; comparing strictly would silently match
    nothing.
    """
    if actual == expected:
        return True
    if isinstance(expected, bool) or isinstance(actual, bool):
        return bool(actual) == bool(expected)
    if isinstance(actual, int | float) and isinstance(expected, str):
        try:
            return float(actual) == float(expected)
        except ValueError:
            return False
    if isinstance(expected, int | float) and isinstance(actual, str):
        try:
            return float(actual) == float(expected)
        except ValueError:
            return False
    return False
