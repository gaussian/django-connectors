"""Target field types.

A host declares the *shape* it accepts; these types say what each field holds
and how a landed value is coerced into it. Coercion failures are per-row and
per-field, which is why they raise :class:`CastError` carrying both — a
mapping UI has to point at the cell that failed, and a database ``CAST`` cannot
tell you that (MySQL yields NULL plus a session warning).
"""

import datetime as dt
import decimal
import json
import re
import zoneinfo

from django_connectors.exceptions import CastError

# dlt data types a mapping may read. `wei` and `binary` are deliberately absent:
# neither has a meaningful target representation in v1, and silently coercing
# them would produce plausible-looking wrong values.
MAPPABLE_DLT_TYPES = frozenset(
    {"text", "bigint", "double", "bool", "timestamp", "date", "time", "decimal", "json"}
)
UNMAPPABLE_DLT_TYPES = frozenset({"wei", "binary"})


class Field:
    """Base class for a target field."""

    #: dlt data types that coerce into this field without complaint.
    compatible_dlt_types = frozenset()
    #: Keyword options a mapping may pass when using this type as a cast, e.g.
    #: ``{"cast": "datetime", "format": "%d/%m/%Y"}``. Empty means none.
    cast_options = frozenset()

    def __init__(self, *, required=False, help_text=""):
        self.required = required
        self.help_text = help_text

    @property
    def type_name(self):
        return type(self).__name__.removesuffix("Field").lower()

    def coerce(self, value, *, field_name):
        if value is None:
            return None
        try:
            return self._coerce(value)
        except CastError:
            raise
        except Exception as exc:
            raise CastError(
                f"{field_name}: cannot read {value!r} as {self.type_name}"
            ) from exc

    def _coerce(self, value):
        raise NotImplementedError

    def describe(self):
        return {
            "type": self.type_name,
            "required": self.required,
            "help_text": self.help_text,
        }


class StringField(Field):
    compatible_dlt_types = frozenset(
        {"text", "bigint", "double", "bool", "timestamp", "date", "time", "decimal"}
    )

    def __init__(self, *, required=False, help_text="", max_length=None):
        super().__init__(required=required, help_text=help_text)
        self.max_length = max_length

    def _coerce(self, value):
        text = value if isinstance(value, str) else str(value)
        if self.max_length is not None and len(text) > self.max_length:
            raise CastError(
                f"value is {len(text)} characters, longer than the field's "
                f"limit of {self.max_length}"
            )
        return text

    def describe(self):
        return {**super().describe(), "max_length": self.max_length}


class NumberField(Field):
    """Shared text handling for the numeric types.

    Spreadsheets and CSV exports hand over numbers the way people write them:
    ``"£1,234.56"``, ``"1.234,56"``, ``"12 500"``. ``strip`` names the
    characters to drop first (currency symbols, thousands separators, spaces);
    ``decimal_separator=","`` then treats the comma as the decimal point. Both
    apply to text only — a number that already is one is left alone.
    """

    cast_options = frozenset({"strip", "decimal_separator"})

    def __init__(
        self, *, required=False, help_text="", strip="", decimal_separator="."
    ):
        super().__init__(required=required, help_text=help_text)
        if not isinstance(strip, str):
            raise ValueError("'strip' must be a string of characters to remove")
        if decimal_separator not in (".", ","):
            raise ValueError("'decimal_separator' must be '.' or ','")
        self.strip = strip
        self.decimal_separator = decimal_separator

    def _text(self, value):
        text = str(value).strip()
        if self.strip:
            text = text.translate(str.maketrans("", "", self.strip))
        if self.decimal_separator == ",":
            text = text.replace(",", ".")
        return text


class IntegerField(NumberField):
    compatible_dlt_types = frozenset({"bigint", "double", "bool", "text", "decimal"})

    def _coerce(self, value):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not value.is_integer():
                raise CastError(f"{value!r} is not a whole number")
            return int(value)
        return int(self._text(value))


class DecimalField(NumberField):
    compatible_dlt_types = frozenset({"decimal", "bigint", "double", "text"})

    def _coerce(self, value):
        if isinstance(value, int | float | decimal.Decimal) and not isinstance(
            value, bool
        ):
            return decimal.Decimal(str(value))
        return decimal.Decimal(self._text(value))


class FloatField(NumberField):
    compatible_dlt_types = frozenset({"double", "bigint", "decimal", "text"})

    def _coerce(self, value):
        if isinstance(value, int | float):
            return float(value)
        return float(self._text(value))


class BooleanField(Field):
    compatible_dlt_types = frozenset({"bool", "bigint", "text"})

    # MySQL has no native boolean, so a landed flag reads back as 0/1.
    TRUE = frozenset({"true", "t", "yes", "y", "1", "on"})
    FALSE = frozenset({"false", "f", "no", "n", "0", "off", ""})

    def _coerce(self, value):
        if isinstance(value, bool):
            return value
        if isinstance(value, int | float):
            return bool(value)
        text = str(value).strip().lower()
        if text in self.TRUE:
            return True
        if text in self.FALSE:
            return False
        raise CastError(f"{value!r} is not a boolean")


class DateTimeField(Field):
    """A timezone-aware datetime.

    Without ``format``, text must be ISO 8601 — which is what a landed
    timestamp column reads back as. With ``format`` (a ``strptime`` pattern),
    text is parsed the way the customer wrote it: ``"%d/%m/%Y"`` for a
    spreadsheet's ``03/04/2024``, because a date a person typed is almost never
    ISO and guessing between day-first and month-first silently corrupts a
    third of a year's rows. ``timezone`` (an IANA name) says which wall clock
    naive *text* is on. It never applies to a native datetime: that came from
    a landed timestamp column, which dlt already converted to UTC.
    """

    compatible_dlt_types = frozenset({"timestamp", "date", "text", "bigint"})
    cast_options = frozenset({"format", "timezone"})

    def __init__(self, *, required=False, help_text="", format=None, timezone=None):
        super().__init__(required=required, help_text=help_text)
        self.format = _validated_format(format)
        self.tzinfo = _validated_timezone(timezone)

    def _coerce(self, value):
        # A native datetime came from a landed timestamp column: dlt converted
        # it to UTC and dropped the offset, so naive means UTC — `timezone`
        # must not relabel it. Only text is the customer's own rendering.
        if isinstance(value, dt.datetime):
            return _as_aware(value, dt.UTC)
        if isinstance(value, dt.date):
            return _as_aware(dt.datetime.combine(value, dt.time.min), dt.UTC)
        if isinstance(value, int | float):
            return dt.datetime.fromtimestamp(value, tz=dt.UTC)
        text = str(value).strip()
        if self.format:
            return _as_aware(dt.datetime.strptime(text, self.format), self.tzinfo)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        return _as_aware(dt.datetime.fromisoformat(text), self.tzinfo)


class DateField(Field):
    compatible_dlt_types = frozenset({"date", "timestamp", "text"})
    cast_options = frozenset({"format"})

    def __init__(self, *, required=False, help_text="", format=None):
        super().__init__(required=required, help_text=help_text)
        self.format = _validated_format(format)

    def _coerce(self, value):
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        text = str(value).strip()
        if self.format:
            return dt.datetime.strptime(text, self.format).date()
        return dt.date.fromisoformat(text)


class JSONField(Field):
    compatible_dlt_types = frozenset({"json", "text"})

    def _coerce(self, value):
        if isinstance(value, dict | list):
            return value
        if isinstance(value, str):
            return json.loads(value)
        raise CastError(f"{type(value).__name__} is not JSON-compatible")


#: Directives ``strptime`` understands. Checked up front because neither
#: ``strftime`` (glibc passes an unknown ``%Q`` straight through) nor
#: ``strptime`` (which only fails on the first row) reports a bad pattern at
#: the time the mapping is written.
STRPTIME_DIRECTIVES = frozenset("aAbBcdfGHIjmMpSuUVwWxXyYzZ%")
_DIRECTIVE_RE = re.compile(r"%(.|$)")


def _as_aware(value, tzinfo):
    """Attach `tzinfo` (or UTC) to a naive datetime; leave an aware one alone."""
    if value.tzinfo is None:
        return value.replace(tzinfo=tzinfo or dt.UTC)
    return value


def _validated_format(pattern):
    """A ``strptime`` pattern, checked now rather than on the first row."""
    if pattern is None:
        return None
    if not isinstance(pattern, str) or not pattern:
        raise ValueError("'format' must be a non-empty strptime pattern")
    for directive in _DIRECTIVE_RE.findall(pattern):
        if directive not in STRPTIME_DIRECTIVES:
            raise ValueError(
                f"'format' {pattern!r} is not a valid strptime pattern: "
                f"%{directive} is not a directive"
            )
    return pattern


def _validated_timezone(name):
    if name is None:
        return None
    if not isinstance(name, str) or not name:
        raise ValueError("'timezone' must be an IANA name such as 'Europe/London'")
    try:
        return zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"'timezone' {name!r} is not a known IANA zone") from exc


#: Cast names available in the mapping DSL, and the field type each builds.
CAST_TYPES = {
    "string": StringField,
    "integer": IntegerField,
    "decimal": DecimalField,
    "float": FloatField,
    "boolean": BooleanField,
    "datetime": DateTimeField,
    "date": DateField,
    "json": JSONField,
}

#: The plain casts, for callers that need no options.
CASTS = {name: cls() for name, cls in CAST_TYPES.items()}


def make_cast(name, options=None):
    """A field instance for ``{"cast": name, **options}``.

    Raises ``ValueError`` — the compiler turns it into a
    ``MappingValidationError`` naming the field — for an unknown cast, an
    option the cast does not take, or an option value the type rejects.
    """
    cls = CAST_TYPES.get(name) if isinstance(name, str) else None
    if cls is None:
        raise ValueError(
            f"unknown cast {name!r}; available: {', '.join(sorted(CAST_TYPES))}"
        )
    options = dict(options or {})
    unknown = sorted(set(options) - cls.cast_options)
    if unknown:
        accepted = ", ".join(sorted(cls.cast_options)) or "none"
        raise ValueError(
            f"cast {name!r} does not take option(s) {unknown}; accepted: {accepted}"
        )
    return cls(**options)
