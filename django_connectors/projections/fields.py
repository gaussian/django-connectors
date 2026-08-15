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


class IntegerField(Field):
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
        return int(str(value).strip())


class DecimalField(Field):
    compatible_dlt_types = frozenset({"decimal", "bigint", "double", "text"})

    def _coerce(self, value):
        return decimal.Decimal(str(value).strip())


class FloatField(Field):
    compatible_dlt_types = frozenset({"double", "bigint", "decimal", "text"})

    def _coerce(self, value):
        return float(value)


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
    compatible_dlt_types = frozenset({"timestamp", "date", "text", "bigint"})

    def _coerce(self, value):
        if isinstance(value, dt.datetime):
            return _as_aware(value)
        if isinstance(value, dt.date):
            return _as_aware(dt.datetime.combine(value, dt.time.min))
        if isinstance(value, int | float):
            return dt.datetime.fromtimestamp(value, tz=dt.UTC)
        text = str(value).strip()
        # dlt stores timestamps as MySQL datetime(6) with the offset dropped
        # after conversion to UTC, so a naive value read back is UTC.
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        return _as_aware(dt.datetime.fromisoformat(text))


class DateField(Field):
    compatible_dlt_types = frozenset({"date", "timestamp", "text"})

    def _coerce(self, value):
        if isinstance(value, dt.datetime):
            return value.date()
        if isinstance(value, dt.date):
            return value
        return dt.date.fromisoformat(str(value).strip())


class JSONField(Field):
    compatible_dlt_types = frozenset({"json", "text"})

    def _coerce(self, value):
        if isinstance(value, dict | list):
            return value
        if isinstance(value, str):
            return json.loads(value)
        raise CastError(f"{type(value).__name__} is not JSON-compatible")


def _as_aware(value):
    """Attach UTC to a naive datetime.

    Landed timestamps are UTC by construction — dlt converts to UTC and drops
    the offset — so a naive value is UTC rather than local time, and guessing
    local time here would shift every event by the server's offset.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value


#: Named casts available in the mapping DSL.
CASTS = {
    "string": StringField(),
    "integer": IntegerField(),
    "decimal": DecimalField(),
    "float": FloatField(),
    "boolean": BooleanField(),
    "datetime": DateTimeField(),
    "date": DateField(),
    "json": JSONField(),
}
