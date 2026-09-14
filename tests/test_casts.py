"""Cast options: parsing values the way people write them.

A landed timestamp column reads back as ISO 8601 and needs no help. A
spreadsheet's date column does not: Google Sheets returns the sheet's locale
rendering (``3/4/2024``), a CSV export carries whatever the author typed, and
``datetime.fromisoformat`` rejects all of it. Numbers arrive as ``"£1,234.56"``
or ``"1.234,56"``. The options here let a mapping state the format instead of
the library guessing — guessing between day-first and month-first silently
corrupts a third of a year's rows, which is worse than failing.
"""

import datetime as dt
import decimal
import zoneinfo

import pytest

from django_connectors.exceptions import CastError, MappingValidationError
from django_connectors.projections.compiler import compile_mapping
from django_connectors.projections.fields import CAST_TYPES, make_cast
from tests.conftest import land_memory, make_projection

pytestmark = pytest.mark.django_db


def _cast(spec, value, column="c"):
    """Evaluate one node spec against one value."""
    return compile_mapping({"f": {"source": column, **spec}}).apply({column: value})[
        "f"
    ]


# --- datetime and date -----------------------------------------------------


def test_datetime_format_parses_a_day_first_date():
    value = _cast({"cast": "datetime", "format": "%d/%m/%Y"}, "03/04/2024")
    assert value == dt.datetime(2024, 4, 3, tzinfo=dt.UTC)


def test_the_same_text_fails_without_a_format():
    with pytest.raises(CastError, match="c"):
        _cast({"cast": "datetime"}, "03/04/2024")


def test_datetime_timezone_localises_a_naive_parsed_value():
    value = _cast(
        {"cast": "datetime", "format": "%d/%m/%Y %H:%M", "timezone": "Europe/London"},
        "03/07/2024 09:30",
    )
    assert value.tzinfo == zoneinfo.ZoneInfo("Europe/London")
    assert value.astimezone(dt.UTC) == dt.datetime(2024, 7, 3, 8, 30, tzinfo=dt.UTC)


def test_timezone_leaves_an_aware_value_alone():
    value = _cast(
        {"cast": "datetime", "timezone": "Europe/London"}, "2024-07-03T09:30:00+02:00"
    )
    assert value.utcoffset() == dt.timedelta(hours=2)


def test_iso_input_still_works_when_a_format_is_not_given():
    assert _cast({"cast": "datetime"}, "2024-03-01T10:00:00Z") == dt.datetime(
        2024, 3, 1, 10, tzinfo=dt.UTC
    )


def test_date_format():
    assert _cast({"cast": "date", "format": "%m/%d/%Y"}, "03/04/2024") == dt.date(
        2024, 3, 4
    )


# --- numbers ---------------------------------------------------------------


def test_decimal_strip_drops_currency_and_thousands_separators():
    assert _cast({"cast": "decimal", "strip": "£,"}, "£1,234.56") == decimal.Decimal(
        "1234.56"
    )


def test_decimal_comma_as_decimal_separator():
    value = _cast(
        {"cast": "decimal", "strip": ".", "decimal_separator": ","}, "1.234,56"
    )
    assert value == decimal.Decimal("1234.56")


def test_integer_strip():
    assert _cast({"cast": "integer", "strip": ", "}, "12 500") == 12500


def test_float_strip():
    assert _cast({"cast": "float", "strip": "$"}, "$3.5") == 3.5


def test_numeric_options_leave_real_numbers_alone():
    assert _cast({"cast": "decimal", "strip": "1"}, 1234) == decimal.Decimal("1234")
    assert _cast({"cast": "float", "strip": "$"}, 1.5) == 1.5


# --- compile-time validation ----------------------------------------------


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"cast": "datetime", "format": "%Q"}, "not a valid strptime pattern"),
        ({"cast": "datetime", "format": ""}, "'format'"),
        ({"cast": "datetime", "timezone": "Mars/Olympus"}, "not a known IANA zone"),
        ({"cast": "integer", "format": "%d"}, "does not take option"),
        ({"cast": "string", "strip": ","}, "does not take option"),
        ({"cast": "decimal", "decimal_separator": ";"}, "'decimal_separator'"),
        ({"cast": "decimal", "strip": 5}, "'strip'"),
        ({"format": "%d/%m/%Y"}, "need a 'cast'"),
        ({"cast": ["datetime"]}, "unknown cast"),
        ({"cast": "nope"}, "unknown cast"),
    ],
)
def test_bad_cast_options_are_mapping_errors_naming_the_field(spec, message):
    with pytest.raises(MappingValidationError, match=f"f: .*{message}"):
        compile_mapping({"f": {"source": "c", **spec}})


def test_function_nodes_take_cast_options_too():
    compiled = compile_mapping(
        {
            "f": {
                "function": "concat",
                "args": [{"source": "d"}, {"constant": "/2024"}],
                "cast": "date",
                "format": "%d/%m/%Y",
            }
        }
    )
    assert compiled.apply({"d": "03/04"})["f"] == dt.date(2024, 4, 3)


def test_every_cast_type_declares_its_options():
    for name, cls in CAST_TYPES.items():
        assert isinstance(cls.cast_options, frozenset), name
        make_cast(name)  # the plain form always constructs


# --- end to end ------------------------------------------------------------


def test_a_locale_date_column_projects_with_a_format(
    connectors_settings,
    make_binding,
    events_target,
    writer,
):
    binding, run = land_memory(
        make_binding, [[{"id": "e1", "happened_at": "03/04/2024 09:30", "kind": "k"}]]
    )
    projection = make_projection(
        binding,
        {
            "external_id": {"source": "id"},
            "occurred_at": {
                "source": "happened_at",
                "cast": "datetime",
                "format": "%d/%m/%Y %H:%M",
                "timezone": "Europe/London",
            },
            "type": {"source": "kind"},
        },
    )
    from django_connectors.services import projections as projection_services

    projection_run = projection_services.run_projection(projection, source_run=run)
    assert projection_run.status == "succeeded", projection_run.error_message
    written = writer.records[0].values["occurred_at"]
    assert written.astimezone(dt.UTC) == dt.datetime(2024, 4, 3, 8, 30, tzinfo=dt.UTC)


def test_colon_z_is_refused_because_strptime_rejects_it():
    with pytest.raises(MappingValidationError, match="%:"):
        compile_mapping({"f": {"source": "c", "cast": "datetime", "format": "%:z"}})


def test_timezone_never_relabels_a_landed_datetime():
    """A native datetime is a landed timestamp column: dlt already made it UTC."""
    landed = dt.datetime(2024, 7, 3, 8, 0)  # naive, UTC by construction
    value = _cast({"cast": "datetime", "timezone": "Europe/London"}, landed)
    assert value == dt.datetime(2024, 7, 3, 8, 0, tzinfo=dt.UTC)


def test_strip_may_not_remove_the_decimal_separator():
    with pytest.raises(MappingValidationError, match="silently become"):
        compile_mapping(
            {
                "f": {
                    "source": "c",
                    "cast": "decimal",
                    "strip": ",",
                    "decimal_separator": ",",
                }
            }
        )
