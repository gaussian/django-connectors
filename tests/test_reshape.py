"""Wide rows become long rows at the source, before they land.

A spreadsheet with one row per case and one column per stage is the most
common layout there is, and the projection is one row in, one record out. The
reshape happens upstream of landing — declared by the source like a cursor,
applied by the landing layer — so landing, merge and projection see ordinary
rows and the merge key includes the stage.
"""

import pytest

from django_connectors.enums import RunTrigger
from django_connectors.exceptions import ConfigurationError
from django_connectors.landing import access
from django_connectors.landing.naming import BINDING_ID_COLUMN, landing_table_name
from django_connectors.services import bindings as binding_services
from django_connectors.services import runs as run_services
from django_connectors.sources.reshape import unpivot, validate_unpivot
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db

WIDE = {
    "case": "C1",
    "owner": "ada",
    "received_at": "d1",
    "approved_at": "d2",
    "shipped_at": None,
}
SPEC = {
    "columns": ["received_at", "approved_at", "shipped_at"],
    "name_to": "stage",
    "value_to": "at",
}


# --- the map -----------------------------------------------------------------


def test_unpivot_emits_one_row_per_stage_and_keeps_the_rest():
    rows = list(unpivot(SPEC)(dict(WIDE)))
    assert rows == [
        {"case": "C1", "owner": "ada", "stage": "received_at", "at": "d1"},
        {"case": "C1", "owner": "ada", "stage": "approved_at", "at": "d2"},
    ]  # shipped_at is empty: "did not happen", not "happened at null"


def test_unpivot_keeps_empty_stages_when_asked():
    rows = list(unpivot({**SPEC, "drop_empty": False})(dict(WIDE)))
    assert [r["stage"] for r in rows] == ["received_at", "approved_at", "shipped_at"]


def test_unpivot_by_glob_and_keep():
    spec = {
        "columns_matching": "*_at",
        "name_to": "stage",
        "value_to": "at",
        "keep": ["case"],
    }
    rows = list(unpivot(spec)(dict(WIDE)))
    assert rows == [
        {"case": "C1", "stage": "received_at", "at": "d1"},
        {"case": "C1", "stage": "approved_at", "at": "d2"},
    ]


@pytest.mark.parametrize(
    "spec, message",
    [
        ({}, "needs 'columns'"),
        ({"columns": ["a"], "name_to": "s"}, "value_to"),
        ({"columns": ["a"], "name_to": "s", "value_to": "s"}, "must differ"),
        ({"columns": ["a"], "name_to": "a", "value_to": "v"}, "both a new column"),
        ({"columns": "a", "name_to": "s", "value_to": "v"}, "list of column names"),
        (
            {"columns": ["a"], "name_to": "s", "value_to": "v", "bogus": 1},
            "unknown key",
        ),
        (
            {"columns": ["a"], "name_to": "s", "value_to": "v", "drop_empty": "no"},
            "true or false",
        ),
        ("not an object", "must be an object"),
    ],
)
def test_bad_specs_are_configuration_errors(spec, message):
    with pytest.raises(ConfigurationError, match=message):
        validate_unpivot(spec)


# --- end to end through the landing layer ------------------------------------


def _wide_config(extra=None, **spec_overrides):
    return memory_config(
        batches=[[WIDE, {**WIDE, "case": "C2"}]],
        primary_key="case",
        unpivot={**SPEC, **spec_overrides},
        **(extra or {}),
    )


def _landed(binding):
    return sorted(
        (r["case"], r["stage"], r["at"])
        for r in access.iter_rows(
            access.binding_relation(binding, "events"), binding=binding
        )
    )


def test_wide_rows_land_long_with_the_stage_in_the_merge_key(
    connectors_settings, make_binding
):
    binding = make_binding(config=_wide_config())
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == "succeeded", run.error_message

    assert _landed(binding) == [
        ("C1", "approved_at", "d2"),
        ("C1", "received_at", "d1"),
        ("C2", "approved_at", "d2"),
        ("C2", "received_at", "d1"),
    ]
    table = landing_table_name("memory", "events", binding.landing_key)
    schema = access.binding_dataset(binding).schema
    merge_key = {
        name
        for name, c in schema.tables[table]["columns"].items()
        if c.get("primary_key")
    }
    assert merge_key == {BINDING_ID_COLUMN, "case", "stage"}


def test_a_second_run_updates_the_stage_rows_instead_of_stacking_them(
    connectors_settings, make_binding
):
    later = {**WIDE, "approved_at": "d2-fixed", "shipped_at": "d3"}
    binding = make_binding(
        config=memory_config(
            batches=[[WIDE], [later]], primary_key="case", unpivot=SPEC
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert _landed(binding) == [
        ("C1", "approved_at", "d2-fixed"),
        ("C1", "received_at", "d1"),
        ("C1", "shipped_at", "d3"),
    ]


def test_every_landed_row_carries_tenant_metadata_after_the_reshape(
    connectors_settings, make_binding
):
    """The injector runs after the reshape, so it stamps what actually lands."""
    binding = make_binding(config=_wide_config())
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    rows = list(
        access.iter_rows(access.binding_relation(binding, "events"), binding=binding)
    )
    assert len(rows) == 4
    assert {r[BINDING_ID_COLUMN] for r in rows} == {str(binding.id)}


def test_a_cursor_that_the_unpivot_consumes_is_refused(
    connectors_settings, make_binding
):
    binding = make_binding(
        config=memory_config(
            batches=[[WIDE]], primary_key="case", cursor="received_at", unpivot=SPEC
        )
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == "failed"
    assert "unpivoted columns" in run.error_message


def test_a_bad_unpivot_is_refused_when_the_binding_is_saved(
    connectors_settings, make_binding
):
    binding = make_binding(
        config=memory_config(
            batches=[[WIDE]], primary_key="case", unpivot={"name_to": "s"}
        )
    )
    with pytest.raises(ConfigurationError, match="unpivot"):
        binding_services.validate_binding(binding)


def test_the_projection_can_identify_by_case_and_stage(
    connectors_settings, make_binding, events_target, writer
):
    """The whole point: one target record per (case, stage), off ordinary rows."""
    from django_connectors.services import projections as projection_services
    from tests.conftest import make_projection

    binding = make_binding(config=_wide_config())
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    projection = make_projection(
        binding,
        {
            "external_id": {
                "function": "concat",
                "args": [{"source": "case"}, {"constant": ":"}, {"source": "stage"}],
            },
            "occurred_at": {"constant": "2024-01-01T00:00:00Z"},
            "type": {"source": "stage"},
        },
    )
    result = projection_services.validate_projection(projection)
    assert result.ok, result.errors
    projection_run = projection_services.run_projection(projection, source_run=run)
    assert projection_run.status == "succeeded", projection_run.error_message
    assert sorted(r.identity["external_id"] for r in writer.records) == [
        "C1:approved_at",
        "C1:received_at",
        "C2:approved_at",
        "C2:received_at",
    ]
