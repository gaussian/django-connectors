"""Landing index provisioning.

dlt creates no index on a landing table and its merge is ``DELETE ... WHERE
EXISTS``, so an unindexed merge key makes every load scan the whole table —
measured at 179 seconds for 10,000 rows into a 60,000-row table. These tests
cover *that the indexes are asked for*, that asking twice is harmless, and that
failing to get them degrades a Binding rather than failing its Run.

What sqlite cannot show lives in ``tests/test_landing_server.py``: MySQL's
refusal to index a ``TEXT`` column without a prefix length (error 1170), its
lack of ``CREATE INDEX IF NOT EXISTS``, and whether the indexes survive dlt's
``ALTER TABLE`` schema evolution. Those are the reasons this feature is
dialect-aware at all, and none of them is expressible here.
"""

import pytest

from django_connectors.enums import BindingStatus, RunStatus, RunTrigger
from django_connectors.landing import index as landing_index
from django_connectors.landing.destination import build_pipeline
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DLT_LOAD_ID_COLUMN,
    MAX_IDENTIFIER_LENGTH,
)
from django_connectors.services import runs as run_services
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db


def landing_indexes(binding, table):
    """``{index name: [column names]}`` as the database actually holds them."""
    from sqlalchemy import inspect as sqlalchemy_inspect

    pipeline = build_pipeline(binding)
    with (
        pipeline.sql_client(schema_name=binding.schema_name) as client,
        client.begin_transaction(),
    ):
        inspector = sqlalchemy_inspect(client.native_connection)
        return {
            found["name"]: list(found["column_names"])
            for found in inspector.get_indexes(table, schema=client.dataset_name)
        }


def table_of(binding, resource="events"):
    from django_connectors.landing.naming import landing_table_name

    return landing_table_name(binding.source, resource, binding.landing_key)


# --- what gets created ------------------------------------------------------


def test_a_successful_run_provisions_the_merge_and_load_indexes(
    connectors_settings, make_binding
):
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}, {"id": "2"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    indexes = landing_indexes(binding, table_of(binding))
    assert len(indexes) == 2, indexes
    assert [BINDING_ID_COLUMN, "id"] in indexes.values()
    assert [DLT_LOAD_ID_COLUMN, BINDING_ID_COLUMN] in indexes.values()


def test_the_load_index_leads_with_the_load_id_not_the_binding_id():
    """The column the projection window actually filters on has to come first.

    ``access.binding_relation`` emits no tenant WHERE clause — PostgreSQL
    rejects the ``CAST(x AS TEXT(36))`` dlt renders against a precision-hinted
    column — so the only predicate on the incremental read is ``_dlt_load_id IN
    (...)``. An index led by ``_connector_binding_id`` could not serve it, and
    would look provisioned while the read still scanned.
    """
    specs = landing_index.index_specs(
        "memory_events_b0000000001",
        {
            "columns": {
                BINDING_ID_COLUMN: {"data_type": "text", "primary_key": True},
                "id": {"data_type": "text", "primary_key": True},
                DLT_LOAD_ID_COLUMN: {"data_type": "text"},
            }
        },
    )
    by_columns = {spec.columns: spec.name for spec in specs}
    assert (BINDING_ID_COLUMN, "id") in by_columns
    assert (DLT_LOAD_ID_COLUMN, BINDING_ID_COLUMN) in by_columns


def test_a_composite_merge_key_is_indexed_in_declared_order():
    specs = landing_index.index_specs(
        "memory_events_b0000000001",
        {
            "columns": {
                BINDING_ID_COLUMN: {"data_type": "text", "primary_key": True},
                "tenant": {"data_type": "text", "primary_key": True},
                "id": {"data_type": "text", "primary_key": True},
                DLT_LOAD_ID_COLUMN: {"data_type": "text"},
            }
        },
    )
    assert specs[0].columns == (BINDING_ID_COLUMN, "tenant", "id")


def test_an_append_only_resource_gets_no_merge_index(connectors_settings, make_binding):
    """There is no merge predicate to serve, so a merge index would be dead weight."""
    binding = make_binding(
        config={
            "resources": {
                "events": {
                    "write_disposition": "append",
                    "batches": [[{"id": "1"}]],
                }
            }
        }
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    indexes = landing_indexes(binding, table_of(binding))
    assert list(indexes.values()) == [[DLT_LOAD_ID_COLUMN, BINDING_ID_COLUMN]]


def test_every_selected_resource_is_indexed(connectors_settings, make_binding):
    binding = make_binding(
        config={
            "resources": {
                "events": {"primary_key": "id", "batches": [[{"id": "1"}]]},
                "people": {"primary_key": "id", "batches": [[{"id": "p1"}]]},
            }
        }
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    for resource in ("events", "people"):
        assert len(landing_indexes(binding, table_of(binding, resource))) == 2


# --- idempotency ------------------------------------------------------------


def test_provisioning_twice_creates_nothing_and_fails_nothing(
    connectors_settings, make_binding
):
    """MySQL has no ``CREATE INDEX IF NOT EXISTS``, so this cannot be assumed."""
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}], [{"id": "2"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    second = landing_index.ensure_landing_indexes(binding)
    assert second["created"] == []
    assert second["failed"] == []
    assert len(second["existing"]) == 2

    third = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert third.status == RunStatus.SUCCEEDED, third.error_message
    assert len(landing_indexes(binding, table_of(binding))) == 2


def test_a_binding_that_landed_before_this_existed_can_be_provisioned_later(
    connectors_settings, make_binding, settings
):
    """The operator escape hatch: an existing Binding, indexed after the fact."""
    settings.DJANGO_CONNECTORS = {
        **settings.DJANGO_CONNECTORS,
        "PROVISION_LANDING_INDEXES": False,
    }
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert landing_indexes(binding, table_of(binding)) == {}

    settings.DJANGO_CONNECTORS = {
        **settings.DJANGO_CONNECTORS,
        "PROVISION_LANDING_INDEXES": True,
    }
    report = landing_index.ensure_landing_indexes(binding)
    assert len(report["created"]) == 2
    assert len(landing_indexes(binding, table_of(binding))) == 2


# --- naming -----------------------------------------------------------------


def test_an_index_name_never_exceeds_the_identifier_limit():
    """PostgreSQL truncates at 63 and index names are unique per *schema* there.

    Two landing tables differing only past the cut would otherwise collide on
    one index name.
    """
    longest = "x" * MAX_IDENTIFIER_LENGTH
    name = landing_index.index_name(longest, landing_index.MERGE_INDEX_SUFFIX)
    assert len(name) <= MAX_IDENTIFIER_LENGTH
    assert name.endswith(landing_index.MERGE_INDEX_SUFFIX)


def test_shortened_index_names_stay_distinct():
    first = landing_index.index_name(
        "a" * (MAX_IDENTIFIER_LENGTH - 1) + "b", landing_index.MERGE_INDEX_SUFFIX
    )
    second = landing_index.index_name(
        "a" * (MAX_IDENTIFIER_LENGTH - 1) + "c", landing_index.MERGE_INDEX_SUFFIX
    )
    assert first != second


def test_a_short_table_name_keeps_a_readable_index_name():
    assert (
        landing_index.index_name("memory_events_b0000000001", "merge_idx")
        == "memory_events_b0000000001_merge_idx"
    )


# --- failure is degradation, not a failed Run -------------------------------


def test_a_failed_index_needs_review_but_the_run_still_succeeds(
    connectors_settings, make_binding, monkeypatch
):
    """The rows landed correctly. What is broken is the cost of the next load."""

    def refuse(client, reflected, spec):
        raise RuntimeError("permission denied for CREATE INDEX")

    monkeypatch.setattr(landing_index, "_create_index_sql", refuse)

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.SUCCEEDED, run.error_message
    binding.refresh_from_db()
    assert binding.status == BindingStatus.NEEDS_REVIEW
    assert "permission denied" in binding.last_error
    # needs_review must keep syncing — a Binding that stops because its index
    # is missing has turned a slow merge into no merge at all.
    assert binding.is_runnable


def test_a_needs_review_binding_still_runs_and_still_retries_the_index(
    connectors_settings, make_binding, monkeypatch
):
    calls = []
    original = landing_index._create_index_sql

    def refuse_once(client, reflected, spec):
        calls.append(spec.name)
        if len(calls) <= 2:
            raise RuntimeError("lock wait timeout exceeded")
        return original(client, reflected, spec)

    monkeypatch.setattr(landing_index, "_create_index_sql", refuse_once)

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}], [{"id": "2"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    binding.refresh_from_db()
    assert binding.status == BindingStatus.NEEDS_REVIEW

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert len(landing_indexes(binding, table_of(binding))) == 2

    # Still needs_review: the status records that a human has not looked yet,
    # and a status that healed itself is one an operator learns to ignore.
    binding.refresh_from_db()
    assert binding.status == BindingStatus.NEEDS_REVIEW


def test_an_unreachable_landing_database_does_not_fail_the_report(
    connectors_settings, make_binding, monkeypatch
):
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    def explode(self, *args, **kwargs):
        raise RuntimeError("landing database is gone")

    monkeypatch.setattr(landing_index, "_apply", explode)
    report = landing_index.ensure_landing_indexes(binding)
    assert report["failed"]
    assert "landing database is gone" in report["failed"][0]["error"]


def test_nothing_is_attempted_before_anything_has_landed(
    connectors_settings, make_binding
):
    binding = make_binding(config=memory_config(batches=[]))
    report = landing_index.ensure_landing_indexes(binding)
    assert report == {"created": [], "existing": [], "skipped": [], "failed": []}


# --- the off switch ---------------------------------------------------------


def test_provisioning_can_be_turned_off(connectors_settings, make_binding, settings):
    """A landing role with no DDL rights must not mean needs_review forever."""
    settings.DJANGO_CONNECTORS = {
        **settings.DJANGO_CONNECTORS,
        "PROVISION_LANDING_INDEXES": False,
    }
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.SUCCEEDED, run.error_message
    binding.refresh_from_db()
    assert binding.status == BindingStatus.ACTIVE
    assert landing_indexes(binding, table_of(binding)) == {}
