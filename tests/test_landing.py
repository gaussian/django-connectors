"""Landing-layer invariants.

Each test here corresponds to a behaviour measured against dlt 1.30 that loses
or corrupts data silently — no exception, no failed job, no warning that a
caller could act on.
"""

import ast
import pathlib

import pytest

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.exceptions import LandingSchemaError, SourceError
from django_connectors.landing import access, naming
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    RUN_ID_COLUMN,
)
from django_connectors.services import runs as run_services
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db


# --- naming ----------------------------------------------------------------


def test_landing_table_name_is_stable_under_dlt_normalization(make_binding):
    binding = make_binding()
    name = naming.landing_table_name("memory", "events", binding.landing_key)
    assert name == f"memory_events_{binding.landing_key}"
    # dlt collapses `__` to `_`, so a name built with double underscores would
    # not survive. Idempotence is the property that matters.
    assert naming.normalize_identifier(name) == name
    assert "__" not in name


def test_two_bindings_get_different_landing_tables(make_binding):
    """Sharing one table between Bindings loses loads silently."""
    first, second = make_binding(), make_binding()
    assert naming.landing_table_name(
        "memory", "events", first.landing_key
    ) != naming.landing_table_name("memory", "events", second.landing_key)


def test_overlong_identifier_is_rejected_rather_than_silently_hashed():
    """Past 64 chars dlt injects a hash mid-string; the name becomes unguessable."""
    with pytest.raises(LandingSchemaError, match="64"):
        naming.landing_table_name("memory", "e" * 60, "b0a1b2c3d4e")


def test_metadata_column_names_survive_normalization():
    """`_dlt` is the only reserved prefix; `_connector_` passes through."""
    for column in naming.METADATA_COLUMNS:
        assert naming.normalize_identifier(column) == column


# --- instrumentation -------------------------------------------------------


def test_metadata_injector_takes_exactly_one_parameter():
    """Any other count makes dlt call it as f(item, meta) and null the tenant."""
    import inspect

    from django_connectors.landing.instrument import make_metadata_injector

    injector = make_metadata_injector("binding-1", "run-1")
    assert len(inspect.signature(injector).parameters) == 1

    row = injector({"id": 1})
    assert row[BINDING_ID_COLUMN] == "binding-1"
    assert row[RUN_ID_COLUMN] == "run-1"
    assert row[DELETED_COLUMN] is False


def test_injector_preserves_a_source_supplied_tombstone_flag():
    from django_connectors.landing.instrument import make_metadata_injector

    injector = make_metadata_injector("b", "r")
    assert injector({"id": 1, DELETED_COLUMN: True})[DELETED_COLUMN] is True


def test_injector_rejects_non_dict_records():
    """Arrow batches cannot be annotated; the message must say what to do."""
    from django_connectors.landing.instrument import make_metadata_injector

    with pytest.raises(SourceError, match="backend='sqlalchemy'"):
        make_metadata_injector("b", "r")(["not", "a", "dict"])


def test_merge_resource_without_a_primary_key_is_refused(
    connectors_settings, make_binding
):
    binding = make_binding(
        config={"resources": {"events": {"batches": [[{"id": "1"}]]}}}
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.MANUAL)
    assert run.status == RunStatus.FAILED
    assert "primary_key" in run.error_message


def test_hard_delete_hint_is_rejected(connectors_settings, make_binding):
    """dlt's hard_delete physically removes the row, hiding the deletion."""
    from django_connectors.landing.instrument import _assert_no_hard_delete

    class FakeResource:
        name = "events"

        def __init__(self):
            self._hints = {"columns": {DELETED_COLUMN: {"hard_delete": True}}}

    with pytest.raises(SourceError, match="hard_delete"):
        _assert_no_hard_delete(FakeResource())


# --- end-to-end ingestion --------------------------------------------------


def test_first_run_lands_records_with_tenant_metadata(
    connectors_settings, make_binding
):
    binding = make_binding(
        config=memory_config(batches=[[{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]])
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert run.dlt_load_ids

    rows = access.sample_rows(binding, "events", limit=10)
    assert len(rows) == 2
    assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}
    assert {row[RUN_ID_COLUMN] for row in rows} == {str(run.id)}
    # MySQL has no native boolean, so this reads back as 0/1 there too.
    assert all(not row[DELETED_COLUMN] for row in rows)


def test_record_updated_at_the_exact_cursor_boundary_is_not_dropped(
    connectors_settings, make_binding
):
    """dlt's default dedup key silently drops this record.

    Verified: with the dedup key left at the resource's primary key, run 2
    emitted an updated row and the table still held the old value.
    """
    timestamp = "2024-01-01T00:00:00Z"
    binding = make_binding(
        config=memory_config(
            cursor="updated_at",
            batches=[
                [{"id": "1", "updated_at": timestamp, "v": "original"}],
                [{"id": "1", "updated_at": timestamp, "v": "UPDATED"}],
            ],
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "events", limit=10)
    assert [row["v"] for row in rows] == ["UPDATED"]


def test_identity_only_tombstone_lands_and_nulls_other_columns(
    connectors_settings, make_binding
):
    """delete-insert replaces the whole row, so a tombstone nulls everything else.

    This is exactly why Projection validation refuses to source a target
    identity field from outside the merge key.
    """
    from django_connectors.sources.memory import tombstone

    binding = make_binding(
        config=memory_config(
            batches=[
                [{"id": "1", "v": "a", "extra": "keep"}],
                [tombstone({"id": "1"})],
            ]
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    rows = access.sample_rows(binding, "events", limit=10)
    assert len(rows) == 1
    assert rows[0][DELETED_COLUMN]
    assert rows[0]["v"] is None
    assert rows[0]["extra"] is None


def test_two_bindings_with_identical_remote_ids_do_not_collide(
    connectors_settings, make_binding
):
    """Keyed on the remote id alone, one Binding deleted 10 of the other's 50 rows."""
    records = [[{"id": "shared-1", "v": "first"}]]
    first = make_binding(config=memory_config(batches=records), owner_id="1")
    second = make_binding(
        config=memory_config(batches=[[{"id": "shared-1", "v": "second"}]]),
        owner_id="2",
    )

    run_services.run_binding(first, trigger=RunTrigger.INITIAL)
    run_services.run_binding(second, trigger=RunTrigger.INITIAL)

    first_rows = access.sample_rows(first, "events", limit=10)
    second_rows = access.sample_rows(second, "events", limit=10)
    assert [row["v"] for row in first_rows] == ["first"]
    assert [row["v"] for row in second_rows] == ["second"]


def test_nested_structures_land_as_json_not_child_tables(
    connectors_settings, make_binding
):
    """Child tables carry no tenant scope and are structurally unprojectable."""
    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1", "tags": ["a", "b"], "meta": {"k": "v"}}]]
        )
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    dataset = access.binding_dataset(binding)
    tables = [name for name in dataset.schema.tables if not name.startswith("_dlt")]
    assert len(tables) == 1, f"nested child tables were created: {tables}"

    rows = access.sample_rows(binding, "events", limit=10)
    assert rows[0]["tags"] is not None
    assert rows[0]["meta"] is not None


def test_run_metrics_are_json_serializable_and_path_free(
    connectors_settings, make_binding, tmp_path
):
    """dlt's own trace is not JSON-serializable and embeds worker paths."""
    import json

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    serialized = json.dumps(run.metrics)
    assert str(tmp_path) not in serialized
    assert "/home/" not in serialized


def test_a_source_failure_fails_the_run_and_records_the_binding_error(
    connectors_settings, make_binding
):
    binding = make_binding(
        config=memory_config(batches=[[{"id": "1"}]], fail_on_batch=1)
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.FAILED
    assert run.finished_at is not None
    binding.refresh_from_db()
    assert binding.last_failure_at is not None
    assert binding.last_error


def test_load_ids_are_recorded_and_unique_per_run(connectors_settings, make_binding):
    """The incremental projection window keys on these, not on the run id."""
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}], [{"id": "2"}]]))
    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    assert first.dlt_load_ids and second.dlt_load_ids
    assert not set(first.dlt_load_ids) & set(second.dlt_load_ids)


def test_relation_can_be_scoped_to_one_runs_load_ids(connectors_settings, make_binding):
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}], [{"id": "2"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    relation = access.binding_relation(binding, "events", load_ids=second.dlt_load_ids)
    rows = list(access.iter_rows(relation))
    assert [row["id"] for row in rows] == ["2"]


def test_an_empty_load_id_scope_selects_nothing(connectors_settings, make_binding):
    """An empty scope means "nothing landed", never "everything"."""
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    relation = access.binding_relation(binding, "events", load_ids=[])
    assert list(access.iter_rows(relation)) == []


def test_unknown_resource_is_refused_before_touching_the_dataset(
    connectors_settings, make_binding
):
    """`resource` is customer-writable, so it must never index the dataset."""
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    with pytest.raises(LandingSchemaError):
        access.binding_relation(binding, "_dlt_loads")


# --- locking ---------------------------------------------------------------


def test_a_second_worker_records_a_skipped_run_rather_than_running(
    connectors_settings, make_binding
):
    from django_connectors import locks

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    token = locks.acquire(binding)
    assert token is not None

    run = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert run.status == RunStatus.SKIPPED
    assert "lease" in run.note

    locks.release(binding, token)


def test_a_lease_cannot_be_released_with_the_wrong_token(
    connectors_settings, make_binding
):
    """A worker whose lease expired must not release the new holder's lock."""
    import uuid

    from django_connectors import locks

    binding = make_binding()
    token = locks.acquire(binding)
    assert locks.release(binding, uuid.uuid4()) is False
    assert locks.is_locked(binding) is True
    assert locks.release(binding, token) is True


def test_an_expired_lease_can_be_taken_over(connectors_settings, make_binding):
    import datetime as dt

    from django_connectors import locks

    binding = make_binding()
    first = locks.acquire(binding, timeout=dt.timedelta(seconds=-1))
    assert first is not None
    second = locks.acquire(binding)
    assert second is not None and second != first


def test_reap_stale_runs_fails_orphaned_runs(connectors_settings, make_binding):
    import datetime as dt

    from django_connectors import locks
    from django_connectors.models import Run

    binding = make_binding()
    locks.acquire(binding, timeout=dt.timedelta(seconds=-1))
    run = Run.objects.create(
        binding=binding, trigger=RunTrigger.SCHEDULED, status=RunStatus.RUNNING
    )

    result = run_services.reap_stale_runs()
    run.refresh_from_db()
    assert result["leases_reaped"] == 1
    assert run.status == RunStatus.FAILED
    assert run.error_type == "LeaseExpired"


# --- queue coalescing ------------------------------------------------------


def test_repeated_enqueues_collapse_to_one_queued_run(
    connectors_settings, make_binding
):
    from django_connectors.models import Run

    binding = make_binding()
    created = [
        run_services.enqueue_run(binding, trigger=RunTrigger.WEBHOOK) for _ in range(10)
    ]
    assert len({run.pk for run in created}) == 1
    assert Run.objects.filter(binding=binding, status=RunStatus.QUEUED).count() == 1


# --- structural guards -----------------------------------------------------


def test_no_module_constructs_sql_outside_the_access_chokepoint():
    """Five features need tenant-scoped landing reads; one place implements it."""
    package_root = pathlib.Path(__file__).resolve().parent.parent / "django_connectors"
    allowed = {"landing/access.py"}
    forbidden = ("sql_client", "execute_sql", "drop_tables")

    offenders = []
    for path in sorted(package_root.rglob("*.py")):
        relative = path.relative_to(package_root).as_posix()
        if relative in allowed:
            continue
        source = path.read_text()
        for marker in forbidden:
            if marker in source:
                offenders.append(f"{relative}: {marker}")
    assert not offenders, (
        f"landing SQL access outside landing/access.py: {offenders}. Every "
        f"caller that builds its own query is another chance to forget the "
        f"tenant filter."
    )


def test_no_module_scope_dlt_import_in_the_landing_layer():
    """Enforced package-wide, but the landing layer is where it is tempting."""
    package_root = pathlib.Path(__file__).resolve().parent.parent / "django_connectors"
    for path in sorted((package_root / "landing").rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in tree.body:
            if isinstance(node, ast.Import):
                assert not any(a.name.split(".")[0] == "dlt" for a in node.names), path
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] != "dlt", path
