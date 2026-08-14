"""Schema discovery, drift detection, purge and reset."""

import pytest

from django_connectors.enums import BindingStatus, ProjectionStatus, RunTrigger
from django_connectors.landing import schema as landing_schema
from django_connectors.models import Projection
from django_connectors.services import discovery, retention
from django_connectors.services import runs as run_services
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db

RECORD = {"id": "e1", "amount": 10, "kind": "created"}


def _land(make_binding, batches, **kwargs):
    binding = make_binding(config=memory_config(batches=batches, **kwargs))
    binding.resources = ["events"]
    binding.save(update_fields=["resources"])
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == "succeeded", run.error_message
    return binding, run


# --- schema discovery ------------------------------------------------------


def test_schema_is_pending_before_the_first_run_and_never_raises(
    connectors_settings, make_binding
):
    """A Binding that has not run yet is an ordinary setup state, not an error."""
    binding = make_binding(config=memory_config(batches=[[RECORD]]))
    result = discovery.get_landing_schema(binding)
    assert result["status"] == "pending_first_run"
    assert result["resources"] == {}


def test_schema_is_available_after_a_successful_run(connectors_settings, make_binding):
    binding, _ = _land(make_binding, [[RECORD]])

    result = discovery.get_landing_schema(binding)
    assert result["status"] == "ready"
    assert "events" in result["resources"]

    columns = result["resources"]["events"]["columns"]
    assert set(columns) >= {"id", "amount", "kind"}
    assert columns["amount"]["data_type"] == "bigint"
    # Internal columns are ours, not the customer's — exposing them invites a
    # mapping that reads the tenant column.
    assert not any(name.startswith(("_connector_", "_dlt_")) for name in columns)


def test_schema_snapshot_is_plain_json(connectors_settings, make_binding):
    """dlt's own trace/schema dicts are not stdlib-JSON serializable."""
    import json

    binding, _ = _land(make_binding, [[RECORD]])
    binding.refresh_from_db()
    json.dumps(binding.landing_schema)
    assert binding.landing_schema_version_hash
    assert binding.landing_schema_at is not None


def test_sample_rows_are_bounded_and_hide_internal_columns(
    connectors_settings, make_binding, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "SAMPLE_MAX_ROWS": 2}
    records = [{"id": f"e{index}", "amount": index} for index in range(10)]
    binding, _ = _land(make_binding, [records])

    rows = discovery.sample_resource(binding, "events", limit=100)
    assert len(rows) == 2
    assert not any(
        key.startswith(("_connector_", "_dlt_")) for row in rows for key in row
    )


def test_list_resources(connectors_settings, make_binding):
    binding, _ = _land(make_binding, [[RECORD]])
    assert discovery.list_resources(binding) == ["events"]


# --- drift -----------------------------------------------------------------


def test_a_vanished_column_makes_the_projection_invalid(
    connectors_settings, make_binding
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = Projection.objects.create(
        binding=binding,
        resource="events",
        target="events",
        name="p",
        mapping={"external_id": {"source": "gone_away"}},
        status=ProjectionStatus.ACTIVE,
    )
    status, messages = landing_schema.detect_drift(projection)
    assert status == ProjectionStatus.INVALID
    assert "gone_away" in messages[0]


def test_a_variant_sibling_flags_the_projection_for_review(
    connectors_settings, make_binding
):
    """dlt never changes a column's type — it adds `amount__v_text` and NULLs
    the original, so the mapping keeps validating and silently returns NULLs."""
    binding, _ = _land(
        make_binding,
        [
            [{"id": "e1", "amount": 10}],
            [{"id": "e2", "amount": "ten"}],
        ],
    )
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    binding.refresh_from_db()

    columns = landing_schema.columns_for(binding, "events")
    assert any(name.startswith("amount__v_") for name in columns), columns

    projection = Projection.objects.create(
        binding=binding,
        resource="events",
        target="events",
        name="p",
        mapping={"external_id": {"source": "id"}, "amount": {"source": "amount"}},
        status=ProjectionStatus.ACTIVE,
    )
    status, messages = landing_schema.detect_drift(projection)
    assert status == ProjectionStatus.NEEDS_REVIEW
    assert "amount" in messages[0]


def test_drift_refresh_regrades_projections_after_a_run(
    connectors_settings, make_binding
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = Projection.objects.create(
        binding=binding,
        resource="events",
        target="events",
        name="p",
        mapping={"external_id": {"source": "nope"}},
        status=ProjectionStatus.ACTIVE,
    )
    discovery.refresh_projection_drift(binding)
    projection.refresh_from_db()
    assert projection.status == ProjectionStatus.INVALID


def test_drift_refresh_leaves_drafts_alone(connectors_settings, make_binding):
    """A draft is being edited; promoting it behind the author's back is wrong."""
    binding, _ = _land(make_binding, [[RECORD]])
    projection = Projection.objects.create(
        binding=binding,
        resource="events",
        target="events",
        name="p",
        mapping={"external_id": {"source": "id"}},
        status=ProjectionStatus.DRAFT,
    )
    discovery.refresh_projection_drift(binding)
    projection.refresh_from_db()
    assert projection.status == ProjectionStatus.DRAFT


# --- retention, purge, reset ----------------------------------------------


def test_time_based_retention_is_not_offered(connectors_settings, make_binding):
    """Pruning a merge resource deletes current state, not history: the cursor
    has advanced past those rows and they will never be re-emitted."""
    from django.core.exceptions import ValidationError

    from django_connectors.enums import LandingRetention

    assert set(LandingRetention.values) == {"current_state", "permanent"}

    binding = make_binding()
    binding.landing_retention = "7_days"
    with pytest.raises(ValidationError):
        binding.full_clean()


def test_purge_drops_the_landing_tables(connectors_settings, make_binding):
    binding, _ = _land(make_binding, [[RECORD]])
    assert discovery.get_landing_schema(binding)["status"] == "ready"

    result = retention.purge_binding_landing(binding)
    binding.refresh_from_db()

    assert result["dropped_tables"]
    assert binding.landing_purged_at is not None
    assert binding.status == BindingStatus.DISABLED
    assert discovery.get_landing_schema(binding)["status"] == "pending_first_run"


def test_deleting_a_binding_with_landed_data_is_refused(
    connectors_settings, make_binding
):
    """Otherwise its rows are stranded with _connector_binding_id pointing at
    a row that no longer exists — a retention compliance failure."""
    binding, _ = _land(make_binding, [[RECORD]])
    with pytest.raises(retention.LandingNotPurged):
        binding.delete()


def test_delete_binding_purges_first(connectors_settings, make_binding):
    from django_connectors.models import Binding

    binding, _ = _land(make_binding, [[RECORD]])
    binding_id = binding.pk
    retention.delete_binding(binding, purge=True)
    assert not Binding.objects.filter(pk=binding_id).exists()


def test_a_binding_that_never_landed_can_be_deleted_directly(
    connectors_settings, make_binding
):
    from django_connectors.models import Binding

    binding = make_binding(config=memory_config(batches=[[RECORD]]))
    binding_id = binding.pk
    binding.delete()
    assert not Binding.objects.filter(pk=binding_id).exists()


def test_reset_clears_the_cursor_so_the_source_is_re_extracted(
    connectors_settings, make_binding
):
    """The escape hatch from a cursor that advanced past records that never landed."""
    from django_connectors.landing import access

    binding, _ = _land(
        make_binding,
        [
            [{"id": "e1", "amount": 1}],
            [{"id": "e2", "amount": 2}],
        ],
        cursor=None,
    )
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert len(access.sample_rows(binding, "events", limit=10)) == 2

    retention.reset_binding_state(binding, backfill=True)
    binding.refresh_from_db()
    assert binding.status == BindingStatus.PENDING

    # The memory source restarts from its first batch because its own state
    # went with the pipeline state.
    run = run_services.run_binding(binding, trigger=RunTrigger.BACKFILL)
    assert run.status == "succeeded", run.error_message


def test_reset_without_backfill_drops_the_landing_tables(
    connectors_settings, make_binding
):
    binding, _ = _land(make_binding, [[RECORD]])
    result = retention.reset_binding_state(binding, backfill=False)
    binding.refresh_from_db()

    assert result["dropped"] is True
    assert binding.status == BindingStatus.PENDING
    assert binding.enabled is True
    assert binding.landing_purged_at is None
