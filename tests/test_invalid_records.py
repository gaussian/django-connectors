"""Rows that cannot become records: fail the run, or skip and say so.

Customer spreadsheets are full of half-typed rows. Under the default policy
one blank identity cell fails the whole run before the writer is called; under
``on_invalid_record="skip"`` the row is counted, the reason recorded on the
ProjectionRun, and the rest of the batch lands. Three rules keep skipping from
becoming silent loss, and each has a test below: a null identity never writes
and never deletes; a skipped row never produces a delete; a full replay
projects the row once the mapping is fixed.

A writer has the same channel: returning ``WriterResult`` instead of an int
puts its own skips and warnings on the run.
"""

import pytest

from django_connectors import WriterResult
from django_connectors.enums import InvalidRecordPolicy, ProjectionRunStatus
from django_connectors.projections.fields import StringField
from django_connectors.projections.targets import TargetDefinition, register_target
from django_connectors.services import projections as projection_services
from django_connectors.sources.memory import tombstone
from tests.conftest import BASIC_MAPPING, land_memory, make_projection

pytestmark = pytest.mark.django_db

GOOD = {"id": "ok", "ref": "ok", "happened_at": "2024-03-01T10:00:00Z", "kind": "k"}
# The landing merge key (`id`) is never null — dlt refuses that at normalize.
# A null *identity* comes from mapping the target's key off another column
# that a customer left blank, which is what `ref` stands for here.
NO_IDENTITY = {
    "id": "blank",
    "ref": None,
    "happened_at": "2024-03-01T10:00:00Z",
    "kind": "k",
}
BAD_DATE = {"id": "bad", "ref": "bad", "happened_at": "yesterday-ish", "kind": "k"}

#: Identity from `ref`, not the merge key.
REF_MAPPING = {**BASIC_MAPPING, "external_id": {"source": "ref"}}


def _skipping(binding, mapping=REF_MAPPING):
    projection = make_projection(binding, mapping)
    projection.on_invalid_record = InvalidRecordPolicy.SKIP
    projection.save(update_fields=["on_invalid_record"])
    return projection


# --- the default still fails ------------------------------------------------


def test_the_default_policy_fails_the_run_on_the_first_bad_row(
    connectors_settings, make_binding, events_target, writer
):
    binding, run = land_memory(make_binding, [[GOOD, BAD_DATE]])
    projection_run = projection_services.run_projection(
        make_projection(binding), source_run=run
    )
    assert projection_run.status == ProjectionRunStatus.FAILED
    assert projection_run.records_skipped == 0
    assert writer.records == []


# --- skip: count, explain, carry on ----------------------------------------


def test_skip_lands_the_good_rows_and_explains_the_bad_ones(
    connectors_settings, make_binding, events_target, writer
):
    binding, run = land_memory(make_binding, [[GOOD, BAD_DATE, NO_IDENTITY]])
    projection_run = projection_services.run_projection(
        _skipping(binding), source_run=run
    )

    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert projection_run.records_seen == 3
    assert projection_run.records_written == 1
    assert projection_run.records_skipped == 2
    assert [r.identity["external_id"] for r in writer.records] == ["ok"]

    reasons = "\n".join(w["reason"] for w in projection_run.warnings)
    assert "occurred_at" in reasons  # the cast that failed
    assert "external_id" in reasons  # the identity that was None
    assert all("row" in w for w in projection_run.warnings)


def test_a_null_identity_tombstone_is_skipped_not_deleted(
    connectors_settings, make_binding, events_target, writer
):
    """Rule: a skipped row never produces a delete.

    delete-insert merge nulls every column a tombstone does not carry, so an
    identity mapped off `ref` is None on the delete path. The validator refuses
    that configuration up front for tombstone-emitting sources; this pins what
    the runner does if it is reached anyway: skip, never delete.
    """
    binding, _ = land_memory(make_binding, [[GOOD], [tombstone({"id": "ok"})]])
    from django_connectors.enums import RunTrigger
    from django_connectors.services import runs as run_services

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    projection_run = projection_services.run_projection(
        _skipping(binding), source_run=second
    )
    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert projection_run.records_deleted == 0
    assert projection_run.records_skipped == 1
    assert writer.records == []


def test_a_full_replay_projects_a_row_skipped_under_the_old_mapping(
    connectors_settings, make_binding, events_target, writer
):
    """Rule: fix the mapping, replay, and the skipped row lands."""
    binding, run = land_memory(
        make_binding,
        [[{"id": "d1", "ref": "d1", "happened_at": "03/04/2024", "kind": "k"}]],
    )
    projection = _skipping(binding)
    first = projection_services.run_projection(projection, source_run=run)
    assert first.records_skipped == 1 and first.records_written == 0

    projection_services.update_mapping(
        projection,
        mapping={
            **REF_MAPPING,
            "occurred_at": {
                "source": "happened_at",
                "cast": "datetime",
                "format": "%d/%m/%Y",
            },
        },
    )
    replay = projection_services.replay_projection(projection)
    assert replay.status == ProjectionRunStatus.SUCCEEDED, replay.error_message
    assert replay.records_skipped == 0
    assert [r.identity["external_id"] for r in writer.records] == ["d1"]


def test_warnings_are_capped_but_the_count_is_exact(
    connectors_settings, make_binding, events_target, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "PROJECTION_MAX_WARNINGS": 2}
    rows = [dict(BAD_DATE, id=f"b{i}") for i in range(5)]
    binding, run = land_memory(make_binding, [rows])
    projection_run = projection_services.run_projection(
        _skipping(binding), source_run=run
    )
    assert projection_run.records_skipped == 5
    assert len(projection_run.warnings) == 2


# --- the writer's own channel ----------------------------------------------


def test_a_writer_result_puts_its_skips_and_warnings_on_the_run(
    connectors_settings, make_binding
):
    def writer(records, context):
        return WriterResult(
            written=len(records) - 1,
            skipped=1,
            warnings=["reference 'X-42' matched no parent record; landed unlinked"],
        )

    register_target(
        TargetDefinition(
            key="events",
            fields={"external_id": StringField(required=True)},
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=writer,
        )
    )
    binding, run = land_memory(make_binding, [[{"id": "a"}, {"id": "b"}]])
    projection_run = projection_services.run_projection(
        make_projection(binding, {"external_id": {"source": "id"}}), source_run=run
    )
    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert projection_run.records_written == 1
    assert projection_run.records_skipped == 1
    assert projection_run.warnings == [
        {"reason": "reference 'X-42' matched no parent record; landed unlinked"}
    ]


def test_an_int_return_still_means_the_batch_was_applied(
    connectors_settings, make_binding, events_target, writer
):
    binding, run = land_memory(make_binding, [[GOOD]])
    projection_run = projection_services.run_projection(
        make_projection(binding), source_run=run
    )
    assert projection_run.records_written == 1
    assert projection_run.records_skipped == 0
    assert projection_run.warnings == []


def test_the_api_exposes_the_policy_and_the_counters():
    pytest.importorskip("rest_framework")
    from django_connectors.api.serializers import (
        ProjectionRunSerializer,
        ProjectionSerializer,
    )

    assert "on_invalid_record" in ProjectionSerializer.Meta.fields
    assert "records_skipped" in ProjectionRunSerializer.Meta.fields
    assert "warnings" in ProjectionRunSerializer.Meta.fields


# --- what skip must never swallow ------------------------------------------


def test_skip_does_not_swallow_a_broken_mapping(
    connectors_settings, make_binding, events_target, writer
):
    """A column that is not in the landed row is the mapping's fault. Skipping
    every row over it would report success on nothing."""
    binding, run = land_memory(make_binding, [[GOOD]])
    projection = _skipping(binding, {**REF_MAPPING, "type": {"source": "gone"}})
    projection_run = projection_services.run_projection(projection, source_run=run)
    assert projection_run.status == ProjectionRunStatus.FAILED
    assert "gone" in projection_run.error_message
    assert projection_run.records_skipped == 0


def test_a_delete_needs_its_identity_and_nothing_else(
    connectors_settings, make_binding, events_target, writer
):
    """A soft-deleted row whose unused column will not cast still deletes."""
    from django_connectors.enums import RunTrigger
    from django_connectors.landing.naming import DELETED_COLUMN
    from django_connectors.services import runs as run_services

    binding, _ = land_memory(
        make_binding,
        [[GOOD], [{**GOOD, "happened_at": "n/a", DELETED_COLUMN: True}]],
    )
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    projection_run = projection_services.run_projection(
        _skipping(binding), source_run=second
    )
    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert projection_run.records_deleted == 1
    assert projection_run.records_skipped == 0
    assert [r.operation for r in writer.records] == ["delete"]
