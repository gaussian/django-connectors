"""Projection: mapping landed rows into a host-declared target.

The writer used throughout is a recording stub — the library never learns what
a real writer does, so a stub is a faithful stand-in for one.
"""

import pytest

from django_connectors.enums import (
    ProjectionRunMode,
    ProjectionRunStatus,
    ProjectionStatus,
    RunTrigger,
)
from django_connectors.exceptions import (
    MappingValidationError,
    ProjectionError,
)
from django_connectors.models import Projection
from django_connectors.projections.compiler import compile_mapping
from django_connectors.projections.fields import (
    DateTimeField,
    IntegerField,
    JSONField,
    StringField,
)
from django_connectors.projections.targets import (
    TargetDefinition,
    register_target,
    unregister_all,
)
from django_connectors.services import projections as projection_services
from django_connectors.services import runs as run_services
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db


class RecordingWriter:
    """Stands in for a host writer and records what it was handed."""

    def __init__(self, *, fail_times=0):
        self.batches = []
        self.contexts = []
        self.fail_times = fail_times
        self.calls = 0

    def __call__(self, records, context):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("host writer exploded")
        self.batches.append(list(records))
        self.contexts.append(context)
        return len(records)

    @property
    def records(self):
        return [record for batch in self.batches for record in batch]


@pytest.fixture(autouse=True)
def clean_target_registry():
    unregister_all()
    yield
    unregister_all()


@pytest.fixture
def writer():
    return RecordingWriter()


@pytest.fixture
def events_target(writer):
    return register_target(
        TargetDefinition(
            key="events",
            fields={
                "external_id": StringField(required=True),
                "occurred_at": DateTimeField(required=True),
                "type": StringField(required=True),
                "payload": JSONField(),
                "count": IntegerField(),
            },
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=writer,
            supports_scope_replace=True,
        )
    )


BASIC_MAPPING = {
    "external_id": {"source": "id"},
    "occurred_at": {"source": "happened_at", "cast": "datetime"},
    "type": {"source": "kind"},
}


def _land(make_binding, batches, **kwargs):
    binding = make_binding(config=memory_config(batches=batches, **kwargs))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == "succeeded", run.error_message
    return binding, run


def _projection(binding, mapping=None, *, filters=None, target="events"):
    return Projection.objects.create(
        binding=binding,
        resource="events",
        target=target,
        name="p",
        mapping=mapping if mapping is not None else BASIC_MAPPING,
        filters=filters or [],
        status=ProjectionStatus.ACTIVE,
    )


RECORD = {"id": "e1", "happened_at": "2024-03-01T10:00:00Z", "kind": "created"}


# --- compiler --------------------------------------------------------------


def test_direct_mapping_constants_objects_and_functions():
    compiled = compile_mapping(
        {
            "external_id": {"source": "id"},
            "source_system": {"constant": "memory"},
            "type": {"function": "upper", "args": [{"source": "kind"}]},
            "label": {
                "function": "concat",
                "args": [{"source": "kind"}, {"constant": ":"}, {"source": "id"}],
            },
            "fallback": {
                "function": "coalesce",
                "args": [{"source": "missing"}, {"constant": "default"}],
            },
            "payload": {
                "object": {
                    "kind": {"source": "kind"},
                    "nested": {"object": {"id": {"source": "id"}}},
                }
            },
        }
    )
    values = compiled.apply({**RECORD, "missing": None})
    assert values["external_id"] == "e1"
    assert values["source_system"] == "memory"
    assert values["type"] == "CREATED"
    assert values["label"] == "created:e1"
    assert values["fallback"] == "default"
    assert values["payload"] == {"kind": "created", "nested": {"id": "e1"}}


def test_json_path_reads_into_a_landed_json_column():
    """Nesting is off, so structures land as JSON — this is how they're read."""
    compiled = compile_mapping(
        {
            "a": {"source": "meta", "json_path": "user.name"},
            "b": {"source": "meta", "json_path": "tags.0"},
            "c": {"source": "meta", "json_path": "nope.deep", "default": "fallback"},
        }
    )
    row = {"meta": {"user": {"name": "ada"}, "tags": ["x", "y"]}}
    values = compiled.apply(row)
    assert values == {"a": "ada", "b": "x", "c": "fallback"}


def test_json_path_parses_a_json_string_column():
    compiled = compile_mapping({"a": {"source": "meta", "json_path": "k"}})
    assert compiled.apply({"meta": '{"k": "v"}'})["a"] == "v"


@pytest.mark.parametrize(
    ("mapping", "message"),
    [
        ({}, "non-empty"),
        ({"a": "not-an-object"}, "must be an object"),
        ({"a": {}}, "exactly one of"),
        ({"a": {"source": "x", "constant": 1}}, "exactly one of"),
        ({"a": {"source": "x", "cast": "sql"}}, "unknown cast"),
        ({"a": {"function": "exec", "args": [{"constant": 1}]}}, "unknown function"),
        ({"a": {"function": "lower", "args": []}}, "non-empty args"),
        ({"a": {"object": {}}}, "non-empty"),
    ],
)
def test_invalid_mappings_are_rejected_by_name(mapping, message):
    with pytest.raises(MappingValidationError, match=message):
        compile_mapping(mapping)


def test_non_json_literals_are_rejected():
    """The field is writable from Python; a callable would run against every row."""
    with pytest.raises(MappingValidationError, match="not a JSON literal"):
        compile_mapping({"a": {"constant": object()}})


@pytest.mark.parametrize(
    ("filters", "row", "expected"),
    [
        ([{"field": "kind", "op": "eq", "value": "created"}], RECORD, True),
        ([{"field": "kind", "op": "ne", "value": "created"}], RECORD, False),
        ([{"field": "kind", "op": "in", "value": ["created", "x"]}], RECORD, True),
        ([{"field": "kind", "op": "not_in", "value": ["created"]}], RECORD, False),
        ([{"field": "kind", "op": "contains", "value": "reat"}], RECORD, True),
        ([{"field": "kind", "op": "startswith", "value": "cre"}], RECORD, True),
        ([{"field": "kind", "op": "endswith", "value": "ted"}], RECORD, True),
        ([{"field": "absent", "op": "is_null"}], RECORD, True),
        ([{"field": "kind", "op": "is_not_null"}], RECORD, True),
        ([{"field": "n", "op": "gt", "value": 5}], {"n": 10}, True),
        ([{"field": "n", "op": "lte", "value": 5}], {"n": 10}, False),
    ],
)
def test_filter_operators(filters, row, expected):
    assert compile_mapping({"a": {"constant": 1}}, filters).matches(row) is expected


def test_boolean_filters_tolerate_mysql_returning_ints():
    """MySQL has no native boolean; a landed flag reads back as 0/1."""
    compiled = compile_mapping(
        {"a": {"constant": 1}}, [{"field": "flag", "op": "eq", "value": True}]
    )
    assert compiled.matches({"flag": 1}) is True
    assert compiled.matches({"flag": 0}) is False


def test_a_sql_injection_string_is_just_a_value():
    """Filters are evaluated in Python; there is no SQL to inject into."""
    compiled = compile_mapping(
        {"a": {"constant": 1}},
        [{"field": "kind", "op": "eq", "value": "x' OR '1'='1"}],
    )
    assert compiled.matches(RECORD) is False


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        ([{"op": "eq", "value": 1}], "'field' is required"),
        ([{"field": "a", "op": "regex", "value": 1}], "unknown op"),
        ([{"field": "a", "op": "eq"}], "'value' is required"),
        ([{"field": "a", "op": "in", "value": "notalist"}], "needs a list"),
    ],
)
def test_invalid_filters_are_rejected(filters, message):
    with pytest.raises(MappingValidationError, match=message):
        compile_mapping({"a": {"constant": 1}}, filters)


# --- validation ------------------------------------------------------------


def test_validation_requires_identity_and_required_fields(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding, {"type": {"source": "kind"}})

    result = projection_services.validate_projection(projection)
    assert not result.ok
    joined = "; ".join(result.errors)
    assert "external_id" in joined
    assert "occurred_at" in joined


def test_validation_rejects_unknown_source_columns(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(
        binding, {**BASIC_MAPPING, "type": {"source": "does_not_exist"}}
    )
    result = projection_services.validate_projection(projection)
    assert not result.ok
    assert "does_not_exist" in "; ".join(result.errors)


def test_validation_rejects_fields_the_target_does_not_declare(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding, {**BASIC_MAPPING, "nope": {"constant": 1}})
    result = projection_services.validate_projection(projection)
    assert not result.ok
    assert "nope" in "; ".join(result.errors)


def test_a_valid_projection_validates_clean(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding)
    result = projection_services.validate_projection(projection)
    assert result.ok, result.errors


def test_activation_is_refused_before_the_binding_has_landed_anything(
    connectors_settings, make_binding, events_target
):
    binding = make_binding(config=memory_config(batches=[[RECORD]]))
    projection = _projection(binding)
    with pytest.raises(ProjectionError, match="has not landed anything"):
        projection_services.activate_projection(projection)


# --- execution -------------------------------------------------------------


def test_incremental_run_projects_only_the_scoped_loads(
    connectors_settings, make_binding, events_target, writer
):
    binding = make_binding(
        config=memory_config(
            batches=[
                [{"id": "e1", "happened_at": "2024-03-01T10:00:00Z", "kind": "a"}],
                [{"id": "e2", "happened_at": "2024-03-02T10:00:00Z", "kind": "b"}],
            ]
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    projection = _projection(binding)
    projection_run = projection_services.run_projection(projection, source_run=second)

    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert [record.identity["external_id"] for record in writer.records] == ["e2"]


def test_the_incremental_window_uses_load_ids_not_the_run_id(
    connectors_settings, make_binding, events_target
):
    """A run id is stamped at extract time and is wrong for a pending package."""
    binding, run = _land(make_binding, [[RECORD]])
    projection = _projection(binding)
    projection_run = projection_services.run_projection(projection, source_run=run)
    assert projection_run.load_ids == run.dlt_load_ids


def test_full_replay_projects_every_landed_row(
    connectors_settings, make_binding, events_target, writer
):
    binding = make_binding(
        config=memory_config(
            batches=[
                [{"id": "e1", "happened_at": "2024-03-01T10:00:00Z", "kind": "a"}],
                [{"id": "e2", "happened_at": "2024-03-02T10:00:00Z", "kind": "b"}],
            ]
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    projection = _projection(binding)
    projection_run = projection_services.replay_projection(projection)

    assert projection_run.mode == ProjectionRunMode.FULL
    assert sorted(r.identity["external_id"] for r in writer.records) == ["e1", "e2"]
    assert writer.contexts[-1].replace_scope is True


def test_replay_is_refused_for_a_target_that_cannot_replace_a_scope(
    connectors_settings, make_binding, writer
):
    register_target(
        TargetDefinition(
            key="events",
            fields={"external_id": StringField(required=True)},
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=writer,
            supports_scope_replace=False,
        )
    )
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding, {"external_id": {"source": "id"}})

    with pytest.raises(ProjectionError, match="supports_scope_replace"):
        projection_services.replay_projection(projection)


def test_a_tombstone_produces_a_delete_record(
    connectors_settings, make_binding, events_target, writer
):
    from django_connectors.sources.memory import tombstone

    binding = make_binding(
        config=memory_config(batches=[[RECORD], [tombstone({"id": "e1"})]])
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    projection = _projection(binding)
    projection_run = projection_services.replay_projection(projection)

    assert projection_run.status == ProjectionRunStatus.SUCCEEDED
    assert [record.operation for record in writer.records] == ["delete"]
    assert writer.records[0].identity == {"external_id": "e1"}
    # The host decides what delete means; the library sends no values.
    assert writer.records[0].values == {}


def test_writer_context_carries_the_owner(
    connectors_settings, make_binding, events_target, writer
):
    """Otherwise the tenant guarantee is discarded at the final step."""
    binding, run = _land(make_binding, [[RECORD]])
    projection = _projection(binding)
    projection_services.run_projection(projection, source_run=run)

    context = writer.contexts[0]
    assert context.owner_object_id == binding.connection.owner_object_id
    assert context.owner_content_type_id == binding.connection.owner_content_type_id
    assert context.binding_id == binding.id
    assert context.projection_version == projection.version


def test_two_owners_with_the_same_remote_id_produce_distinct_contexts(
    connectors_settings, make_binding, events_target, writer
):
    first, first_run = _land(make_binding, [[RECORD]])
    second_binding = make_binding(
        config=memory_config(batches=[[RECORD]]), owner_id="2"
    )
    second_run = run_services.run_binding(second_binding, trigger=RunTrigger.INITIAL)

    projection_services.run_projection(_projection(first), source_run=first_run)
    projection_services.run_projection(
        _projection(second_binding), source_run=second_run
    )

    owners = {context.owner_object_id for context in writer.contexts}
    assert owners == {"1", "2"}
    assert all(r.identity["external_id"] == "e1" for r in writer.records)


def test_filters_exclude_rows_before_the_writer_sees_them(
    connectors_settings, make_binding, events_target, writer
):
    binding, run = _land(
        make_binding,
        [
            [
                {"id": "e1", "happened_at": "2024-03-01T10:00:00Z", "kind": "keep"},
                {"id": "e2", "happened_at": "2024-03-01T10:00:00Z", "kind": "drop"},
            ]
        ],
    )
    projection = _projection(
        binding, filters=[{"field": "kind", "op": "eq", "value": "keep"}]
    )
    projection_run = projection_services.run_projection(projection, source_run=run)

    assert [r.identity["external_id"] for r in writer.records] == ["e1"]
    assert projection_run.records_seen == 2
    assert projection_run.records_written == 1


def test_a_cast_failure_fails_the_run_and_names_the_field(
    connectors_settings, make_binding, events_target
):
    binding, run = _land(
        make_binding, [[{"id": "e1", "happened_at": "not-a-date", "kind": "a"}]]
    )
    projection = _projection(binding)
    projection_run = projection_services.run_projection(projection, source_run=run)

    assert projection_run.status == ProjectionRunStatus.FAILED
    assert "occurred_at" in projection_run.error_message


def test_a_writer_failure_fails_the_run_without_losing_the_landed_data(
    connectors_settings, make_binding
):
    """This is the whole point of the landing boundary."""
    failing = RecordingWriter(fail_times=1)
    register_target(
        TargetDefinition(
            key="events",
            fields={"external_id": StringField(required=True)},
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=failing,
        )
    )
    binding, run = _land(make_binding, [[RECORD]])
    projection = _projection(binding, {"external_id": {"source": "id"}})

    first = projection_services.run_projection(projection, source_run=run)
    assert first.status == ProjectionRunStatus.FAILED
    assert first.error_type == "TargetWriteError"

    # Retried against landing data — the provider is never contacted again.
    retry = projection_services.retry_projection_run(first)
    assert retry.status == ProjectionRunStatus.SUCCEEDED
    assert [r.identity["external_id"] for r in failing.records] == ["e1"]


def test_retrying_is_idempotent_for_the_host(
    connectors_settings, make_binding, events_target, writer
):
    binding, run = _land(make_binding, [[RECORD]])
    projection = _projection(binding)

    first = projection_services.run_projection(projection, source_run=run)
    assert first.status == ProjectionRunStatus.SUCCEEDED
    projection_services.run_projection(projection, source_run=run)

    # Same identity both times: a host upserting by identity converges.
    assert {r.identity["external_id"] for r in writer.records} == {"e1"}
    assert all(r.operation == "upsert" for r in writer.records)


def test_a_superseded_projection_run_is_refused_not_executed(
    connectors_settings, make_binding, events_target, writer
):
    """A stale queued run would revert rows a newer replay already corrected."""
    from django_connectors.models import ProjectionRun

    binding, run = _land(make_binding, [[RECORD]])
    projection = _projection(binding)

    stale = ProjectionRun.objects.create(
        projection=projection,
        mode=ProjectionRunMode.INCREMENTAL,
        projection_version=projection.version,
        load_ids=run.dlt_load_ids,
    )
    projection.version += 1
    projection.save(update_fields=["version"])

    from django_connectors.projections import runner as projection_runner

    projection_runner.execute(stale)
    stale.refresh_from_db()
    assert stale.status == ProjectionRunStatus.SUPERSEDED
    assert writer.records == []


def test_changing_an_identity_mapping_requires_acknowledgement(
    connectors_settings, make_binding, events_target
):
    """The library cannot clean up records written under the old identity."""
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding)

    with pytest.raises(ProjectionError, match="acknowledge_identity_change"):
        projection_services.update_mapping(
            projection, mapping={**BASIC_MAPPING, "external_id": {"source": "kind"}}
        )

    updated = projection_services.update_mapping(
        projection,
        mapping={**BASIC_MAPPING, "external_id": {"source": "kind"}},
        acknowledge_identity_change=True,
    )
    assert updated.version == 2


def test_a_non_identity_mapping_change_just_bumps_the_version(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding)
    updated = projection_services.update_mapping(
        projection, mapping={**BASIC_MAPPING, "type": {"constant": "fixed"}}
    )
    assert updated.version == 2


def test_the_sweeper_heals_a_dropped_dispatch(
    connectors_settings, make_binding, events_target, writer
):
    """A per-Run flag would heal this; only a set difference also heals a
    ProjectionRun that failed and was never retried."""
    binding, _ = _land(make_binding, [[RECORD]])
    _projection(binding)

    dispatched = projection_services.dispatch_pending_projections()
    assert len(dispatched) == 1
    assert [r.identity["external_id"] for r in writer.records] == ["e1"]

    # Nothing outstanding the second time.
    assert projection_services.dispatch_pending_projections() == []


def test_batches_are_bounded_and_ordered(
    connectors_settings, make_binding, events_target, writer, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "PROJECTION_BATCH_SIZE": 2}
    records = [
        {"id": f"e{index}", "happened_at": "2024-03-01T10:00:00Z", "kind": "a"}
        for index in range(5)
    ]
    binding, run = _land(make_binding, [records])
    projection = _projection(binding)
    projection_services.run_projection(projection, source_run=run)

    assert [len(batch) for batch in writer.batches] == [2, 2, 1]
    assert [context.batch_index for context in writer.contexts] == [0, 1, 2]


def test_a_delete_and_an_upsert_for_one_identity_collapse_to_the_delete(
    events_target, writer
):
    from django_connectors.projections.runner import _collapse
    from django_connectors.projections.targets import ProjectedRecord, get_target

    target = get_target("events")
    collapsed = _collapse(
        [
            ProjectedRecord(
                operation="upsert", identity={"external_id": "e1"}, values={}
            ),
            ProjectedRecord(operation="delete", identity={"external_id": "e1"}),
            ProjectedRecord(
                operation="upsert", identity={"external_id": "e2"}, values={}
            ),
        ],
        target,
    )
    assert [(r.operation, r.identity["external_id"]) for r in collapsed] == [
        ("upsert", "e2"),
        ("delete", "e1"),
    ]


# --- preview ---------------------------------------------------------------


def test_preview_never_invokes_the_writer(
    connectors_settings, make_binding, events_target, writer
):
    binding, _ = _land(make_binding, [[RECORD]])
    projection = _projection(binding)

    result = projection_services.preview_projection(projection)
    assert result["ok_count"] == 1
    assert result["rows"][0]["record"]["identity"] == {"external_id": "e1"}
    assert writer.batches == []


def test_preview_reports_filtered_rows_and_cast_errors(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(
        make_binding,
        [
            [
                {"id": "ok", "happened_at": "2024-03-01T10:00:00Z", "kind": "keep"},
                {"id": "bad", "happened_at": "nonsense", "kind": "keep"},
                {"id": "out", "happened_at": "2024-03-01T10:00:00Z", "kind": "drop"},
            ]
        ],
    )
    projection = _projection(
        binding, filters=[{"field": "kind", "op": "eq", "value": "keep"}]
    )
    result = projection_services.preview_projection(projection)

    statuses = {row["source"]["id"]: row["status"] for row in result["rows"]}
    assert statuses == {"ok": "ok", "bad": "error", "out": "filtered"}
    assert "occurred_at" in next(
        row["error"] for row in result["rows"] if row["status"] == "error"
    )


def test_preview_hides_internal_columns(
    connectors_settings, make_binding, events_target
):
    binding, _ = _land(make_binding, [[RECORD]])
    result = projection_services.preview_projection(_projection(binding))
    assert not any(
        key.startswith(("_connector_", "_dlt_")) for key in result["rows"][0]["source"]
    )


def test_preview_is_bounded_server_side(
    connectors_settings, make_binding, events_target, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "PREVIEW_MAX_ROWS": 2}
    records = [
        {"id": f"e{index}", "happened_at": "2024-03-01T10:00:00Z", "kind": "a"}
        for index in range(10)
    ]
    binding, _ = _land(make_binding, [records])
    result = projection_services.preview_projection(_projection(binding), limit=100)
    assert result["row_count"] == 2


# --- target registry -------------------------------------------------------


def test_duplicate_target_registration_is_refused(writer):
    definition = TargetDefinition(
        key="dupe",
        fields={"external_id": StringField(required=True)},
        identity_fields=("external_id",),
        identity_scope="global",
        writer=writer,
    )
    register_target(definition)
    register_target(definition)  # same object is a no-op

    from django_connectors.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="already registered"):
        register_target(
            TargetDefinition(
                key="dupe",
                fields={"external_id": StringField(required=True)},
                identity_fields=("external_id",),
                identity_scope="global",
                writer=writer,
            )
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"identity_fields": ()}, "identity_fields"),
        ({"identity_fields": ("nope",)}, "not declared"),
        ({"identity_scope": "team"}, "identity_scope"),
        ({"fields": {}}, "no fields"),
        ({"writer": "not-callable"}, "not callable"),
    ],
)
def test_target_definitions_are_validated(writer, kwargs, message):
    from django_connectors.exceptions import ConfigurationError

    base = {
        "key": "t",
        "fields": {"external_id": StringField(required=True)},
        "identity_fields": ("external_id",),
        "identity_scope": "owner",
        "writer": writer,
    }
    with pytest.raises(ConfigurationError, match=message):
        TargetDefinition(**{**base, **kwargs})


def test_identity_scope_has_no_default(writer):
    """A wrong guess here is a cross-tenant collision, so the host must state it."""
    import inspect

    signature = inspect.signature(TargetDefinition.__init__)
    assert signature.parameters["identity_scope"].default is inspect.Parameter.empty
