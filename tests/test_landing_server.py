"""Landing invariants against real database servers.

Marked ``serverdb`` and parameterized over every configured backend
(``DJANGO_CONNECTORS_TEST_MYSQL_URL`` / ``DJANGO_CONNECTORS_TEST_POSTGRES_URL``);
unconfigured backends skip.

sqlite cannot stand in for these. It reports a 9999-character identifier limit
where PostgreSQL truncates at 63, has no meaningful server-side concurrency, and
returns timestamps as strings. Each real backend also diverges somewhere the
projection layer depends on — booleans, JSON columns, timestamp awareness — so
the same assertions run against all of them rather than against a
representative one.

A handful of failures are genuinely single-dialect (MySQL's 64KB ``TEXT`` cap
has no PostgreSQL equivalent); those skip on the others rather than being
asserted loosely enough to pass everywhere.
"""

import datetime as dt
import threading

import pytest
from sqlalchemy import create_engine, text

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.landing import access
from django_connectors.landing.index import (
    MERGE_INDEX_SUFFIX,
    MYSQL_TEXT_PREFIX_LENGTH,
    index_name,
)
from django_connectors.landing.instrument import instrument_source
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    DLT_LOAD_ID_COLUMN,
    MAX_IDENTIFIER_LENGTH,
    RUN_ID_COLUMN,
    landing_table_name,
)
from django_connectors.models import Run
from django_connectors.registry import sources
from django_connectors.services import runs as run_services
from django_connectors.sources.memory import tombstone
from tests.conftest import SERVER_DATASET, memory_config

pytestmark = [pytest.mark.serverdb, pytest.mark.django_db]


def _columns(url, table, schema=SERVER_DATASET):
    with create_engine(url).connect() as connection:
        rows = connection.execute(
            text(
                "SELECT column_name, data_type, character_maximum_length "
                "FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table"
            ),
            {"schema": schema, "table": table},
        ).fetchall()
    return {name: (data_type, length) for name, data_type, length in rows}


def _indexes(url, table, schema=SERVER_DATASET):
    """``{index name: [column names]}`` as the server actually holds them."""
    from sqlalchemy import inspect as sqlalchemy_inspect

    with create_engine(url).connect() as connection:
        return {
            found["name"]: list(found["column_names"])
            for found in sqlalchemy_inspect(connection).get_indexes(
                table, schema=schema
            )
        }


def _mysql_prefix_lengths(url, table, index, schema=SERVER_DATASET):
    """``{column: sub_part}`` — the prefix length MySQL recorded, or None."""
    with create_engine(url).connect() as connection:
        rows = connection.execute(
            text(
                "SELECT column_name, sub_part FROM information_schema.statistics "
                "WHERE table_schema = :schema AND table_name = :table "
                "AND index_name = :index ORDER BY seq_in_index"
            ),
            {"schema": schema, "table": table, "index": index},
        ).fetchall()
    return dict(rows)


def _count(url, table, schema=SERVER_DATASET):
    with create_engine(url).connect() as connection:
        return connection.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            if not url.startswith("mysql")
            else text(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
        ).scalar()


# --- portability -----------------------------------------------------------


def test_tenant_columns_are_bounded_varchar_not_unbounded_text(
    server_settings, make_binding
):
    """Without a precision hint dlt maps str to an unindexable text type.

    MySQL cannot index TEXT without a prefix length (error 1170), and merge is
    ``DELETE ... WHERE EXISTS``, so an unindexable tenant column makes merge
    cost linear in table size.
    """
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    table = landing_table_name("memory", "events", binding.landing_key)
    columns = _columns(server_settings.url, table)

    for column in (BINDING_ID_COLUMN, RUN_ID_COLUMN):
        data_type, length = columns[column]
        assert "char" in data_type, f"{column} landed as {data_type}"
        # 36, not 32: a canonical UUID string carries four hyphens. MySQL
        # rejects the overflow outright; sqlite accepts any width.
        assert length == 36, f"{column} is {length} wide"


def test_generated_identifiers_fit_every_backend(server_settings, make_binding):
    """63 is the binding constraint — PostgreSQL's, not MySQL's 64."""
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    table = landing_table_name("memory", "events", binding.landing_key)
    assert len(table) <= MAX_IDENTIFIER_LENGTH
    assert MAX_IDENTIFIER_LENGTH == 63
    assert len(binding.pipeline_name) <= 63
    assert len(binding.schema_name) <= 63

    assert table in _columns(server_settings.url, table) or _columns(
        server_settings.url, table
    ), "the table name we computed is not the one the server created"


def test_json_columns_read_back_parsed_on_every_backend(server_settings, make_binding):
    """PostgreSQL returns parsed JSON, sqlite returns text.

    Left unnormalised the same customer mapping produces a dict on one backend
    and a string on another.
    """
    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1", "meta": {"plan": "pro"}, "tags": ["a", "b"]}]]
        )
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    row = access.sample_rows(binding, "events", limit=1)[0]
    assert row["meta"] == {"plan": "pro"}
    assert row["tags"] == ["a", "b"]


def test_booleans_and_timestamps_coerce_on_every_backend(server_settings, make_binding):
    """PostgreSQL has native booleans and tz-aware timestamps; MySQL neither.

    A landed flag is `True` on one and `1` on the other, and a timestamp comes
    back tz-aware on one and naive on the other — so the field coercions have to
    accept all of it, or a mapping that works in development fails in production.
    """
    from django_connectors.projections.fields import BooleanField, DateTimeField

    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1", "ts": "2024-03-01T10:30:00Z", "flag": True}]]
        )
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    row = access.sample_rows(binding, "events", limit=1)[0]

    # Whatever the backend returned, both must land on the same Python value.
    assert BooleanField().coerce(row["flag"], field_name="flag") is True
    assert BooleanField().coerce(row[DELETED_COLUMN], field_name="d") is False

    moment = DateTimeField().coerce(row["ts"], field_name="ts")
    assert moment.tzinfo is not None, "a naive datetime would shift by the offset"
    assert moment == dt.datetime(2024, 3, 1, 10, 30, tzinfo=dt.UTC)


# --- merge indexes ---------------------------------------------------------


def test_landing_indexes_are_created_on_every_backend(server_settings, make_binding):
    """dlt creates none, and merge is ``DELETE ... WHERE EXISTS``.

    Unindexed, merge cost is linear in table size: measured at 0.50s/1.41s/3.30s
    for 200 rows into 5k/20k/50k on MySQL, and 179 seconds for 10,000 rows into
    60,000. With these two indexes the same loads were flat at 0.13-0.17s across
    50k/100k/200k.
    """
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    table = landing_table_name("memory", "events", binding.landing_key)
    indexes = _indexes(server_settings.url, table)

    assert [BINDING_ID_COLUMN, "id"] in indexes.values(), indexes
    assert [DLT_LOAD_ID_COLUMN, BINDING_ID_COLUMN] in indexes.values(), indexes


def test_mysql_gets_a_prefix_length_and_postgresql_does_not(
    server_settings, make_binding
):
    """The one rule that makes this dialect-aware rather than portable SQL.

    dlt maps a ``str`` column with no precision hint to ``TEXT``, and MySQL
    refuses to index one without a prefix — error 1170. PostgreSQL has no such
    limit and rejects the prefix syntax outright, so the same statement cannot
    be sent to both. The ``_connector_*`` columns are hinted ``precision=36``
    precisely so they index whole on either.
    """
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    table = landing_table_name("memory", "events", binding.landing_key)
    merge_index = index_name(table, MERGE_INDEX_SUFFIX)
    assert merge_index in _indexes(server_settings.url, table)

    if server_settings.backend != "mysql":
        # Nothing to assert positively: PostgreSQL records no prefix at all,
        # and the index existing above is the whole proof.
        pytest.skip("prefix lengths are a MySQL concept")

    prefixes = _mysql_prefix_lengths(server_settings.url, table, merge_index)
    assert prefixes[BINDING_ID_COLUMN] is None, "varchar(36) needs no prefix"
    assert prefixes["id"] == MYSQL_TEXT_PREFIX_LENGTH, "TEXT key needs one"


def test_mysql_really_does_refuse_the_unprefixed_index(server_settings, make_binding):
    """Proves the prefix above is load-bearing rather than defensive habit.

    Without this, a change that dropped the prefix would still pass every other
    test in this file — the index would simply never be created on MySQL, and
    merges would quietly go back to scanning.
    """
    if server_settings.backend != "mysql":
        pytest.skip("only MySQL rejects an unprefixed TEXT key")

    from django_connectors.landing.destination import build_pipeline

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    table = landing_table_name("memory", "events", binding.landing_key)

    pipeline = build_pipeline(binding)
    with pipeline.sql_client(schema_name=binding.schema_name) as client:
        statement = (
            f"CREATE INDEX naive_idx ON {SERVER_DATASET}.{table} "
            f"({BINDING_ID_COLUMN}, id)"
        )
        with pytest.raises(Exception) as excinfo:
            client.execute_sql(statement)
    assert "1170" in str(excinfo.value)


def test_the_indexes_survive_dlt_schema_evolution(server_settings, make_binding):
    """A new source column means ``ALTER TABLE``, which must not drop them."""
    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1"}], [{"id": "2", "brand_new_column": "x"}]]
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    table = landing_table_name("memory", "events", binding.landing_key)
    before = _indexes(server_settings.url, table)
    assert len(before) == 2

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert "brand_new_column" in _columns(server_settings.url, table)
    assert _indexes(server_settings.url, table) == before


def test_provisioning_a_second_time_is_harmless_on_every_backend(
    server_settings, make_binding
):
    """MySQL has no ``CREATE INDEX IF NOT EXISTS``; sqlite and PostgreSQL do.

    So "idempotent" has to be established by reflection rather than by syntax,
    and the backend that would break is exactly the one sqlite cannot stand in
    for.
    """
    from django_connectors.landing.index import ensure_landing_indexes

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    report = ensure_landing_indexes(binding)
    assert report["failed"] == []
    assert report["created"] == []
    assert len(report["existing"]) == 2


# --- multi-tenancy ---------------------------------------------------------


def test_two_bindings_with_identical_remote_ids_stay_separate(
    server_settings, make_binding
):
    """Keyed on the remote id alone, one Binding's merge deleted another's rows."""
    first = make_binding(
        config=memory_config(batches=[[{"id": "shared", "v": "first"}]]), owner_id="1"
    )
    second = make_binding(
        config=memory_config(batches=[[{"id": "shared", "v": "second"}]]), owner_id="2"
    )
    run_services.run_binding(first, trigger=RunTrigger.INITIAL)
    run_services.run_binding(second, trigger=RunTrigger.INITIAL)

    assert [row["v"] for row in access.sample_rows(first, "events", limit=5)] == [
        "first"
    ]
    assert [row["v"] for row in access.sample_rows(second, "events", limit=5)] == [
        "second"
    ]


def test_concurrent_bindings_lose_no_loads(server_settings, make_binding):
    """Concurrent pipelines sharing a landing table lose loads silently.

    Measured 8-12 lost of 45 with ``errors=0`` every time, because dlt keys its
    staging table on ``(dataset, table_name)`` alone and each load issues an
    unconditional auto-committed DELETE against it. Per-Binding tables measured
    zero lost.

    Only dlt runs on the worker threads — the ORM objects are created up front —
    because the control-plane database here is in-memory sqlite.
    """
    worker_count, rounds = 3, 5
    bindings = [
        make_binding(
            config=memory_config(
                batches=[
                    [{"id": f"w{worker}-r{round_}", "v": f"{worker}:{round_}"}]
                    for round_ in range(rounds)
                ]
            ),
            owner_id=str(worker),
        )
        for worker in range(worker_count)
    ]
    runs = {
        binding.pk: [
            Run.objects.create(binding=binding, trigger=RunTrigger.SCHEDULED)
            for _ in range(rounds)
        ]
        for binding in bindings
    }

    definition = sources.get("memory")
    failures = []

    def work(binding):
        from django_connectors.landing.destination import build_pipeline

        try:
            for run in runs[binding.pk]:
                pipeline = build_pipeline(binding)
                source = definition.build_source(
                    binding=binding, credentials=None, run=run
                )
                instrument_source(
                    source, binding=binding, run=run, source_definition=definition
                )
                load_info = pipeline.run(source)
                if load_info.has_failed_jobs:
                    failures.append(f"{binding.landing_key}: failed jobs")
        except Exception as exc:
            failures.append(f"{binding.landing_key}: {type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=work, args=(binding,)) for binding in bindings]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not failures, failures

    for binding in bindings:
        table = landing_table_name("memory", "events", binding.landing_key)
        landed = _count(server_settings.url, table)
        assert landed == rounds, f"{binding.landing_key} landed {landed}/{rounds}"


# --- deletes ---------------------------------------------------------------


def test_identity_only_tombstone_lands_and_nulls_other_columns(
    server_settings, make_binding
):
    """delete-insert replaces the whole row, on every backend.

    This is why validation refuses a target identity field sourced from outside
    the merge key: it would be NULL on the delete path.
    """
    binding = make_binding(
        config=memory_config(
            batches=[
                [{"id": "1", "v": "a", "extra": "keep"}],
                [tombstone({"id": "1"})],
            ]
        )
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "events", limit=5)
    assert len(rows) == 1
    assert bool(rows[0][DELETED_COLUMN]) is True
    assert rows[0]["v"] is None
    assert rows[0]["extra"] is None


def test_record_updated_at_the_cursor_boundary_survives(server_settings, make_binding):
    """dlt's default dedup key drops this; the library forces primary_key=()."""
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

    assert [row["v"] for row in access.sample_rows(binding, "events", limit=5)] == [
        "UPDATED"
    ]


def test_a_key_duplicated_inside_one_batch_keeps_the_newest_on_every_backend(
    server_settings, make_binding
):
    """In-package dedup is done by the destination, so it is dialect-shaped.

    Without a ``dedup_sort`` hint the merge job orders the ROW_NUMBER() window
    by ``(SELECT NULL)`` and keeps the oldest version — measured identically on
    sqlite, MySQL and PostgreSQL — while the cursor advances past the newest,
    which the source will therefore never re-send.
    """
    binding = make_binding(
        config=memory_config(
            cursor="updated_at",
            batches=[
                [
                    {"id": "1", "updated_at": "2024-01-01T00:00:00Z", "v": "old"},
                    {"id": "1", "updated_at": "2024-01-02T00:00:00Z", "v": "NEW"},
                ],
                [
                    {"id": "1", "updated_at": "2024-01-03T00:00:00Z", "v": "edited"},
                    {
                        "id": "1",
                        "updated_at": "2024-01-04T00:00:00Z",
                        DELETED_COLUMN: True,
                    },
                ],
            ],
        )
    )
    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert [row["v"] for row in access.sample_rows(binding, "events", limit=5)] == [
        "NEW"
    ]

    # And a delete emitted after an edit for the same key must win, or the
    # deletion is dropped and the host keeps a record the provider removed.
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    rows = access.sample_rows(binding, "events", limit=5)
    assert len(rows) == 1
    assert bool(rows[0][DELETED_COLUMN]) is True


# --- pending packages and purge --------------------------------------------


def test_an_extract_stage_pending_package_is_drained_on_every_backend(
    server_settings, make_binding
):
    """`pipeline.load()` cannot see a package that was never normalized.

    A worker killed between extract and normalize leaves one. Every later Run
    then refuses to extract, and a recovery Run that drained nothing used to be
    recorded as succeeded — advancing `last_success_at` on a wedged Binding.
    """
    from django_connectors.landing.destination import build_pipeline

    binding = make_binding(
        config=memory_config(batches=[[{"id": "1", "v": "extracted"}], [{"id": "2"}]])
    )
    definition = sources.get("memory")
    interrupted = Run.objects.create(binding=binding, trigger=RunTrigger.INITIAL)
    pipeline = build_pipeline(binding)
    source = definition.build_source(binding=binding, credentials=None, run=interrupted)
    instrument_source(
        source, binding=binding, run=interrupted, source_definition=definition
    )
    pipeline.extract(source)
    assert pipeline.list_extracted_load_packages()

    run = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    recovery = Run.objects.get(binding=binding, trigger=RunTrigger.RECOVERY)
    assert recovery.status == RunStatus.SUCCEEDED
    assert recovery.dlt_load_ids, "the recovery run landed nothing"

    table = landing_table_name("memory", "events", binding.landing_key)
    assert _count(server_settings.url, table) == 2
    assert not build_pipeline(binding).has_pending_data


def test_purge_from_a_worker_that_never_ran_the_binding_drops_the_tables(
    server_settings, make_binding, tmp_path, settings
):
    """dlt's schema store is local to a pipeline directory; the tables are not.

    Issued from any worker but the one that ran the Binding, the purge resolved
    no schema, dropped nothing, and still stamped `landing_purged_at` — after
    which the pre_delete guard allowed the Binding to be deleted and its rows
    were stranded with a `_connector_binding_id` pointing at nothing.
    """
    from django_connectors.services import retention

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}, {"id": "2"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    table = landing_table_name("memory", "events", binding.landing_key)
    assert _count(server_settings.url, table) == 2

    settings.DJANGO_CONNECTORS = {
        **settings.DJANGO_CONNECTORS,
        "PIPELINES_DIR": str(tmp_path / "second_worker"),
    }

    assert retention.purge_binding_landing(binding)["dropped_tables"] == [table]
    # Asserted against the server, not against the purge's own report — the
    # report is exactly what used to lie.
    assert _columns(server_settings.url, table) == {}


# --- dialect-specific ------------------------------------------------------


def test_oversize_value_fails_the_run_rather_than_wedging_silently(
    server_settings, make_binding
):
    """MySQL-only: dlt maps str to TEXT (65,535 bytes).

    It advertises ``max_text_data_type_length`` of 1GB, so the value is accepted
    at normalize time and rejected at load time, after which dlt retries that
    job forever. PostgreSQL's ``text`` is unbounded, so the failure mode simply
    does not exist there.
    """
    if server_settings.backend != "mysql":
        pytest.skip("TEXT is unbounded on this backend")

    binding = make_binding(
        config=memory_config(batches=[[{"id": "1", "blob": "x" * 70_000}]])
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.FAILED
    assert run.error_message


def test_a_wedged_pipeline_never_reports_a_later_run_as_succeeded(
    server_settings, make_binding
):
    if server_settings.backend != "mysql":
        pytest.skip("requires a backend with a bounded text type")

    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1", "blob": "x" * 70_000}], [{"id": "2"}]]
        )
    )
    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.FAILED

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    if second.status == RunStatus.SUCCEEDED:
        assert second.dlt_load_ids
        assert not set(second.dlt_load_ids) & set(first.dlt_load_ids or [])


# --- full vertical slice ---------------------------------------------------


def test_projection_runs_end_to_end_on_every_backend(server_settings, make_binding):
    """Landing correctness is not enough — the mapping layer reads these rows.

    Booleans, timestamps and JSON all come back differently per backend, so the
    proof that a mapping is portable is a mapping actually running on each.
    """
    from django_connectors.enums import ProjectionRunStatus, ProjectionStatus
    from django_connectors.models import Projection
    from django_connectors.projections.fields import (
        BooleanField,
        DateTimeField,
        JSONField,
        StringField,
    )
    from django_connectors.projections.targets import (
        TargetDefinition,
        register_target,
        unregister_all,
    )
    from django_connectors.services import projections as projection_services

    written = []
    unregister_all()
    register_target(
        TargetDefinition(
            key="events",
            fields={
                "external_id": StringField(required=True),
                "occurred_at": DateTimeField(required=True),
                "active": BooleanField(),
                "payload": JSONField(),
            },
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=lambda records, context: written.extend(records) or len(records),
            supports_scope_replace=True,
        )
    )
    try:
        binding = make_binding(
            config=memory_config(
                batches=[
                    [
                        {
                            "id": "e1",
                            "ts": "2024-03-01T10:30:00Z",
                            "flag": True,
                            "meta": {"plan": "pro"},
                        }
                    ],
                    [tombstone({"id": "e1"})],
                ]
            )
        )
        run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
        assert run.status == RunStatus.SUCCEEDED, run.error_message

        projection = Projection.objects.create(
            binding=binding,
            resource="events",
            target="events",
            name="p",
            mapping={
                "external_id": {"source": "id"},
                "occurred_at": {"source": "ts", "cast": "datetime"},
                "active": {"source": "flag", "cast": "boolean"},
                "payload": {"source": "meta"},
            },
            status=ProjectionStatus.ACTIVE,
        )
        first = projection_services.run_projection(projection, source_run=run)
        assert first.status == ProjectionRunStatus.SUCCEEDED, first.error_message

        record = written[-1]
        assert record.operation == "upsert"
        assert record.identity == {"external_id": "e1"}
        assert record.values["active"] is True
        assert record.values["occurred_at"] == dt.datetime(
            2024, 3, 1, 10, 30, tzinfo=dt.UTC
        )
        assert record.values["payload"] == {"plan": "pro"}

        # And the delete path, which is where a tombstone's nulled columns bite.
        second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
        assert second.status == RunStatus.SUCCEEDED, second.error_message
        replay = projection_services.replay_projection(projection)
        assert replay.status == ProjectionRunStatus.SUCCEEDED, replay.error_message
        assert written[-1].operation == "delete"
        assert written[-1].identity == {"external_id": "e1"}
    finally:
        unregister_all()
