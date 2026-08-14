"""Landing defects that sqlite cannot express.

Every test here is marked `mysql` and skips unless
``DJANGO_CONNECTORS_TEST_MYSQL_URL`` is set. They are not redundant with the
sqlite tier: each covers a failure that only exists on MySQL, and each was an
observed data-loss mechanism rather than a hypothetical.
"""

import threading

import pytest
from sqlalchemy import create_engine, text

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.landing import access
from django_connectors.landing.instrument import instrument_source
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    RUN_ID_COLUMN,
    landing_table_name,
)
from django_connectors.landing.warm import warm_landing_dataset
from django_connectors.models import Run
from django_connectors.registry import sources
from django_connectors.services import runs as run_services
from tests.conftest import MEMORY_SOURCE, memory_config

pytestmark = [pytest.mark.mysql, pytest.mark.django_db]

DATASET = "connectors_landing"


@pytest.fixture
def mysql_settings(mysql_url, tmp_path, settings):
    settings.DJANGO_CONNECTORS = {
        "LANDING_URL": mysql_url,
        "LANDING_DATASET": DATASET,
        "PIPELINES_DIR": str(tmp_path / "pipelines"),
        "SOURCES": {"memory": MEMORY_SOURCE},
    }
    # Cold-start concurrency always races on the shared _dlt_version/_dlt_loads
    # and staging objects, whatever the table isolation. Warm serially first —
    # which is exactly what production must do too.
    warm_landing_dataset()
    return settings.DJANGO_CONNECTORS


def _engine(mysql_url):
    return create_engine(mysql_url)


def test_metadata_columns_are_indexable_varchar(
    mysql_settings, mysql_url, make_binding
):
    """Without a precision hint dlt maps str to TEXT, which MySQL cannot index.

    Indexing a TEXT column without a prefix length is error 1170, and merge is
    ``DELETE ... WHERE EXISTS``, so an unindexable tenant column makes merge
    cost linear in table size — measured at 179s for 10k rows into 60k.
    """
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    table = landing_table_name("memory", "events", binding.landing_key)
    with _engine(mysql_url).connect() as connection:
        rows = connection.execute(
            text(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table"
            ),
            {"schema": DATASET, "table": table},
        ).fetchall()
    types = dict(rows)

    assert types[BINDING_ID_COLUMN] == "varchar"
    assert types[RUN_ID_COLUMN] == "varchar"


def test_identifiers_stay_within_the_mysql_limit(mysql_settings, make_binding):
    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    table = landing_table_name("memory", "events", binding.landing_key)
    assert len(table) <= 64
    assert len(binding.pipeline_name) <= 64
    assert len(binding.schema_name) <= 64


def test_concurrent_bindings_lose_no_loads(mysql_settings, mysql_url, make_binding):
    """The blocker: concurrent pipelines sharing a landing table lose loads silently.

    Measured 8-12 lost of 45 with ``errors=0`` every time, because dlt keys its
    staging table on ``(dataset, table_name)`` alone and each load issues an
    unconditional auto-committed DELETE against it. Per-Binding tables measured
    0 lost of 45.

    Only dlt is exercised on the worker threads — the ORM objects are created up
    front — because the control-plane database here is in-memory sqlite.
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

    source_definition = sources.get("memory")
    failures = []

    def work(binding):
        from django_connectors.landing.destination import build_pipeline

        try:
            for run in runs[binding.pk]:
                pipeline = build_pipeline(binding)
                source = source_definition.build_source(
                    binding=binding, credentials=None, run=run
                )
                instrument_source(
                    source,
                    binding=binding,
                    run=run,
                    source_definition=source_definition,
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

    # Every row of every round must be present — this is the assertion that
    # failed 8-12 times in 45 under the shared-table topology.
    with _engine(mysql_url).connect() as connection:
        for binding in bindings:
            table = landing_table_name("memory", "events", binding.landing_key)
            count = connection.execute(
                text(f"SELECT COUNT(*) FROM `{DATASET}`.`{table}`")
            ).scalar()
            assert count == rounds, f"{binding.landing_key} landed {count}/{rounds}"


def test_oversize_value_fails_the_run_rather_than_wedging_silently(
    mysql_settings, make_binding
):
    """A >64KB string is a poison pill.

    dlt maps str to MySQL TEXT (65,535 bytes) while advertising
    ``max_text_data_type_length`` of 1,073,741,824, so the value is accepted at
    normalize time and rejected at load time. dlt then retries that job forever,
    ``has_pending_data`` stays true, and every later run raises. The Run must
    fail loudly rather than report success.
    """
    binding = make_binding(
        config=memory_config(batches=[[{"id": "1", "blob": "x" * 70_000}]])
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.FAILED
    assert run.error_message


def test_a_wedged_pipeline_does_not_report_a_later_run_as_succeeded(
    mysql_settings, make_binding
):
    """The second run must not silently inherit the first's pending package."""
    binding = make_binding(
        config=memory_config(
            batches=[[{"id": "1", "blob": "x" * 70_000}], [{"id": "2"}]]
        )
    )
    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.FAILED

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    # Either it fails, or it succeeds having genuinely landed its own load ids —
    # never succeeds while reusing ids already attributed to an earlier Run.
    if second.status == RunStatus.SUCCEEDED:
        assert second.dlt_load_ids
        assert not set(second.dlt_load_ids) & set(first.dlt_load_ids or [])


def test_two_bindings_with_identical_remote_ids_stay_separate(
    mysql_settings, make_binding
):
    """Keyed on the remote id alone, one Binding's merge deleted the other's rows."""
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
