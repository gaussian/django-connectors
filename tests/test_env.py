"""Phase 0: prove the landing layer can execute at all.

Before this, the package declared a bare ``dlt`` dependency, which resolves
without SQLAlchemy — so no landing destination could be constructed and not one
line of the ingestion layer could run. These tests fail loudly if that regresses.
"""

from tests.conftest import landing_sqlite_file


def test_sqlalchemy_destination_dependencies_are_installed():
    """`dlt[sqlalchemy]`, not bare `dlt` — alembic included."""
    import alembic  # noqa: F401
    import sqlalchemy

    assert sqlalchemy.__version__ >= "1.4"


def test_destination_accepts_wrapped_credentials(landing_url):
    """The configured DSN must always be wrapped in ``SqlalchemyCredentials``.

    Passing the raw DSN string instead is not reliable: it constructs, and even
    answers ``capabilities()``, but then fails inside ``dlt.pipeline()`` —
    observed as ``AttributeError: 'str' object has no attribute
    'get_dialect_class'`` in one context and ``ConfigFieldMissingException`` in
    another, depending on what dlt has already resolved in the process. The
    failure mode is not stable enough to assert on, which is precisely why the
    landing layer must never hand dlt a bare string.
    """
    import dlt
    from dlt.destinations import sqlalchemy as sqlalchemy_destination
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    pipeline = dlt.pipeline(
        pipeline_name="wrapped_credentials",
        destination=sqlalchemy_destination(
            credentials=SqlalchemyCredentials(landing_url)
        ),
        dataset_name="ds",
    )
    assert pipeline.destination is not None


def test_merge_strategy_support_is_delete_insert_only(landing_url):
    """`upsert` is unavailable on the only MySQL-capable destination.

    The landing layer must emit ``delete-insert`` explicitly rather than relying
    on dlt's default, which would break the moment the default changed.
    """
    from dlt.destinations import sqlalchemy as sqlalchemy_destination
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    caps = sqlalchemy_destination(
        credentials=SqlalchemyCredentials(landing_url)
    ).capabilities()

    assert "delete-insert" in caps.supported_merge_strategies
    assert "upsert" not in caps.supported_merge_strategies
    # MySQL reports 0/1 for booleans, so landed tombstone flags read back as int.
    assert caps.supports_native_boolean is False


def test_pipeline_lands_and_reads_back(tmp_path, landing_url):
    """A trivial end-to-end pipeline, asserted against the file dlt really writes."""
    import dlt
    from dlt.destinations import sqlalchemy as sqlalchemy_destination
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    dataset_name = "connectors_landing"
    pipeline = dlt.pipeline(
        pipeline_name="env_smoke",
        destination=sqlalchemy_destination(
            credentials=SqlalchemyCredentials(landing_url)
        ),
        dataset_name=dataset_name,
    )
    load_info = pipeline.run(
        dlt.resource(
            [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}],
            name="events",
            write_disposition={"disposition": "merge", "strategy": "delete-insert"},
            primary_key="id",
        )
    )

    assert not load_info.has_failed_jobs
    assert load_info.loads_ids

    rows = pipeline.dataset()["events"].fetchall()
    assert len(rows) == 3

    # dlt does not put the dataset in the file the DSN names. It emulates each
    # dataset as a sibling database file and attaches it, so assertions against
    # the DSN's own file silently pass against an empty database.
    assert landing_sqlite_file(tmp_path, dataset_name).exists()
    assert landing_sqlite_file(tmp_path, dataset_name, staging=True).exists()
    assert _table_names(tmp_path / "landing.db") == set()
    assert "events" in _table_names(landing_sqlite_file(tmp_path, dataset_name))


def _table_names(sqlite_path):
    import sqlite3

    if not sqlite_path.exists():
        return set()
    with sqlite3.connect(sqlite_path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {name for (name,) in rows}


def test_dlt_state_is_isolated_to_the_test(dlt_env, landing_url):
    """The autouse fixture must actually redirect dlt's working directory.

    Without this, pipelines write into the developer's real ``~/.dlt`` and leak
    restored state (including incremental cursors) between tests.
    """
    import os

    import dlt
    from dlt.destinations import sqlalchemy as sqlalchemy_destination
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    assert os.environ["DLT_DATA_DIR"] == str(dlt_env)

    pipeline = dlt.pipeline(
        pipeline_name="isolation_check",
        destination=sqlalchemy_destination(
            credentials=SqlalchemyCredentials(landing_url)
        ),
        dataset_name="ds",
    )
    assert pipeline.pipelines_dir.startswith(str(dlt_env))
