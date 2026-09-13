"""Every SourceDefinition must pass the same contract.

Two layers:

* **Static** — no credentials, no network, runs against every source class
  shipped in the package *and* every source a host registers. This is the gate
  the connector contract lives behind: a source that constructs its own
  ``Incremental``, omits a write disposition, or crashes ``validate_config``
  with a ``TypeError`` fails here, not in a customer's Run.
* **Dynamic** — for sources that can run with no provider at all (memory,
  filesystem, sql over sqlite), a canned payload is landed through the real
  instrumentation and the landing invariants are asserted on what arrived.

The enumeration test at the bottom is what makes this a gate rather than a
suite: a source class added to the package without being listed here fails.
"""

import importlib
import inspect
import pkgutil
from types import SimpleNamespace
from typing import ClassVar

import pytest
from django.utils.module_loading import import_string

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.landing import access
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    RUN_ID_COLUMN,
    landing_table_name,
)
from django_connectors.sources.base import SourceDefinition
from django_connectors.sources.conformance import (
    GARBAGE_CONFIGS,
    assert_source_conforms,
    static_problems,
)
from tests.test_sources import (
    SQLITE_ROWS,
    files_config,
    make_sqlite_database,
    sql_config,
    write_jsonl,
)

#: Every source class this package ships. The enumeration test below fails if
#: a class exists on disk that is not listed here.
SHIPPED_SOURCES = (
    "django_connectors.sources.memory.MemorySource",
    "django_connectors.sources.rest.RestSource",
    "django_connectors.sources.sql.SqlSource",
    "django_connectors.sources.filesystem.FilesystemSource",
    "django_connectors.providers.google.gmail.GmailSource",
    "django_connectors.providers.google.sheets.GoogleSheetsSource",
    "django_connectors.providers.microsoft.files.EntraFilesSource",
    "django_connectors.providers.microsoft.excel.EntraExcelSource",
    "django_connectors.providers.salesforce.source.SalesforceSource",
)

SOURCE_PACKAGES = ("django_connectors.sources", "django_connectors.providers")


def _shipped_keys():
    return [import_string(path).key for path in SHIPPED_SOURCES]


# --- static: every shipped source ------------------------------------------


@pytest.mark.parametrize("path", SHIPPED_SOURCES, ids=_shipped_keys())
def test_shipped_source_conforms(path):
    assert_source_conforms(import_string(path)())


def test_every_source_class_on_disk_is_listed():
    """Adding a source without listing it here must fail, or the gate is prose."""
    found = set()
    for package_name in SOURCE_PACKAGES:
        package = importlib.import_module(package_name)
        for info in pkgutil.walk_packages(package.__path__, f"{package_name}."):
            module = importlib.import_module(info.name)
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, SourceDefinition)
                    and obj is not SourceDefinition
                    and obj.__module__ == module.__name__
                    and obj.key
                ):
                    found.add(f"{obj.__module__}.{obj.__qualname__}")
    assert found == set(SHIPPED_SOURCES), (
        f"unlisted: {sorted(found - set(SHIPPED_SOURCES))}; "
        f"missing on disk: {sorted(set(SHIPPED_SOURCES) - found)}"
    )


def test_registered_sources_conform(connectors_settings):
    """What a host runs in its own suite: every key in SOURCES passes."""
    from django_connectors.registry import sources

    for key, definition in sources.all().items():
        assert not static_problems(definition), (key, static_problems(definition))


# --- static: the checks themselves catch what they claim to ----------------


class _Crashing(SourceDefinition):
    key = "crashing"
    provider = "test"

    def validate_config(self, config):
        return config.get("resources")  # AttributeError on a string


class _BuildsItsOwnIncremental(SourceDefinition):
    key = "own_incremental"
    provider = "test"

    def incremental_for(self, resource_name, binding):
        from dlt.extract.incremental import Incremental

        return Incremental(cursor_path="updated_at")


class _BadFlags(SourceDefinition):
    key = "Bad Key"
    provider = ""
    emits_tombstones = 1
    required_extras: ClassVar[dict] = {"pandas": None}
    supported_auth_backends = "static"


def test_static_checks_catch_a_validate_config_that_crashes():
    problems = static_problems(_Crashing())
    assert any("AttributeError" in p for p in problems)
    assert any("must return None" in p for p in problems)


def test_static_checks_catch_a_source_that_constructs_its_own_incremental():
    problems = static_problems(_BuildsItsOwnIncremental())
    assert any("never construct the Incremental" in p for p in problems)


def test_static_checks_catch_bad_identity_and_flags():
    problems = "\n".join(static_problems(_BadFlags()))
    assert "naming convention" in problems
    assert "`provider`" in problems
    assert "`emits_tombstones`" in problems
    assert "`required_extras`" in problems
    assert "`supported_auth_backends`" in problems


def test_assert_helper_names_every_problem_at_once():
    with pytest.raises(AssertionError) as excinfo:
        assert_source_conforms(_BadFlags())
    assert excinfo.value.args[0].count("\n  - ") >= 4


def test_garbage_configs_cover_the_json_shapes_a_binding_can_store():
    """A JSON field stores any JSON value; the fuzz set must include non-objects."""
    kinds = {type(item).__name__ for item in GARBAGE_CONFIGS}
    assert {"NoneType", "dict", "list", "str", "int"} <= kinds


# --- dynamic: offline-capable sources land with the invariants -------------


def _offline_cases():
    def memory(tmp_path):
        return (
            "memory",
            {
                "resources": {
                    "events": {
                        "primary_key": "id",
                        "batches": [
                            [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}],
                        ],
                    }
                }
            },
            "events",
            ("id",),
        )

    def filesystem(tmp_path):
        write_jsonl(
            tmp_path / "in" / "a.jsonl", [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}]
        )
        return "filesystem", files_config(tmp_path / "in"), "events", ("id",)

    def sql(tmp_path):
        url = make_sqlite_database(tmp_path, SQLITE_ROWS)
        return "sql", sql_config(url), "orders", ("id",)

    return {"memory": memory, "filesystem": filesystem, "sql": sql}


OFFLINE_SOURCES = {
    "memory": "django_connectors.sources.memory.MemorySource",
    "filesystem": "django_connectors.sources.filesystem.FilesystemSource",
    "sql": "django_connectors.sources.sql.SqlSource",
}


@pytest.fixture
def offline_settings(connectors_settings, settings):
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], **OFFLINE_SOURCES},
    }
    return settings.DJANGO_CONNECTORS


@pytest.mark.parametrize("case", sorted(_offline_cases()))
def test_offline_source_declares_disposition_and_instruments_cleanly(
    case, offline_settings, make_binding, tmp_path
):
    """Before instrumentation: disposition stated. After: every invariant applied."""
    from django_connectors.landing.instrument import instrument_source
    from django_connectors.registry import sources

    source_key, config, resource_name, pk = _offline_cases()[case](tmp_path)
    binding = make_binding(source=source_key, config=config)
    definition = sources.get(source_key)
    run = SimpleNamespace(id="00000000-0000-0000-0000-000000000000")

    source = definition.build_source(binding=binding, credentials=None, run=run)
    assert type(source).__name__ == "DltSource"
    for resource in source.resources.values():
        # dlt defaults this hint to "append" when omitted; a source must say it.
        assert "write_disposition" in resource._hints, (
            f"{source_key}:{resource.name} does not state write_disposition"
        )

    instrument_source(source, binding=binding, run=run, source_definition=definition)
    assert source.max_table_nesting == 0
    resource = source.resources[resource_name]
    hints = resource._hints
    assert tuple(hints["primary_key"]) == (BINDING_ID_COLUMN, *pk)
    for column, spec in (hints.get("columns") or {}).items():
        assert not spec.get("hard_delete"), f"{column} carries hard_delete"


@pytest.mark.parametrize("case", sorted(_offline_cases()))
def test_offline_source_lands_rows_carrying_tenant_metadata(
    case, offline_settings, make_binding, tmp_path
):
    from django_connectors.services import runs as run_services

    source_key, config, resource_name, pk = _offline_cases()[case](tmp_path)
    binding = make_binding(source=source_key, config=config)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    relation = access.binding_relation(binding, resource_name)
    rows = list(access.iter_rows(relation, binding=binding))
    assert rows, "nothing landed"
    for row in rows:
        assert row[BINDING_ID_COLUMN] == str(binding.id)
        assert row[RUN_ID_COLUMN] == str(run.id)
        assert row[DELETED_COLUMN] in (True, False, 0, 1)

    # No child tables: nesting is off, so dicts/lists land as JSON columns.
    table = landing_table_name(source_key, resource_name, binding.landing_key)
    schema = access.binding_dataset(binding).schema
    children = [name for name in schema.tables if name.startswith(f"{table}__")]
    assert not children, children

    # Merge identity in the landed schema leads with the binding id.
    merge_key = {
        name
        for name, column in schema.tables[table]["columns"].items()
        if column.get("primary_key")
    }
    assert merge_key == {BINDING_ID_COLUMN, *pk}


def test_tombstone_source_marks_the_row_deleted_and_keeps_its_key(
    offline_settings, make_binding
):
    """emits_tombstones=True means a later batch can delete, and the key survives."""
    from django_connectors.registry import sources
    from django_connectors.services import runs as run_services
    from django_connectors.sources.memory import tombstone

    assert sources.get("memory").emits_tombstones is True
    binding = make_binding(
        source="memory",
        config={
            "resources": {
                "events": {
                    "primary_key": "id",
                    "batches": [
                        [{"id": "1", "v": "a"}],
                        [tombstone({"id": "1"})],
                    ],
                }
            }
        },
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    rows = list(
        access.iter_rows(access.binding_relation(binding, "events"), binding=binding)
    )
    assert len(rows) == 1
    assert rows[0]["id"] == "1"
    assert bool(rows[0][DELETED_COLUMN]) is True
