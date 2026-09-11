"""The connector contract, applied uniformly to every source this repo ships.

The provider connectors are mock-tested with zero network, and their own
modules are thorough about *provider* behaviour — pagination, throttling, delta
tokens, cell types. What none of them does is assert the same things about all
of them, and a rule that holds for four connectors and not the fifth is exactly
the rule nobody notices.

So the contract is a suite, and it lives in
``django_connectors.testing.conformance`` — in the wheel, not here, because it
applies to host-defined SourceDefinitions too and a suite that only exists in
this repository can only be run against the connectors this repository happens
to ship.

Two halves, split by what they need:

* **This module** runs the credential-free half over every shipped source, and
  gates the list against the filesystem so a new connector cannot silently opt
  out.
* **Each connector's own module** runs the landing half, because that needs the
  in-process fake that module already has. One fake per provider, not one per
  suite: a second copy would be a second belief about the provider, drifting
  independently of the first. This module asserts that each of those tests
  exists, by name.
"""

import ast
import importlib.util
import pathlib
from typing import ClassVar

import pytest

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.exceptions import ConfigurationError
from django_connectors.landing.naming import DELETED_COLUMN
from django_connectors.providers.google.gmail import GmailSource
from django_connectors.providers.google.sheets import GoogleSheetsSource
from django_connectors.providers.microsoft.excel import EntraExcelSource
from django_connectors.providers.microsoft.files import EntraFilesSource
from django_connectors.providers.salesforce.source import SalesforceSource
from django_connectors.services import runs as run_services
from django_connectors.sources.filesystem import FilesystemSource
from django_connectors.sources.memory import MemorySource, tombstone
from django_connectors.sources.rest import RestSource
from django_connectors.sources.sql import SqlSource
from django_connectors.testing import conformance
from tests.conftest import memory_config

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
PACKAGE_ROOT = REPO_ROOT / "django_connectors"


class Case:
    """One connector, plus the smallest input the credential-free half needs."""

    def __init__(self, definition, *, invalid_configs):
        self.definition = definition
        #: Configs the source must refuse with ConfigurationError. Supplied per
        #: connector rather than invented generically: a source with no required
        #: keys legitimately accepts ``{}``, so "reject an empty dict" would be
        #: wrong for it and vacuous for everyone else.
        self.invalid_configs = invalid_configs

    @property
    def key(self):
        return self.definition.key

    def __repr__(self):
        return self.key


CASES = [
    Case(MemorySource(), invalid_configs=[{}, {"resources": {"e": {"batches": 1}}}]),
    Case(
        RestSource(),
        invalid_configs=[{}, {"base_url": "https://api.test/"}, {"resources": {}}],
    ),
    Case(SqlSource(), invalid_configs=[{}, {"url": "sqlite:///x.db"}]),
    Case(FilesystemSource(), invalid_configs=[{}, {"resources": {}}]),
    Case(
        GmailSource(),
        invalid_configs=[
            {"user_id": "someone/else"},
            {"message_format": "raw"},
            {"label_ids": "INBOX"},
        ],
    ),
    Case(GoogleSheetsSource(), invalid_configs=[{}, {"spreadsheet_id": "s"}]),
    Case(EntraFilesSource(), invalid_configs=[{}, {"drive_id": ""}]),
    Case(EntraExcelSource(), invalid_configs=[{}, {"drive_id": ""}]),
    Case(SalesforceSource(), invalid_configs=[{}, {"objects": {}}]),
]

CASES_BY_KEY = {case.key: case for case in CASES}

#: Where each connector's landing half lives. The test is named
#: ``test_<key>_conformance`` in that file, and the gate below checks it is
#: really there — the fakes cannot move here without duplicating them.
LANDING_SUITES = {
    "memory": "tests/test_source_conformance.py",
    "rest": "tests/test_sources.py",
    "sql": "tests/test_sources.py",
    "filesystem": "tests/test_sources.py",
    "gmail": "tests/test_providers_google.py",
    "google_sheets": "tests/test_providers_google.py",
    "entra_files": "tests/test_providers_microsoft.py",
    "entra_excel": "tests/test_providers_microsoft.py",
    "salesforce": "tests/test_providers_salesforce.py",
}


# --- the suite covers every source, and cannot quietly stop --------------


def _shipped_source_keys():
    """Every ``key`` declared by a SourceDefinition subclass in the package.

    Read from the filesystem rather than from the registry: a registry contains
    only what a *host* configured, so a source can ship and be registered by
    nobody — which is precisely the source most likely to go unchecked.
    """
    known_bases = {"SourceDefinition"} | {
        type(case.definition).__name__ for case in CASES
    }
    keys = set()
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        if path.parent.name == "testing":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.ClassDef):
                continue
            bases = {
                base.id if isinstance(base, ast.Name) else getattr(base, "attr", "")
                for base in node.bases
            }
            if not bases & known_bases:
                continue
            for statement in node.body:
                if (
                    isinstance(statement, ast.Assign)
                    and any(
                        isinstance(target, ast.Name) and target.id == "key"
                        for target in statement.targets
                    )
                    and isinstance(statement.value, ast.Constant)
                    and statement.value.value
                ):
                    keys.add(statement.value.value)
    return keys


def test_every_source_the_library_ships_has_a_conformance_case():
    shipped = _shipped_source_keys()
    assert shipped, "found no SourceDefinition subclasses at all"
    missing = sorted(shipped - set(CASES_BY_KEY))
    assert not missing, (
        f"sources {missing} ship with no conformance case. Add one to CASES in "
        f"this module — the contract is not optional for a new connector."
    )


def test_every_source_also_has_a_landing_conformance_test():
    """The credential-free half alone would let a connector land nonsense."""
    missing = sorted(set(CASES_BY_KEY) - set(LANDING_SUITES))
    assert not missing, f"no landing suite declared for {missing}"

    for key, relative in sorted(LANDING_SUITES.items()):
        path = REPO_ROOT / relative
        assert path.exists(), f"{relative} does not exist"
        assert f"def test_{key}_conformance(" in path.read_text(), (
            f"{relative} declares no `test_{key}_conformance`. The landing half "
            f"of the suite lives beside the fake it needs; add it there."
        )


# --- the credential-free half, over every source -------------------------


@pytest.mark.parametrize("case", CASES, ids=repr)
def test_the_definition_honours_the_contract(case):
    failures = conformance.check_definition(
        case.definition, invalid_configs=case.invalid_configs
    )
    assert failures == [], "\n".join(failures)


@pytest.mark.parametrize("case", CASES, ids=repr)
def test_validate_config_raises_configuration_error_and_not_something_else(case):
    """Restated on its own because the save path keys on the exception *type*.

    ``Binding.clean()`` and the API serializer catch ``ConnectorError`` and let
    anything else through, so a source raising ``ValueError`` produces a 500
    where it should have produced a field error.
    """
    for config in case.invalid_configs:
        with pytest.raises(ConfigurationError):
            case.definition.validate_config(config)


# --- the suite must itself be able to fail --------------------------------


def test_the_suite_notices_a_source_that_breaks_the_contract():
    """A conformance suite nothing can fail is a suite that proves nothing."""

    class Sloppy:
        key = "sloppy"
        required_extras: ClassVar[list] = ["pandas"]  # not a dict
        emits_tombstones = "yes"  # not a bool
        supported_auth_backends = "static"  # a string, not a sequence of keys

        def validate_config(self, config):
            return None  # accepts anything

    failures = conformance.check_definition(Sloppy(), invalid_configs=[{}])
    assert len(failures) == 4, failures
    assert any("required_extras" in failure for failure in failures)
    assert any("emits_tombstones" in failure for failure in failures)
    assert any("supported_auth_backends" in failure for failure in failures)
    assert any("accepted a config" in failure for failure in failures)


def test_a_source_with_no_invalid_configs_is_reported_rather_than_passing():
    """Silence is not conformance: supplying nothing must not look like success."""
    failures = conformance.check_definition(MemorySource(), invalid_configs=[])
    assert any("never proven to reject" in failure for failure in failures)


@pytest.fixture
def load_source_module(tmp_path):
    """Import a throwaway source module from disk, and make it introspectable.

    Registered in ``sys.modules`` because ``inspect.getsourcefile`` resolves a
    class's file through it — without that the conformance checks would find no
    source to parse and report the deliberately-broken module as clean, which
    is the failure mode these tests exist to rule out.
    """
    import sys

    loaded_names = []

    def load(name, body):
        module = tmp_path / f"{name}.py"
        module.write_text(body)
        spec = importlib.util.spec_from_file_location(name, module)
        loaded = importlib.util.module_from_spec(spec)
        sys.modules[name] = loaded
        loaded_names.append(name)
        spec.loader.exec_module(loaded)
        return loaded

    yield load

    for name in loaded_names:
        sys.modules.pop(name, None)


def test_an_omitted_write_disposition_is_caught_at_the_call_site(load_source_module):
    """The only place the omission is still visible.

    ``dlt.resource()`` defaults the hint to ``"append"`` rather than to None, so
    a resource that omits it is byte-identical at runtime to one that states
    ``"append"`` deliberately. Nothing observable distinguishes them; the call
    site does.
    """
    loaded = load_source_module(
        "sloppy_source",
        "from django_connectors.sources.base import SourceDefinition\n"
        "class Sloppy(SourceDefinition):\n"
        "    key = 'sloppy'\n"
        "    def build_source(self, *, binding, credentials, run):\n"
        "        import dlt\n"
        "        return dlt.resource(lambda: [], name='x', primary_key='id')()\n",
    )
    failures = conformance.check_definition(
        loaded.Sloppy(), invalid_configs=[{"impossible": True}]
    )
    assert any("write_disposition" in failure for failure in failures), failures


def test_a_module_scope_dlt_import_is_caught(load_source_module):
    loaded = load_source_module(
        "eager_source",
        "import dlt\n"
        "from django_connectors.sources.base import SourceDefinition\n"
        "class Eager(SourceDefinition):\n"
        "    key = 'eager'\n",
    )
    failures = conformance.check_definition(
        loaded.Eager(), invalid_configs=[{"impossible": True}]
    )
    assert any("module-scope dlt import" in failure for failure in failures), failures


def test_a_subclass_does_not_escape_its_parents_module(load_source_module):
    """A host points a shipped connector at a sandbox by subclassing it.

    Checking only the subclass's own module would then parse an almost empty
    file and report conformance.
    """
    loaded = load_source_module(
        "subclassed_source",
        "import dlt\n"
        "from django_connectors.sources.base import SourceDefinition\n"
        "class Parent(SourceDefinition):\n"
        "    key = 'parent'\n",
    )

    class Child(loaded.Parent):
        key = "child"

    failures = conformance.check_definition(
        Child(), invalid_configs=[{"impossible": True}]
    )
    assert any("module-scope dlt import" in failure for failure in failures), failures


def test_incremental_kwargs_that_construct_an_incremental_are_caught(tmp_path):
    """dlt strips the incremental from a *bound* resource's signature.

    A source that builds its own is beyond the library's reach entirely, and
    the two settings the library forces each drop records with no error.
    """
    from dlt.extract.incremental import Incremental

    class BuildsItsOwn(MemorySource):
        key = "builds_its_own"

        def incremental_for(self, resource_name, binding):
            return Incremental(cursor_path="updated_at")

    failures = conformance._check_incremental_kwargs(
        "builds_its_own", BuildsItsOwn(), "events", None, Incremental
    )
    assert any("returned an Incremental" in failure for failure in failures), failures


def test_incremental_kwargs_setting_the_dedup_key_are_caught():
    """The library forces it empty, so setting it here is silently discarded.

    dlt's default drops a record updated at exactly the stored cursor value.
    """
    from dlt.extract.incremental import Incremental

    class SetsDedupKey(MemorySource):
        key = "sets_dedup_key"

        def incremental_for(self, resource_name, binding):
            return {"cursor_path": "updated_at", "primary_key": "id"}

    failures = conformance._check_incremental_kwargs(
        "sets_dedup_key", SetsDedupKey(), "events", None, Incremental
    )
    assert any("primary_key" in failure for failure in failures), failures


# --- tombstones ------------------------------------------------------------


def test_a_source_claiming_tombstones_emits_one_carrying_only_the_merge_key():
    """``delete-insert`` merge replaces the whole row.

    Every column a tombstone omits lands as NULL, which is why a target
    identity field sourced from outside the merge key is None on the delete
    path — and why Projection validation refuses that configuration up front.
    """
    assert MemorySource().emits_tombstones is True
    assert (
        conformance.check_tombstone_shape(
            tombstone({"id": "1"}), merge_key_columns=["id"]
        )
        == []
    )


def test_a_tombstone_carrying_a_payload_column_is_reported():
    failures = conformance.check_tombstone_shape(
        {"id": "1", DELETED_COLUMN: True, "name": "still here"},
        merge_key_columns=["id"],
    )
    assert any("non-key column" in failure for failure in failures), failures


def test_a_tombstone_missing_the_deleted_flag_is_reported():
    failures = conformance.check_tombstone_shape({"id": "1"}, merge_key_columns=["id"])
    assert any(DELETED_COLUMN in failure for failure in failures), failures


# --- the landing half, for the one source that needs no fake ---------------


@pytest.mark.django_db
def test_memory_conformance(connectors_settings, make_binding):
    from django_connectors.models import Run
    from django_connectors.registry import sources

    definition = sources.get("memory")
    binding = make_binding(
        config=memory_config(
            batches=[
                [{"id": "1", "meta": {"nested": "value"}}],
                [tombstone({"id": "1"})],
            ]
        )
    )

    built = conformance.check_built_source(
        definition,
        binding=binding,
        credentials=None,
        run=Run.objects.create(binding=binding),
    )
    assert built == [], "\n".join(built)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    landed = conformance.check_landing_invariants(
        binding, expected_resources=["events"]
    )
    assert landed == [], "\n".join(landed)


@pytest.mark.django_db
def test_the_landing_half_notices_a_missing_tenant_column(
    connectors_settings, make_binding, monkeypatch
):
    """The `add_map` arity bug writes NULL binding ids with no error at all."""
    from django_connectors.landing import instrument

    monkeypatch.setattr(
        instrument,
        "METADATA_COLUMN_HINTS",
        {
            key: value
            for key, value in instrument.METADATA_COLUMN_HINTS.items()
            if key != instrument.RUN_ID_COLUMN
        },
    )
    monkeypatch.setattr(
        instrument,
        "make_metadata_injector",
        lambda binding_id, run_id: (
            lambda row: {
                **row,
                instrument.BINDING_ID_COLUMN: binding_id,
                DELETED_COLUMN: row.get(DELETED_COLUMN, False),
            }
        ),
    )

    binding = make_binding(config=memory_config(batches=[[{"id": "1"}]]))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    failures = conformance.check_landing_invariants(binding)
    assert any("_connector_run_id" in failure for failure in failures), failures
