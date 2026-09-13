"""Shared pytest fixtures.

Two dlt behaviours drive almost everything here:

1. dlt keeps pipeline working state in ``$HOME/.dlt`` by default and reads
   ``./.dlt/*.toml`` relative to the *process* working directory. Left alone, a
   test run leaks state between tests and scatters files into the repo. Every
   test therefore gets an isolated ``DLT_DATA_DIR``.

2. dlt never writes to the sqlite file named in the DSN. It emulates datasets as
   separate database files, so ``sqlite:///<tmp>/landing.db`` with dataset
   ``ds`` produces ``<tmp>/landing__ds.db`` (plus ``landing__ds_staging.db``),
   and ``landing.db`` itself stays empty. Assert against the file
   :func:`landing_sqlite_file` names, never the one in the DSN.
"""

import os
from types import SimpleNamespace

import pytest

MYSQL_URL_ENV = "DJANGO_CONNECTORS_TEST_MYSQL_URL"
POSTGRES_URL_ENV = "DJANGO_CONNECTORS_TEST_POSTGRES_URL"

# Real database servers the landing layer is asserted against. sqlite is the
# default tier and covers logic; these cover everything that is dialect-shaped.
SERVER_BACKENDS = {
    "mysql": MYSQL_URL_ENV,
    "postgres": POSTGRES_URL_ENV,
}

SERVER_DATASET = "connectors_landing"


@pytest.fixture(autouse=True)
def dlt_env(tmp_path, monkeypatch):
    """Isolate dlt's working state per test.

    Autouse: a single test that runs a pipeline without this would write into
    the developer's real ``~/.dlt`` and be visible to every later test.
    """
    data_dir = tmp_path / "dlt_data"
    data_dir.mkdir()
    monkeypatch.setenv("DLT_DATA_DIR", str(data_dir))
    # dlt resolves config/secrets TOML relative to the process CWD. Point it at
    # an empty directory so the repo's own files can never be picked up.
    project_dir = tmp_path / "dlt_project"
    project_dir.mkdir()
    monkeypatch.setenv("DLT_PROJECT_DIR", str(project_dir))
    return data_dir


@pytest.fixture
def landing_url(tmp_path):
    """A sqlite landing DSN. The default, no-docker test tier runs on this."""
    return f"sqlite:///{tmp_path / 'landing.db'}"


def landing_sqlite_file(tmp_path, dataset_name, *, staging=False):
    """Path dlt actually writes for a sqlite dataset (not the DSN's own file)."""
    suffix = "_staging" if staging else ""
    return tmp_path / f"landing__{dataset_name}{suffix}.db"


MEMORY_SOURCE = "django_connectors.sources.memory.MemorySource"


@pytest.fixture
def connectors_settings(landing_url, tmp_path, settings):
    """Configure django-connectors against an isolated sqlite landing DB."""
    settings.DJANGO_CONNECTORS = {
        "LANDING_URL": landing_url,
        "LANDING_DATASET": "connectors_landing",
        "PIPELINES_DIR": str(tmp_path / "pipelines"),
        "SOURCES": {"memory": MEMORY_SOURCE},
    }
    return settings.DJANGO_CONNECTORS


@pytest.fixture
def make_connection(db):
    from django.contrib.contenttypes.models import ContentType

    from django_connectors.enums import ConnectionStatus
    from django_connectors.models import Connection

    def factory(owner_id="1", **kwargs):
        return Connection.objects.create(
            owner_content_type=ContentType.objects.get_for_model(ContentType),
            owner_object_id=owner_id,
            provider=kwargs.pop("provider", "memory"),
            auth_backend=kwargs.pop("auth_backend", ""),
            status=kwargs.pop("status", ConnectionStatus.ACTIVE),
            **kwargs,
        )

    return factory


@pytest.fixture
def make_binding(make_connection):
    from django_connectors.models import Binding

    def factory(*, config=None, owner_id="1", connection=None, **kwargs):
        return Binding.objects.create(
            connection=connection or make_connection(owner_id=owner_id),
            source=kwargs.pop("source", "memory"),
            config=config or {},
            **kwargs,
        )

    return factory


def memory_config(
    *, resource="events", batches, primary_key="id", cursor=None, **extra
):
    """Build a MemorySource config for one resource."""
    spec = {"primary_key": primary_key, "batches": batches}
    if cursor:
        spec["cursor"] = cursor
    return {"resources": {resource: spec}, **extra}


@pytest.fixture
def mysql_url():
    """A real MySQL landing DSN, or skip."""
    url = os.environ.get(MYSQL_URL_ENV)
    if not url:
        pytest.skip(f"{MYSQL_URL_ENV} is not set")
    return url


@pytest.fixture(params=sorted(SERVER_BACKENDS))
def server_landing(request):
    """A real database server to land into, one parameter per configured backend.

    The landing layer must not be MySQL-shaped, and sqlite cannot prove that:
    it reports a 9999-character identifier limit where PostgreSQL truncates at
    63, has no server-side concurrency to speak of, and returns timestamps as
    strings. Each backend diverges somewhere that matters — booleans, JSON
    columns, timestamp awareness — so the invariants are asserted against every
    one that is configured.
    """
    backend = request.param
    env = SERVER_BACKENDS[backend]
    url = os.environ.get(env)
    if not url:
        pytest.skip(f"{env} is not set")
    return SimpleNamespace(backend=backend, url=url)


@pytest.fixture
def server_settings(server_landing, tmp_path, settings):
    """Point django-connectors at a real server and warm the dataset.

    Warming is not test scaffolding — the first *concurrent* loads into a fresh
    dataset race on the shared `_dlt_version`/`_dlt_loads`/staging objects
    whatever the table isolation, so production must warm serially too.
    """
    from django_connectors.landing.warm import warm_landing_dataset

    settings.DJANGO_CONNECTORS = {
        "LANDING_URL": server_landing.url,
        "LANDING_DATASET": SERVER_DATASET,
        "PIPELINES_DIR": str(tmp_path / "pipelines"),
        "SOURCES": {"memory": MEMORY_SOURCE},
    }
    warm_landing_dataset()
    return server_landing


# --- projection helpers ----------------------------------------------------


@pytest.fixture(autouse=True)
def clean_target_registry():
    """The target registry is process-global; no test may see another's."""
    from django_connectors.projections.targets import unregister_all

    unregister_all()
    yield
    unregister_all()


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


@pytest.fixture
def writer():
    return RecordingWriter()


@pytest.fixture
def events_target(writer):
    from django_connectors.projections.fields import (
        DateTimeField,
        IntegerField,
        JSONField,
        StringField,
    )
    from django_connectors.projections.targets import TargetDefinition, register_target

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


def land_memory(make_binding, batches, *, resources=None, **kwargs):
    """Land `batches` through a memory Binding and return (binding, run)."""
    from django_connectors.enums import RunTrigger
    from django_connectors.services import runs as run_services

    binding = make_binding(config=memory_config(batches=batches, **kwargs))
    if resources is not None:
        binding.resources = resources
        binding.save(update_fields=["resources"])
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == "succeeded", run.error_message
    return binding, run


def make_projection(binding, mapping=None, *, filters=None, target="events"):
    from django_connectors.enums import ProjectionStatus
    from django_connectors.models import Projection

    return Projection.objects.create(
        binding=binding,
        resource="events",
        target=target,
        name="p",
        mapping=mapping if mapping is not None else BASIC_MAPPING,
        filters=filters or [],
        status=ProjectionStatus.ACTIVE,
    )
