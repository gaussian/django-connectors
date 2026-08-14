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

import pytest

MYSQL_URL_ENV = "DJANGO_CONNECTORS_TEST_MYSQL_URL"


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


@pytest.fixture
def mysql_url():
    """A real MySQL landing DSN, or skip.

    Guards the tier that covers defects sqlite cannot express: silent load loss
    under concurrent pipelines, unindexed merge cost, and >64KB TEXT poison
    pills.
    """
    url = os.environ.get(MYSQL_URL_ENV)
    if not url:
        pytest.skip(f"{MYSQL_URL_ENV} is not set")
    return url
