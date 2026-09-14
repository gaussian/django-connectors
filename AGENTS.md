# django-connectors — agent guide

A Django framework for connecting applications to third-party systems, with
support for authentication, polling, webhooks, incremental sync, and data
ingestion via dlt. Django library published to PyPI.

## Repo shape

- Source: `django_connectors/`
- Tests: `tests/` — run with `uv run --all-extras pytest`. See "Test tiers" below.
- Lint + format: `uv run --all-extras ruff check django_connectors/ tests/ example/` and
  `ruff format --check django_connectors/ tests/ example/` (config in `pyproject.toml`).
  `example/` is linted because hosts copy it verbatim.
- Default working branch: `develop`. Releases flow `develop` → `main`.

## Test tiers

Three tiers, all gated behind the aggregate `ci` check:

| Tier | Command | Covers |
| --- | --- | --- |
| default | `uv run --all-extras pytest` | everything; landing runs on sqlite, no docker needed |
| minimal | `uv run pytest` | proves the package works with **no extras installed** |
| serverdb | `uv run --all-extras pytest -m serverdb` | defects sqlite cannot express, on MySQL **and** PostgreSQL |
| example | `cd example && python manage.py demo` | the host-integration story end to end |

`@pytest.mark.serverdb` tests are parameterized over every configured backend
and skip the ones that are not; set `DJANGO_CONNECTORS_TEST_MYSQL_URL` and/or
`DJANGO_CONNECTORS_TEST_POSTGRES_URL`. They are not optional polish. sqlite
reports a 9999-character identifier limit where PostgreSQL truncates at **63**,
has no meaningful server-side concurrency, and returns timestamps as strings —
so silent load loss under concurrent pipelines, identifier truncation, unindexed
merge cost and dialect type round-trips are all invisible without them.

The landing layer must stay dialect-agnostic: PostgreSQL is a supported target,
not a hypothetical one. Run both locally:

```bash
docker run -d --name dc-mysql -e MYSQL_ROOT_PASSWORD=connectors \
  -e MYSQL_DATABASE=connectors_landing -p 33062:3306 mysql:8.4
docker run -d --name dc-postgres -e POSTGRES_PASSWORD=connectors \
  -e POSTGRES_DB=connectors_landing -p 55432:5432 postgres:16

DJANGO_CONNECTORS_TEST_MYSQL_URL=mysql+pymysql://root:connectors@127.0.0.1:33062/connectors_landing \
DJANGO_CONNECTORS_TEST_POSTGRES_URL=postgresql+psycopg2://postgres:connectors@127.0.0.1:55432/connectors_landing \
  uv run --all-extras pytest -m serverdb
```

Two connector suites ride inside the default tier. `tests/test_source_conformance.py`
holds every shipped `SourceDefinition` to the connector contract (add a case for
a new source, or that test fails). `tests/test_recorded.py` replays recorded
provider exchanges from `tests/cassettes/`; with no cassette it skips and prints
the recording command. Record with `--record-mode=rewrite` and the
`DJANGO_CONNECTORS_RECORD_*` variables, then run the module again so the
credential scanner checks what was written. See `docs/TESTING.md`.

The `minimal` tier matters because `--all-extras` installs every extra, so it
never exercises the "this extra is absent" path that the whole optional
dependency design depends on. **Locally it is vacuous by default**: `uv run`
reuses whatever virtualenv `uv sync --all-extras` populated. To reproduce what
CI does, point it at a clean environment:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/dc-minimal uv run pytest
```

This caught a real failure: a test module importing `rest_framework` at module
scope, which would have failed collection in that CI job.

## Branching & releases

- `main` is protected: PRs only, and all checks must pass before merge.
- `develop` is the integration branch and is where version bumps land.
- Publishing to PyPI is automatic once a `develop` → `main` PR merges (a tag is
  cut from the version already in the files, then `publish.yml` ships it). The
  release workflow does **not** bump the version — it only reads it.

## Opening PRs & versioning

The version is a static string (`pyproject.toml`, `django_connectors/__init__.py`,
`uv.lock`) and is **not** bumped automatically on merge — it must be bumped
deliberately, or no release is cut.

**Follow the `create-merge-pr` skill** (`.agents/skills/create-merge-pr/`) for the
full PR workflow, including when and how to bump the version.
