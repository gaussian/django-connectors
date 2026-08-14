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
| mysql | `uv run --all-extras pytest -m mysql` | defects sqlite cannot express |
| example | `cd example && python manage.py demo` | the host-integration story end to end |

`@pytest.mark.mysql` tests skip unless `DJANGO_CONNECTORS_TEST_MYSQL_URL` is
set. They are not optional polish — three verified data-loss defects (silent
load loss when pipelines share a landing table, unindexed merge cost, and
`>64KB` `TEXT` values that wedge a pipeline permanently) are **invisible on
sqlite**. Locally:

```bash
docker run -d --name dc-mysql -e MYSQL_ROOT_PASSWORD=connectors \
  -e MYSQL_DATABASE=connectors_landing -p 33062:3306 mysql:8.4

DJANGO_CONNECTORS_TEST_MYSQL_URL=mysql+pymysql://root:connectors@127.0.0.1:33062/connectors_landing \
  uv run --all-extras pytest -m mysql
```

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
