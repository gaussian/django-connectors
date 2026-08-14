# django-connectors — agent guide

A Django framework for connecting applications to third-party systems, with
support for authentication, polling, webhooks, incremental sync, and data
ingestion via dlt. Django library published to PyPI.

## Repo shape

- Source: `django_connectors/`
- Tests: `tests/` — run with `uv run --all-extras pytest`
- Lint + format: `uv run --all-extras ruff check django_connectors/ tests/` and `ruff format --check django_connectors/ tests/` (config in `pyproject.toml`)
- Default working branch: `develop`. Releases flow `develop` → `main`.

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
