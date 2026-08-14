# django-connectors

A Django framework for connecting applications to third-party systems, with
support for authentication, polling, webhooks, incremental sync, and data
ingestion via [dlt](https://dlthub.com/).

## Installation

```bash
pip install django-connectors
```

Then add it to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    ...
    "django_connectors",
]
```

## Development

This project uses [uv](https://docs.astral.sh/uv/).

```bash
uv sync --all-extras

uv run --all-extras pytest
uv run --all-extras ruff check django_connectors/ tests/
uv run --all-extras ruff format --check django_connectors/ tests/
```

`develop` is the working branch; releases flow `develop` → `main` and publish to
PyPI automatically. See [AGENTS.md](AGENTS.md) for the full workflow.

## License

MIT — see [LICENSE](LICENSE).
