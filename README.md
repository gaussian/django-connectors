# django-connectors

Connect Django applications to third-party APIs, SaaS platforms, files, databases
and warehouses — with pluggable authentication, [dlt](https://dlthub.com/)-powered
synchronization, webhooks, and customer-configurable data projections.

> **Status: v0.1.** The service layer is the intended integration surface. The
> DRF API and the provider connectors are provisional and may change.

## What it does

```
  external system  →  Connection / Binding  →  dlt  →  landing tables
                                                            ↓
                                                       Projection
                                                            ↓
                                              your TargetDefinition + writer
                                                            ↓
                                                     your Django models
```

Source data lands first, in a source-shaped form, and is only then mapped into
shapes your application declares. That boundary is what lets a mapping change be
replayed without re-fetching from the provider, and a failed write to your models
be retried without touching the provider at all.

**The library never imports your models.** You declare a *shape* and a function
that persists it; it never learns what that function does.

## Install

```bash
pip install django-connectors
```

```python
INSTALLED_APPS = [
    "django.contrib.contenttypes",  # required: Connection.owner is a GenericForeignKey
    ...
    "django_connectors",
]

DJANGO_CONNECTORS = {
    # A SQLAlchemy DSN, NOT a Django DATABASES alias — it is reached only
    # through dlt, which makes routing an ORM model there impossible.
    "LANDING_URL": "mysql+pymysql://user:pw@host:3306/connectors_landing",
    "SOURCES": {"rest": "django_connectors.sources.rest.RestSource"},
}
```

Extras: `mysql`, `drf`, `celery`, `allauth`, `secrets`, `sql`, `csv`, `parquet`,
`s3`, `google`, `microsoft`. Installing one never enables behaviour by itself —
the corresponding source or backend must also be named in the setting.

## Declare a target

In any installed app's `connectors.py` (auto-discovered, like `admin.py`):

```python
from django_connectors import (
    DateTimeField, JSONField, StringField, TargetDefinition, register_target,
)

def event_writer(records, context):
    """Must be idempotent per identity: a failed batch retries the whole run."""
    for record in records:
        if record.operation == "delete":
            Event.objects.filter(external_id=record.identity["external_id"]).delete()
            continue
        Event.objects.update_or_create(
            team_id=context.owner_object_id,
            external_id=record.identity["external_id"],
            defaults=record.values,
        )
    return len(records)

register_target(TargetDefinition(
    key="events",
    fields={
        "external_id": StringField(required=True),
        "occurred_at": DateTimeField(required=True),
        "type": StringField(required=True),
        "payload": JSONField(),
    },
    identity_fields=("external_id",),
    identity_scope="owner",   # required: decides whether two tenants may collide
    writer=event_writer,
))
```

Your customers then map landed columns onto those fields declaratively — no
Python — and the library validates, previews and executes the mapping.

## Try it

`example/` is a runnable Django project demonstrating the whole flow with no
credentials required:

```bash
cd example
python manage.py migrate
python manage.py demo
```

## Sources

Built in: `memory` (a test driver with injectable failure modes), `rest`
(config-driven, over `dlt.sources.rest_api`), `sql` (warehouses and databases),
`filesystem` (JSONL/CSV/Parquet). Provider connectors for Gmail, Google Sheets,
Microsoft/Entra files and Excel, and Salesforce ship under
`django_connectors.providers` — see their module docstrings for what is and is
not verified against a live provider.

Writing your own means subclassing `SourceDefinition` and returning a dlt source.

## Documentation

- [docs/quickstart.md](docs/quickstart.md) — end to end in ten minutes
- [docs/architecture.md](docs/architecture.md) — why the pieces are shaped as they are
- [docs/operations.md](docs/operations.md) — deploying, MySQL, concurrency, retention
- [AGENTS.md](AGENTS.md) — development workflow and test tiers

## Development

```bash
uv sync --all-extras
uv run --all-extras pytest
uv run --all-extras ruff check django_connectors/ tests/
uv run --all-extras ruff format django_connectors/ tests/
```

Three test tiers — default (sqlite, no docker), minimal (no extras installed),
and MySQL-backed. See [AGENTS.md](AGENTS.md); the MySQL tier is not optional
polish, it covers data-loss modes that are invisible on sqlite.

`develop` is the working branch; releases flow `develop` → `main` and publish to
PyPI automatically.

## License

MIT — see [LICENSE](LICENSE).
