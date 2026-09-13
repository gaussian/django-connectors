# Quickstart

Ten minutes from install to your first projected record. The `example/`
directory is this guide, runnable.

## 1. Install and configure

```bash
pip install 'django-connectors[mysql]'
```

```python
INSTALLED_APPS = [
    "django.contrib.contenttypes",   # required — Connection.owner is a GenericForeignKey
    "django_connectors",
    "myapp",
]

DJANGO_CONNECTORS = {
    # A SQLAlchemy DSN. Deliberately NOT a Django DATABASES alias: the landing
    # database is reached only through dlt, which makes routing an ORM model
    # there structurally impossible rather than merely discouraged.
    "LANDING_URL": "mysql+pymysql://user:pw@host:3306/connectors_landing",
    "SOURCES": {
        "rest": "django_connectors.sources.rest.RestSource",
    },
}
```

```bash
python manage.py migrate
python manage.py check     # names every misconfiguration by setting
```

The landing database must exist; the tables inside it are created by dlt.

## 2. Declare what your app accepts

`myapp/connectors.py` — auto-discovered, the same way `admin.py` is:

```python
from django_connectors import (
    DateTimeField, JSONField, StringField, TargetDefinition, register_target,
)
from myapp.models import Event

def event_writer(records, context):
    for record in records:
        if record.operation == "delete":
            Event.objects.filter(
                team_id=context.owner_object_id,
                external_id=record.identity["external_id"],
            ).update(deleted_at=timezone.now())
            continue
        Event.objects.update_or_create(
            team_id=context.owner_object_id,
            external_id=record.identity["external_id"],
            defaults={
                "occurred_at": record.values["occurred_at"],
                "type": record.values["type"],
                "payload": record.values.get("payload") or {},
            },
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
    identity_scope="owner",
    writer=event_writer,
))
```

A target that stores files gets the bytes on request — landed rows carry
file *metadata*, never content:

```python
from django_connectors.services import content

fetched = content.fetch_record_content(
    Binding.objects.get(pk=context.binding_id), "drive_items",
    {"id": record.values["item_id"], "drive_id": record.values["drive_id"]},
)
fetched.data, fetched.content_type   # under CONTENT_MAX_BYTES
```

Three rules the runner relies on:

- **Be idempotent per identity.** A raised exception means the batch was not
  applied and the whole ProjectionRun retries from the start — there is no
  mid-run checkpoint. `update_or_create`, not `create`.
- **Use `context.owner_object_id`.** Ignoring it discards the multi-tenant
  guarantee at the last step.
- **`identity_scope` has no default.** It decides whether two owners may share
  an identity value; guessing wrong is a cross-tenant collision.

## 3. Connect a customer's system

Before a Binding exists, discovery shows what could be synchronized — one
shape for every source, so a "pick what to sync" screen is one screen:

```python
from django_connectors.services import discovery

page = discovery.discover_remote(connection)                 # the top level
page = discovery.discover_remote(connection, query="orders") # narrowed by name
page = discovery.discover_remote(connection, path=page["items"][0]["path"])
page = discovery.discover_remote(connection, cursor=page["next_cursor"])
# {"items": [{"id", "name", "kind", "path", ...}, ...], "next_cursor": None | str}
```

`kind` is what the item is (`folder`, `file`, `schema`, `table`,
`spreadsheet`, `sheet`, `site`, `object`, …); `path` is what to pass back to
descend, and is `None` on a leaf. Items also carry whatever a Binding needs
verbatim — a `spreadsheet_id`, a `db_schema` and `table`, a bucket prefix.


```python
from django_connectors.models import Binding, Connection
from django_connectors.services import runs

connection = Connection.objects.create(
    owner_content_type=ContentType.objects.get_for_model(Team),
    owner_object_id=str(team.pk),
    provider="acme",
    auth_backend="static",
    status="active",
)

binding = Binding.objects.create(
    connection=connection,
    source="rest",
    resources=["events"],
    config={
        "base_url": "https://api.acme.example/v1/",
        "resources": {
            "events": {
                "path": "events",
                "primary_key": "id",
                "write_disposition": "merge",     # state it: dlt defaults to append
                "data_selector": "data",
                "incremental": {"cursor_path": "updated_at", "start_param": "since"},
            }
        },
    },
    poll_interval=timedelta(minutes=15),
)

run = runs.run_binding(binding, trigger="manual")
print(run.status, run.dlt_load_ids)
```

## 4. Let the customer map it

```python
from django_connectors.services import discovery, projections

discovery.get_landing_schema(binding)          # columns and types
discovery.sample_resource(binding, "events")   # a bounded sample
```

```python
from django_connectors.models import Projection

projection = Projection.objects.create(
    binding=binding, resource="events", target="events", name="Acme events",
    mapping={
        "external_id": {"source": "id"},
        "occurred_at": {"source": "created_at", "cast": "datetime"},
        "type":        {"source": "event_type"},
        "payload":     {"object": {
            "detail": {"source": "metadata"},
            "system": {"constant": "acme"},
        }},
    },
    filters=[{"field": "environment", "op": "eq", "value": "production"}],
)
```

A cast takes options when the source writes values the way people do rather
than the way databases do — a spreadsheet's `03/04/2024`, a CSV's `£1,234.56`:

```python
"occurred_at": {"source": "order_date", "cast": "datetime",
                "format": "%d/%m/%Y", "timezone": "Europe/London"},
"amount":      {"source": "total", "cast": "decimal", "strip": "£,"},
```

`format` and `timezone` apply to `datetime` (`format` alone to `date`);
`strip` and `decimal_separator` to `integer`, `decimal` and `float`. A bad
option is a validation error naming the field, not a failed run.

A sheet with one row per case and one **column** per stage is reshaped at the
source, so the mapping sees one row per stage:

```python
"ranges": {"orders": {"range": "Orders!A:F", "key_column": "order_id",
           "unpivot": {"columns": ["received", "approved", "shipped"],
                       "name_to": "stage", "value_to": "on"}}}
```

The merge key becomes `(order_id, stage)`; map identity from both. The same
`unpivot` key works on the Excel, filesystem, SQL, REST and memory sources.

```python
projections.validate_projection(projection)   # errors and warnings, by field
projections.preview_projection(projection)    # real rows, writer never called
projections.activate_projection(projection)
```

Preview returns customer data to its caller — scope it to the owner.

## 5. Schedule it

```python
from django_connectors.scheduler import base as scheduler

scheduler.run_due_bindings()             # honours poll_interval and min_run_interval
scheduler.renew_due_webhooks()           # provider subscriptions expire silently
scheduler.dispatch_pending_projections() # heals anything a push dispatch dropped
scheduler.reap_stale_runs()              # frees leases held by dead workers
```

With the `celery` extra, `django_connectors.scheduler.celery` provides task
wrappers and a beat schedule. A successful Run already dispatches its projections;
the sweeper is the safety net.

## Where to go next

- [architecture.md](architecture.md) — why the pieces are shaped this way
- [operations.md](operations.md) — MySQL, concurrency, retention, deployment
- [api.md](api.md) — the optional REST API, and composing it with your own
