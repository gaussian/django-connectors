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

Three rules the runner relies on:

- **Be idempotent per identity.** A raised exception means the batch was not
  applied and the whole ProjectionRun retries from the start — there is no
  mid-run checkpoint. `update_or_create`, not `create`.
- **Use `context.owner_object_id`.** Ignoring it discards the multi-tenant
  guarantee at the last step.
- **`identity_scope` has no default.** It decides whether two owners may share
  an identity value; guessing wrong is a cross-tenant collision.

## 3. Connect a customer's system

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

`Binding.objects.create()` does **not** validate the config — that is ordinary
Django, and a Binding whose source key has stopped being registered has to stay
savable so an operator can disable it. Validation runs in `full_clean()`, which
the admin and every ModelForm call, and in the DRF serializer. Call it yourself
if you are creating Bindings from code and want the same errors:

```python
binding.full_clean(exclude=["landing_key"])   # ValidationError, per field
```

or, if you want it to raise the library's own exception:

```python
from django_connectors.services.bindings import validate_binding

validate_binding(binding)   # ConfigurationError, returns the SourceDefinition
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

To renew one subscription out of band — an admin action, a support request —
call the service the sweep is a loop over, rather than moving `renew_at`:

```python
from django_connectors.webhooks.services import renew_subscription

renew_subscription(subscription, actor=request.user)
```

It renews with the provider immediately, records a failure with the same
back-off the sweep uses (one transient error must not retire a subscription
still hours from expiry), and then re-raises so the caller can report it. The
admin action and `POST /webhook-subscriptions/<id>/renew/` both go through it.

## Where to go next

- [architecture.md](architecture.md) — why the pieces are shaped this way
- [operations.md](operations.md) — MySQL, concurrency, retention, deployment
- [TESTING.md](TESTING.md) — the conformance suite your own
  `SourceDefinition` must pass
