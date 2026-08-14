# Operations

## The landing database

**MySQL and PostgreSQL are both supported**, and both are exercised by CI on
every pull request. Nothing in the landing layer is dialect-specific; where the
backends genuinely differ, the difference is normalised rather than assumed away.

A separate database (MySQL) or schema (PostgreSQL) on the same server is the
intended deployment:

```
database server
├── app                    Django models, incl. this library's control plane
└── connectors_landing     dlt landing tables, schemas, state and load metadata
```

```python
"LANDING_URL": "mysql+pymysql://user:pw@host:3306/connectors_landing"
"LANDING_URL": "postgresql+psycopg2://user:pw@host:5432/connectors_landing"
```

Install the matching driver extra: `django-connectors[mysql]` or
`django-connectors[postgres]`.

### Where the backends differ

These are handled for you; they are listed because they explain why some code
looks more careful than it needs to.

| | MySQL 8.4 | PostgreSQL 16 |
| --- | --- | --- |
| identifier limit | 64 | **63** — the library uses 63 everywhere |
| native boolean | no (`0`/`1`) | yes (`True`/`False`) |
| JSON column reads back as | text | parsed |
| `str` maps to | `TEXT`, capped at 65,535 bytes | `text`, unbounded |
| dataset is a | database | schema |

The identifier limit is the one that bites hardest: sqlite reports 9999, so a
64-character table name passes in development and is **silently truncated** by
PostgreSQL — and two Bindings differing only past that point would collide onto
one landing table. The library rejects anything over 63 for every backend.

The `TEXT` cap is MySQL-only: a >64KB string is a poison pill there (see below)
and a non-event on PostgreSQL.

It is **not** a Django `DATABASES` alias. A router's `allow_migrate=False` does
not stop `.using("landing")`, `migrate --database=landing` would create
`django_migrations` there, and the test runner would create a test database for
it. Keeping it out of `DATABASES` makes routing an ORM model there impossible
rather than discouraged.

The database (MySQL) or schema (PostgreSQL) must exist before first use; dlt
creates the tables inside it, plus a `<dataset>_staging` sibling. The application
user needs `CREATE`/`ALTER`/`DROP` on both — pre-provision them if that conflicts
with least-privilege policy.

## Warm the dataset before running workers concurrently

```python
from django_connectors.landing.warm import warm_landing_dataset
warm_landing_dataset()
```

Run once, serially, at deploy time. The first *concurrent* loads into a fresh
dataset race on the shared `_dlt_version`, `_dlt_loads` and staging objects —
`Table '_dlt_version' already exists` — no matter how well the landing tables
themselves are isolated. Warm steady-state concurrency is clean; only the cold
start is not.

## Concurrency

One Run per Binding, enforced by a database lease. dlt provides no cross-process
lock, and concurrent access to one pipeline was measured stealing load packages
between processes and reporting success for rows that never landed. Do not
bypass `services.runs.run_binding`.

`PIPELINES_DIR` should be **durable across a Run**. Incremental cursors restore
from the destination and survive an ephemeral filesystem, but a partially loaded
package does not: a killed worker on ephemeral storage loses it, and the Run is
reaped as failed by `reap_stale_runs()`.

## Table count

Each Binding gets its own landing table per resource. At ~5 resources and 2,000
Bindings that is ~10,000 InnoDB tables plus staging. That is fine for MySQL with
`innodb_file_per_table` and a raised `open_files_limit`, but it is an operational
commitment worth knowing about. The alternative — a shared table — silently loses
loads, so it is not on the table.

## Retention and deletion

`landing_retention` offers only `current_state` and `permanent`, and there is no
sweeper. Time-based pruning of a merge resource deletes current state, not
history: the cursor has advanced past those rows and the source will never
re-emit them.

To remove a customer's landed data:

```python
from django_connectors.services import retention
retention.purge_binding_landing(binding)     # DROPs the tables, forgets dlt state
retention.delete_binding(binding, purge=True)
```

Deleting a Binding whose tables still exist raises. Its rows would otherwise
remain with `_connector_binding_id` pointing at a row that no longer exists.

To re-extract from scratch after a poisoned cursor:

```python
retention.reset_binding_state(binding, backfill=True)
```

## A wedged pipeline

dlt retries a failing load job indefinitely, so one un-loadable row leaves
`has_pending_data` true and makes every later run raise. On MySQL the commonest
cause is a value too large for its column: dlt maps `str` to `TEXT`
(65,535 bytes) while advertising a limit of 1 GB, so a >64KB string is accepted
at normalize time and rejected at load time. PostgreSQL's `text` is unbounded and
does not have this failure mode.

The Run fails loudly rather than reporting success. Fix the source data or widen
the column, then re-run; `reset_binding_state()` is the last resort.

## Secrets

The default `SecretStore` refuses to store anything, rather than silently writing
plaintext. Choose deliberately:

```python
DJANGO_CONNECTORS = {
    "SECRET_STORE": "django_connectors.secrets.ModelSecretStore",
    "SECRET_ENCRYPTION": "fernet",        # or "none"
    "SECRET_KEY": os.environ["CONNECTORS_SECRET_KEY"],
}
```

`"none"` is django-allauth's trust model — `SocialToken.token` is a plaintext
column — and is legitimate when the database is your trust boundary. `"fernet"`
needs the `secrets` extra and a **dedicated** key: using `settings.SECRET_KEY`
would destroy every stored credential the moment it rotated.

`Connection.auth_metadata` rejects credential-shaped keys, because that field is
rendered in the admin and returned by the API.

## Errors and logs

Every exception reaching `Run.error_message`, `Binding.last_error` or a log
record passes through `errors.scrub()`, which is default-deny on URL query
values, strips userinfo, and masks JWTs and high-entropy runs. Never interpolate
a dlt destination factory, credentials object or pipeline into a log line: their
`repr()` and `to_native_representation()` render the landing password in
plaintext, while `str()` masks it.

## Monitoring

Worth alerting on: Runs in `failed`, Bindings in `blocked` (revoked credentials)
or `needs_review`, Projections in `invalid` (a mapped column vanished) or
`needs_review` (a source changed a column's type and the original now holds
NULLs), and webhook subscriptions past `renew_at`.
