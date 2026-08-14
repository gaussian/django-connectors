# Architecture

Every decision here is either a boundary the design depends on, or a workaround
for a behaviour that was measured rather than assumed. Where a rule exists to
prevent a specific silent failure, that failure is named — because a rule whose
reason is forgotten gets "simplified away" by the next person.

## The layers

```
Connection   How can this customer reach this external system?
Binding      Which slice of it should we keep synchronized?
Run          What happened during one synchronization attempt?
dlt          How do we reliably acquire and maintain that data?
Landing      What source-shaped data do we currently hold for this Binding?
Projection   How does this customer map one landed resource into a host target?
Target       What record shape does the host accept?
Writer       How does the host persist those records?
```

The hard boundary sits between Projection and Target: this library understands
external systems, synchronization, landed data, abstract target shapes and
customer mappings. It does not understand your models or your domain.

## Why land data before interpreting it

Storing source data before mapping it costs storage and widens the data-retention
surface. It buys:

- **Replay.** A customer who mapped the wrong column can re-map and replay in
  seconds, without re-fetching from a provider that may rate-limit, charge, or
  no longer have the data.
- **Independent failure.** Ingestion succeeding and a write to your models
  failing are different events with different retries. `Run` and `ProjectionRun`
  are separate for this reason: a failed projection is retried against landed
  data, never against the provider.
- **Offline schema inspection.** The mapping UI needs columns, types and sample
  rows. Reading a snapshot beats calling the provider on every page load.

## Landing topology: one table per Binding

Bindings share one landing database but never a table.

This is not tidiness. With Bindings writing the same landing table, concurrent
loads were measured **silently losing 20–27% of loads** — `errors=0`, no
exception, no failed job — because dlt keys its staging table on
`(dataset, table_name)` alone and every load issues an unconditional,
auto-committed `DELETE` against it. Table-per-Binding measured zero lost loads
over the same run, creates no extra databases, bounds merge cost to one tenant's
rows, and makes purging a `DROP` rather than a tenant-filtered `DELETE`.

Each Binding also gets its own dlt **schema name**. A cold restore resolves a
schema by name from `_dlt_version` and takes the newest row, so a shared name
hands one Binding another's columns.

## Tenant identity

Every root landing record carries `_connector_binding_id`, `_connector_run_id`
and `_connector_deleted`. Merge identity is always
`(_connector_binding_id, *resource_primary_key)` — keyed on the remote id alone,
one Binding's merge was measured deleting another's rows.

Two details that look like fussiness and are not:

- The injector **must be a one-argument closure**. dlt decides how to call a map
  function by counting its signature parameters: exactly one means `f(item)`,
  anything else means `f(item, meta)` and dlt passes `meta=None` into your second
  parameter. The natural `def inject(row, binding_id=...)` therefore writes NULL
  binding ids into every row — cross-tenant contamination with no error at all.
- The columns are pinned to `varchar(36)`, not 32: a canonical UUID string
  includes four hyphens. MySQL rejects the overflow; sqlite accepts any width, so
  getting it wrong is invisible until production.

## Nesting is off, with no opt-out

`max_table_nesting=0`, always. Three independent reasons:

1. `add_map` stamps root records only, so child tables carry no tenant scope and
   no run filter — they are structurally unprojectable.
2. Nested merges inject `DROP`/`CREATE TABLE` into the merge SQL. MySQL commits
   implicitly on DDL, and a concurrent reader was measured seeing a landing table
   at `COUNT(*) = 0` of 20,000 rows, fully committed.
3. With nesting off, dicts and lists land as JSON columns, which is exactly what
   the mapping DSL's `json_path` needs.

## What "a Run succeeded" means

It means the source was reconciled into the landing tables. It does **not** mean
`pipeline.run()` returned without raising — those are different claims.

When a pipeline holds a pending normalized load package, `pipeline.run(new_data)`
loads the *old* package, returns a clean `LoadInfo`, and never extracts the new
data. dlt's own source comment reads "load them and exit"; only a `logger.warn`
distinguishes it. A runner treating "did not raise" as success records
`succeeded` for data it never fetched — and the cursor then advances past records
nobody holds.

So a Run is marked succeeded only when there are no failed jobs, at least one
load id, and no load id already claimed by an earlier Run of that Binding.
Pending packages are drained by their own `recovery` Run so rows are attributed
to the run that actually landed them.

## Incremental projection keys on load ids

Not on `_connector_run_id`. A run id is stamped by `add_map` at *extract* time;
`_dlt_load_id` is written at *load* time. When a run loads a stale pending
package, the load-id window attributes rows correctly and the run-id window loses
them permanently. The run id is an audit breadcrumb.

A sweeper computes outstanding work as a set difference — the Binding's succeeded
load ids minus the Projection's — which heals both a dropped dispatch and a
ProjectionRun that failed and was never retried. A per-run "dispatched" flag would
only heal the first.

## Incremental cursors are built by the library

Sources declare *what* the cursor is; they never construct the `Incremental`.
Two settings are forced on, because both silently lose records and neither can be
repaired afterwards:

- `primary_key=()` — dlt defaults the deduplication key to the resource's primary
  key, and a record updated at *exactly* the stored cursor value is then dropped.
  Measured: run 2 emitted an updated row and the table still held the old one.
- `on_cursor_value_missing="include"` — otherwise a record with no cursor value
  (a tombstone carries identity columns only) raises and fails the whole Run.

Assigning these after the fact does not work: it is silently ineffective, and dlt
strips the incremental from a *bound* resource's signature, so a source declaring
its own in a parameter default is beyond reach.

## Write disposition must be stated

`dlt.resource()` defaults the hint to `"append"`, **not** to `None`. A source
omitting it appends a fresh copy of every re-fetched record on every run — merge
key correctly configured, no error. Since an explicit `"append"` is
indistinguishable from the default, a declared `primary_key` is used as the
signal: it means nothing under append, so the combination is refused.

## Deletes

dlt's `hard_delete` column hint is never set. It **physically deletes the row**
during merge, which would make the deletion unobservable to Projection forever
and leave your records stale. Instead `_connector_deleted` marks the row, and
Projection emits a delete record; what delete *means* is the host's decision.

`delete-insert` merge replaces the whole row, so a tombstone carrying only
identity columns nulls every other column. Validation therefore refuses a target
identity field sourced from outside the merge key — otherwise it would be `None`
on the delete path, and the host would receive a delete it cannot match.

Not every source can detect deletions. A cursor-based source simply never hears
about them, so `SourceDefinition.emits_tombstones` is honest about it rather than
implying support.

## The mapping DSL is evaluated, not compiled to SQL

Customer JSON compiles to small Python objects evaluated against a row dict. No
SQL text is built anywhere; no customer Python is executed.

Compiling to SQL would have meant hand-rolling an identifier escaper, because
dlt's Relation surface cannot express what the DSL needs — no `coalesce`,
`concat`, `lower`, `json_extract` or casts. And a database `CAST` cannot report
*which* row and field failed, which the preview requires (MySQL yields NULL plus
a session warning). Filtering in Python costs little: landing tables are
per-Binding so scans are already bounded, and no filter column is indexed.

All landing reads go through one function, `landing/access.binding_relation`, so
the tenant scope exists in exactly one place. A test greps the package to keep it
that way.

## Concurrency

dlt has **no cross-process lock** on a pipeline working directory. Two processes
on one pipeline were measured producing hard failures, one process loading the
other's load package under its own `LoadInfo`, and a case reporting success while
its own 200 rows never landed.

Exclusion is therefore this library's job: a database lease per Binding, not
`SELECT GET_LOCK()` — that is MySQL-only, and a lock exercised by one CI job is a
lock nobody tests. Leases expire, so a killed worker does not block its Binding
forever.

## Retention

Only `current_state` and `permanent`, both no-ops. Time-based pruning of a merge
resource does not delete history, it deletes *current state*: the cursor has
advanced past those records, the source will never re-emit them, and replay
becomes structurally impossible. `reset_binding_state()` is the escape hatch.

Deleting a Binding is two-phase and guarded, because a plain delete cascades
through the control plane and strands its landing rows with
`_connector_binding_id` pointing at a row that no longer exists.

## What is deliberately not here

Bidirectional sync, remote writes, conflict resolution, joins or aggregation
after landing, arbitrary SQL or Python in a mapping, cross-Binding projection,
and a general secret manager beyond the `SecretStore` interface and its three
backends.

If a customer needs several tables joined into one logical record, that shaping
belongs upstream — in their own SQL view, or in the dlt source — not in a
projection DSL slowly growing into a query planner.
