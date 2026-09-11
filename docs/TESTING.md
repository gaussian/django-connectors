# Testing connectors

The five provider connectors ship **mock-tested with zero network**, and their
docstrings are honest about what was never exercised: real OAuth and admin
consent, real delta-token and `historyId` expiry, real throttling vocabularies,
real spreadsheet cell types.

That honesty is the problem this document is about. A hand-written mock verifies
*our* logic against *our* belief about the provider — and our belief about the
provider is exactly the thing most likely to be wrong. A mock cannot tell you
that Graph renamed a field, that Gmail's `historyId` expires sooner than
documented, or that Salesforce returns a `SystemModstamp` with a different
precision than the sandbox did last year.

Two tiers, both in the default test run, both free of credentials at test time.

| Tier | Credentials | Runs | Catches |
| --- | --- | --- | --- |
| **A — conformance** | none | every PR | a connector breaking the library's own contract |
| **B — recorded cassettes** | once, to record | every PR | our mock being wrong about the provider's response shape |

A third tier — scheduled runs against live sandboxes, the only thing that
catches a provider changing behaviour under us — is not built. It needs
continuously available sandbox credentials and a scheduling decision. What it
should look like when that decision is made is at the [end of this
document](#tier-c--live-sandbox-runs-not-built).

---

## Tier A — the conformance suite (built)

`django_connectors.testing.conformance` is a connector-agnostic suite that every
registered `SourceDefinition` must pass, **including host-defined ones**. It
ships in the wheel rather than living in this repository's `tests/`, because the
contract applies to a host's own connectors and a suite that only exists here
could only ever be run against the connectors here.

It needs no credentials and reaches no network, so it runs on every pull request
in the default tier.

### What it checks, and why each one is there

Every check exists because the mistake it catches is **silent**. That is the bar
for adding one: a rule whose violation produces an error does not need a test,
because the error is the test.

| Check | What is silent without it |
| --- | --- |
| `build_source` returns a `DltSource` | a source returning `None` fails deep inside dlt, in a message that names dlt rather than the connector |
| every resource states a write disposition that survives instrumentation | `dlt.resource()` defaults the hint to `"append"`, so an omission appends a fresh copy of every re-fetched record forever — with the merge key correct and no error |
| `incremental_for` returns kwargs, never an `Incremental` | dlt strips the incremental from a *bound* resource, so a source that builds its own is beyond the library's reach; the two settings the library forces each drop records silently |
| no module-scope `import dlt` | 0.6s on every `manage.py`, autoreload cycle, worker fork and test collection, paid by hosts that never run a pipeline |
| `validate_config` rejects garbage with `ConfigurationError` | a bad config becomes a failed Run in front of a customer instead of a form error in front of its author — and the save path keys on the *type*, so `ValueError` becomes a 500 |
| `required_extras` is declared | the W005 system check has nothing to report, and a missing optional dependency surfaces mid-Run |
| `emits_tombstones` implies a tombstone carrying only merge-key columns | `delete-insert` merge replaces the whole row, so any target identity sourced from outside the merge key is `None` on the delete path |
| the landed schema carries the tenant columns, has no child tables, and leads the merge key with `_connector_binding_id` | the `add_map` arity bug writes NULL binding ids on every row; a child table carries no tenant scope and is unprojectable; a merge keyed on the remote id alone deletes another Binding's rows |

The write-disposition check is a **source-code** check, deliberately. The
behaviour is unobservable: `dlt.resource()` defaults the hint to `"append"`
rather than to `None`, so a resource that omits it is byte-identical at runtime
to one that states `"append"` on purpose. The call site is the only place the
difference still exists.

### Running it against your own connector

```python
from django_connectors.testing import (
    check_definition, check_built_source, check_landing_invariants,
)

def test_my_connector_honours_the_contract():
    failures = check_definition(MySource(), invalid_configs=[{}, {"bad": 1}])
    assert failures == [], "\n".join(failures)

def test_my_connector_lands_conformantly(binding, my_fake_server):
    failures = check_built_source(MySource(), binding=binding, credentials=token)
    assert failures == [], "\n".join(failures)

    run_binding(binding, trigger="initial")
    assert check_landing_invariants(binding, expected_resources=["things"]) == []
```

Every function **returns a list of strings** rather than asserting. A
conformance run should report every violation at once — a suite that stops at
the first one takes as many iterations to fix as there are problems — and
returning data keeps the module free of a pytest dependency, which matters
because it ships in the wheel.

`invalid_configs` is required, not optional: a source with no required keys
legitimately accepts `{}`, so a generic "reject an empty dict" would be wrong
for it and vacuous for everyone else. Supplying none is reported as a failure,
because silence is not conformance.

### How it is wired here

- `tests/test_source_conformance.py` runs the credential-free half over every
  source this repository ships, and derives the list of sources **from the
  filesystem** — not from a registry, which contains only what a host
  configured. A new connector with no case is a failing test.
- The landing half lives in each connector's own module, named
  `test_<source key>_conformance`, because it needs the in-process WSGI fake
  that module already runs. One fake per provider, not one per suite: a second
  copy would be a second belief about the provider, drifting independently.
  `test_source_conformance.py` asserts each of those tests exists, by name.

---

## Tier B — recorded cassettes (built)

`tests/test_recorded.py` records one real exchange per provider connector
against a sandbox, commits it redacted under `tests/cassettes/test_recorded/`,
and replays it on every pull request with [VCR.py](https://vcrpy.readthedocs.io/)
via [pytest-recording](https://github.com/kiwicom/pytest-recording).

This is the tier that catches *"our mock is wrong about the provider's actual
response shape"*, which no hand-written fake ever can. Each replay does a first
Run and an incremental second Run, then holds the result to the same landing
invariants as Tier A and to a committed schema snapshot.

### State of the cassettes

**No cassette is committed yet.** Recording needs a sandbox per provider and a
person with its credentials in their shell; until then each replay test skips
with the exact command and variables it needs. The machinery is proven without
a credential: `test_the_recorder_round_trips_through_a_redacted_cassette`
records against the in-process Salesforce fake with a made-up token, checks
that neither the token nor the fake's address reached the cassette, then replays
against a placeholder host that resolves to nothing and lands the same rows.

### Recording

Once per connector, against a sandbox with synthetic data — the cassettes hold
whatever the sandbox held, and they are committed:

```bash
export DJANGO_CONNECTORS_RECORD_GOOGLE_ACCESS_TOKEN=ya29....
export DJANGO_CONNECTORS_RECORD_GOOGLE_SPREADSHEET_ID=1BxiM...
uv run --all-extras pytest tests/test_recorded.py -k "gmail or sheets" --record-mode=rewrite

uv run --all-extras pytest tests/test_recorded.py    # replays and scans what was written
git add tests/cassettes/
```

| Connector | Variables (`DJANGO_CONNECTORS_RECORD_…`) | Sandbox |
| --- | --- | --- |
| `gmail` | `GOOGLE_ACCESS_TOKEN` | any Google account with a few messages |
| `google_sheets` | `GOOGLE_ACCESS_TOKEN`, `GOOGLE_SPREADSHEET_ID` | a sheet whose first tab has a header row and an `id` column |
| `entra_files` | `MICROSOFT_ACCESS_TOKEN`, `MICROSOFT_DRIVE_ID` | an [M365 developer sandbox](https://developer.microsoft.com/en-us/microsoft-365/dev-program) drive with a few files |
| `entra_excel` | the two above plus `MICROSOFT_WORKBOOK_ITEM_ID` | one `.xlsx` in that drive, ideally saved by Excel rather than by a library — the cached-formula-result difference is precisely what the fake cannot reproduce |
| `salesforce` | `SALESFORCE_ACCESS_TOKEN`, `SALESFORCE_INSTANCE_URL` | a [Developer Edition](https://developer.salesforce.com/signup) org |

Use a bare access token, not a refreshable credential: a refresh is a
token-endpoint exchange, and this tier records the data API only. A recording
with a variable missing **fails** rather than skips. `rewrite`, not `once`: a
stale cassette is replaced whole, never appended to.

Google domain-wide delegation needs a real Workspace domain and cannot be
recorded from an ordinary account; that path stays fake-tested.

### What keeps a cassette safe

Redaction is a two-sided problem: filtering request headers is the easy half,
and the dangerous half is the response body, where a token refresh *is* a
credential and an error body routinely quotes the request that caused it. So:

- Every value from a recording variable becomes a fixed placeholder before it
  is written — in the URI, in headers, in request and response bodies. That is
  also what makes replay deterministic: the placeholders are used *as* the
  values on replay, so the URIs the connector builds match the cassette.
- Everything is then run through `errors.redact_secrets`, the same
  credential-shaped rules the scrubber applies to error messages (JWTs,
  `Bearer …`, `key: value` pairs whose key names a secret, including
  `tempauth` on a SharePoint download URL). Reusing them means the recorder and
  the log scrubber cannot disagree about what a credential looks like.
- Response headers are allow-listed to the five a connector reads. Request ids,
  cookies, tracing headers and the `x-ms-*` / `x-goog-*` families are dropped.
- `test_committed_cassettes_carry_no_credentials` reads every committed
  cassette back and refuses one where those rules would still change anything,
  where an `Authorization` header is not masked, or where a header survived
  that should not have. Redaction at record time is a fixture someone can
  forget; the scanner runs in the required `ci` check.

Anything *named* like a credential is masked even when it is a cursor —
Graph's `token=` delta parameter, say — but consistently on both sides, so the
request built from a response still matches the recording.

### The schema snapshot

A provider renaming a field does not fail a replay: the connector lands a NULL
where a value used to be. So each recording also writes `<key>.schema.json`,
the reduced landing schema the library already stores on the Binding after a
Run, and the replay compares. A rename shows up as a diff in code review rather
than as a column nobody notices is empty.

### Two sources replay with their address guard lifted

`SalesforceSource` (and `RestSource`) resolve the host by DNS before every
request and pin each socket to the address it resolved. A replay against a
placeholder host can satisfy neither, and neither is what a cassette verifies,
so the recorded tier registers a subclass with `allow_private_addresses = True`
— exactly as the loopback tests do. The guard has its own tests.

### Cassettes rot

A cassette is a claim about a provider frozen at a moment. Re-record when a
replay starts failing for a reason that is the provider's, and treat the diff
as a finding: it is the provider change this tier exists to surface.

---

## Tier C — live sandbox runs (not built)

Nightly or weekly runs against real sandboxes. This is the only tier that
catches a provider changing behaviour under us, and the only one that notices
a sandbox has lapsed.

The natural shape is one scheduled workflow that runs `tests/test_recorded.py`
in `--record-mode=rewrite` against the sandboxes and opens a pull request with
the cassette diff. A provider change then surfaces twice: as a failing live
run, and as a reviewable diff of what the provider now returns.

Four rules, each because the obvious version of this job becomes a nuisance
and then gets disabled:

1. **Never on pull requests from forks.** A fork PR would otherwise get the
   secrets. `pull_request_target` is not a fix; gate on
   `github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'`.
2. **Keep it out of the required `ci` check.** A provider outage must not
   block merges. A failing scheduled job that opens an issue is useful; a red
   required check nobody can fix is how a suite gets bypassed.
3. **Small and idempotent.** Read a handful of records, write nothing that
   accumulates. Quotas are the limiting resource, and the M365 sandbox is
   renewed on the basis of activity, not volume.
4. **Secrets from a secret manager, not from repository variables.** These
   credentials are long-lived by construction, so the place they live has to
   support rotating them without a commit.

### Sandbox availability

This, not the tooling, is the real constraint.

| Provider | Sandbox | Practical note |
| --- | --- | --- |
| Salesforce | [Developer Edition](https://developer.salesforce.com/signup) — free, non-expiring with periodic login | Easiest of the five. JWT bearer needs a self-signed certificate on a Connected App, once. |
| Microsoft | [M365 E5 developer sandbox](https://developer.microsoft.com/en-us/microsoft-365/dev-program) — free, 25 seats, **90-day renewal conditional on activity** | The scheduled job doubles as the keep-alive. App-only access needs one-time admin consent. |
| Google (Gmail, Sheets) | an ordinary Google account plus a free GCP project | Fine for the OAuth-delegated paths, which is most of what these connectors do. |
| Google (domain-wide delegation) | needs a real Workspace domain — only a **14-day trial** | The one path that cannot be continuously tested cheaply. Stated plainly rather than pretended otherwise: this path stays fake-tested. |

### What is deliberately not proposed

- **Contract tests generated from provider OpenAPI documents.** Microsoft and
  Salesforce publish them; they describe what the API is documented to return,
  which is the same thing the fakes encode. Generating from them would automate
  the belief, not check it.
- **A shared fake-provider service.** The in-process WSGI fakes are
  per-provider and live beside the tests that use them, on purpose. A shared
  one is a second place for a belief about the provider to live, and it drifts.
