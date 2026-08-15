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

Three tiers, in descending order of value per unit of effort.

| Tier | Credentials | Runs | Catches |
| --- | --- | --- | --- |
| **A — conformance** | none | every PR | a connector breaking the library's own contract |
| **B — recorded cassettes** | once, to record | every PR | our mock being wrong about the provider's response shape |
| **C — live sandbox** | continuously | scheduled | the provider changing behaviour under us |

**Tier A is built.** Tiers B and C are specified here and not built: both need
credentials and a scheduling decision that has not been made. The rest of this
document is what to build when it has been.

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

## Tier B — recorded cassettes (not built)

[VCR.py](https://vcrpy.readthedocs.io/) via
[pytest-recording](https://github.com/kiwicom/pytest-recording): record against
a real sandbox once, redact, commit the cassettes, replay offline in CI forever.

This is the tier that catches *"our mock is wrong about the provider's actual
response shape"*, which no hand-written mock ever can.

### Non-negotiables

**Redaction is a two-sided problem.** Filtering request headers is the obvious
half and the easy one; the dangerous half is the response body. A token refresh
response *is* a credential, and an error body routinely quotes the request that
caused it — including its `Authorization` header.

```python
# tests/conftest.py
@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [("authorization", "REDACTED"), ("cookie", "REDACTED")],
        "filter_query_parameters": ["access_token", "code", "client_secret"],
        "filter_post_data_parameters": ["client_secret", "assertion", "code"],
        "before_record_response": _scrub_response,
        "record_mode": "none",          # replay only; recording is explicit
        "decode_compressed_response": True,
    }
```

`_scrub_response` should run the body through the same patterns
`django_connectors.errors.scrub` already uses. Reusing them rather than writing
new ones means the redactor and the log scrubber cannot disagree about what a
credential looks like.

**A CI check must grep the committed cassettes.** Redaction that is only applied
at record time fails open: a cassette recorded before a filter was added, or by
someone who forgot the fixture, is committed plaintext and stays that way. Add a
job that scans `tests/cassettes/**` for token-shaped strings using
`errors.scrub`'s own patterns as the linter, and fails the build on a hit. This
job is cheap and belongs in the required `ci` check.

**Freeze the clock.** Every cursor in this library is time-derived — Gmail's
`historyId` window, Graph's delta tokens, Salesforce's `SystemModstamp`
predicate, and the webhook `renew_at` lead. A replay test that computes "now"
from the wall clock passes on the day it is recorded and starts producing
different requests afterwards, which VCR then reports as an unmatched request in
a way that looks like a connector bug. Pin the clock (`time-machine` or
`freezegun`) to the recording date, and record that date in the cassette
directory so the pin and the cassette move together.

**Snapshot the landed schema.** Cassettes prove the connector still parses what
the provider *said last year*. Pair each replay test with a snapshot of the
resulting `Binding.landing_schema` so a provider renaming a field shows up as a
diff rather than as a column that silently lands NULL. The schema snapshot the
library already writes after every successful Run is exactly the right artefact
— it is deliberately reduced to what a mapping can depend on, so it does not
churn on dlt version bumps.

**Cassettes rot.** A cassette is a claim about a provider frozen at a moment.
Pair Tier B with a scheduled re-record (Tier C's job can do it) and treat a
re-record diff as a finding, not a chore.

### What to record per connector

Only the flows a mock cannot get right — the shape-sensitive ones:

| Connector | Worth recording |
| --- | --- |
| Gmail | a full sync page, a `history.list` window, an expired `historyId` (404), a message with no headers |
| Google Sheets | a ragged range, a range with a formula and a date cell, an empty range |
| Entra files | a delta page with `@odata.nextLink`, a delta page with `@odata.deltaLink`, a deleted item, a 429 with `Retry-After` |
| Entra Excel | one real `.xlsx` download, ideally saved by Excel rather than by openpyxl — the cached-formula-result difference is precisely what our fake cannot reproduce |
| Salesforce | a paged `query` response, a `queryMore`, an `INVALID_SESSION_ID` error body, a `REQUEST_LIMIT_EXCEEDED` error body |

### Sandbox availability

This, not the tooling, is the real constraint.

| Provider | Sandbox | Practical note |
| --- | --- | --- |
| Salesforce | [Developer Edition](https://developer.salesforce.com/signup) — free, non-expiring with periodic login | Easiest of the five. JWT bearer needs a self-signed certificate on a Connected App, once. |
| Microsoft | [M365 E5 developer sandbox](https://developer.microsoft.com/en-us/microsoft-365/dev-program) — free, 25 seats, **90-day renewal conditional on activity** | The scheduled job doubles as the keep-alive. App-only access needs one-time admin consent. |
| Google (Gmail, Sheets) | an ordinary Google account plus a free GCP project | Fine for the OAuth-delegated paths, which is most of what these connectors do. |
| Google (domain-wide delegation) | needs a real Workspace domain — only a **14-day trial** | The one path that cannot be continuously tested cheaply. Stated plainly rather than pretended otherwise: this path stays mock-tested. |

---

## Tier C — live sandbox runs (not built)

Nightly or weekly runs against the real sandboxes above. This is the only tier
that catches a provider changing behaviour under us, and the only one that
notices a sandbox has lapsed.

Four rules, each of which exists because the obvious version of this job becomes
a nuisance and then gets disabled:

1. **Never on pull requests from forks.** A fork PR would otherwise get the
   secrets. `pull_request_target` is not a fix; gate on
   `github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'`.
2. **Keep it out of the required `ci` check.** A provider outage must not block
   merges. Report it separately — a failing scheduled job that opens an issue is
   useful; a red required check nobody can fix is how a suite gets bypassed.
3. **Small and idempotent.** Read a handful of records, write nothing that
   accumulates. Quotas are the limiting resource, and the M365 sandbox in
   particular is renewed on the basis of activity, not volume.
4. **Secrets from a secret manager, not from repository variables.** Rotation is
   the whole point: these credentials are long-lived by construction, so the
   place they live has to support rotating them without a commit.

The natural shape is one scheduled workflow that runs Tier C *and* re-records
Tier B's cassettes, so a provider change surfaces both as a failing live run and
as a reviewable cassette diff.

---

## What is deliberately not proposed

- **Contract tests generated from provider OpenAPI documents.** Microsoft and
  Salesforce publish them; they describe what the API is documented to return,
  which is the same thing our mocks encode. Generating from them would automate
  the belief, not check it.
- **A shared fake-provider service.** The in-process WSGI fakes are per-provider
  and live beside the tests that use them, on purpose. A shared one is a second
  place for a belief about the provider to live, and it drifts.
