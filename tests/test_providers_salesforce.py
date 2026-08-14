"""The Salesforce connector, exercised entirely offline.

Nothing here touches a network the test process does not own: an in-process
``wsgiref`` server plays a Salesforce org, replaying the documented shapes of
the token endpoint, the query/queryAll endpoints, ``nextRecordsUrl``
pagination, describe, and the error bodies Salesforce returns for a dead
session and a spent API allocation.

Most assertions land through ``services.runs.run_binding`` rather than stopping
at ``build_source``, because the interesting failures live in the seam between
a source and the landing layer: a source that builds a plausible ``DltSource``
and lands a half-paginated table, or duplicates every record on run 2, passes
any test that stops earlier.

This is a best-effort v0.1 connector written without access to a Salesforce
org. What the mocks *cannot* establish is listed in each module's "Unverified
against a live provider" section; these tests establish that the connector does
what it says it does against the payloads Salesforce documents.
"""

import base64
import datetime as dt
import json
import re
import threading
import time
from typing import ClassVar
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

from django_connectors.enums import (
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
)
from django_connectors.exceptions import (
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
    SourceError,
)
from django_connectors.landing import access
from django_connectors.landing.naming import DELETED_COLUMN, RUN_ID_COLUMN
from django_connectors.providers.salesforce import auth as sf_auth
from django_connectors.providers.salesforce import source as sf_source
from django_connectors.providers.salesforce.auth import SalesforceBackend
from django_connectors.providers.salesforce.source import SalesforceSource
from django_connectors.secrets import SecretStore
from django_connectors.services import bindings as binding_services
from django_connectors.services import discovery as discovery_services
from django_connectors.services import runs as run_services

pytestmark = pytest.mark.django_db

API_VERSION = "60.0"


# --- registry wiring -------------------------------------------------------


class LoopbackSalesforceSource(SalesforceSource):
    """The source with the address guard lifted, as a host would register it.

    ``allow_private_addresses`` is a class attribute on purpose: the party
    editing a Connection's instance_url is exactly the party the guard exists to
    constrain, so lifting it takes a deliberate registration in settings — which
    is what this is.
    """

    allow_private_addresses = True


class LoopbackSalesforceBackend(SalesforceBackend):
    allow_private_addresses = True


class MemoryStore(SecretStore):
    """A writable SecretStore for the JWT-bearer tests."""

    values: ClassVar[dict] = {}

    def get(self, connection, key):
        return self.values.get(key)

    def set(self, connection, key, value):
        self.values[key] = value

    def delete(self, connection, key):
        return self.values.pop(key, None) is not None


class TokenAuthBackend:
    """Hands back whatever ``Connection.metadata['connection_params']`` holds.

    Stands in for the real backend in the source tests, so that a query-layer
    assertion cannot fail for a reason that lives in the token exchange.
    """

    def get_credentials(self, connection):
        return (connection.metadata or {}).get("connection_params")


SOURCE_PATH = "tests.test_providers_salesforce.LoopbackSalesforceSource"
BACKEND_PATH = "tests.test_providers_salesforce.LoopbackSalesforceBackend"
TOKEN_BACKEND_PATH = "tests.test_providers_salesforce.TokenAuthBackend"
STORE_PATH = "tests.test_providers_salesforce.MemoryStore"


@pytest.fixture
def salesforce_settings(connectors_settings, settings):
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], "salesforce": SOURCE_PATH},
        "AUTH_BACKENDS": {
            "salesforce": BACKEND_PATH,
            "token": TOKEN_BACKEND_PATH,
        },
        "SECRET_STORE": STORE_PATH,
    }
    MemoryStore.values = {}
    return settings.DJANGO_CONNECTORS


# --- an in-process Salesforce org ------------------------------------------


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        """Keep pytest output readable."""


ATTRIBUTES = {"type": "Account", "url": "/services/data/v60.0/sobjects/Account/001x"}

ACCOUNTS = [
    {
        "Id": "001A",
        "Name": "Acme",
        "IsDeleted": False,
        "SystemModstamp": "2024-01-01T00:00:00.000+0000",
    },
    {
        "Id": "001B",
        "Name": "Beta",
        "IsDeleted": False,
        "SystemModstamp": "2024-01-02T00:00:00.000+0000",
    },
    {
        "Id": "001C",
        "Name": "Gamma",
        "IsDeleted": False,
        "SystemModstamp": "2024-01-03T00:00:00.000+0000",
    },
]

DESCRIBE_FIELDS = [
    {"name": "Id", "type": "id"},
    {"name": "Name", "type": "string"},
    {"name": "IsDeleted", "type": "boolean"},
    {"name": "SystemModstamp", "type": "datetime"},
    # Compound and blob fields: selecting either in a plain SELECT is a
    # MALFORMED_QUERY, so the source has to leave them out.
    {"name": "BillingAddress", "type": "address"},
    {"name": "Body", "type": "base64"},
    {"name": "OldField", "type": "string", "deprecatedAndHidden": True},
]

SOBJECT_LIST = [
    {"name": "Account", "label": "Account", "queryable": True, "custom": False},
    {"name": "Contact", "label": "Contact", "queryable": True, "custom": False},
    {"name": "Widget__c", "label": "Widget", "queryable": True, "custom": True},
    # Not queryable: offering it would only produce a Binding that fails.
    {"name": "AggregateResult", "label": "Aggregate", "queryable": False},
]

_CURSOR_PREDICATE_RE = re.compile(r"(\w+)\s*>=\s*(\S+?)(?:\s|\)|$)")
_SELECT_RE = re.compile(r"^SELECT\s+(.*?)\s+FROM\s+(\w+)", re.IGNORECASE | re.DOTALL)


def _parse_stamp(text):
    return dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))


class _InvalidField(Exception):
    """The fake org's INVALID_FIELD, raised out of the query engine."""


class FakeSalesforce:
    """A Salesforce org that records every request it serves.

    Deliberately implements the *documented* behaviour and no more: the
    ``attributes`` envelope on every record, an unquoted datetime literal in the
    WHERE clause, ``nextRecordsUrl`` pagination, and error bodies as a JSON
    array of ``{message, errorCode}``.
    """

    def __init__(self, records=None, *, page_size=2, token="sf-access-token"):
        self.records = [dict(record) for record in (records or ACCOUNTS)]
        self.page_size = page_size
        self.token = token
        self.base_url = ""
        self.requests = []
        #: Canned ``(status, payload, headers)`` served before normal routing,
        #: one per request, oldest first.
        self.canned = []
        self.token_response = None
        self.token_status = 200
        self._pages = {}
        self._next_locator = 0

    # -- wsgi ------------------------------------------------------------

    def __call__(self, environ, start_response):
        path = environ["PATH_INFO"]
        query = parse_qs(environ.get("QUERY_STRING", ""))
        length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(length).decode() if length else ""
        self.requests.append(
            {
                "method": environ["REQUEST_METHOD"],
                "path": path,
                "query": query,
                "form": parse_qs(body) if body else {},
                "authorization": environ.get("HTTP_AUTHORIZATION"),
                "query_options": environ.get("HTTP_SFORCE_QUERY_OPTIONS"),
            }
        )

        if self.canned:
            status, payload, headers = self.canned.pop(0)
            return self._json(start_response, status, payload, headers)

        return self._route(environ, start_response, path, query)

    def _route(self, environ, start_response, path, query):
        version = f"/services/data/v{API_VERSION}"
        if environ["REQUEST_METHOD"] == "POST" and path == sf_auth.TOKEN_PATH:
            return self._json(
                start_response,
                self.token_status,
                self.token_response
                if self.token_response is not None
                else {
                    "access_token": self.token,
                    "instance_url": self.base_url.rstrip("/"),
                    "token_type": "Bearer",
                    "issued_at": "1700000000000",
                },
            )
        if path == "/services/data/":
            return self._json(
                start_response, 200, [{"version": "59.0"}, {"version": API_VERSION}]
            )
        if path == f"{version}/sobjects/":
            return self._json(start_response, 200, {"sobjects": SOBJECT_LIST})
        if path.startswith(f"{version}/sobjects/") and path.endswith("/describe/"):
            return self._json(start_response, 200, {"fields": DESCRIBE_FIELDS})
        if path in (f"{version}/query/", f"{version}/queryAll/"):
            soql = query.get("q", [""])[0]
            try:
                payload = self._run_query(soql, query_all=path.endswith("queryAll/"))
            except _InvalidField as exc:
                return self._json(
                    start_response,
                    400,
                    [{"message": str(exc), "errorCode": "INVALID_FIELD"}],
                )
            return self._json(start_response, 200, payload)
        if path.startswith(f"{version}/query/"):
            return self._json(start_response, 200, self._page(path.rsplit("/", 1)[-1]))
        return self._json(
            start_response,
            404,
            [{"message": f"no such resource {path}", "errorCode": "NOT_FOUND"}],
        )

    def _json(self, start_response, status, payload, headers=None):
        body = json.dumps(payload).encode()
        response_headers = [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
        ]
        response_headers.extend((headers or {}).items())
        start_response(f"{status} X", response_headers)
        return [body]

    # -- query engine ----------------------------------------------------

    def _run_query(self, soql, *, query_all):
        match = _SELECT_RE.match(soql)
        if not match:
            raise AssertionError(f"unparseable SOQL: {soql!r}")
        selected = [name.strip() for name in match.group(1).split(",")]

        rows = list(self.records)
        if not query_all:
            rows = [row for row in rows if not row.get("IsDeleted")]

        where = soql.partition(" WHERE ")[2]
        for column, bound in _CURSOR_PREDICATE_RE.findall(where):
            limit = _parse_stamp(bound)
            rows = [row for row in rows if _parse_stamp(row[column]) >= limit]

        projected = [
            {"attributes": dict(ATTRIBUTES), **self._project(row, selected)}
            for row in rows
        ]
        return self._page_out(projected)

    def _project(self, row, selected):
        """Resolve field names case-insensitively, answer in canonical casing.

        This is the behaviour that makes a mis-cased cursor dangerous rather
        than merely wrong: SOQL happily accepts ``systemmodstamp`` and then
        answers with a ``SystemModstamp`` key, so the query succeeds and the
        incremental silently finds nothing to advance on.
        """
        canonical = {name.lower(): name for name in row}
        projection = {}
        for name in selected:
            actual = canonical.get(name.lower())
            if actual is None:
                raise _InvalidField(f"No such column '{name}' on entity 'Account'.")
            projection[actual] = row[actual]
        return projection

    def _page_out(self, projected):
        head, tail = projected[: self.page_size], projected[self.page_size :]
        payload = {"totalSize": len(projected), "done": not tail, "records": head}
        if tail:
            self._next_locator += 1
            locator = f"01g{self._next_locator:06d}-{self.page_size}"
            self._pages[locator] = tail
            payload["nextRecordsUrl"] = f"/services/data/v{API_VERSION}/query/{locator}"
        return payload

    def _page(self, locator):
        return self._page_out(self._pages.pop(locator, []))

    # -- assertions helpers ----------------------------------------------

    def soql_queries(self):
        return [
            request["query"]["q"][0]
            for request in self.requests
            if request["query"].get("q")
        ]

    def paths(self):
        return [request["path"] for request in self.requests]


@pytest.fixture
def serve():
    """Start a WSGI app on a loopback port and return its base URL."""
    running = []

    def start(app):
        server = make_server("127.0.0.1", 0, app, handler_class=_QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        running.append((server, thread))
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start

    for server, thread in running:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


@pytest.fixture
def org(serve):
    """A running fake org whose ``base_url`` is already filled in."""

    def start(records=None, **kwargs):
        api = FakeSalesforce(records, **kwargs)
        api.base_url = serve(api)
        return api

    return start


def landed_ids(rows):
    """Sorted record ids from landed rows.

    ``id``, not ``Id``: dlt's snake_case naming convention rewrites every
    column on the way into the landing table, so Salesforce's ``SystemModstamp``
    lands as ``system_modstamp``. The *incremental* cursor still names the
    Salesforce spelling — it is applied at extract time, before normalization —
    which is exactly the asymmetry a mapping author trips over.
    """
    return sorted(row["id"] for row in rows)


def sf_config(*, fields=None, **spec):
    account = {"fields": fields or ["Id", "Name", "SystemModstamp"], **spec}
    return {"api_version": API_VERSION, "objects": {"Account": account}}


@pytest.fixture
def make_sf_binding(make_binding, make_connection):
    """A Salesforce Binding whose credentials come from the token backend."""

    def factory(api, config, **kwargs):
        connection = make_connection(
            provider="salesforce",
            auth_backend="token",
            metadata={
                "connection_params": {
                    "access_token": api.token,
                    "instance_url": api.base_url,
                }
            },
        )
        return make_binding(
            source="salesforce", connection=connection, config=config, **kwargs
        )

    return factory


# --- SOQL construction ------------------------------------------------------


def test_soql_selects_the_configured_fields_filter_and_cursor_bound():
    spec = {
        "fields": ["Id", "Name"],
        "where": "Type = 'Customer'",
        "cursor": "SystemModstamp",
    }
    statement = sf_source.build_soql(
        "Account",
        ["Id", "Name", "SystemModstamp"],
        spec,
        "2024-01-03T00:00:00.000+0000",
    )

    assert statement == (
        "SELECT Id, Name, SystemModstamp FROM Account "
        "WHERE (Type = 'Customer') AND SystemModstamp >= 2024-01-03T00:00:00Z"
    )


def test_a_customer_where_clause_is_parenthesised():
    """``A OR B AND cursor >= x`` binds as ``A OR (B AND …)``.

    Unparenthesised, every record matching the first disjunct escapes the
    incremental predicate — the whole object is re-downloaded every run, with
    correct-looking data and no error anywhere.
    """
    statement = sf_source.build_soql(
        "Account",
        ["Id", "SystemModstamp"],
        {"where": "A = 1 OR B = 2", "cursor": "SystemModstamp"},
        "2024-01-03T00:00:00Z",
    )
    assert "WHERE (A = 1 OR B = 2) AND SystemModstamp >=" in statement


def test_the_first_run_queries_without_a_cursor_predicate():
    statement = sf_source.build_soql(
        "Account", ["Id"], {"cursor": "SystemModstamp"}, None
    )
    assert statement == "SELECT Id FROM Account"


@pytest.mark.parametrize(
    "sobject",
    [
        "Account WHERE Id != null OR Name != null",
        "Account'",
        "Account) OR (1=1",
        "Account;DROP",
        "Account,User",
        "1Account",
        "",
    ],
)
def test_an_injected_sobject_name_is_refused(sobject):
    """SOQL has no bind parameters, so the identifier check *is* the defence."""
    with pytest.raises(ConfigurationError, match="API name"):
        sf_source.build_soql(sobject, ["Id"], {}, None)

    with pytest.raises(ConfigurationError, match="API name"):
        LoopbackSalesforceSource().validate_config(
            {"objects": {sobject: {"fields": ["Id"]}}}
        )


@pytest.mark.parametrize(
    "field",
    [
        "Name FROM User WHERE Id != null--",
        "Name, (SELECT Id FROM Contacts)",
        "Name)",
        "Name'",
        "(SELECT Id FROM User)",
    ],
)
def test_an_injected_field_name_is_refused(field):
    with pytest.raises(ConfigurationError, match="API name"):
        sf_source.build_soql("Account", ["Id", field], {}, None)

    with pytest.raises(ConfigurationError, match="API name"):
        LoopbackSalesforceSource().validate_config(
            {"objects": {"Account": {"fields": ["Id", field]}}}
        )


def test_a_relationship_field_path_is_allowed():
    """`Owner.Profile.Name` is a legitimate SELECT entry, not an injection."""
    statement = sf_source.build_soql(
        "Account", ["Id", "Owner.Profile.Name", "Widget__c"], {}, None
    )
    assert statement == "SELECT Id, Owner.Profile.Name, Widget__c FROM Account"


def test_an_injected_cursor_value_cannot_reach_the_where_clause():
    with pytest.raises(ConfigurationError, match="not a Salesforce date"):
        sf_source.build_soql(
            "Account",
            ["Id"],
            {"cursor": "SystemModstamp"},
            "2024-01-01T00:00:00Z OR Id != null",
        )


def test_soql_literal_reformats_what_salesforce_actually_emits():
    """Salesforce emits `+0000`; SOQL rejects it. Echoing it back breaks run 2."""
    assert (
        sf_source.soql_literal("2024-01-03T12:34:56.789+0000") == "2024-01-03T12:34:56Z"
    )
    assert sf_source.soql_literal("2024-01-03T12:34:56Z") == "2024-01-03T12:34:56Z"
    # A naive stamp is UTC, not the worker's local timezone.
    assert sf_source.soql_literal("2024-01-03T12:34:56") == "2024-01-03T12:34:56Z"
    # A Date field's value is already a valid unquoted literal.
    assert sf_source.soql_literal("2024-01-03") == "2024-01-03"
    # Truncated *down*: with `>=` the boundary record is merely re-fetched,
    # whereas rounding up would step over it and lose it.
    assert (
        sf_source.soql_literal("2024-01-03T12:34:56.999+0000") == "2024-01-03T12:34:56Z"
    )


def test_attributes_are_stripped_at_every_level():
    record = sf_source.strip_attributes(
        {
            "attributes": dict(ATTRIBUTES),
            "Id": "001A",
            "Owner": {"attributes": dict(ATTRIBUTES), "Name": "Ada"},
            "Contacts": {"records": [{"attributes": dict(ATTRIBUTES), "Id": "003A"}]},
        }
    )
    assert record == {
        "Id": "001A",
        "Owner": {"Name": "Ada"},
        "Contacts": {"records": [{"Id": "003A"}]},
    }


# --- configuration ----------------------------------------------------------


def test_an_unknown_object_key_is_refused_at_save(salesforce_settings, make_binding):
    """`filter` silently ignored would mean downloading the whole object."""
    binding = make_binding(
        source="salesforce",
        config={"objects": {"Account": {"fields": ["Id"], "filter": "x"}}},
    )
    with pytest.raises(ConfigurationError, match="unknown key"):
        binding_services.validate_binding(binding)


def test_an_unparseable_initial_value_is_refused_at_save(salesforce_settings):
    with pytest.raises(ConfigurationError, match="not a Salesforce date"):
        LoopbackSalesforceSource().validate_config(
            sf_config(cursor="SystemModstamp", initial_value="last tuesday")
        )


def test_objects_may_be_a_bare_list_of_names():
    assert sf_source.object_specs({"objects": ["Account", "Contact"]}) == {
        "Account": {},
        "Contact": {},
    }


def test_an_http_instance_url_is_refused_by_default():
    """Every request carries a bearer token."""
    with pytest.raises(ConfigurationError, match="https"):
        SalesforceSource().validate_config(
            {
                "objects": {"Account": {"fields": ["Id"]}},
                "instance_url": "http://93.184.216.34",
            }
        )


def test_an_internal_instance_url_is_refused_by_default():
    with pytest.raises(ConfigurationError, match="internal address"):
        SalesforceSource().validate_config(
            {
                "objects": {"Account": {"fields": ["Id"]}},
                "instance_url": "https://169.254.169.254",
            }
        )


def test_an_absolute_next_records_url_is_refused():
    """A response must not choose where this org's bearer token is sent."""
    client = sf_source.SalesforceClient(
        instance_url="https://acme.my.salesforce.com",
        access_token="t",
        api_version=API_VERSION,
        session=None,
    )
    with pytest.raises(SourceError, match="somewhere else"):
        client.get_path("https://evil.test/services/data/v60.0/query/01g")


# --- extraction through the landing layer -----------------------------------


def test_pagination_follows_next_records_url_to_the_end(
    salesforce_settings, make_sf_binding, org
):
    """A source that stops at page 1 lands a plausible-looking partial table."""
    api = org(page_size=2)
    binding = make_sf_binding(api, sf_config())

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert any("/query/01g" in path for path in api.paths()), api.paths()
    rows = access.sample_rows(binding, "Account", limit=20)
    assert landed_ids(rows) == ["001A", "001B", "001C"]


def test_the_attributes_envelope_never_reaches_the_landing_table(
    salesforce_settings, make_sf_binding, org
):
    api = org()
    binding = make_sf_binding(api, sf_config())

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "Account", limit=20)
    assert rows
    for row in rows:
        assert not [name for name in row if "attributes" in name.lower()], sorted(row)


def test_the_second_run_sends_the_cursor_and_merges_rather_than_duplicating(
    salesforce_settings, make_sf_binding, org
):
    """Two independent failures are covered here.

    If the cursor never reaches the WHERE clause, run 2 re-downloads the whole
    object — expensive, and against the org's daily allocation eventually fatal.
    If the library's forced ``Incremental`` is not the one that wins, dlt's
    default deduplication key silently drops the record sitting exactly on the
    cursor boundary.
    """
    api = org(page_size=2)
    binding = make_sf_binding(api, sf_config(cursor="SystemModstamp"))

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert all("WHERE" not in soql for soql in api.soql_queries()), api.soql_queries()

    api.records.append(
        {
            "Id": "001D",
            "Name": "Delta",
            "IsDeleted": False,
            "SystemModstamp": "2024-01-05T00:00:00.000+0000",
        }
    )
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    # The org is asked for everything at or after run 1's high-water mark, in
    # the literal form SOQL accepts — not the `+0000` form Salesforce emits.
    assert api.soql_queries()[0].endswith(
        "WHERE SystemModstamp >= 2024-01-03T00:00:00Z"
    ), api.soql_queries()

    rows = access.sample_rows(binding, "Account", limit=20)
    assert landed_ids(rows) == ["001A", "001B", "001C", "001D"], (
        "records duplicated instead of merging on Id"
    )
    by_id = {row["id"]: row for row in rows}
    # The boundary record was re-emitted and merged, not dropped.
    assert by_id["001C"][RUN_ID_COLUMN] == str(second.id)
    assert by_id["001A"][RUN_ID_COLUMN] == str(first.id)


def test_a_deleted_record_lands_as_a_tombstone(
    salesforce_settings, make_sf_binding, org
):
    """queryAll + IsDeleted is what makes deletion propagation real here."""
    api = org(page_size=10)
    binding = make_sf_binding(api, sf_config(cursor="SystemModstamp"))

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert all("queryAll" in path for path in api.paths() if "query" in path)

    # Deleting a record bumps SystemModstamp, so the deletion arrives through
    # the same incremental window an update would.
    api.records[1].update(
        {"IsDeleted": True, "SystemModstamp": "2024-01-04T00:00:00.000+0000"}
    )

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = {row["id"]: row for row in access.sample_rows(binding, "Account", limit=20)}
    assert rows["001B"][DELETED_COLUMN] is True or rows["001B"][DELETED_COLUMN] == 1
    assert rows["001A"][DELETED_COLUMN] in (False, 0)
    # Identity-only, so merge nulls the rest: that lossiness is the documented
    # shape of a tombstone, not a bug in this source.
    assert rows["001B"]["name"] is None

    # The cursor advanced past the deletion, so the tombstone is not re-emitted
    # forever in an otherwise quiet org.
    api.requests.clear()
    third = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert third.status == RunStatus.SUCCEEDED, third.error_message
    assert api.soql_queries()[0].endswith(
        "WHERE SystemModstamp >= 2024-01-04T00:00:00Z"
    ), api.soql_queries()


def test_deletion_detection_can_be_turned_off_per_object(
    salesforce_settings, make_sf_binding, org
):
    """Not every object exposes IsDeleted, and the query fails outright if not."""
    api = org()
    binding = make_sf_binding(
        api, sf_config(detect_deletes=False, fields=["Id", "Name"])
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert any(path.endswith("/query/") for path in api.paths()), api.paths()
    assert not any("queryAll" in path for path in api.paths()), api.paths()
    assert "IsDeleted" not in api.soql_queries()[0]


def test_omitting_fields_describes_the_object_and_skips_unselectable_types(
    salesforce_settings, make_sf_binding, org
):
    """`FIELDS(ALL)` caps at 200 rows, so describe is the only way to say "all"."""
    api = org()
    binding = make_sf_binding(api, {"api_version": API_VERSION, "objects": ["Account"]})

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    soql = api.soql_queries()[0]
    assert "Name" in soql and "SystemModstamp" in soql
    # Compound, blob and hidden fields are MALFORMED_QUERY waiting to happen.
    assert "BillingAddress" not in soql
    assert "Body" not in soql
    assert "OldField" not in soql


def test_the_cursor_and_id_are_forced_into_a_field_selection_that_omits_them(
    salesforce_settings, make_sf_binding, org
):
    """A cursor missing from SELECT means an incremental that never advances."""
    api = org()
    binding = make_sf_binding(api, sf_config(fields=["Name"], cursor="SystemModstamp"))

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert api.soql_queries()[0].startswith(
        "SELECT Id, SystemModstamp, IsDeleted, Name FROM Account"
    ), api.soql_queries()


def test_a_cursor_spelled_with_the_wrong_case_fails_loudly(
    salesforce_settings, make_sf_binding, org
):
    """SOQL accepts the query; the records then carry a differently-cased key.

    With ``on_cursor_value_missing="include"`` — which the library forces, and
    must — this would otherwise be an unbounded full refresh on every run,
    forever, with no error.
    """
    api = org()
    # The org resolves the mis-cased name and answers with its own spelling, so
    # nothing about the request or the response looks wrong.
    binding = make_sf_binding(
        api, sf_config(fields=["Id", "Name"], cursor="systemmodstamp")
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert "systemmodstamp" in api.soql_queries()[0], api.soql_queries()
    assert run.status == RunStatus.FAILED
    assert "canonical API casing" in run.error_message, run.error_message


# --- errors -----------------------------------------------------------------


def _errors(code, message):
    return [{"message": message, "errorCode": code}]


def test_invalid_session_id_maps_to_credentials_revoked():
    """Mapped here, not in the source: 401 is a claim about the credential."""

    class Response:
        status_code = 401
        text = ""

        def json(self):
            return _errors("INVALID_SESSION_ID", "Session expired or invalid")

    with pytest.raises(CredentialsRevoked, match="INVALID_SESSION_ID"):
        sf_source.raise_for_salesforce_error(Response())


def test_invalid_session_id_mid_run_fails_the_run_and_names_the_session(
    salesforce_settings, make_sf_binding, org
):
    api = org()
    api.canned.append((401, _errors("INVALID_SESSION_ID", "Session expired"), None))
    binding = make_sf_binding(api, sf_config())

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "INVALID_SESSION_ID" in run.error_message, run.error_message
    # One request: a dead session is not retried into the org's allocation.
    assert len(api.requests) == 1, api.paths()

    # A session rejected mid-extraction must take the Connection out of service.
    # dlt wraps whatever a resource generator raises in PipelineStepFailed, so
    # the runner classifies on the innermost cause rather than on what it caught
    # — otherwise the Binding would be retried against a dead session forever,
    # spending the org's daily allocation on requests that cannot succeed.
    binding.refresh_from_db()
    binding.connection.refresh_from_db()
    assert run.error_type == "CredentialsRevoked"
    assert binding.connection.status == ConnectionStatus.REVOKED
    assert binding.status == BindingStatus.BLOCKED


def test_request_limit_exceeded_is_a_clear_error_and_is_not_retried(
    salesforce_settings, make_sf_binding, org
):
    """Retrying the daily allocation spends what is left and breaks the org."""
    api = org()
    api.canned.append(
        (
            403,
            _errors(
                "REQUEST_LIMIT_EXCEEDED",
                "TotalRequests Limit exceeded.",
            ),
            None,
        )
    )
    binding = make_sf_binding(api, sf_config())

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    message = run.error_message
    assert "REQUEST_LIMIT_EXCEEDED" in message, message
    assert "24-hour API request allocation" in message, message
    assert "not retried" in message, message
    assert len(api.requests) == 1, api.paths()

    # And it is not misfiled as a permission problem, which would send the
    # operator to profiles and sharing rules instead of to the org's limits.
    assert "not permitted" not in message, message


def test_request_limit_exceeded_is_not_classified_as_a_credential_error():
    class Response:
        status_code = 403
        text = ""

        def json(self):
            return _errors("REQUEST_LIMIT_EXCEEDED", "TotalRequests Limit exceeded.")

    assert sf_auth.credential_error_for(Response()) is None
    with pytest.raises(SourceError, match="allocation"):
        sf_source.raise_for_salesforce_error(Response())


def test_a_429_is_retried_honouring_retry_after(
    salesforce_settings, make_sf_binding, org
):
    """503/429 are transient; the org tells us how long to wait and we wait."""
    api = org()
    api.canned.append((429, _errors("SERVER_BUSY", "slow down"), {"Retry-After": "1"}))
    binding = make_sf_binding(api, sf_config())

    started = time.monotonic()
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    elapsed = time.monotonic() - started

    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert len(api.requests) >= 2, api.paths()
    assert elapsed >= 0.9, f"Retry-After was ignored ({elapsed:.2f}s)"
    assert len(access.sample_rows(binding, "Account", limit=20)) == 3


def test_a_malformed_query_on_an_object_without_isdeleted_says_what_to_change(
    salesforce_settings, make_sf_binding, org
):
    api = org()
    api.canned.append(
        (
            400,
            _errors("INVALID_FIELD", "No such column 'IsDeleted' on entity 'Foo'"),
            None,
        )
    )
    binding = make_sf_binding(api, sf_config())

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "detect_deletes" in run.error_message, run.error_message


# --- discovery and connection tests ----------------------------------------


def test_discover_lists_the_orgs_queryable_sobjects(
    salesforce_settings, make_connection, org
):
    api = org()
    connection = make_connection(
        provider="salesforce",
        auth_backend="token",
        metadata={
            "connection_params": {
                "access_token": api.token,
                "instance_url": api.base_url,
            }
        },
    )

    result = discovery_services.discover_remote(connection, source_key="salesforce")

    names = [entry["name"] for entry in result["resources"]]
    assert names == ["Account", "Contact", "Widget__c"]
    assert "AggregateResult" not in names, "a non-queryable object cannot be synced"
    assert {"name": "Widget__c", "custom": True}.items() <= result["resources"][
        2
    ].items()


def test_discover_with_an_exact_name_returns_that_objects_fields(
    salesforce_settings, make_connection, org
):
    api = org()
    connection = make_connection(
        provider="salesforce",
        auth_backend="token",
        metadata={
            "connection_params": {
                "access_token": api.token,
                "instance_url": api.base_url,
            }
        },
    )

    result = discovery_services.discover_remote(
        connection, source_key="salesforce", query="account"
    )

    assert [entry["name"] for entry in result["resources"]] == ["Account"]
    field_names = [field["name"] for field in result["resources"][0]["fields"]]
    assert "SystemModstamp" in field_names


def test_check_connection_probes_the_configured_object_with_one_request(
    salesforce_settings, make_sf_binding, org
):
    api = org()
    binding = make_sf_binding(api, sf_config())

    status = LoopbackSalesforceSource().check_connection(
        connection=binding.connection,
        credentials={"access_token": api.token, "instance_url": api.base_url},
        binding=binding,
    )

    assert status.startswith("ok (Account")
    assert len(api.requests) == 1
    assert api.soql_queries() == ["SELECT Id FROM Account LIMIT 1"]


# --- the JWT bearer flow ----------------------------------------------------


@pytest.fixture
def rsa_key():
    """A throwaway RSA key pair. Skips where `cryptography` is not installed."""
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return key, pem


@pytest.fixture
def jwt_connection(salesforce_settings, make_connection, rsa_key):
    """A Connection wired for the JWT-bearer flow against a fake org."""
    _, pem = rsa_key

    def factory(api, **metadata):
        MemoryStore.values["sf-key"] = {"private_key": pem}
        return make_connection(
            provider="salesforce",
            auth_backend="salesforce",
            auth_reference="sf-key",
            metadata={
                "client_id": "3MVG9consumer",
                "username": "integration@example.test",
                "login_url": api.base_url,
                "audience": sf_auth.PRODUCTION_LOGIN_URL,
                **metadata,
            },
        )

    return factory


def _decode_segment(segment):
    padded = segment + "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


def test_the_jwt_bearer_grant_signs_a_verifiable_rs256_assertion(
    jwt_connection, org, rsa_key
):
    """Signed by `cryptography`, never hand-rolled: the padding must be right."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    key, _ = rsa_key
    api = org()
    connection = jwt_connection(api)

    credentials = LoopbackSalesforceBackend().get_credentials(connection)

    assert credentials["access_token"] == api.token
    # The instance URL comes back from the token response, because it cannot be
    # derived: every org lives on its own host.
    assert credentials["instance_url"] == api.base_url

    assertion = api.requests[0]["form"]["assertion"][0]
    assert api.requests[0]["form"]["grant_type"] == [sf_auth.JWT_BEARER_GRANT]

    header_b64, claims_b64, signature_b64 = assertion.split(".")
    assert _decode_segment(header_b64) == {"alg": "RS256", "typ": "JWT"}
    claims = _decode_segment(claims_b64)
    assert claims["iss"] == "3MVG9consumer"
    assert claims["sub"] == "integration@example.test"
    # The login *service*, not the (My Domain) login URL.
    assert claims["aud"] == sf_auth.PRODUCTION_LOGIN_URL
    assert 0 < claims["exp"] - time.time() <= sf_auth.ASSERTION_LIFETIME_SECONDS

    signature = base64.urlsafe_b64decode(
        signature_b64 + "=" * (-len(signature_b64) % 4)
    )
    key.public_key().verify(
        signature,
        f"{header_b64}.{claims_b64}".encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )


def test_the_sandbox_flag_selects_the_test_login_host(salesforce_settings):
    backend = SalesforceBackend()

    class FakeConnection:
        id = "c"
        auth_reference = "sf-key"
        auth_metadata: ClassVar[dict] = {}
        metadata: ClassVar[dict] = {"sandbox": True, "client_id": "x", "username": "y"}

    MemoryStore.values["sf-key"] = {"private_key": "-----BEGIN PRIVATE KEY-----\nx\n"}
    settings = backend.resolve_settings(FakeConnection())

    assert settings["login_url"] == sf_auth.SANDBOX_LOGIN_URL
    assert settings["audience"] == sf_auth.SANDBOX_LOGIN_URL


def test_invalid_grant_is_reported_as_revoked(jwt_connection, org):
    api = org()
    api.token_status = 400
    api.token_response = {
        "error": "invalid_grant",
        "error_description": "user hasn't approved this consumer",
    }
    connection = jwt_connection(api)

    with pytest.raises(CredentialsRevoked, match="pre-authorised"):
        LoopbackSalesforceBackend().get_credentials(connection)


def test_an_expired_grant_is_reported_as_expired_not_revoked(jwt_connection, org):
    """Both are terminal for the runner; only one tells the operator what to do."""
    api = org()
    api.token_status = 400
    api.token_response = {
        "error": "invalid_grant",
        "error_description": "expired access/refresh token",
    }
    connection = jwt_connection(api)

    with pytest.raises(CredentialsExpired, match="Re-authorise"):
        LoopbackSalesforceBackend().get_credentials(connection)


def test_a_wrong_consumer_key_is_configuration_not_revocation(jwt_connection, org):
    api = org()
    api.token_status = 400
    api.token_response = {
        "error": "invalid_client_id",
        "error_description": "client identifier invalid",
    }
    connection = jwt_connection(api)

    with pytest.raises(ConfigurationError, match="consumer key"):
        LoopbackSalesforceBackend().get_credentials(connection)


def test_a_missing_stored_credential_reads_as_revoked(jwt_connection, org):
    api = org()
    connection = jwt_connection(api)
    MemoryStore.values.pop("sf-key")

    with pytest.raises(CredentialsRevoked, match="auth_reference"):
        LoopbackSalesforceBackend().get_credentials(connection)


def test_a_garbage_private_key_is_configuration_and_leaks_nothing(jwt_connection, org):
    api = org()
    connection = jwt_connection(api)
    MemoryStore.values["sf-key"] = {
        "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----"
    }

    with pytest.raises(ConfigurationError) as caught:
        LoopbackSalesforceBackend().get_credentials(connection)

    assert "not-a-key" not in str(caught.value)


def test_the_refresh_token_flow_exchanges_the_stored_token(
    salesforce_settings, make_connection, org
):
    api = org()
    MemoryStore.values["sf-refresh"] = {
        "refresh_token": "5Aep861refresh",
        "client_secret": "shhh",
    }
    connection = make_connection(
        provider="salesforce",
        auth_backend="salesforce",
        auth_reference="sf-refresh",
        metadata={"client_id": "3MVG9consumer", "login_url": api.base_url},
    )

    credentials = LoopbackSalesforceBackend().get_credentials(connection)

    assert credentials["access_token"] == api.token
    form = api.requests[0]["form"]
    assert form["grant_type"] == ["refresh_token"]
    assert form["refresh_token"] == ["5Aep861refresh"]
    assert form["client_secret"] == ["shhh"]


def test_health_reports_what_is_configured_and_never_what_it_holds(
    salesforce_settings, make_connection, org, rsa_key
):
    _, pem = rsa_key
    api = org()
    MemoryStore.values["sf-key"] = {"private_key": pem}
    connection = make_connection(
        provider="salesforce",
        auth_backend="salesforce",
        auth_reference="sf-key",
        metadata={
            "client_id": "3MVG9consumer",
            "username": "integration@example.test",
            "login_url": api.base_url,
        },
    )

    health = LoopbackSalesforceBackend().health(connection)

    assert health["private_key_present"] is True
    assert health["client_id_set"] is True
    assert pem not in json.dumps(health)
    # A boolean about the key, never the key: health() is rendered in the admin.
    assert "private_key" not in health


def test_test_connection_mints_a_token_and_calls_the_org(jwt_connection, org):
    api = org()
    connection = jwt_connection(api)

    status = LoopbackSalesforceBackend().test(connection)

    assert status == "ok (2 API versions available)"
    assert api.paths() == [sf_auth.TOKEN_PATH, "/services/data/"]


def test_the_backend_and_the_source_compose_end_to_end(
    salesforce_settings, make_binding, jwt_connection, org
):
    """The whole path: JWT assertion, token, SOQL, pagination, landing."""
    api = org(page_size=2)
    connection = jwt_connection(api)
    connection.status = ConnectionStatus.ACTIVE
    connection.save(update_fields=["status"])

    binding = make_binding(
        source="salesforce",
        connection=connection,
        config=sf_config(cursor="SystemModstamp"),
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert api.paths()[0] == sf_auth.TOKEN_PATH
    assert api.requests[1]["authorization"] == f"Bearer {api.token}"
    rows = access.sample_rows(binding, "Account", limit=20)
    assert landed_ids(rows) == ["001A", "001B", "001C"]
