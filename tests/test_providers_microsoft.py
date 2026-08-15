"""The Microsoft connectors, exercised entirely offline.

Two fakes stand in for Microsoft, and both are deliberately *protocol* fakes
rather than method stubs, because the bugs worth catching here live in the
protocol:

``FakeGraph``
    a WSGI application serving the subset of Graph these sources speak — the
    ``/delta`` change feed with real ``@odata.nextLink`` paging and
    ``@odata.deltaLink`` tokens, ``/content`` downloads, and a scripting hook so
    a test can force the 429/401/410 responses that decide whether a Binding
    survives contact with a throttled tenant. It runs on a loopback port, so the
    requests are real HTTP made by the real session the source builds.

``FakeEntraTransport``
    an ``azure.core`` transport serving the OIDC discovery document and the
    token endpoint. azure-identity's own code path runs on top of it: the client
    assertion in the certificate test is genuinely signed with a key the test
    generated, and the ``ClientAuthenticationError`` the error-mapping tests
    classify is the one azure-identity really raises.

Nothing here reaches a network the test process does not own, and the source
tests land through ``services.runs.run_binding`` — a source that builds a
plausible ``DltSource`` and lands nothing at all passes any test that stops at
``build_source``.
"""

import datetime as dt
import io
import json
import re
import threading
import tracemalloc
import zipfile
from collections import deque
from http import HTTPStatus
from typing import ClassVar
from urllib.parse import parse_qs, quote
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

openpyxl = pytest.importorskip("openpyxl")
pytest.importorskip("azure.identity")

from django.core.exceptions import ImproperlyConfigured  # noqa: E402

from django_connectors.enums import (  # noqa: E402
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
)
from django_connectors.exceptions import (  # noqa: E402
    AuthError,
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
    SourceError,
)
from django_connectors.landing import access  # noqa: E402
from django_connectors.landing.naming import (  # noqa: E402
    DELETED_COLUMN,
    RUN_ID_COLUMN,
    is_internal_column,
)
from django_connectors.providers.microsoft import excel as excel_module  # noqa: E402
from django_connectors.providers.microsoft import files as files_module  # noqa: E402
from django_connectors.providers.microsoft.auth import EntraBackend  # noqa: E402
from django_connectors.providers.microsoft.excel import EntraExcelSource  # noqa: E402
from django_connectors.providers.microsoft.files import EntraFilesSource  # noqa: E402
from django_connectors.secrets import SecretStore  # noqa: E402
from django_connectors.services import bindings as binding_services  # noqa: E402
from django_connectors.services import runs as run_services  # noqa: E402

pytestmark = pytest.mark.django_db

TENANT_ID = "11111111-1111-1111-1111-111111111111"
CLIENT_ID = "22222222-2222-2222-2222-222222222222"
DRIVE_ID = "b!driveIdentifier"
SITE_ID = "contoso.sharepoint.com,33333333,44444444"


# --- registry wiring -------------------------------------------------------


class LoopbackFilesSource(EntraFilesSource):
    """The file source with the Graph-endpoint allow-list lifted.

    The guard is a class attribute rather than a config key precisely so that
    lifting it takes a deliberate registration — which is what this is, and what
    a host pointing at a recorded-traffic proxy would do.
    """

    allow_custom_graph_base_url = True


class LoopbackExcelSource(EntraExcelSource):
    allow_custom_graph_base_url = True


class StaticTokenBackend:
    """Hands over whatever ``Connection.metadata['connection_params']`` holds.

    Stands in for any host that already has a Graph token from its own
    infrastructure. The sources must work with one: that is what
    ``access_token()`` accepting a plain mapping is for.
    """

    def get_credentials(self, connection):
        return (connection.metadata or {}).get("connection_params")


class MemorySecretStore(SecretStore):
    """A writable store whose contents a test controls directly."""

    values: ClassVar[dict[str, object]] = {}

    def get(self, connection, key):
        return self.values.get(key)

    def set(self, connection, key, value):
        self.values[key] = value

    def delete(self, connection, key):
        return self.values.pop(key, None) is not None


class FakeTransportEntraBackend(EntraBackend):
    """EntraBackend talking to :class:`FakeEntraTransport` instead of Microsoft.

    Overrides the documented extension point rather than patching internals, so
    what is under test is the real credential construction and the real error
    handling.
    """

    transport = None

    def credential_options(self, connection):
        return {
            "transport": type(self).transport,
            # The fake serves the tenant's own discovery document; instance
            # discovery would otherwise call out to login.microsoftonline.com.
            "disable_instance_discovery": True,
        }


SOURCE_PATHS = {
    "entra_files": "tests.test_providers_microsoft.LoopbackFilesSource",
    "entra_excel": "tests.test_providers_microsoft.LoopbackExcelSource",
}


@pytest.fixture
def microsoft_settings(connectors_settings, settings):
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], **SOURCE_PATHS},
        "AUTH_BACKENDS": {
            "token": "tests.test_providers_microsoft.StaticTokenBackend",
            "entra": "tests.test_providers_microsoft.FakeTransportEntraBackend",
        },
        "SECRET_STORE": "tests.test_providers_microsoft.MemorySecretStore",
    }
    return settings.DJANGO_CONNECTORS


@pytest.fixture(autouse=True)
def _clear_secret_store():
    MemorySecretStore.values.clear()
    yield
    MemorySecretStore.values.clear()
    FakeTransportEntraBackend.transport = None


# --- a Microsoft Graph that runs on loopback -------------------------------


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        """Keep pytest output readable."""


def drive_item(
    item_id,
    name,
    *,
    folder_path="/Reports",
    drive_id=DRIVE_ID,
    size=2048,
    modified="2024-05-01T10:00:00Z",
    version=1,
    is_folder=False,
):
    """One Graph ``driveItem``, shaped the way the real API shapes them."""
    item = {
        "id": item_id,
        "name": name,
        "size": size,
        "eTag": f'"{{{item_id}}},{version}"',
        "cTag": f'"c:{{{item_id}}},{version}"',
        "webUrl": f"https://contoso.sharepoint.com/Docs/{quote(name)}",
        "createdDateTime": "2024-01-01T00:00:00Z",
        "lastModifiedDateTime": modified,
        "createdBy": {"user": {"displayName": "Ada Lovelace"}},
        # An application identity, not a user: files written by Power Automate
        # or the sync client have no `user` slot at all.
        "lastModifiedBy": {"application": {"displayName": "Power Automate"}},
        "parentReference": {
            "driveId": drive_id,
            "id": "PARENTID",
            # Graph percent-encodes this, which is why item_path unquotes it.
            "path": f"/drive/root:{quote(folder_path)}",
        },
    }
    if is_folder:
        item["folder"] = {"childCount": 1}
    else:
        item["file"] = {
            "mimeType": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            "hashes": {"quickXorHash": f"hash-of-{item_id}"},
        }
    return item


def deleted_item(item_id, name=None):
    """What Graph's delta feed emits for a removed driveItem.

    The id is the only field that is reliably there; `name` is optional exactly
    because Graph frequently omits it, which is why a glob cannot be applied to
    a deletion.
    """
    entry = {"id": item_id, "deleted": {"state": "deleted"}}
    if name:
        entry["name"] = name
    return entry


ROOT_ITEM = {"id": "ROOTID", "name": "root", "root": {}, "folder": {"childCount": 3}}


class FakeGraph:
    """A Graph subset: delta paging, delta tokens, item metadata and content."""

    def __init__(self, *, page_size=2):
        self.page_size = page_size
        #: Current state of the hierarchy, keyed by id. A delta call with no
        #: token returns all of it, which is exactly what Graph documents.
        self.items = {}
        #: One entry per generation; a delta call with `token=k` returns
        #: everything from generation k onwards.
        self.changes = []
        self.files = {}
        self.requests = []
        self.scripted = deque()
        self.origin = ""

    # -- test-side mutation --

    def seed(self, *items):
        for item in items:
            self.items[item["id"]] = item

    def change(self, *items):
        """Record a generation of changes and apply them to current state."""
        for item in items:
            if item.get("deleted"):
                self.items.pop(item["id"], None)
            else:
                self.items[item["id"]] = item
        self.changes.append(list(items))

    def script(self, status, body=None, headers=None):
        """Force the next request — whatever it is — to get this response."""
        self.scripted.append((status, body, headers or {}))

    # -- request log helpers --

    def delta_requests(self):
        return [r for r in self.requests if r["path"].endswith("/delta")]

    def delta_tokens(self):
        return [r["query"].get("token", [None])[0] for r in self.delta_requests()]

    def paths(self):
        return [r["path"] for r in self.requests]

    # -- WSGI --

    def __call__(self, environ, start_response):
        path = environ["PATH_INFO"]
        query = parse_qs(environ.get("QUERY_STRING", ""))
        self.requests.append(
            {
                "path": path,
                "query": query,
                "authorization": environ.get("HTTP_AUTHORIZATION"),
            }
        )

        if self.scripted:
            status, body, headers = self.scripted.popleft()
            return self._respond(start_response, status, body, headers)

        if path.endswith("/delta"):
            return self._delta(start_response, path, query)
        if path.endswith("/content"):
            return self._content(start_response, path)
        if path.endswith("/children"):
            return self._respond(
                start_response,
                200,
                {"value": [item for item in self.items.values() if "root" not in item]},
            )
        if "/items/" in path:
            item_id = path.split("/items/", 1)[1].split("/")[0]
            item = self.items.get(item_id)
            if item is None:
                return self._respond(
                    start_response, 404, {"error": {"code": "itemNotFound"}}
                )
            return self._respond(start_response, 200, item)
        if path.endswith("/drive") or path.rstrip("/").split("/")[-2] == "drives":
            return self._respond(
                start_response,
                200,
                {"id": DRIVE_ID, "name": "Documents", "driveType": "documentLibrary"},
            )
        if path.endswith("/drives"):
            return self._respond(
                start_response,
                200,
                {"value": [{"id": DRIVE_ID, "name": "Documents"}]},
            )
        if path.endswith("/sites"):
            return self._respond(
                start_response,
                200,
                {"value": [{"id": SITE_ID, "displayName": "Contoso"}]},
            )
        return self._respond(start_response, 404, {"error": {"code": "unknownRoute"}})

    def _delta(self, start_response, path, query):
        token = query.get("token", [None])[0]
        if token is None:
            results = list(self.items.values())
        else:
            results = [item for batch in self.changes[int(token) :] for item in batch]

        page = int(query.get("_page", ["0"])[0])
        window = results[page * self.page_size : (page + 1) * self.page_size]
        body = {"value": window}

        if (page + 1) * self.page_size < len(results):
            carry = f"&token={token}" if token is not None else ""
            body["@odata.nextLink"] = f"{self.origin}{path}?_page={page + 1}{carry}"
        else:
            body["@odata.deltaLink"] = f"{self.origin}{path}?token={len(self.changes)}"
        return self._respond(start_response, 200, body)

    def _content(self, start_response, path):
        item_id = path.split("/items/", 1)[1].split("/")[0]
        data = self.files.get(item_id)
        if data is None:
            return self._respond(
                start_response, 404, {"error": {"code": "itemNotFound"}}
            )
        return self._respond(start_response, 200, data)

    def _respond(self, start_response, status, body, headers=None):
        if isinstance(body, bytes):
            payload, content_type = body, "application/octet-stream"
        else:
            payload = json.dumps(body if body is not None else {}).encode()
            content_type = "application/json"
        base_headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(payload)),
            "request-id": "fake-request-id",
        }
        base_headers.update(headers or {})
        start_response(
            f"{status} {HTTPStatus(status).phrase}", list(base_headers.items())
        )
        return [payload]


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
def graph(serve):
    """A running FakeGraph, with its own origin recorded for nextLink building."""
    api = FakeGraph()
    api.origin = serve(api)
    return api


@pytest.fixture
def make_graph_binding(make_binding, make_connection, graph):
    """A Binding pointed at the fake Graph, with a token-bearing Connection."""

    def factory(source="entra_files", **config):
        connection = make_connection(
            provider="microsoft",
            auth_backend="token",
            external_tenant_id=TENANT_ID,
            metadata={"connection_params": {"access_token": "APP-ONLY-TOKEN"}},
        )
        return make_binding(
            source=source,
            connection=connection,
            config={
                "graph_base_url": f"{graph.origin}/v1.0",
                "drive_id": DRIVE_ID,
                **config,
            },
        )

    return factory


def business_columns(rows):
    return sorted(name for name in rows[0] if not is_internal_column(name))


# ---------------------------------------------------------------------------
# files: the delta change feed
# ---------------------------------------------------------------------------


def test_delta_enumerates_the_whole_drive_and_follows_every_page(
    microsoft_settings, make_graph_binding, graph
):
    """A source that stops after page 1 lands a plausible-looking partial drive."""
    graph.seed(
        ROOT_ITEM,
        drive_item("F1", "budget.xlsx"),
        drive_item("F2", "notes.docx", folder_path="/Q3 Reports"),
        drive_item("F3", "summary.xlsx"),
        drive_item("D1", "Subfolder", is_folder=True),
    )
    binding = make_graph_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    # Page size is 2 and five items were seeded: paging really happened.
    assert len(graph.delta_requests()) >= 3, graph.paths()

    rows = access.sample_rows(binding, "drive_items", limit=50)
    by_id = {row["id"]: row for row in rows}
    # The root folder and the sub-folder are not documents and must not land.
    assert sorted(by_id) == ["F1", "F2", "F3"], sorted(by_id)

    assert by_id["F2"]["path"] == "/Q3 Reports/notes.docx", (
        "Graph percent-encodes parentReference.path; landing it raw would put "
        "%20 in front of every folder-name mapping"
    )
    assert by_id["F1"]["last_modified_by"] == "Power Automate", (
        "an identitySet with no `user` slot must still yield a name"
    )
    assert by_id["F1"]["created_by"] == "Ada Lovelace"
    assert by_id["F1"]["drive_id"] == DRIVE_ID
    assert by_id["F1"][DELETED_COLUMN] is False or by_id["F1"][DELETED_COLUMN] == 0


def test_second_run_replays_the_stored_delta_token(
    microsoft_settings, make_graph_binding, graph
):
    """The token must be stored, sent back, and must narrow what is fetched.

    Without it, run 2 re-downloads the entire document estate — which against a
    throttled tenant is not merely wasteful, it is the thing that gets the
    application rate-limited into uselessness.
    """
    graph.seed(ROOT_ITEM, drive_item("F1", "budget.xlsx"))
    binding = make_graph_binding()

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert graph.delta_tokens() == [None], "the first run must not send a token"

    graph.change(drive_item("F2", "new.xlsx"))
    graph.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert graph.delta_tokens() == ["0"], graph.delta_tokens()

    rows = access.sample_rows(binding, "drive_items", limit=50)
    by_id = {row["id"]: row for row in rows}
    assert sorted(by_id) == ["F1", "F2"], "rows duplicated instead of merging"
    # F1 was not re-fetched: it still carries run 1's id.
    assert by_id["F1"][RUN_ID_COLUMN] == str(first.id)
    assert by_id["F2"][RUN_ID_COLUMN] == str(second.id)


def test_a_deleted_driveitem_lands_as_a_tombstone(
    microsoft_settings, make_graph_binding, graph
):
    """The whole reason this source uses delta rather than a modified-time cursor.

    A cursor-based source emits *nothing* for a deleted file, so the landing row
    survives forever and a Projection keeps writing a target record for a
    document that no longer exists.
    """
    graph.seed(ROOT_ITEM, drive_item("F1", "keep.xlsx"), drive_item("F2", "gone.xlsx"))
    binding = make_graph_binding()

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert sorted(
        row["id"] for row in access.sample_rows(binding, "drive_items", limit=50)
    ) == ["F1", "F2"]

    graph.change(deleted_item("F2"))
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "drive_items", limit=50)
    by_id = {row["id"]: row for row in rows}
    assert sorted(by_id) == ["F1", "F2"], "the tombstone must land, not vanish"
    assert bool(by_id["F2"][DELETED_COLUMN]) is True
    assert not bool(by_id["F1"][DELETED_COLUMN])
    # delete-insert merge replaces the whole row, so a tombstone carrying only
    # identity nulls everything else. That is the documented lossiness.
    assert by_id["F2"]["name"] is None
    assert by_id["F1"]["name"] == "keep.xlsx"


def test_the_source_declares_that_it_can_detect_deletions():
    """The API reports this to customers; it must not lie in either direction."""
    assert EntraFilesSource.emits_tombstones is True
    assert EntraExcelSource.emits_tombstones is False


def test_a_name_glob_filters_documents_but_never_drops_a_deletion(
    microsoft_settings, make_graph_binding, graph
):
    graph.seed(
        ROOT_ITEM,
        drive_item("F1", "budget.xlsx"),
        drive_item("F2", "readme.txt"),
        # SharePoint names are case-insensitive; fnmatch on Linux is not, so a
        # naive glob drops this file in production and keeps it on macOS.
        drive_item("F3", "YEAR-END.XLSX"),
    )
    binding = make_graph_binding(name_glob="*.xlsx")

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    rows = access.sample_rows(binding, "drive_items", limit=50)
    assert sorted(row["id"] for row in rows) == ["F1", "F3"]

    # Graph usually omits the name on a deletion, so the glob cannot be applied
    # to it. Emitting anyway is the deliberate choice: a spurious tombstone
    # inserts one dead row, a dropped one leaves a deleted document live forever.
    graph.change(deleted_item("F1"))
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    by_id = {
        row["id"]: row for row in access.sample_rows(binding, "drive_items", limit=50)
    }
    assert bool(by_id["F1"][DELETED_COLUMN]) is True


def test_a_429_is_retried_after_the_interval_graph_asked_for(
    microsoft_settings, make_graph_binding, graph, monkeypatch
):
    """Ignoring Retry-After is how an integration escalates from slow to blocked.

    Real sleeping is replaced so the assertion can be about the *value* honoured
    rather than about a test that takes seven seconds.
    """
    slept = []
    monkeypatch.setattr(files_module, "pause", slept.append)

    graph.seed(ROOT_ITEM, drive_item("F1", "budget.xlsx"))
    graph.script(429, {"error": {"code": "activityLimitReached"}}, {"Retry-After": "7"})
    binding = make_graph_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert slept == [7.0], slept
    assert [
        row["id"] for row in access.sample_rows(binding, "drive_items", limit=5)
    ] == ["F1"]


def test_a_retry_after_beyond_the_budget_gives_up_instead_of_parking_the_worker(
    microsoft_settings, make_graph_binding, graph, monkeypatch
):
    """A worker asleep past its lease is reaped, and the delta token never moves."""
    slept = []
    monkeypatch.setattr(files_module, "pause", slept.append)

    graph.seed(ROOT_ITEM, drive_item("F1", "budget.xlsx"))
    for _ in range(6):
        graph.script(
            429, {"error": {"code": "activityLimitReached"}}, {"Retry-After": "3600"}
        )
    binding = make_graph_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "throttl" in run.error_message.lower(), run.error_message
    # Capped at MAX_RETRY_AFTER_SECONDS, and stopped once the budget was spent.
    assert slept and all(
        delay <= files_module.MAX_RETRY_AFTER_SECONDS for delay in slept
    )
    assert sum(slept) <= files_module.MAX_RETRY_BUDGET_SECONDS


def test_a_410_discards_the_delta_token_and_re_enumerates(
    microsoft_settings, make_graph_binding, graph
):
    """Graph expires delta tokens; a source that cannot resync is wedged forever."""
    graph.seed(ROOT_ITEM, drive_item("F1", "budget.xlsx"))
    binding = make_graph_binding()
    assert (
        run_services.run_binding(binding, trigger=RunTrigger.INITIAL).status
        == RunStatus.SUCCEEDED
    )

    graph.change(drive_item("F2", "new.xlsx"))
    graph.script(410, {"error": {"code": "resyncRequired", "message": "gone"}})
    graph.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    # The stored token was tried, refused, and then dropped for a cold start.
    assert graph.delta_tokens()[0] == "0"
    assert None in graph.delta_tokens(), graph.delta_tokens()

    rows = access.sample_rows(binding, "drive_items", limit=50)
    assert sorted(row["id"] for row in rows) == ["F1", "F2"]


def test_a_continuation_link_to_another_host_is_refused(microsoft_settings, graph):
    """nextLink comes out of a response body and is followed carrying the token."""
    source = LoopbackFilesSource()
    config = {"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID}
    graph.script(
        200,
        {
            "value": [drive_item("F1", "a.xlsx")],
            "@odata.nextLink": "https://evil.example/v1.0/steal",
        },
    )

    with pytest.raises(SourceError, match="Refusing to follow"):
        list(source.iter_items(config, "APP-ONLY-TOKEN"))


def test_discover_lists_a_drives_children_when_one_is_configured(
    microsoft_settings, make_connection, graph
):
    """Discovery runs before any Binding exists, so it reads Connection.metadata."""
    graph.seed(ROOT_ITEM, drive_item("F1", "budget.xlsx"), drive_item("F2", "a.docx"))
    connection = make_connection(
        provider="microsoft",
        auth_backend="token",
        metadata={"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID},
    )

    result = LoopbackFilesSource().discover(
        connection=connection, credentials={"access_token": "APP-ONLY-TOKEN"}
    )
    assert sorted(entry["name"] for entry in result["items"]) == [
        "a.docx",
        "budget.xlsx",
    ]


def test_discover_searches_for_sites_when_nothing_is_configured_yet(
    microsoft_settings, make_connection, graph
):
    connection = make_connection(
        provider="microsoft",
        auth_backend="token",
        metadata={"graph_base_url": f"{graph.origin}/v1.0"},
    )

    result = LoopbackFilesSource().discover(
        connection=connection,
        credentials={"access_token": "APP-ONLY-TOKEN"},
        query="contoso",
    )
    assert [entry["id"] for entry in result["sites"]] == [SITE_ID]
    assert graph.requests[0]["query"] == {"search": ["contoso"]}


def test_check_connection_makes_exactly_one_request(
    microsoft_settings, make_graph_binding, graph
):
    binding = make_graph_binding()
    status = LoopbackFilesSource().check_connection(
        connection=binding.connection,
        credentials={"access_token": "APP-ONLY-TOKEN"},
        binding=binding,
    )
    assert status.startswith("ok")
    assert len(graph.requests) == 1
    assert graph.requests[0]["authorization"] == "Bearer APP-ONLY-TOKEN"


# ---------------------------------------------------------------------------
# files: error mapping
# ---------------------------------------------------------------------------


def test_a_401_from_graph_is_revocation_and_names_the_aadsts_code(
    microsoft_settings, make_graph_binding, graph
):
    """An app-only token minted seconds ago being refused means the grant is gone.

    Graph puts the AADSTS code in the message body, and it is the only part of
    the failure a Microsoft support case can be opened with — so it has to
    survive into the message this package stores.
    """
    graph.script(
        401,
        {
            "error": {
                "code": "InvalidAuthenticationToken",
                "message": (
                    "AADSTS7000229: The client application is missing a service "
                    "principal in the tenant."
                ),
            }
        },
    )
    source = LoopbackFilesSource()
    config = {"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID}

    with pytest.raises(CredentialsRevoked) as caught:
        list(source.iter_items(config, "APP-ONLY-TOKEN"))

    message = str(caught.value)
    assert "AADSTS7000229" in message, message
    assert "InvalidAuthenticationToken" in message, message


def test_a_403_from_graph_is_not_treated_as_revocation(microsoft_settings, graph):
    """A missing permission must not take every other Binding on the tenant down.

    ``CredentialsRevoked`` flips the whole Connection to revoked and blocks each
    Binding under it. One unreadable folder is not a reason for that.
    """
    graph.script(
        403,
        {"error": {"code": "accessDenied", "message": "Insufficient privileges."}},
    )
    source = LoopbackFilesSource()
    config = {"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID}

    with pytest.raises(AuthError) as caught:
        list(source.iter_items(config, "APP-ONLY-TOKEN"))
    assert not isinstance(caught.value, CredentialsRevoked)
    assert "Files.Read.All" in str(caught.value)


def test_a_404_names_what_could_not_be_found(microsoft_settings, graph):
    graph.script(404, {"error": {"code": "itemNotFound", "message": "no drive"}})
    source = LoopbackFilesSource()
    config = {"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID}

    with pytest.raises(SourceError, match="could not find"):
        list(source.iter_items(config, "APP-ONLY-TOKEN"))


def test_a_missing_token_is_an_auth_error_naming_the_backend(microsoft_settings):
    with pytest.raises(AuthError, match="EntraBackend"):
        files_module.access_token(None)


# ---------------------------------------------------------------------------
# files: configuration
# ---------------------------------------------------------------------------


def test_a_non_microsoft_graph_endpoint_is_refused_by_default():
    """graph_base_url is Binding config and every request carries a tenant token."""
    with pytest.raises(ConfigurationError, match="organisation-wide"):
        EntraFilesSource().validate_config(
            {"drive_id": DRIVE_ID, "graph_base_url": "https://evil.example/v1.0"}
        )


@pytest.mark.parametrize(
    "value",
    ["../../me/drive", "b!x/../y", "id with spaces", "a#b", "a?b", "a%2Fb"],
)
def test_an_id_that_could_repoint_the_request_is_refused(value):
    with pytest.raises(ConfigurationError, match="Graph identifier"):
        EntraFilesSource().validate_config({"drive_id": value})


def test_exactly_one_drive_address_is_required():
    with pytest.raises(ConfigurationError, match="exactly one"):
        EntraFilesSource().validate_config({})
    with pytest.raises(ConfigurationError, match="exactly one"):
        EntraFilesSource().validate_config({"drive_id": DRIVE_ID, "site_id": SITE_ID})


def test_a_folder_path_may_not_walk_out_of_its_subtree():
    with pytest.raises(ConfigurationError, match=r"\.\."):
        EntraFilesSource().validate_config(
            {"drive_id": DRIVE_ID, "folder_path": "/Finance/../../Legal"}
        )


def test_a_bad_config_is_rejected_when_the_binding_is_saved(
    microsoft_settings, make_binding, make_connection
):
    """Validation belongs in front of the person editing, not inside a 3am Run."""
    binding = make_binding(
        source="entra_files",
        connection=make_connection(provider="microsoft", auth_backend="token"),
        config={"drive_id": DRIVE_ID, "page_size": 5000},
    )
    with pytest.raises(ConfigurationError, match="page_size"):
        binding_services.validate_binding(binding)


def test_delta_scope_carries_the_folder_so_a_repoint_resets_the_token():
    """A token issued for one folder describes a different hierarchy elsewhere."""
    whole = files_module.delta_scope({"drive_id": DRIVE_ID})
    folder = files_module.delta_scope(
        {"drive_id": DRIVE_ID, "folder_path": "/Finance/Q3 Reports"}
    )
    assert whole == f"drives/{DRIVE_ID}/root"
    assert folder == f"drives/{DRIVE_ID}/root:/Finance/Q3%20Reports:"
    assert whole != folder


def test_retry_after_is_read_as_seconds_or_as_an_http_date():
    assert files_module.parse_retry_after("12") == 12.0
    future = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=30)
    stamp = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    seconds = files_module.parse_retry_after(stamp)
    assert 20 <= seconds <= 31, seconds
    assert files_module.parse_retry_after("soon") is None


# ---------------------------------------------------------------------------
# excel
# ---------------------------------------------------------------------------


def build_workbook(build):
    """Return the bytes of a real .xlsx built by `build(workbook)`."""
    from openpyxl import Workbook

    workbook = Workbook()
    build(workbook)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def awkward_workbook():
    """Every parsing hazard this source claims to cope with, in one file.

    Blank header cell, duplicate headers, a ragged (short) row, a blank spacer
    row, a formula with no cached result, and trailing empty rows.
    """

    def build(workbook):
        sheet = workbook.active
        sheet.title = "Data"
        sheet.append(["Region", None, "id", "id", "Total"])
        # Column B has data but no header — the classic "somebody forgot to
        # name it" case, which must land under a positional name rather than
        # being dropped or colliding with column A.
        sheet.append(["North", "unnamed", "1", "x", 10])
        sheet.append(["South", None, "2", "y", 20])
        sheet.append([])
        sheet.append(["East", None, "3"])
        sheet["A6"] = "Grand total"
        sheet["E6"] = "=SUM(E2:E3)"
        sheet.append([None, None, None, None, None])

        hidden = workbook.create_sheet("Scratch")
        hidden.sheet_state = "hidden"
        hidden.append(["id", "note"])
        hidden.append(["999", "working copy, not real data"])

    return build_workbook(build)


def simple_workbook(values, *, header=("id", "amount"), title="Data"):
    """A clean keyed sheet: one header row, one value per column, no surprises."""

    def build(workbook):
        sheet = workbook.active
        sheet.title = title
        sheet.append(list(header))
        for row in values:
            sheet.append(list(row))

    return build_workbook(build)


def excel_binding(make_graph_binding, graph, item_id="X1", name="data.xlsx", **config):
    graph.seed(ROOT_ITEM, drive_item(item_id, name))
    graph.files[item_id] = config.pop("data", awkward_workbook())
    return make_graph_binding(source="entra_excel", **config)


def test_excel_parses_a_real_workbook_including_every_awkward_shape(
    microsoft_settings, make_graph_binding, graph
):
    # No key_columns: this sheet has a totals row with no business identity, so
    # it is exactly the shape that must be loaded in full-replace mode.
    binding = excel_binding(make_graph_binding, graph)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    assert business_columns(rows) == [
        "_file_id",
        "_file_modified_at",
        "_file_name",
        "_file_path",
        "_row_number",
        "_sheet_name",
        "column_2",
        "id",
        "id_2",
        "region",
        "total",
    ], business_columns(rows)

    by_row = {row["_row_number"]: row for row in rows}
    # Row 4 was blank and is not a record; rows 7+ were trailing emptiness.
    assert sorted(by_row) == [2, 3, 5, 6], sorted(by_row)

    assert by_row[2]["region"] == "North"
    assert by_row[2]["total"] == 10
    # Duplicate header: the second "id" became id_2 instead of overwriting.
    assert by_row[2]["id"] == "1"
    assert by_row[2]["id_2"] == "x"
    # Blank header cell got a positional name rather than being dropped.
    assert by_row[2]["column_2"] == "unnamed"
    assert by_row[3]["column_2"] is None

    # Ragged row: the missing trailing cells are NULL, not an error.
    assert by_row[5]["region"] == "East"
    assert by_row[5]["total"] is None

    # The formula trap, asserted rather than described: openpyxl writes no
    # cached result, so data_only=True reads None. A workbook that has been
    # opened and saved by Excel would carry the number here.
    assert by_row[6]["region"] == "Grand total"
    assert by_row[6]["total"] is None

    # The hidden "Scratch" sheet is somebody's working copy, not data.
    assert {row["_sheet_name"] for row in rows} == {"Data"}
    assert all(row["_file_name"] == "data.xlsx" for row in rows)
    assert all(row["_file_path"] == "/Reports/data.xlsx" for row in rows)


def test_excel_hidden_sheets_land_when_explicitly_asked_for(
    microsoft_settings, make_graph_binding, graph
):
    binding = excel_binding(make_graph_binding, graph, include_hidden_sheets=True)
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    assert {row["_sheet_name"] for row in rows} == {"Data", "Scratch"}


def test_excel_keyed_rows_merge_across_two_runs_instead_of_duplicating(
    microsoft_settings, make_graph_binding, graph
):
    """The point of key_columns: editing a cell updates a row, it does not add one."""
    graph.seed(ROOT_ITEM, drive_item("X1", "data.xlsx"))
    graph.files["X1"] = simple_workbook([["a", 1], ["b", 2]])
    binding = make_graph_binding(source="entra_excel", key_columns=["id"])

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    # Somebody edits row "b" and adds row "c"; Graph reports the file changed.
    graph.files["X1"] = simple_workbook([["a", 1], ["b", 99], ["c", 3]])
    graph.change(drive_item("X1", "data.xlsx", version=2))

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    by_key = {row["id"]: row for row in rows}
    assert sorted(by_key) == ["a", "b", "c"], "rows duplicated instead of merging"
    assert by_key["b"]["amount"] == 99
    assert by_key["a"]["amount"] == 1


def test_excel_keyed_mode_skips_a_workbook_graph_did_not_report_as_changed(
    microsoft_settings, make_graph_binding, graph
):
    """Delta is what makes keyed mode cheap; without it every run re-downloads."""
    binding = excel_binding(
        make_graph_binding,
        graph,
        key_columns=["id"],
        data=simple_workbook([["a", 1]]),
    )
    assert (
        run_services.run_binding(binding, trigger=RunTrigger.INITIAL).status
        == RunStatus.SUCCEEDED
    )
    graph.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert not [path for path in graph.paths() if path.endswith("/content")], (
        "an unchanged workbook was downloaded again"
    )


def test_excel_without_key_columns_rebuilds_the_table_every_run(
    microsoft_settings, make_graph_binding, graph
):
    """Full replace, stated: the alternative is merging on a row's position.

    Inserting one row at the top of a spreadsheet would then rewrite the
    identity of every row beneath it, and every downstream record would silently
    become somebody else's data.
    """

    graph.seed(ROOT_ITEM, drive_item("X1", "data.xlsx"))
    graph.files["X1"] = simple_workbook([["a", 1], ["b", 2], ["c", 3]])
    binding = make_graph_binding(source="entra_excel")

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert len(access.sample_rows(binding, "worksheet_rows", limit=50)) == 3

    # Two rows deleted from the sheet. Merge would leave them; replace must not.
    graph.files["X1"] = simple_workbook([["b", 2]])
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    assert [row["id"] for row in rows] == ["b"], rows
    # Replace mode re-reads every workbook, so the delta token is never stored.
    assert graph.delta_tokens() == [None, None], graph.delta_tokens()


def test_excel_refuses_to_merge_on_a_column_the_sheet_does_not_have(
    microsoft_settings, make_graph_binding, graph
):
    """Silently merging on a missing key collapses every row onto one identity."""
    binding = excel_binding(make_graph_binding, graph, key_columns=["order_number"])

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "order_number" in run.error_message, run.error_message


def test_excel_refuses_a_blank_merge_key_unless_told_to_skip(
    microsoft_settings, make_graph_binding, graph
):
    """A NULL merge key never matches itself, so the row re-inserts every run."""

    def build(workbook):
        sheet = workbook.active
        sheet.title = "Data"
        sheet.append(["id", "amount"])
        sheet.append(["a", 1])
        sheet.append([None, 2])

    graph.seed(ROOT_ITEM, drive_item("X1", "data.xlsx"))
    graph.files["X1"] = build_workbook(build)
    binding = make_graph_binding(source="entra_excel", key_columns=["id"])

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "merge key" in run.error_message, run.error_message

    binding.config = {**binding.config, "on_missing_key": "skip"}
    binding.save(update_fields=["config"])
    retried = run_services.run_binding(binding, trigger=RunTrigger.MANUAL)
    assert retried.status == RunStatus.SUCCEEDED, retried.error_message
    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    assert [row["id"] for row in rows] == ["a"]


def test_excel_skips_lock_files_and_formats_openpyxl_cannot_read(
    microsoft_settings, make_graph_binding, graph
):
    """A shared library is full of these; failing the Run on them would be absurd."""
    graph.seed(
        ROOT_ITEM,
        drive_item("X1", "data.xlsx"),
        drive_item("X2", "~$data.xlsx"),
        drive_item("X3", "legacy.xls"),
        drive_item("X4", "notes.docx"),
    )
    graph.files["X1"] = simple_workbook([["a", 1]])
    binding = make_graph_binding(source="entra_excel", key_columns=["id"])

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    downloaded = [path for path in graph.paths() if path.endswith("/content")]
    assert len(downloaded) == 1 and "X1" in downloaded[0], downloaded


def test_excel_unmerges_a_merged_header_when_asked(microsoft_settings):
    """openpyxl's read-only worksheets expose no merge information at all.

    So the fill is opt-in, and this proves it actually fills — a merged "Q1"
    spanning two columns otherwise yields one name and one blank.
    """

    def build(workbook):
        sheet = workbook.active
        sheet.title = "Data"
        sheet["A1"] = "Quarter"
        sheet["C1"] = "id"
        sheet.merge_cells("A1:B1")
        sheet.append(["Q1", None, "1"])

    data = build_workbook(build)

    plain = list(excel_module.worksheet_records(data))
    assert sorted(plain[0][2]) == ["column_2", "id", "quarter"]

    filled = list(excel_module.worksheet_records(data, unmerge=True))
    assert sorted(filled[0][2]) == ["id", "quarter", "quarter_2"], filled[0][2]


def test_excel_unmerge_does_not_resurrect_the_rows_below_the_data(microsoft_settings):
    """Somebody merged A2:A200 over two rows of data; there are still two rows.

    The fill writes the merged value into every cell of the region, so a region
    running past the last real row makes those rows non-blank — and the
    trailing-blank trim, which exists precisely to stop all-NULL records
    landing, then finds nothing to trim. Each resurrected row is blank in every
    key column but the merged one, so a keyed Binding either fails the Run or
    collapses 197 empty rows onto one identity.
    """

    def build(workbook):
        sheet = workbook.active
        sheet.title = "Data"
        sheet.append(["Region", "Amount"])
        sheet.append(["North", 1])
        sheet.append([None, 2])
        sheet.merge_cells("A2:A200")

    data = build_workbook(build)

    records = list(excel_module.worksheet_records(data, unmerge=True))

    assert [(row_number, values) for _, row_number, values in records] == [
        (2, {"region": "North", "amount": 1}),
        # Still filled where there is data to fill alongside.
        (3, {"region": "North", "amount": 2}),
    ]


def test_excel_refuses_a_workbook_larger_than_the_configured_cap(
    microsoft_settings, make_graph_binding, graph
):
    """openpyxl holds the whole thing in memory; one bad upload OOMs the worker."""
    binding = excel_binding(
        make_graph_binding, graph, key_columns=["id"], max_file_bytes=64
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "max_file_bytes" in run.error_message, run.error_message


def workbook_with_declared_dimension(rows, dimension):
    """A workbook of `rows` honest rows whose ``<dimension>`` says otherwise.

    Only the declaration is rewritten; the cell data is untouched, so the file
    stays small and passes ``max_file_bytes`` with room to spare. This is what
    a hostile (or merely buggy) generator produces.
    """

    def build(workbook):
        sheet = workbook.active
        sheet.title = "Data"
        sheet.append(["id", "amount"])
        for index in range(rows):
            sheet.append([f"r{index}", index])

    original = build_workbook(build)
    rewritten = io.BytesIO()
    with (
        zipfile.ZipFile(io.BytesIO(original)) as source,
        zipfile.ZipFile(rewritten, "w", zipfile.ZIP_DEFLATED) as target,
    ):
        for info in source.infolist():
            payload = source.read(info.filename)
            if info.filename.endswith("sheet1.xml"):
                payload = re.sub(
                    rb'<dimension ref="[^"]*" ?/>',
                    f'<dimension ref="{dimension}"/>'.encode(),
                    payload,
                )
                assert dimension.encode() in payload, "the dimension was not rewritten"
            target.writestr(info, payload)
    return rewritten.getvalue()


def test_excel_does_not_size_its_memory_from_a_workbooks_own_dimension(
    microsoft_settings,
):
    """`max_file_bytes` bounds the download; nothing bounded the parse.

    openpyxl trusts ``<dimension>``, so a read-only sheet pads every row it
    streams out to the declared width and the grid costs
    ``real_rows x declared_width``. A 50 KB file 600x under the byte cap
    therefore asks for hundreds of megabytes, and an unlimited worker is
    OOM-killed — taking every co-tenant Run with it, which is the exact outcome
    the byte cap's docstring promises to prevent.
    """
    honest = workbook_with_declared_dimension(20, "A1:B21")
    bomb = workbook_with_declared_dimension(1000, "A1:XFD1048576")
    assert len(bomb) < 200_000, len(bomb)
    # Warm every lazy import up, so the measurement below is the parse alone.
    list(excel_module.worksheet_records(honest))

    tracemalloc.start()
    try:
        records = list(excel_module.worksheet_records(bomb, source="bomb.xlsx"))
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert len(records) == 1000
    assert records[-1][2] == {"id": "r999", "amount": 999}
    # The honest content is ~2,000 cells. Before the pin on openpyxl's declared
    # extents this measured ~130 MB.
    assert peak < 20 * 1024 * 1024, f"peak was {peak / 1e6:.0f}MB"


def test_excel_refuses_a_sheet_with_more_cells_than_the_cap(microsoft_settings):
    """The byte cap cannot see cell count; a Run must fail rather than the worker."""
    data = workbook_with_declared_dimension(500, "A1:B501")

    with pytest.raises(SourceError, match="max_cells"):
        list(excel_module.worksheet_records(data, source="wide.xlsx", max_cells=100))


def test_excel_reports_a_workbook_it_cannot_open(microsoft_settings):
    with pytest.raises(SourceError, match="could not be opened"):
        list(excel_module.worksheet_records(b"not a zip file at all", source="x.xlsx"))


def test_excel_names_a_missing_worksheet_rather_than_landing_nothing(
    microsoft_settings,
):
    def build(workbook):
        workbook.active.title = "Data"
        workbook.active.append(["id"])

    with pytest.raises(SourceError, match="Budget"):
        list(excel_module.worksheet_records(build_workbook(build), sheets=["Budget"]))


def test_excel_column_names_are_deduplicated_after_normalization():
    """'ID' and 'id' become one identifier; dlt would keep only the last value."""
    assert excel_module.column_keys(["ID", "id", "Order Date", None], 4) == [
        "id",
        "id_2",
        "order_date",
        "column_4",
    ]


def test_excel_refuses_a_positional_merge_configuration():
    with pytest.raises(ConfigurationError, match="key_columns"):
        EntraExcelSource().validate_config(
            {"drive_id": DRIVE_ID, "key_columns": ["", 3]}
        )


def test_excel_item_id_and_folder_narrowing_are_mutually_exclusive():
    with pytest.raises(ConfigurationError, match="one workbook"):
        EntraExcelSource().validate_config(
            {"drive_id": DRIVE_ID, "item_id": "X1", "folder_path": "/Finance"}
        )


def test_excel_syncs_one_named_workbook(microsoft_settings, make_graph_binding, graph):
    graph.seed(ROOT_ITEM, drive_item("X1", "data.xlsx"))
    graph.files["X1"] = awkward_workbook()
    binding = make_graph_binding(source="entra_excel", item_id="X1")

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert not graph.delta_requests(), "a named workbook needs no delta feed"
    rows = access.sample_rows(binding, "worksheet_rows", limit=50)
    assert {row["_file_id"] for row in rows} == {"X1"}


def test_excel_refuses_a_named_workbook_openpyxl_cannot_read(
    microsoft_settings, make_graph_binding, graph
):
    """Skipping silently would sync nothing and report success."""
    graph.seed(ROOT_ITEM, drive_item("X9", "legacy.xls"))
    binding = make_graph_binding(source="entra_excel", item_id="X9")

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "legacy.xls" in run.error_message, run.error_message


# ---------------------------------------------------------------------------
# auth: Entra app-only credentials
# ---------------------------------------------------------------------------


def openid_configuration(tenant):
    """The discovery document msal fetches before it will ask for a token."""
    base = f"https://login.microsoftonline.com/{tenant}"
    return {
        "token_endpoint": f"{base}/oauth2/v2.0/token",
        "authorization_endpoint": f"{base}/oauth2/v2.0/authorize",
        "issuer": f"{base}/v2.0",
        "device_authorization_endpoint": f"{base}/oauth2/v2.0/devicecode",
        "response_modes_supported": ["query", "fragment", "form_post"],
        "response_types_supported": ["code", "id_token", "token"],
        "scopes_supported": ["openid"],
        "subject_types_supported": ["pairwise"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "cloud_instance_name": "microsoftonline.com",
        "msgraph_host": "graph.microsoft.com",
    }


class FakeEntraTransport:
    """An ``azure.core`` transport serving Entra's OIDC and token endpoints.

    Everything above it — the request, the signed client assertion, the error
    translation — is azure-identity's own code.
    """

    def __init__(self, *, token_status=200, token_payload=None):
        self.token_status = token_status
        self.token_payload = token_payload or {
            "token_type": "Bearer",
            "expires_in": 3599,
            "ext_expires_in": 3599,
            "access_token": "APP-ONLY-TOKEN",
        }
        self.requests = []

    def send(self, request, **kwargs):
        import requests as requests_lib
        from azure.core.pipeline.transport import RequestsTransportResponse

        self.requests.append((request.method, request.url))
        if ".well-known/openid-configuration" in request.url:
            status, payload = 200, openid_configuration(request.url.split("/")[3])
        elif request.url.endswith("/token"):
            status, payload = self.token_status, self.token_payload
        else:
            status, payload = 404, {"error": "not_found"}

        response = requests_lib.Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = request.url
        response.reason = HTTPStatus(status).phrase
        return RequestsTransportResponse(request, response)

    def open(self):
        """No pool to open."""

    def close(self):
        """No pool to close."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def token_requests(self):
        return [url for method, url in self.requests if url.endswith("/token")]


def entra_connection(make_connection, *, payload=None, **kwargs):
    connection = make_connection(
        provider="microsoft",
        auth_backend="entra",
        external_tenant_id=kwargs.pop("tenant_id", TENANT_ID),
        auth_reference="entra-app",
        auth_metadata={"client_id": CLIENT_ID},
        **kwargs,
    )
    MemorySecretStore.values["entra-app"] = (
        payload if payload is not None else {"client_secret": "s3cret-value"}
    )
    return connection


def use_transport(**kwargs):
    transport = FakeEntraTransport(**kwargs)
    FakeTransportEntraBackend.transport = transport
    return transport


def test_entra_acquires_an_app_only_token_for_the_connections_tenant(
    microsoft_settings, make_connection
):
    transport = use_transport()
    connection = entra_connection(make_connection)

    credentials = FakeTransportEntraBackend().get_credentials(connection)

    assert credentials["access_token"] == "APP-ONLY-TOKEN"
    assert credentials["tenant_id"] == TENANT_ID
    assert credentials["scope"] == "https://graph.microsoft.com/.default"
    assert credentials["expires_at"] > dt.datetime.now(dt.UTC)
    assert transport.token_requests() == [
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    ]
    # The credential payload never reaches a repr, which is what Credentials is for.
    assert "s3cret-value" not in repr(credentials)


def test_entra_signs_a_client_assertion_with_a_certificate(
    microsoft_settings, make_connection
):
    """Certificate auth means a real signed JWT, built by azure-identity."""
    cryptography = pytest.importorskip("cryptography")
    assert cryptography  # imported for the skip only

    transport = use_transport()
    connection = entra_connection(
        make_connection,
        payload={"client_id": CLIENT_ID, "certificate_data": self_signed_pem()},
    )

    credentials = FakeTransportEntraBackend().get_credentials(connection)
    assert credentials["access_token"] == "APP-ONLY-TOKEN"
    assert transport.token_requests(), "no token request was made"


def self_signed_pem():
    """A throwaway key + certificate, in the PEM bundle azure-identity wants."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "django-connectors")])
    now = dt.datetime.now(dt.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(days=1))
        .not_valid_after(now + dt.timedelta(days=365))
        .sign(key, hashes.SHA256())
    )
    return (
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        + certificate.public_bytes(serialization.Encoding.PEM)
    ).decode()


def aadsts_error(code, description, *, error="invalid_client"):
    return {
        "error": error,
        "error_description": f"{code}: {description} Trace ID: 0000",
        "error_codes": [int(code.removeprefix("AADSTS"))],
    }


def test_an_invalid_client_secret_is_revocation_and_carries_the_aadsts_code(
    microsoft_settings, make_connection
):
    """The code is the only diagnostic; 'Authentication failed' is not one."""
    use_transport(
        token_status=401,
        token_payload=aadsts_error("AADSTS7000215", "Invalid client secret provided."),
    )
    connection = entra_connection(make_connection)

    with pytest.raises(CredentialsRevoked) as caught:
        FakeTransportEntraBackend().get_credentials(connection)
    assert "AADSTS7000215" in str(caught.value)


def test_an_expired_client_secret_is_expiry_not_revocation(
    microsoft_settings, make_connection
):
    """Different remedies: rotate the secret, versus re-consent in the tenant."""
    use_transport(
        token_status=401,
        token_payload=aadsts_error(
            "AADSTS7000222", "The provided client secret keys are expired."
        ),
    )
    connection = entra_connection(make_connection)

    with pytest.raises(CredentialsExpired) as caught:
        FakeTransportEntraBackend().get_credentials(connection)
    assert "AADSTS7000222" in str(caught.value)
    assert "rotate" in str(caught.value)


def test_a_wrong_tenant_id_is_configuration_not_revocation(
    microsoft_settings, make_connection
):
    """Marking the Connection revoked for a typo helps nobody."""
    use_transport(
        token_status=400,
        token_payload=aadsts_error(
            "AADSTS90002", "Tenant not found.", error="invalid_request"
        ),
    )
    connection = entra_connection(make_connection)

    with pytest.raises(ConfigurationError, match="AADSTS90002"):
        FakeTransportEntraBackend().get_credentials(connection)


def test_an_unclassified_failure_stays_retryable(microsoft_settings, make_connection):
    """Guessing 'revoked' would let one Entra outage disable every Connection."""
    use_transport(
        token_status=503,
        token_payload={"error": "temporarily_unavailable", "error_description": "busy"},
    )
    connection = entra_connection(make_connection)

    with pytest.raises(AuthError) as caught:
        FakeTransportEntraBackend().get_credentials(connection)
    assert not isinstance(caught.value, CredentialsRevoked | CredentialsExpired)


def test_a_missing_stored_credential_is_reported_as_revocation(
    microsoft_settings, make_connection
):
    """The ordinary way a stored credential ends is that somebody deleted it."""
    use_transport()
    connection = entra_connection(make_connection)
    MemorySecretStore.values.clear()

    with pytest.raises(CredentialsRevoked, match="no credential is stored"):
        FakeTransportEntraBackend().get_credentials(connection)


def test_a_connection_with_no_tenant_cannot_authenticate(
    microsoft_settings, make_connection
):
    use_transport()
    connection = entra_connection(make_connection, tenant_id="")

    with pytest.raises(ConfigurationError, match="external_tenant_id"):
        FakeTransportEntraBackend().get_credentials(connection)


def test_the_client_secret_is_never_posted_to_a_non_microsoft_authority(
    microsoft_settings, make_connection
):
    """Connection.metadata is admin-editable; this is where the secret goes."""
    use_transport()
    connection = entra_connection(
        make_connection, metadata={"authority": "login.evil.example"}
    )

    with pytest.raises(ConfigurationError, match="not a Microsoft login host"):
        FakeTransportEntraBackend().get_credentials(connection)


def test_a_granular_scope_is_refused_because_app_only_cannot_use_one(
    microsoft_settings, make_connection
):
    use_transport()
    connection = entra_connection(make_connection)
    connection.auth_metadata = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/Files.Read.All",
    }
    connection.save(update_fields=["auth_metadata"])

    with pytest.raises(ConfigurationError, match=r"\.default"):
        FakeTransportEntraBackend().get_credentials(connection)


def test_begin_setup_returns_an_admin_consent_url_bound_to_this_connection(
    microsoft_settings, make_connection
):
    connection = entra_connection(
        make_connection,
        metadata={"redirect_uri": "https://app.example.com/connect/microsoft/"},
        tenant_id="",
    )

    result = FakeTransportEntraBackend().begin_setup(connection)
    connection.refresh_from_db()

    url = result["authorization_url"]
    assert url.startswith(
        "https://login.microsoftonline.com/organizations/v2.0/adminconsent?"
    )
    assert f"client_id={CLIENT_ID}" in url
    assert result["state"] == str(connection.setup_token)
    assert connection.setup_token is not None


def test_complete_setup_records_the_consenting_tenant_and_proves_a_token(
    microsoft_settings, make_connection, rf
):
    """Activating on the strength of a redirect gives a Connection that fails
    every Binding under it."""
    use_transport()
    connection = entra_connection(
        make_connection,
        metadata={"redirect_uri": "https://app.example.com/connect/microsoft/"},
        tenant_id="",
        status=ConnectionStatus.PENDING,
    )
    state = FakeTransportEntraBackend().begin_setup(connection)["state"]
    connection.refresh_from_db()

    consented = "55555555-5555-5555-5555-555555555555"
    request = rf.get(
        "/callback", {"admin_consent": "True", "tenant": consented, "state": state}
    )

    FakeTransportEntraBackend().complete_setup(connection, request=request)
    connection.refresh_from_db()

    assert connection.external_tenant_id == consented
    assert connection.status == ConnectionStatus.ACTIVE
    assert connection.setup_token is None


def test_complete_setup_refuses_a_callback_that_does_not_carry_our_state(
    microsoft_settings, make_connection, rf
):
    """Otherwise anyone who can make an operator follow a URL repoints the tenant."""
    use_transport()
    connection = entra_connection(
        make_connection,
        metadata={"redirect_uri": "https://app.example.com/connect/microsoft/"},
        status=ConnectionStatus.PENDING,
    )
    FakeTransportEntraBackend().begin_setup(connection)
    connection.refresh_from_db()

    request = rf.get(
        "/callback",
        {"admin_consent": "True", "tenant": "attacker-tenant", "state": "not-ours"},
    )
    with pytest.raises(AuthError, match="setup token"):
        FakeTransportEntraBackend().complete_setup(connection, request=request)

    connection.refresh_from_db()
    assert connection.external_tenant_id == TENANT_ID
    assert connection.status != ConnectionStatus.ACTIVE


def test_health_reports_the_credential_kind_and_never_the_credential(
    microsoft_settings, make_connection
):
    connection = entra_connection(make_connection)
    health = FakeTransportEntraBackend().health(connection)

    assert health["credential_kind"] == "client_secret"
    assert health["tenant_id"] == TENANT_ID
    assert "s3cret-value" not in json.dumps(health, default=str)


def test_a_bare_string_credential_is_treated_as_the_client_secret():
    """One injected environment variable is a normal deployment shape."""
    from django_connectors.providers.microsoft.auth import normalize_payload

    assert normalize_payload("  s3cret  ") == {"client_secret": "s3cret"}
    assert normalize_payload('{"client_id": "a", "client_secret": "b"}') == {
        "client_id": "a",
        "client_secret": "b",
    }


def test_an_unusable_stored_credential_never_appears_in_the_error():
    from django_connectors.providers.microsoft.auth import normalize_payload

    with pytest.raises(ConfigurationError) as caught:
        normalize_payload(["s3cret-value"])
    assert "s3cret-value" not in str(caught.value)
    assert "list" in str(caught.value)


def test_revocation_during_a_run_blocks_the_binding_and_the_connection(
    microsoft_settings, make_binding, make_connection, graph
):
    """The end-to-end claim: a revoked app registration stops the schedule.

    Credentials are resolved before the pipeline starts, so this is the path
    where ``services.runs`` sees the CredentialsRevoked it acts on.
    """
    use_transport(
        token_status=401,
        token_payload=aadsts_error("AADSTS7000215", "Invalid client secret provided."),
    )
    connection = entra_connection(make_connection)
    binding = make_binding(
        source="entra_files",
        connection=connection,
        config={"graph_base_url": f"{graph.origin}/v1.0", "drive_id": DRIVE_ID},
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    assert run.status == RunStatus.FAILED
    assert run.error_type == "CredentialsRevoked"
    assert "AADSTS7000215" in run.error_message, run.error_message

    binding.refresh_from_db()
    connection.refresh_from_db()
    assert binding.status == BindingStatus.BLOCKED
    assert connection.status == ConnectionStatus.REVOKED
    assert not graph.requests, "a revoked credential must not reach the provider"


def test_the_backend_reports_a_missing_extra_rather_than_an_import_error(
    microsoft_settings, make_connection, monkeypatch
):
    """A missing extra inside a customer's Run must name the fix."""

    class WithoutAzure(EntraBackend):
        def _identity_module(self):
            raise ConfigurationError(
                "the 'entra' auth backend needs azure-identity: "
                "pip install 'django-connectors[microsoft]'"
            )

    connection = entra_connection(make_connection)
    with pytest.raises(ConfigurationError, match=r"django-connectors\[microsoft\]"):
        WithoutAzure().get_credentials(connection)

    assert EntraBackend.required_extras == {"azure.identity": "microsoft"}


def test_the_registry_can_resolve_every_shipped_microsoft_class(microsoft_settings):
    """A dotted path that does not import is a runtime failure nobody sees."""
    from django.utils.module_loading import import_string

    for path in (
        "django_connectors.providers.microsoft.auth.EntraBackend",
        "django_connectors.providers.microsoft.files.EntraFilesSource",
        "django_connectors.providers.microsoft.excel.EntraExcelSource",
    ):
        assert import_string(path) is not None

    try:
        import_string("django_connectors.providers.microsoft.NoSuchThing")
    except ImportError:
        pass
    else:  # pragma: no cover - guards the assertion above from being vacuous
        raise AssertionError("import_string did not fail for a missing name")
    assert ImproperlyConfigured  # imported for the registry contract above
