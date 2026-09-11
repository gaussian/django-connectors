"""The Google connectors, exercised offline against in-process HTTP servers.

Nothing here reaches a network the test process does not own: both fakes are
``wsgiref`` apps on a loopback port, and the sources are pointed at them by
subclassing — the same deliberate registration ``tests/test_sources.py`` uses to
lift ``RestSource``'s SSRF guard. There is no ``api_base_url`` config key to set,
because a Binding-controlled base URL would forward a Workspace OAuth token to a
host of the customer's choosing.

Most assertions go through ``services.runs.run_binding`` rather than stopping at
``build_source``. A Google source that builds a plausible ``DltSource``, walks
one page, and lands nothing passes any test that stops earlier — and the
interesting failures (a cursor that never advances, rows that duplicate on every
run, a tombstone that does not land) all live in the seam between the source and
the landing layer.

``google-auth`` is an optional extra, so the tests that need it skip rather than
fail on the zero-extras tier. Everything else runs without it: the sources
themselves speak plain HTTP and need no SDK.
"""

import datetime as dt
import json
import threading
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

from django_connectors.auth.base import Credentials
from django_connectors.enums import (
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
)
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsRevoked,
    SourceError,
)
from django_connectors.landing import access
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    RUN_ID_COLUMN,
    is_internal_column,
)
from django_connectors.providers.google import auth as google_auth
from django_connectors.providers.google.gmail import GmailSource, message_record
from django_connectors.providers.google.sheets import (
    GoogleSheetsSource,
    header_columns,
    rows_from_values,
)
from django_connectors.services import bindings as binding_services
from django_connectors.services import runs as run_services

pytestmark = pytest.mark.django_db


# --- pointing the sources at a loopback server ------------------------------

#: Set by the server fixtures. A module-level mapping rather than a constructor
#: argument because the registry instantiates a source with no arguments, and
#: the port is not known until the fixture runs.
LOOPBACK = {}


class LoopbackGmailSource(GmailSource):
    """GmailSource aimed at the in-process fake, as a host would subclass it."""

    @property
    def api_base_url(self):
        return LOOPBACK["gmail"]


class LoopbackSheetsSource(GoogleSheetsSource):
    """GoogleSheetsSource with both Google hosts aimed at one fake.

    Sheets and Drive are different hosts in production and one WSGI app here;
    the paths (``/spreadsheets/…`` and ``/files/…``) do not collide.
    """

    @property
    def api_base_url(self):
        return LOOPBACK["sheets"]

    @property
    def drive_api_base_url(self):
        return LOOPBACK["sheets"]


class TokenAuthBackend:
    """Returns a bare access-token string, which is one of the three shapes.

    Deliberately the *simplest* shape: it proves the sources do not require a
    google-auth credentials object, which is what makes the delegated
    (``AllauthBackend``) path work.
    """

    key = "token"

    def get_credentials(self, connection):
        return (connection.metadata or {}).get("test_token") or "test-access-token"


class RevokingCredentials:
    """A google-auth-shaped credential whose refresh reports a dead grant."""

    token = None
    valid = False

    def refresh(self, request):
        from google.auth.exceptions import RefreshError

        raise RefreshError("invalid_grant: Bad Request", {"error": "invalid_grant"})


class RevokedTokenBackend:
    """An auth backend whose credential can never be renewed."""

    key = "revoked"

    def get_credentials(self, connection):
        return RevokingCredentials()


SOURCE_PATHS = {
    "gmail": "tests.test_providers_google.LoopbackGmailSource",
    "google_sheets": "tests.test_providers_google.LoopbackSheetsSource",
}
AUTH_PATHS = {
    "token": "tests.test_providers_google.TokenAuthBackend",
    "revoked": "tests.test_providers_google.RevokedTokenBackend",
    "google_workspace": (
        "django_connectors.providers.google.auth.GoogleWorkspaceBackend"
    ),
}


@pytest.fixture
def google_settings(connectors_settings, settings):
    """`connectors_settings`, plus the Google sources, backends and a store."""
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], **SOURCE_PATHS},
        "AUTH_BACKENDS": dict(AUTH_PATHS),
        # A writable store, so the service-account key has somewhere to live.
        # "none" mirrors allauth's trust model and keeps cryptography optional.
        "SECRET_STORE": "django_connectors.secrets.ModelSecretStore",
        "SECRET_ENCRYPTION": "none",
    }
    return settings.DJANGO_CONNECTORS


# --- in-process servers ------------------------------------------------------


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        """Keep pytest output readable."""


@pytest.fixture
def serve():
    """Start a WSGI app on a loopback port and return its base URL."""
    running = []

    def start(app):
        server = make_server("127.0.0.1", 0, app, handler_class=_QuietHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        running.append((server, thread))
        return f"http://127.0.0.1:{server.server_address[1]}/"

    yield start

    for server, thread in running:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def _json(start_response, payload, *, status="200 OK", headers=()):
    body = json.dumps(payload).encode()
    start_response(
        status,
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
            *headers,
        ],
    )
    return [body]


def google_error(code, message, reason=None):
    """A Google JSON error body, in the shape the real APIs return."""
    error = {"code": code, "message": message, "status": reason or ""}
    if reason:
        error["errors"] = [{"reason": reason, "message": message}]
    return {"error": error}


class _FakeApi:
    """Shared request recording and forced-response queue."""

    def __init__(self):
        self.requests = []
        self.forced = []

    def force(self, status, payload, *, headers=()):
        """Queue one canned reply, consumed by the next request of any kind."""
        self.forced.append((status, payload, list(headers)))

    def force_always(self, status, payload):
        """Answer every request with this until cleared."""
        self.always = (status, payload)

    always = None

    def record(self, environ):
        self.requests.append(
            {
                "path": environ["PATH_INFO"],
                "query": parse_qs(environ.get("QUERY_STRING", "")),
                "authorization": environ.get("HTTP_AUTHORIZATION"),
            }
        )

    def paths(self):
        return [request["path"] for request in self.requests]

    def calls(self, suffix):
        return [
            request for request in self.requests if request["path"].endswith(suffix)
        ]


class FakeGmail(_FakeApi):
    """The four Gmail endpoints these connectors touch.

    Pagination, history filtering and the expired-history 404 are all real
    behaviours here rather than canned responses, because the connector's job is
    precisely to walk them correctly.
    """

    def __init__(self, messages=(), labels=(), history_id="1000"):
        super().__init__()
        self.messages = {message["id"]: message for message in messages}
        self.labels = list(labels)
        self.history_id = history_id
        self.history = []
        self.history_expired = False
        # When set, every history page answers with this same nextPageToken —
        # the provider/proxy bug that a walk without a circuit breaker turns
        # into an unbounded loop holding the Binding's lease. The repeat stops
        # after `history_repeat_limit` pages purely so that a regression in the
        # breaker is a failing assertion rather than a hung test suite.
        self.history_repeat_token = None
        self.history_repeat_limit = 25
        self.history_repeats = 0

    def __call__(self, environ, start_response):
        self.record(environ)
        path = environ["PATH_INFO"]
        query = parse_qs(environ.get("QUERY_STRING", ""))

        if self.always is not None:
            status, payload = self.always
            return _json(start_response, payload, status=status)
        if self.forced:
            status, payload, headers = self.forced.pop(0)
            return _json(start_response, payload, status=status, headers=headers)

        if path.endswith("/profile"):
            return _json(
                start_response,
                {
                    "emailAddress": "analytics@example.test",
                    "messagesTotal": len(self.messages),
                    "historyId": self.history_id,
                },
            )
        if path.endswith("/labels"):
            return _json(start_response, {"labels": self.labels})
        if path.endswith("/history"):
            return self._history(start_response, query)
        if "/messages/" in path:
            return self._message(start_response, path.rsplit("/", 1)[1])
        if path.endswith("/messages"):
            return self._list(start_response, query)
        return _json(
            start_response,
            google_error(404, f"no such path {path}"),
            status="404 Not Found",
        )

    def _history(self, start_response, query):
        if self.history_expired:
            # Exactly what Gmail answers for a startHistoryId older than its
            # ~1 week retention window.
            return _json(
                start_response,
                google_error(404, "Requested entity was not found.", "notFound"),
                status="404 Not Found",
            )
        start = int(query.get("startHistoryId", ["0"])[0])
        records = [record for record in self.history if int(record["id"]) > start]
        # history.list paginates like every other Google collection, and the
        # walk has to follow it or it lands a mailbox missing whatever fell
        # after page one.
        token = query.get("pageToken", ["0"])[0]
        offset = int(token) if token.isdigit() else 0
        size = int(query.get("maxResults", ["100"])[0])
        page = records[offset : offset + size]
        payload = {"historyId": self.history_id}
        if page:
            payload["history"] = page
        if self.history_repeat_token is not None:
            self.history_repeats += 1
            if self.history_repeats <= self.history_repeat_limit:
                payload["nextPageToken"] = self.history_repeat_token
        elif offset + size < len(records):
            payload["nextPageToken"] = str(offset + size)
        return _json(start_response, payload)

    def _message(self, start_response, message_id):
        message = self.messages.get(message_id)
        if message is None:
            return _json(
                start_response,
                google_error(404, "Requested entity was not found.", "notFound"),
                status="404 Not Found",
            )
        return _json(start_response, message)

    def _list(self, start_response, query):
        ids = list(self.messages)
        offset = int(query.get("pageToken", ["0"])[0])
        size = int(query.get("maxResults", ["100"])[0])
        page = ids[offset : offset + size]
        payload = {
            "messages": [
                {"id": message_id, "threadId": f"t-{message_id}"} for message_id in page
            ],
            "resultSizeEstimate": len(ids),
        }
        if offset + size < len(ids):
            payload["nextPageToken"] = str(offset + size)
        return _json(start_response, payload)


class FakeSheets(_FakeApi):
    """Sheets ``values.batchGet`` and ``spreadsheets.get``, plus Drive's file."""

    def __init__(self, grids, *, title="Test sheet", modified_time="2024-01-01T00:00Z"):
        super().__init__()
        self.grids = dict(grids)
        self.title = title
        self.modified_time = modified_time
        self.sheet_titles = ["Orders", "Lookup"]

    def __call__(self, environ, start_response):
        self.record(environ)
        path = environ["PATH_INFO"]
        query = parse_qs(environ.get("QUERY_STRING", ""))

        if self.always is not None:
            status, payload = self.always
            return _json(start_response, payload, status=status)
        if self.forced:
            status, payload, headers = self.forced.pop(0)
            return _json(start_response, payload, status=status, headers=headers)

        if path.startswith("/files/"):
            return _json(start_response, {"modifiedTime": self.modified_time})
        if path.endswith("/values:batchGet"):
            return self._batch_get(start_response, query)
        if path.startswith("/spreadsheets/"):
            return self._metadata(start_response, query)
        return _json(
            start_response,
            google_error(404, f"no such path {path}"),
            status="404 Not Found",
        )

    def _batch_get(self, start_response, query):
        value_ranges = []
        for a1 in query.get("ranges", []):
            grid = self.grids.get(a1)
            value_range = {"range": f"{a1}1:Z1000", "majorDimension": "ROWS"}
            if grid:
                # An empty range omits "values" entirely, as the real API does.
                value_range["values"] = grid
            value_ranges.append(value_range)
        return _json(
            start_response, {"spreadsheetId": "sheet-1", "valueRanges": value_ranges}
        )

    def _metadata(self, start_response, query):
        fields = query.get("fields", [""])[0]
        if "sheets" in fields:
            return _json(
                start_response,
                {
                    "sheets": [
                        {
                            "properties": {
                                "title": title,
                                "gridProperties": {"rowCount": 9, "columnCount": 4},
                            }
                        }
                        for title in self.sheet_titles
                    ]
                },
            )
        return _json(start_response, {"properties": {"title": self.title}})


@pytest.fixture
def gmail_server(serve, google_settings):
    def start(api):
        LOOPBACK["gmail"] = serve(api)
        return api

    return start


@pytest.fixture
def sheets_server(serve, google_settings):
    def start(api):
        LOOPBACK["sheets"] = serve(api)
        return api

    return start


@pytest.fixture
def google_connection(make_connection):
    def factory(**kwargs):
        kwargs.setdefault("provider", "google")
        kwargs.setdefault("auth_backend", "token")
        kwargs.setdefault("metadata", {"test_token": "ya29.test-token"})
        return make_connection(**kwargs)

    return factory


@pytest.fixture
def gmail_binding(make_binding, google_connection):
    def factory(*, connection=None, resources=("messages",), **config):
        return make_binding(
            source="gmail",
            connection=connection or google_connection(),
            config=config,
            resources=list(resources),
        )

    return factory


@pytest.fixture
def sheets_binding(make_binding, google_connection):
    def factory(*, connection=None, resources=("orders",), **config):
        config.setdefault("spreadsheet_id", "sheet-1")
        return make_binding(
            source="google_sheets",
            connection=connection or google_connection(),
            config=config,
            resources=list(resources),
        )

    return factory


@pytest.fixture
def recorded_backoff(monkeypatch):
    """Capture the delays throttling *would* have slept, without sleeping."""
    delays = []
    monkeypatch.setattr(google_auth, "wait_before_retry", delays.append)
    return delays


# --- fixtures for message payloads ------------------------------------------


def gmail_message(
    message_id,
    *,
    subject=None,
    history_id="1000",
    internal_date="1700000000000",
    label_ids=("INBOX", "UNREAD"),
    body=None,
):
    """A ``users.messages.get`` payload in Gmail's real (nested) shape."""
    payload = {
        "mimeType": "text/plain",
        "headers": [
            {"name": "Subject", "value": subject or f"subject {message_id}"},
            {"name": "From", "value": f"{message_id}@example.test"},
            {"name": "To", "value": "analytics@example.test"},
            {"name": "Message-ID", "value": f"<{message_id}@example.test>"},
        ],
    }
    if body is not None:
        import base64

        payload["body"] = {
            "data": base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")
        }
    return {
        "id": message_id,
        "threadId": f"t-{message_id}",
        "historyId": history_id,
        "labelIds": list(label_ids),
        "snippet": f"snippet {message_id}",
        "sizeEstimate": 4096,
        "internalDate": internal_date,
        "payload": payload,
    }


def business_columns(rows):
    """Landed column names with the library's and dlt's bookkeeping removed."""
    return sorted(name for name in rows[0] if not is_internal_column(name))


# --- gmail: the full-sync path ----------------------------------------------


def test_gmail_full_sync_follows_pagination_and_lands_every_page(
    gmail_server, gmail_binding
):
    """A connector that stops after page 1 lands a convincing partial mailbox."""
    api = gmail_server(
        FakeGmail(messages=[gmail_message(f"m{n}") for n in range(1, 6)])
    )
    binding = gmail_binding(page_size=2)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    tokens = [
        call["query"].get("pageToken", [None])[0] for call in api.calls("/messages")
    ]
    assert tokens == [None, "2", "4"], tokens

    rows = access.sample_rows(binding, "messages", limit=20)
    assert sorted(row["id"] for row in rows) == ["m1", "m2", "m3", "m4", "m5"]
    assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}


def test_gmail_full_sync_flattens_the_message_and_sends_the_token(
    gmail_server, gmail_binding
):
    """Headers are an array of name/value pairs; nesting is off in landing.

    Left unflattened they would land as one opaque JSON column that no
    Projection mapping can reach into.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1", subject="Invoice 42")]))
    binding = gmail_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert api.requests[0]["authorization"] == "Bearer ya29.test-token"

    rows = access.sample_rows(binding, "messages", limit=5)
    assert len(rows) == 1
    row = rows[0]
    assert row["subject"] == "Invoice 42"
    assert row["from_address"] == "m1@example.test"
    assert row["thread_id"] == "t-m1"
    assert "internal_date" in business_columns(rows)


def test_gmail_backfill_resumes_instead_of_truncating(gmail_server, gmail_binding):
    """A capped run must park the remaining pageToken, not declare victory.

    Committing the historyId after a truncated first page would mark the unread
    remainder of the mailbox as synced, and it would never be fetched again.
    """
    api = gmail_server(
        FakeGmail(messages=[gmail_message(f"m{n}") for n in range(1, 6)])
    )
    binding = gmail_binding(page_size=2, max_messages_per_run=2)

    landed = []
    for _ in range(3):
        run = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
        assert run.status == RunStatus.SUCCEEDED, run.error_message
        landed.append(len(access.sample_rows(binding, "messages", limit=50)))

    assert landed == [2, 4, 5], landed
    # The watermark is read once, at the start of the backfill: re-reading it
    # per resumption would silently skip everything that changed in between.
    assert len(api.calls("/profile")) == 1, api.paths()

    # Only once the backfill completed does the incremental path take over.
    api.requests.clear()
    fourth = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert fourth.status == RunStatus.SUCCEEDED, fourth.error_message
    assert api.calls("/history"), api.paths()
    assert not api.calls("/messages"), api.paths()


# --- gmail: the history path -------------------------------------------------


def test_gmail_second_run_reads_history_and_merges_the_change(
    gmail_server, gmail_binding
):
    """Run 2 must ask history, not re-list the mailbox, and must not duplicate.

    Two independent failures are covered. Re-listing means paying to download
    the entire mailbox every run, which against Gmail's quota is eventually
    fatal. Failing to merge means every run stacks a second copy of every
    message, silently, with the merge key correctly configured.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1"), gmail_message("m2")]))
    binding = gmail_binding()

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    # m2 gets read: a label change, which is the majority of real mailbox churn
    # and exactly what a cursor on internalDate would miss.
    api.messages["m2"] = gmail_message("m2", subject="subject m2", label_ids=("INBOX",))
    api.history_id = "1010"
    api.history = [{"id": "1005", "labelsRemoved": [{"message": {"id": "m2"}}]}]
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    starts = [call["query"]["startHistoryId"][0] for call in api.calls("/history")]
    assert starts == ["1000"], api.paths()
    assert not api.calls("/messages"), "run 2 re-listed the whole mailbox"

    rows = access.sample_rows(binding, "messages", limit=20)
    assert sorted(row["id"] for row in rows) == ["m1", "m2"], (
        "messages duplicated instead of merging on the declared primary key"
    )
    by_id = {row["id"]: row for row in rows}
    assert by_id["m2"][RUN_ID_COLUMN] == str(second.id)
    assert by_id["m1"][RUN_ID_COLUMN] == str(first.id)


def test_gmail_expired_history_falls_back_to_a_full_sync(gmail_server, gmail_binding):
    """Gmail 404s a historyId older than ~a week. Without a fallback, forever.

    A Binding that was paused, or whose Connection was blocked over a holiday,
    would otherwise raise the same 404 on every subsequent run and never
    recover without a human deleting its pipeline state.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1"), gmail_message("m2")]))
    binding = gmail_binding()

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    api.history_expired = True
    api.history_id = "2000"
    api.messages["m3"] = gmail_message("m3")
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert api.calls("/history"), "history was never attempted"
    assert api.calls("/messages"), "the 404 did not fall back to a full sync"

    rows = access.sample_rows(binding, "messages", limit=20)
    assert sorted(row["id"] for row in rows) == ["m1", "m2", "m3"]

    # The fallback also re-armed the cursor: run 3 is incremental again, from
    # the watermark the full sync captured.
    api.history_expired = False
    api.requests.clear()
    third = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert third.status == RunStatus.SUCCEEDED, third.error_message
    starts = [call["query"]["startHistoryId"][0] for call in api.calls("/history")]
    assert starts == ["2000"], api.paths()


def test_gmail_deletion_lands_a_tombstone(gmail_server, gmail_binding):
    """messagesDeleted is why this source can set emits_tombstones at all."""
    api = gmail_server(FakeGmail(messages=[gmail_message("m1"), gmail_message("m2")]))
    binding = gmail_binding()

    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    del api.messages["m2"]
    api.history_id = "1010"
    api.history = [{"id": "1005", "messagesDeleted": [{"message": {"id": "m2"}}]}]

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = {row["id"]: row for row in access.sample_rows(binding, "messages", limit=20)}
    assert sorted(rows) == ["m1", "m2"], "the tombstone did not land"
    assert rows["m2"][DELETED_COLUMN], "m2 was not marked deleted"
    assert not rows["m1"][DELETED_COLUMN]
    # The documented cost of a tombstone: delete-insert merge replaces the whole
    # row, so every column the tombstone did not supply is now NULL.
    assert rows["m2"]["subject"] is None
    assert rows["m1"]["subject"] == "subject m1"


def test_gmail_a_message_deleted_between_history_and_fetch_still_tombstones(
    gmail_server, gmail_binding
):
    """History said it existed; the fetch 404s. A tombstone is the honest answer."""
    api = gmail_server(FakeGmail(messages=[gmail_message("m1"), gmail_message("m2")]))
    binding = gmail_binding()
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    api.history_id = "1010"
    api.history = [{"id": "1005", "labelsAdded": [{"message": {"id": "m2"}}]}]
    del api.messages["m2"]  # gone by the time the fetch happens

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = {row["id"]: row for row in access.sample_rows(binding, "messages", limit=20)}
    assert rows["m2"][DELETED_COLUMN]


def test_gmail_an_empty_history_window_still_advances_the_cursor(
    gmail_server, gmail_binding
):
    """A run that lands nothing must still record where it got to.

    Otherwise every subsequent run re-asks the same widening window, and once
    that window falls out of Gmail's ~1 week retention the connector 404s
    permanently — having never landed a single row to show for it. This also
    pins the dlt behaviour it depends on: resource state is committed even when
    the extract produced no records.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    binding = gmail_binding()
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    # The mailbox moved on, but nothing this Binding cares about changed.
    api.history_id = "1010"
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert len(access.sample_rows(binding, "messages", limit=10)) == 1
    assert not api.calls("/messages/m1"), "an unchanged message was re-fetched"

    api.requests.clear()
    third = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert third.status == RunStatus.SUCCEEDED, third.error_message
    starts = [call["query"]["startHistoryId"][0] for call in api.calls("/history")]
    assert starts == ["1010"], "the cursor did not advance across an empty run"


def test_gmail_history_walk_follows_every_page(gmail_server, gmail_binding):
    """history.list paginates too, and stopping at page 1 loses changes silently.

    The lost rows are the ones that changed least recently in the window, so
    the mailbox looks current and the gap only shows up as rows that never
    catch up.
    """
    api = gmail_server(
        FakeGmail(messages=[gmail_message(f"m{n}") for n in range(1, 6)])
    )
    binding = gmail_binding(page_size=2)

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    # Five separate history records, read two pages at a time.
    api.history_id = "1010"
    api.history = [
        {"id": str(1001 + n), "labelsAdded": [{"message": {"id": f"m{n + 1}"}}]}
        for n in range(5)
    ]
    for n in range(1, 6):
        api.messages[f"m{n}"] = gmail_message(f"m{n}", subject=f"changed {n}")
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    tokens = [
        call["query"].get("pageToken", [None])[0] for call in api.calls("/history")
    ]
    assert tokens == [None, "2", "4"], tokens

    rows = {row["id"]: row for row in access.sample_rows(binding, "messages", limit=20)}
    assert sorted(rows) == ["m1", "m2", "m3", "m4", "m5"]
    assert [rows[f"m{n}"]["subject"] for n in range(1, 6)] == [
        f"changed {n}" for n in range(1, 6)
    ], "a history page after the first was never walked"


def test_gmail_a_repeated_history_page_token_fails_instead_of_looping(
    gmail_server, gmail_binding
):
    """A provider echoing one pageToken back must not become an infinite walk.

    Without a breaker the run never returns: it holds the Binding's lease and
    burns quota until the lease is reaped, and the next worker enters the same
    loop. Failing the run is recoverable; a wedged worker is not.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    binding = gmail_binding()

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    api.history_id = "1010"
    api.history = [{"id": "1005", "labelsAdded": [{"message": {"id": "m1"}}]}]
    api.history_repeat_token = "stuck-token"
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.FAILED, second.status
    assert "pageToken" in second.error_message, second.error_message
    # Second page, and no further: the repeat is caught the first time it is
    # seen again, not after the fake stops repeating.
    assert len(api.calls("/history")) == 2, api.paths()


# --- gmail: labels -----------------------------------------------------------


def test_gmail_labels_resource_lands_separately(gmail_server, gmail_binding):
    api = gmail_server(
        FakeGmail(
            messages=[gmail_message("m1")],
            labels=[
                {"id": "INBOX", "name": "INBOX", "type": "system", "messagesTotal": 3},
                {"id": "Label_1", "name": "Finance", "type": "user"},
            ],
        )
    )
    binding = gmail_binding(resources=["labels"])

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "labels", limit=10)
    assert sorted(row["id"] for row in rows) == ["INBOX", "Label_1"]
    assert {row["name"] for row in rows} == {"INBOX", "Finance"}
    # Selecting only `labels` must not have walked the mailbox.
    assert not api.calls("/messages"), api.paths()


# --- gmail: error mapping ----------------------------------------------------


def test_gmail_http_401_is_a_revoked_credential(gmail_server, google_connection):
    """401 is terminal: retrying spends quota against a withdrawn grant."""
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    api.force_always(
        "401 Unauthorized", google_error(401, "Invalid Credentials", "authError")
    )
    connection = google_connection()

    with pytest.raises(CredentialsRevoked, match="401"):
        LoopbackGmailSource().check_connection(
            connection=connection, credentials="ya29.dead"
        )


def test_gmail_http_401_during_extraction_fails_the_run(gmail_server, gmail_binding):
    """The Run fails and the message names the 401.

    The Connection is *not* auto-blocked from here, and that is a property of
    the library rather than of this source: dlt wraps anything a resource
    generator raises in ``PipelineStepFailed``, and ``services.runs`` matches on
    the exception class. That is why ``build_source`` resolves the bearer token
    eagerly — a credential that cannot be obtained at all fails where the runner
    can still classify it (see the next test).
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    binding = gmail_binding()
    api.force_always("401 Unauthorized", google_error(401, "Invalid Credentials"))

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "401" in run.error_message, run.error_message


def test_gmail_403_permission_denied_is_not_a_revocation(
    gmail_server, google_connection
):
    """A missing scope is fixed by an administrator, not by re-authorising.

    Reporting it as CredentialsRevoked would take the whole Connection out of
    service — and every other Binding on it — for a permissions gap.
    """
    api = gmail_server(FakeGmail())
    api.force_always(
        "403 Forbidden",
        google_error(403, "Insufficient Permission", "insufficientPermissions"),
    )

    with pytest.raises(AuthError) as caught:
        LoopbackGmailSource().check_connection(
            connection=google_connection(), credentials="ya29.scopeless"
        )
    assert not isinstance(caught.value, CredentialsRevoked)
    assert "403" in str(caught.value)


def test_gmail_429_is_retried_after_the_provider_supplied_delay(
    gmail_server, gmail_binding, recorded_backoff
):
    """Retry-After is the provider's own figure and must beat our backoff."""
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    api.force(
        "429 Too Many Requests",
        google_error(429, "Too many requests", "rateLimitExceeded"),
        headers=[("Retry-After", "2")],
    )
    binding = gmail_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert recorded_backoff == [2.0], recorded_backoff
    assert len(access.sample_rows(binding, "messages", limit=10)) == 1


def test_gmail_403_with_a_rate_limit_reason_is_throttling_not_permission(
    gmail_server, gmail_binding, recorded_backoff
):
    """Google reports quota as 403 as often as 429.

    A connector that reads only the status code turns a survivable quota blip
    into a permissions failure and stops syncing.
    """
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    api.force(
        "403 Forbidden",
        google_error(403, "User-rate limit exceeded", "userRateLimitExceeded"),
    )
    binding = gmail_binding()

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    # No Retry-After, so the exponential backoff supplied the delay.
    assert recorded_backoff == [1.0], recorded_backoff


def test_gmail_persistent_throttling_eventually_gives_up(
    gmail_server, google_connection, recorded_backoff
):
    """Retrying forever inside a Run holds the Binding's lease until it expires."""
    api = gmail_server(FakeGmail())
    api.force_always("429 Too Many Requests", google_error(429, "Too many requests"))

    with pytest.raises(SourceError, match="429"):
        LoopbackGmailSource().check_connection(
            connection=google_connection(), credentials="ya29.test"
        )
    assert len(recorded_backoff) == google_auth.MAX_THROTTLE_ATTEMPTS - 1


def test_gmail_sustained_quota_exhaustion_is_not_reported_as_a_permissions_problem(
    gmail_server, google_connection, recorded_backoff
):
    """A quota outage and a missing scope must not land in the same bucket.

    Google spells sustained exhaustion as a 403 as readily as a 429, and the
    newer ``status`` vocabulary (``RESOURCE_EXHAUSTED``) as readily as the older
    ``reason`` one.
    """
    api = gmail_server(FakeGmail())
    api.force_always(
        "403 Forbidden",
        google_error(403, "Quota exceeded", "RESOURCE_EXHAUSTED"),
    )

    with pytest.raises(SourceError) as caught:
        LoopbackGmailSource().check_connection(
            connection=google_connection(), credentials="ya29.test"
        )
    assert not isinstance(caught.value, AuthError)
    assert "throttling" in str(caught.value)
    assert len(recorded_backoff) == google_auth.MAX_THROTTLE_ATTEMPTS - 1


def test_gmail_check_connection_makes_one_request(gmail_server, google_connection):
    api = gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    status = LoopbackGmailSource().check_connection(
        connection=google_connection(), credentials="ya29.test"
    )
    assert status.startswith("ok")
    assert len(api.requests) == 1
    assert api.requests[0]["path"].endswith("/profile")


def test_gmail_discover_lists_resources_and_labels(gmail_server, google_connection):
    gmail_server(
        FakeGmail(labels=[{"id": "Label_1", "name": "Finance", "type": "user"}])
    )
    result = LoopbackGmailSource().discover(
        connection=google_connection(), credentials="ya29.test"
    )
    assert [entry["name"] for entry in result["resources"]] == ["messages", "labels"]
    assert result["labels"] == [{"id": "Label_1", "name": "Finance"}]


# --- gmail: configuration and record shape -----------------------------------


def test_gmail_incremental_for_is_none_because_the_cursor_is_a_sync_token():
    """A record-field incremental would silently drop every label change."""
    assert GmailSource().incremental_for("messages", binding=None) is None


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"message_format": "raw"}, "message_format"),
        ({"message_format": "metadata", "include_body": True}, "include_body"),
        ({"page_size": 0}, "page_size"),
        ({"page_size": 10_000}, "page_size"),
        ({"user_id": "a/b"}, "user_id"),
        ({"max_messages_per_run": 0}, "max_messages_per_run"),
        ({"label_ids": "INBOX"}, "label_ids"),
    ],
)
def test_gmail_bad_config_is_refused(config, message):
    with pytest.raises(ConfigurationError, match=message):
        GmailSource().validate_config(config)


def test_gmail_bad_config_is_refused_when_the_binding_is_saved(
    google_settings, gmail_binding
):
    """Validation belongs at save time, not inside a Run at 3am."""
    binding = gmail_binding(message_format="raw")
    with pytest.raises(ConfigurationError, match="message_format"):
        binding_services.validate_binding(binding)


def test_gmail_message_record_survives_a_message_with_no_headers():
    """A minimal-format message has no payload at all and must not raise."""
    record = message_record({"id": "m1", "internalDate": "1700000000000"}, {})
    assert record["id"] == "m1"
    assert record["subject"] is None
    assert record["headers"] == {}
    assert record["internal_date"] == dt.datetime.fromtimestamp(
        1_700_000_000, tz=dt.UTC
    )


def test_gmail_message_record_lowercases_header_names_and_lifts_the_common_ones():
    record = message_record(gmail_message("m1", subject="Hello"), {})
    assert record["subject"] == "Hello"
    assert record["to_addresses"] == "analytics@example.test"
    assert record["rfc822_message_id"] == "<m1@example.test>"
    assert record["headers"]["from"] == "m1@example.test"


def test_gmail_message_record_truncates_a_body():
    """dlt maps an unbounded str to MySQL TEXT, and one oversized row wedges it."""
    payload = gmail_message("m1", body="x" * 100)
    record = message_record(payload, {"include_body": True, "body_max_chars": 10})
    assert record["body_text"] == "x" * 10


# --- gmail: credential shapes ------------------------------------------------


def test_bearer_token_accepts_a_string_a_mapping_and_a_credentials_object():
    """All three reach a source, and refusing any of them breaks a real host."""
    assert google_auth.bearer_token("ya29.plain") == "ya29.plain"
    assert (
        google_auth.bearer_token(
            Credentials(provider="google", access_token="ya29.map")
        )
        == "ya29.map"
    )

    class LiveCredentials:
        token = "ya29.object"
        valid = True

        def refresh(self, request):  # pragma: no cover - never reached here
            raise AssertionError("a valid token must not be refreshed")

    assert google_auth.bearer_token(LiveCredentials()) == "ya29.object"


def test_bearer_token_refuses_no_credentials_rather_than_sending_none():
    with pytest.raises(AuthError, match="needs credentials"):
        google_auth.bearer_token(None)


def test_a_credential_that_cannot_be_renewed_blocks_the_connection(
    gmail_server, make_binding, google_connection
):
    """The one 'revoked' path the runner can actually act on.

    ``services.runs`` classifies on the exception class, and dlt wraps whatever
    a resource generator raises — so this has to fail before the pipeline runs,
    which is exactly what ``build_source``'s eager token resolution arranges.
    """
    pytest.importorskip("google.auth")
    gmail_server(FakeGmail(messages=[gmail_message("m1")]))
    connection = google_connection(auth_backend="revoked")
    binding = make_binding(
        source="gmail", connection=connection, config={}, resources=["messages"]
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    assert run.status == RunStatus.FAILED
    assert run.error_type == "CredentialsRevoked", run.error_message
    connection.refresh_from_db()
    binding.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED
    assert binding.status == BindingStatus.BLOCKED


# --- sheets: parsing ---------------------------------------------------------

ORDERS_RANGE = "Orders!A:D"


def orders_grid(rows):
    return {ORDERS_RANGE: rows}


def test_sheets_header_row_names_the_columns_and_pads_ragged_rows(
    sheets_server, sheets_binding
):
    """Sheets omits trailing empty cells, so rows arrive at different widths."""
    sheets_server(
        FakeSheets(
            orders_grid(
                [
                    ["Order ID", "Customer", "Total"],
                    ["o1", "Acme", 100],
                    ["o2"],  # ragged: the API dropped two trailing blanks
                    ["o3", "Zeta", 300, "extra"],  # wider than the header
                ]
            )
        )
    )
    binding = sheets_binding(ranges={"orders": ORDERS_RANGE})

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "orders", limit=10)
    assert business_columns(rows) == [
        "column_4",
        "customer",
        "order_id",
        "sheet_row_number",
        "total",
    ]
    by_id = {row["order_id"]: row for row in rows}
    assert by_id["o2"]["customer"] is None
    assert by_id["o3"]["column_4"] == "extra"
    assert by_id["o1"]["sheet_row_number"] == 2


def test_sheets_skips_blank_rows_and_normalises_blank_cells(
    sheets_server, sheets_binding
):
    """A blank cell arrives as "" mid-row and as nothing at all at the end."""
    sheets_server(
        FakeSheets(
            orders_grid(
                [
                    ["Order ID", "Customer"],
                    ["o1", ""],
                    [],
                    ["", "   "],
                    ["o2", "Beta"],
                ]
            )
        )
    )
    binding = sheets_binding(ranges={"orders": ORDERS_RANGE})

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "orders", limit=10)
    assert sorted(row["order_id"] for row in rows) == ["o1", "o2"]
    assert {row["customer"] for row in rows} == {None, "Beta"}


def test_sheets_duplicate_and_colliding_headers_are_suffixed():
    """`Order ID` and `order_id` are the same landing column; only one can win.

    Caught here rather than in the destination, where the second would silently
    overwrite the first.
    """
    columns = header_columns(
        ["Order ID", "order_id", "", "Total", "Total", "sheet_row_number"], 6
    )
    assert columns == [
        "order_id",
        "order_id_2",
        "column_3",
        "total",
        "total_2",
        "sheet_row_number_2",
    ]


def test_sheets_header_normalisation_never_raises_on_an_unusable_cell():
    """A header cell holding punctuation or whitespace is ordinary, not fatal."""
    assert header_columns(["   ", "!!!", None], 3) == ["column_1", "x", "column_3"]


def test_sheets_without_a_header_row_uses_positional_names(
    sheets_server, sheets_binding
):
    sheets_server(FakeSheets(orders_grid([["o1", "Acme"], ["o2", "Beta"]])))
    binding = sheets_binding(ranges={"orders": ORDERS_RANGE}, header_row=False)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "orders", limit=10)
    assert business_columns(rows) == ["column_1", "column_2", "sheet_row_number"]
    assert sorted(row["column_1"] for row in rows) == ["o1", "o2"]


def test_sheets_an_empty_range_lands_nothing_without_raising(
    sheets_server, sheets_binding
):
    """The API omits `values` entirely for an empty range."""
    sheets_server(FakeSheets({ORDERS_RANGE: []}))
    binding = sheets_binding(ranges={"orders": ORDERS_RANGE})

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message


# --- sheets: identity --------------------------------------------------------


def test_sheets_keyed_range_merges_across_runs(sheets_server, sheets_binding):
    """Re-reading a sheet must update rows, which is what key_column buys."""
    api = sheets_server(
        FakeSheets(
            orders_grid(
                [
                    ["Order ID", "Status"],
                    ["o1", "new"],
                    ["o2", "new"],
                    ["o3", "new"],
                ]
            )
        )
    )
    binding = sheets_binding(
        ranges={"orders": {"range": ORDERS_RANGE, "key_column": "order_id"}}
    )

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    api.grids[ORDERS_RANGE][2][1] = "shipped"
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "orders", limit=20)
    assert sorted(row["order_id"] for row in rows) == ["o1", "o2", "o3"], (
        "rows duplicated instead of merging on key_column"
    )
    by_id = {row["order_id"]: row for row in rows}
    assert by_id["o2"]["status"] == "shipped"


def test_sheets_keyless_range_replaces_rather_than_accumulating(
    sheets_server, sheets_binding
):
    """No stable key means full replace — never a merge on row index.

    A row inserted at the top of a sheet shifts every index below it, so an
    index merge would rewrite the identity of every row while reporting success.
    """
    api = sheets_server(
        FakeSheets(orders_grid([["Name"], ["alpha"], ["beta"], ["gamma"]]))
    )
    binding = sheets_binding(ranges={"orders": ORDERS_RANGE})

    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert len(access.sample_rows(binding, "orders", limit=20)) == 3

    api.grids[ORDERS_RANGE] = [["Name"], ["alpha"], ["gamma"]]
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "orders", limit=20)
    assert sorted(row["name"] for row in rows) == ["alpha", "gamma"], (
        "the removed row survived, so this was not a replace"
    )


def test_sheets_a_row_with_no_key_fails_the_run_by_default(
    sheets_server, sheets_binding
):
    """Every keyless row would merge into one. Refusing beats collapsing."""
    sheets_server(
        FakeSheets(
            orders_grid([["Order ID", "Status"], ["o1", "new"], ["", "half typed"]])
        )
    )
    binding = sheets_binding(
        ranges={"orders": {"range": ORDERS_RANGE, "key_column": "order_id"}}
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "key_column" in run.error_message, run.error_message
    assert "row 3" in run.error_message, run.error_message


def test_sheets_missing_key_skip_policy_drops_the_row_deliberately(
    sheets_server, sheets_binding
):
    sheets_server(
        FakeSheets(
            orders_grid([["Order ID", "Status"], ["o1", "new"], ["", "half typed"]])
        )
    )
    binding = sheets_binding(
        ranges={
            "orders": {
                "range": ORDERS_RANGE,
                "key_column": "order_id",
                "missing_key": "skip",
            }
        }
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    rows = access.sample_rows(binding, "orders", limit=10)
    assert [row["order_id"] for row in rows] == ["o1"]


def test_sheets_a_renamed_key_column_fails_loudly(sheets_server, sheets_binding):
    """Silently landing every row with a NULL key would be far worse."""
    sheets_server(FakeSheets(orders_grid([["Reference", "Status"], ["o1", "new"]])))
    binding = sheets_binding(
        ranges={"orders": {"range": ORDERS_RANGE, "key_column": "order_id"}}
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "order_id" in run.error_message


# --- sheets: requests --------------------------------------------------------


def test_sheets_reads_every_range_in_one_batch_request(
    sheets_server, make_binding, google_connection
):
    """N resources must not become N round trips — and each must get its own rows.

    The grids are deliberately different shapes: ``values.batchGet`` answers in
    request order and the response is zipped back onto the resource names by
    position, because the ``range`` Google echoes is normalized and cannot be
    used as a key. Nothing else enforces that association, so asserting only
    row counts would let a reordering land one resource's rows in the other's
    table — silent cross-resource corruption with every run reporting success.
    """
    lookup_range = "Lookup!A:B"
    api = sheets_server(
        FakeSheets(
            {
                ORDERS_RANGE: [["Order ID", "Status"], ["o1", "new"], ["o2", "sent"]],
                lookup_range: [
                    ["Code", "Label"],
                    ["a", "Alpha"],
                    ["b", "Beta"],
                    ["c", "Gamma"],
                ],
            }
        )
    )
    binding = make_binding(
        source="google_sheets",
        connection=google_connection(),
        config={
            "spreadsheet_id": "sheet-1",
            "ranges": {"orders": ORDERS_RANGE, "lookup": lookup_range},
        },
        resources=["orders", "lookup"],
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    batch_calls = api.calls("/values:batchGet")
    assert len(batch_calls) == 1, api.paths()
    assert batch_calls[0]["query"]["ranges"] == [ORDERS_RANGE, lookup_range]

    orders = access.sample_rows(binding, "orders", limit=10)
    assert business_columns(orders) == ["order_id", "sheet_row_number", "status"]
    assert {row["order_id"]: row["status"] for row in orders} == {
        "o1": "new",
        "o2": "sent",
    }

    lookup = access.sample_rows(binding, "lookup", limit=10)
    assert business_columns(lookup) == ["code", "label", "sheet_row_number"]
    assert {row["code"]: row["label"] for row in lookup} == {
        "a": "Alpha",
        "b": "Beta",
        "c": "Gamma",
    }


def test_sheets_skip_unchanged_uses_drive_and_leaves_the_rows_alone(
    sheets_server, sheets_binding
):
    """An unchanged spreadsheet costs one Drive request and touches nothing.

    Safe only under merge: verified against dlt 1.30, a ``replace`` resource
    that yields no rows truncates its table, which is why the keyless case is
    refused at save time.
    """
    api = sheets_server(FakeSheets(orders_grid([["Order ID"], ["o1"], ["o2"]])))
    binding = sheets_binding(
        ranges={"orders": {"range": ORDERS_RANGE, "key_column": "order_id"}},
        skip_unchanged=True,
    )

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert len(api.calls("/values:batchGet")) == 1

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message
    assert len(api.calls("/values:batchGet")) == 1, "the sheet was re-read anyway"
    assert len(access.sample_rows(binding, "orders", limit=10)) == 2, (
        "skipping an unchanged sheet emptied the landing table"
    )

    # A real edit is picked up on the next run.
    api.modified_time = "2024-06-01T00:00Z"
    api.grids[ORDERS_RANGE].append(["o3"])
    third = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert third.status == RunStatus.SUCCEEDED, third.error_message
    assert len(access.sample_rows(binding, "orders", limit=10)) == 3


def test_sheets_skip_unchanged_is_refused_for_a_keyless_range():
    """It would delete every row it had already landed."""
    with pytest.raises(ConfigurationError, match="truncates"):
        GoogleSheetsSource().validate_config(
            {
                "spreadsheet_id": "sheet-1",
                "ranges": {"orders": ORDERS_RANGE},
                "skip_unchanged": True,
            }
        )


def test_sheets_http_401_is_a_revoked_credential(sheets_server, google_connection):
    api = sheets_server(FakeSheets({}))
    api.force_always(
        "401 Unauthorized", google_error(401, "Invalid Credentials", "authError")
    )
    connection = google_connection(metadata={"spreadsheet_id": "sheet-1"})

    with pytest.raises(CredentialsRevoked, match="401"):
        LoopbackSheetsSource().check_connection(
            connection=connection, credentials="ya29.dead"
        )


def test_sheets_check_connection_and_discover(sheets_server, google_connection):
    sheets_server(FakeSheets({}, title="Q3 orders"))
    connection = google_connection(metadata={"spreadsheet_id": "sheet-1"})

    assert (
        LoopbackSheetsSource().check_connection(
            connection=connection, credentials="ya29.test"
        )
        == "ok (Q3 orders)"
    )

    discovered = LoopbackSheetsSource().discover(
        connection=connection, credentials="ya29.test"
    )
    assert [entry["name"] for entry in discovered["resources"]] == ["orders", "lookup"]


# --- sheets: configuration ---------------------------------------------------


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"ranges": {"a": "A:B"}}, "spreadsheet_id"),
        (
            {"spreadsheet_id": "https://docs.google.com/x", "ranges": {"a": "A:B"}},
            "bare id",
        ),
        ({"spreadsheet_id": "s", "ranges": {}}, "ranges"),
        ({"spreadsheet_id": "s", "ranges": {"a": ""}}, "A1 'range'"),
        (
            {"spreadsheet_id": "s", "ranges": {"a": "A:B"}, "value_render_option": "X"},
            "value_render_option",
        ),
        (
            {"spreadsheet_id": "s", "ranges": {"a": "A:B"}, "missing_key": "shrug"},
            "missing_key",
        ),
        (
            {
                "spreadsheet_id": "s",
                "ranges": {"a": {"range": "A:B", "key_column": "id"}},
                "header_row": False,
            },
            "header_row=false",
        ),
        # A range name becomes part of the landing table name, and dlt would
        # rewrite this one — so the physical table would not be the one the
        # Binding computes.
        (
            {"spreadsheet_id": "s", "ranges": {"My Orders": "A:B"}},
            "landing table name",
        ),
    ],
)
def test_sheets_bad_config_is_refused(config, message):
    with pytest.raises(ConfigurationError, match=message):
        GoogleSheetsSource().validate_config(config)


def test_sheets_incremental_for_is_none_because_there_is_no_cursor():
    assert GoogleSheetsSource().incremental_for("orders", binding=None) is None


def test_sheets_rows_from_values_reports_the_offending_row_number():
    """The error has to name the sheet row, or nobody can find it."""
    spec = {
        "range": "A:B",
        "key_column": "id",
        "header_row": True,
        "missing_key": "error",
    }
    with pytest.raises(SourceError, match="row 3"):
        list(rows_from_values("orders", [["id", "v"], ["1", "a"], ["", "b"]], spec))


# --- the auth backend --------------------------------------------------------


@pytest.fixture
def google_auth_installed():
    """Skip cleanly on the zero-extras tier rather than failing at collection."""
    return pytest.importorskip("google.oauth2.service_account")


SERVICE_ACCOUNT_KEY = {
    "type": "service_account",
    "project_id": "example",
    "client_email": "connector@example.iam.gserviceaccount.com",
    "private_key_id": "abc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
    "token_uri": "https://oauth2.googleapis.com/token",
}


class FakeServiceAccountCredentials:
    """Stands in for ``service_account.Credentials``. Records how it was built."""

    def __init__(self, *, error=None, token="ya29.issued"):
        self.error = error
        self._token = token
        self.token = None
        self.valid = False
        self.expiry = None
        self.refreshed = 0

    def refresh(self, request):
        self.refreshed += 1
        if self.error is not None:
            raise self.error
        self.token = self._token
        self.valid = True
        self.expiry = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)


@pytest.fixture
def patched_service_account(google_auth_installed, monkeypatch):
    """Replace the key exchange, keeping everything else real.

    Only the signing and the token round trip are stubbed: scope selection,
    impersonation, secret resolution and error classification are the code
    under test and all still run.
    """
    from google.oauth2 import service_account

    captured = {"credentials": None}

    def from_info(info, scopes=None, subject=None):
        captured["info"] = info
        captured["scopes"] = scopes
        captured["subject"] = subject
        credentials = captured.get("next") or FakeServiceAccountCredentials()
        captured["credentials"] = credentials
        return credentials

    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        staticmethod(from_info),
    )
    return captured


@pytest.fixture
def workspace_connection(google_settings, make_connection):
    def factory(**metadata):
        from django_connectors.registry import auth_backends

        connection = make_connection(
            provider="google", auth_backend="google_workspace", metadata=metadata
        )
        backend = auth_backends.get("google_workspace")
        backend.store_credentials(connection, SERVICE_ACCOUNT_KEY)
        return connection, backend

    return factory


def test_workspace_backend_reads_the_key_from_the_store_and_impersonates(
    patched_service_account, workspace_connection
):
    """The key comes from the SecretStore, never from auth_metadata.

    ``Connection.auth_metadata`` is rendered in the admin and returned by the
    API; a service-account private key there is a domain-wide compromise.
    """
    connection, backend = workspace_connection(
        subject="analytics@example.test",
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
    )
    assert connection.auth_reference == "google_service_account"
    assert connection.auth_metadata == {}

    credentials = backend.get_credentials(connection)

    assert patched_service_account["info"]["client_email"].endswith(
        "gserviceaccount.com"
    )
    assert patched_service_account["subject"] == "analytics@example.test"
    assert patched_service_account["scopes"] == [
        "https://www.googleapis.com/auth/gmail.readonly"
    ]
    # Refreshed during get_credentials, so "resolvable" and "accepted" are the
    # same claim by the time a Run uses it.
    assert credentials.refreshed == 1
    assert google_auth.bearer_token(credentials) == "ya29.issued"


def test_workspace_backend_falls_back_to_the_default_scopes(
    patched_service_account, workspace_connection
):
    connection, backend = workspace_connection()
    backend.get_credentials(connection)
    assert patched_service_account["scopes"] == list(google_auth.DEFAULT_SCOPES)
    assert patched_service_account["subject"] is None


def test_workspace_backend_reads_the_key_as_a_json_string(
    patched_service_account, google_settings, make_connection
):
    """External managers hand back the string that was written, not a dict."""
    from django_connectors.registry import auth_backends

    connection = make_connection(provider="google", auth_backend="google_workspace")
    backend = auth_backends.get("google_workspace")
    backend.store_credentials(connection, json.dumps(SERVICE_ACCOUNT_KEY))

    backend.get_credentials(connection)
    assert patched_service_account["info"]["project_id"] == "example"


def test_workspace_backend_maps_invalid_grant_to_revoked(
    patched_service_account, workspace_connection
):
    """The one classification the runner acts on. Retrying never fixes it."""
    from google.auth.exceptions import RefreshError

    patched_service_account["next"] = FakeServiceAccountCredentials(
        error=RefreshError("invalid_grant: Bad Request", {"error": "invalid_grant"})
    )
    connection, backend = workspace_connection(subject="gone@example.test")

    with pytest.raises(CredentialsRevoked, match="invalid_grant"):
        backend.get_credentials(connection)


def test_workspace_backend_does_not_revoke_on_a_transport_failure(
    patched_service_account, workspace_connection
):
    """A DNS blip must not take a healthy Connection out of service."""
    from google.auth.exceptions import TransportError

    patched_service_account["next"] = FakeServiceAccountCredentials(
        error=TransportError("Failed to resolve oauth2.googleapis.com")
    )
    connection, backend = workspace_connection()

    with pytest.raises(AuthError) as caught:
        backend.get_credentials(connection)
    assert not isinstance(caught.value, CredentialsRevoked)


def test_workspace_backend_treats_a_deleted_key_as_revocation(
    patched_service_account, workspace_connection
):
    """Deleting the stored key is how an operator withdraws access."""
    connection, backend = workspace_connection()
    backend.store.delete(connection, connection.auth_reference)

    with pytest.raises(CredentialsRevoked, match="no Google service-account key"):
        backend.get_credentials(connection)


def test_workspace_backend_revoke_removes_the_stored_key(
    patched_service_account, workspace_connection
):
    connection, backend = workspace_connection()
    backend.revoke(connection)

    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED
    assert backend.store.get(connection, "google_service_account") is None


def test_workspace_backend_health_reports_no_secrets(
    patched_service_account, workspace_connection
):
    connection, backend = workspace_connection(subject="analytics@example.test")
    health = backend.health(connection)
    assert health["usable"] is True
    assert health["subject"] == "analytics@example.test"
    assert "token" not in json.dumps(health).lower()


def test_workspace_backend_refuses_a_malformed_key(
    google_auth_installed, google_settings, make_connection
):
    """A paste-o is a configuration fault, not a revocation."""
    from django_connectors.registry import auth_backends

    connection = make_connection(provider="google", auth_backend="google_workspace")
    backend = auth_backends.get("google_workspace")

    with pytest.raises(ConfigurationError, match="missing"):
        backend.store_credentials(connection, {"client_email": "x@example.test"})

    with pytest.raises(ConfigurationError, match="not valid JSON"):
        google_auth.service_account_info("{oops")


def test_workspace_backend_needs_an_auth_reference(google_settings, make_connection):
    from django_connectors.registry import auth_backends

    connection = make_connection(provider="google", auth_backend="google_workspace")
    with pytest.raises(ConfigurationError, match="auth_reference"):
        auth_backends.get("google_workspace").get_credentials(connection)


def test_workspace_backend_refuses_an_empty_scope_list(
    google_settings, make_connection
):
    """A token with no scopes is accepted by Google and refused by every API."""
    from django_connectors.registry import auth_backends

    connection = make_connection(
        provider="google", auth_backend="google_workspace", metadata={"scopes": []}
    )
    with pytest.raises(ConfigurationError, match="empty"):
        auth_backends.get("google_workspace").scopes_for(connection)


# --- connector conformance ---------------------------------------------------
#
# The shared suite from `django_connectors.testing.conformance`, run against the
# fakes above. `tests/test_source_conformance.py` holds the credential-free half
# and asserts that each `test_<key>_conformance` here exists.


def _assert_conformant(source_key, binding, resources):
    from django_connectors.models import Run
    from django_connectors.registry import auth_backends
    from django_connectors.registry import sources as source_registry
    from django_connectors.testing import conformance

    definition = source_registry.get(source_key)
    credentials = auth_backends.get(binding.connection.auth_backend).get_credentials(
        binding.connection
    )

    built = conformance.check_built_source(
        definition,
        binding=binding,
        credentials=credentials,
        run=Run.objects.create(binding=binding),
    )
    assert built == [], "\n".join(built)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    landed = conformance.check_landing_invariants(binding, expected_resources=resources)
    assert landed == [], "\n".join(landed)


def test_gmail_conformance(gmail_server, gmail_binding):
    """Canned payloads, real instrumentation, sqlite — and the same invariants.

    Gmail is the connector where nesting matters most: a message is headers,
    payload parts and label ids, and dlt would spread all of it into child
    tables that carry no tenant scope and no run filter.
    """
    gmail_server(FakeGmail(messages=[gmail_message("m1"), gmail_message("m2")]))
    binding = gmail_binding(resources=("messages", "labels"))
    _assert_conformant("gmail", binding, ["messages", "labels"])

    rows = access.sample_rows(binding, "messages", limit=10)
    assert sorted(row["id"] for row in rows) == ["m1", "m2"]
    assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}
    assert all(row[RUN_ID_COLUMN] for row in rows)


def test_google_sheets_conformance(sheets_server, sheets_binding):
    sheets_server(
        FakeSheets(
            {ORDERS_RANGE: [["Order ID", "Customer"], ["o1", "Acme"], ["o2", "Zeta"]]}
        )
    )
    binding = sheets_binding(
        ranges={"orders": {"range": ORDERS_RANGE, "key_column": "order_id"}}
    )
    _assert_conformant("google_sheets", binding, ["orders"])

    rows = access.sample_rows(binding, "orders", limit=10)
    assert sorted(row["order_id"] for row in rows) == ["o1", "o2"]
