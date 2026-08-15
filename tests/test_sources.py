"""The three shipped source definitions, exercised offline.

Nothing here touches a network the test process does not own: the REST tests
serve JSON from an in-process ``wsgiref`` server, the SQL tests run against a
sqlite file created in the test, and the filesystem tests read files written in
the test. Every one of them lands through
``django_connectors.services.runs.run_binding``, because the interesting
failures are in the seam between a source and the landing layer — a source that
builds a plausible-looking ``DltSource`` and still lands nothing, or lands rows
that duplicate on every run, passes any test that stops at ``build_source``.
"""

import json
import os
import sqlite3
import threading
from urllib.parse import parse_qs
from wsgiref.simple_server import WSGIRequestHandler, make_server

import pytest

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.exceptions import ConfigurationError
from django_connectors.landing import access
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    RUN_ID_COLUMN,
    is_internal_column,
)
from django_connectors.services import bindings as binding_services
from django_connectors.services import runs as run_services
from django_connectors.sources import filesystem as filesystem_source
from django_connectors.sources import sql as sql_source
from django_connectors.sources.rest import RestSource

pytestmark = pytest.mark.django_db


# --- registry wiring -------------------------------------------------------


class LoopbackRestSource(RestSource):
    """RestSource with the SSRF guard lifted, as a host would register it.

    The guard is a class attribute rather than a Binding config key precisely
    so that lifting it takes a deliberate registration in settings — which is
    exactly what the test suite does here, and what a host with an internal
    API would do.
    """

    allow_private_addresses = True


class StaticAuthBackend:
    """Returns whatever ``Connection.metadata['connection_params']`` holds.

    Real backends resolve a handle through the SecretStore; for these tests the
    only thing that matters is that credentials arrive by the same route.
    """

    def get_credentials(self, connection):
        return (connection.metadata or {}).get("connection_params")


SOURCE_PATHS = {
    "rest": "tests.test_sources.LoopbackRestSource",
    "sql": "django_connectors.sources.sql.SqlSource",
    "files": "django_connectors.sources.filesystem.FilesystemSource",
}


@pytest.fixture
def source_settings(connectors_settings, settings):
    """`connectors_settings`, plus the sources and auth backend these tests use."""
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], **SOURCE_PATHS},
        "AUTH_BACKENDS": {"static": "tests.test_sources.StaticAuthBackend"},
    }
    return settings.DJANGO_CONNECTORS


# --- an in-process API -----------------------------------------------------


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        """Keep pytest output readable."""


class FakeApi:
    """A paginated JSON API that records every request it serves.

    Supports exactly the two shapes the REST source has to get right: page
    numbers, and a ``since`` filter standing in for a provider's own
    incremental parameter.
    """

    def __init__(self, rows, *, page_size=2):
        self.rows = list(rows)
        self.page_size = page_size
        self.requests = []

    def __call__(self, environ, start_response):
        query = parse_qs(environ.get("QUERY_STRING", ""))
        self.requests.append(
            {
                "path": environ["PATH_INFO"],
                "query": query,
                "authorization": environ.get("HTTP_AUTHORIZATION"),
            }
        )
        since = query.get("since", [None])[0]
        page = int(query.get("page", ["1"])[0])
        matching = [
            row for row in self.rows if since is None or row["updated_at"] >= since
        ]
        start = (page - 1) * self.page_size
        body = json.dumps({"data": matching[start : start + self.page_size]}).encode()
        start_response(
            "200 OK",
            [("Content-Type", "application/json"), ("Content-Length", str(len(body)))],
        )
        return [body]

    def pages_requested(self):
        return [request["query"].get("page", ["1"])[0] for request in self.requests]

    def since_values(self):
        return [request["query"].get("since", [None])[0] for request in self.requests]


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


PAGE_NUMBER_PAGINATOR = {
    "type": "page_number",
    "base_page": 1,
    "page_param": "page",
    "total_path": None,
    "stop_after_empty_page": True,
}


def rest_config(base_url, *, cursor=False, auth=None, **overrides):
    resource = {
        "path": "items",
        "primary_key": "id",
        "data_selector": "data",
        "paginator": PAGE_NUMBER_PAGINATOR,
        **overrides,
    }
    if cursor:
        resource["incremental"] = {
            "cursor_path": "updated_at",
            "start_param": "since",
        }
    config = {"base_url": base_url, "resources": {"items": resource}}
    if auth is not None:
        config["auth"] = auth
    return config


# A literal, globally routable address. The SSRF guard resolves the host, and a
# literal resolves without DNS — which keeps the strict-source tests working in
# an offline environment. Nothing is ever requested from it: every test using it
# raises during validation. Documentation ranges (192.0.2.0/24, 203.0.113.0/24)
# are unusable here because `ipaddress` correctly reports them as non-global.
PUBLIC_BASE_URL = "https://93.184.216.34/"

ROWS = [
    {"id": "1", "updated_at": "2024-01-01T00:00:00Z", "v": "a"},
    {"id": "2", "updated_at": "2024-01-02T00:00:00Z", "v": "b"},
    {"id": "3", "updated_at": "2024-01-03T00:00:00Z", "v": "c"},
]


def business_columns(rows):
    """Landed column names with the library's and dlt's own bookkeeping removed."""
    return sorted(name for name in rows[0] if not is_internal_column(name))


# --- rest ------------------------------------------------------------------


def test_rest_follows_pagination_and_lands_every_page(
    source_settings, make_binding, serve
):
    """A source that stops after page 1 lands a plausible-looking partial table."""
    api = FakeApi(ROWS, page_size=2)
    binding = make_binding(source="rest", config=rest_config(serve(api)))

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    assert "2" in api.pages_requested(), api.pages_requested()
    rows = access.sample_rows(binding, "items", limit=10)
    assert sorted(row["id"] for row in rows) == ["1", "2", "3"]
    assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}


def test_rest_second_run_sends_the_cursor_and_merges_rather_than_duplicating(
    source_settings, make_binding, serve
):
    """The cursor must reach the *provider*, not just filter locally.

    Two independent failures are covered. If ``incremental.start_param`` never
    makes it into the request, run 2 re-downloads the whole history — expensive
    and, against a rate-limited API, eventually fatal. If the library's forced
    ``Incremental`` is not the one that wins, dlt's default deduplication key
    silently drops the record sitting exactly on the cursor boundary.
    """
    api = FakeApi(ROWS, page_size=2)
    binding = make_binding(source="rest", config=rest_config(serve(api), cursor=True))

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert set(api.since_values()) == {None}

    api.rows.append({"id": "4", "updated_at": "2024-01-05T00:00:00Z", "v": "d"})
    api.requests.clear()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    # The provider is asked for everything at or after run 1's high-water mark.
    assert set(api.since_values()) == {"2024-01-03T00:00:00Z"}

    rows = access.sample_rows(binding, "items", limit=20)
    assert sorted(row["id"] for row in rows) == ["1", "2", "3", "4"], (
        "rows duplicated instead of merging on the declared primary key"
    )
    by_id = {row["id"]: row for row in rows}
    # The boundary record was re-emitted and merged, not dropped.
    assert by_id["3"][RUN_ID_COLUMN] == str(second.id)
    assert by_id["1"][RUN_ID_COLUMN] == str(first.id)


def test_rest_sends_the_bearer_token_the_auth_backend_returned(
    source_settings, make_binding, make_connection, serve
):
    api = FakeApi(ROWS[:1])
    connection = make_connection(
        auth_backend="static",
        metadata={"connection_params": {"access_token": "s3cret-token"}},
    )
    binding = make_binding(
        source="rest",
        connection=connection,
        config=rest_config(serve(api), auth={"type": "bearer"}),
    )

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert api.requests[0]["authorization"] == "Bearer s3cret-token"


def test_rest_check_connection_makes_one_request(source_settings, make_binding, serve):
    """A connection test must be cheap: one request, no pagination walk."""
    api = FakeApi(ROWS)
    binding = make_binding(source="rest", config=rest_config(serve(api)))

    status = LoopbackRestSource().check_connection(
        connection=binding.connection, credentials=None, binding=binding
    )
    assert status.startswith("ok")
    assert len(api.requests) == 1
    assert api.requests[0]["path"] == "/items"


def test_rest_malformed_config_names_the_json_path_that_is_wrong(source_settings):
    """dlt reports the outermost union failure; the person editing needs the leaf."""
    config = rest_config("https://api.example.test/", method="FETCH")

    with pytest.raises(ConfigurationError) as caught:
        LoopbackRestSource().validate_config(config)

    message = str(caught.value)
    assert "resources.items.endpoint" in message, message
    assert "FETCH" in message, message


def test_rest_malformed_config_is_rejected_when_the_binding_is_saved(
    source_settings, make_binding
):
    """Validation belongs at save time, not inside a Run at 3am."""
    binding = make_binding(
        source="rest",
        config=rest_config("https://api.example.test/", paginator={"type": "nonsense"}),
    )
    with pytest.raises(ConfigurationError):
        binding_services.validate_binding(binding)


def test_rest_cursor_without_a_request_parameter_is_refused_at_save(
    source_settings, make_binding, serve
):
    """dlt would fail this Run on a message naming nothing the customer wrote.

    The hint-applied Incremental reaches ``paginate_resource`` whether or not a
    request parameter exists to bind it to, and dereferences None when it does
    not — so the configuration is refused where it can still be corrected.
    """
    api = FakeApi(ROWS)
    config = rest_config(serve(api))
    config["resources"]["items"]["incremental"] = {"cursor_path": "updated_at"}
    binding = make_binding(source="rest", config=config)

    with pytest.raises(ConfigurationError, match="start_param"):
        binding_services.validate_binding(binding)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://127.0.0.1:8000/",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/internal/",
        "http://[::1]/",
    ],
)
def test_rest_refuses_internal_addresses_by_default(base_url):
    """base_url is customer-controlled; the metadata service is one hop away."""
    with pytest.raises(ConfigurationError, match="internal address"):
        RestSource().validate_config(
            {"base_url": base_url, "resources": {"items": {"primary_key": "id"}}}
        )


@pytest.mark.parametrize("base_url", ["file:///etc/passwd", "gopher://x/", "ftp://x/"])
def test_rest_refuses_non_http_schemes(base_url):
    with pytest.raises(ConfigurationError, match="scheme"):
        RestSource().validate_config(
            {"base_url": base_url, "resources": {"items": {"primary_key": "id"}}}
        )


def test_rest_refuses_an_absolute_url_in_a_resource_path():
    """urljoin drops the base entirely when the "path" carries its own scheme."""
    with pytest.raises(ConfigurationError, match="relative to base_url"):
        RestSource().validate_config(
            {
                "base_url": PUBLIC_BASE_URL,
                "resources": {
                    "items": {
                        "primary_key": "id",
                        "path": "http://169.254.169.254/latest/meta-data/",
                    }
                },
            }
        )


def test_rest_guarded_session_refuses_a_redirect_to_an_internal_host(
    serve, monkeypatch
):
    """A check made once, at configuration time, is defeated by a 302.

    The guard lives in ``Session.send``, which requests calls again for every
    redirect hop. To show that the *second* hop is what gets refused — rather
    than the loopback origin the test server necessarily runs on — the origin
    is waved through and everything else meets the real guard. Nothing ever
    connects to the metadata address: the guard raises before ``send`` runs.
    """
    from django_connectors.sources import rest as rest_module

    def redirector(environ, start_response):
        start_response("302 Found", [("Location", "http://169.254.169.254/latest/")])
        return [b""]

    base_url = serve(redirector)
    real_assert = rest_module.assert_safe_url
    hops = []

    def permit_only_the_origin(url, *, allow_private=False):
        hops.append(url)
        return real_assert(url, allow_private=url.startswith(base_url))

    monkeypatch.setattr(rest_module, "assert_safe_url", permit_only_the_origin)
    session = rest_module.guarded_session(allow_private=False)

    with pytest.raises(ConfigurationError, match="internal address"):
        session.get(base_url, timeout=5)

    assert hops[0].startswith(base_url), hops
    assert hops[-1] == "http://169.254.169.254/latest/", hops


def test_rest_guarded_session_refuses_a_host_that_rebinds_after_the_check(
    serve, monkeypatch
):
    """The guard's DNS answer is not the one urllib3 connects with.

    A name whose zone the attacker runs answers the check with a public address
    and the connection with ``127.0.0.1``; nothing in ``assert_safe_url`` can
    see that, because it never touches the socket. Rebinding is simulated
    rather than performed — ``_resolve`` is the guard's only view of DNS, so
    lying to it and letting the real lookup of ``localhost`` stand reproduces
    exactly the split the attack creates, offline.
    """
    from django_connectors.sources import rest as rest_module

    reached = []

    def app(environ, start_response):
        reached.append(environ["PATH_INFO"])
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"SIMULATED-CLOUD-METADATA-CREDENTIALS"]

    port = serve(app).rsplit(":", 1)[1].rstrip("/")
    real_resolve = rest_module._resolve

    def rebinding_resolve(host, port_number):
        # The answer the guard gets. The socket's own lookup of "localhost"
        # is untouched and still lands on loopback.
        if host == "localhost":
            return {"93.184.216.34"}
        return real_resolve(host, port_number)

    monkeypatch.setattr(rest_module, "_resolve", rebinding_resolve)
    session = rest_module.guarded_session(allow_private=False)

    with pytest.raises(ConfigurationError, match="rebinding"):
        session.get(f"http://localhost:{port}/latest/meta-data/", timeout=5)

    assert reached == [], "the request reached the loopback service anyway"


# --- sql -------------------------------------------------------------------


def make_sqlite_database(tmp_path, rows):
    """A sqlite file with a table wider than any query the tests project from."""
    path = tmp_path / "warehouse.db"
    connection = sqlite3.connect(path)
    with connection:
        connection.execute(
            "CREATE TABLE orders (id TEXT, total INTEGER, note TEXT, updated_at TEXT)"
        )
        connection.executemany(
            "INSERT INTO orders (id, total, note, updated_at) VALUES (?, ?, ?, ?)", rows
        )
    connection.close()
    return f"sqlite:///{path}"


SQLITE_ROWS = [
    ("1", 10, "first", "2024-01-01T00:00:00Z"),
    ("2", 20, "second", "2024-01-02T00:00:00Z"),
    ("3", 30, "third", "2024-01-03T00:00:00Z"),
]


def sql_config(url, **overrides):
    config = {
        "url": url,
        "resource": "orders",
        "query": "SELECT id, total FROM orders",
        "primary_key": "id",
    }
    config.update(overrides)
    return config


def test_sql_lands_exactly_the_querys_projection(
    source_settings, make_binding, tmp_path
):
    """The reflection trap: ``note`` and ``updated_at`` must not appear at all.

    ``sql_database``'s query_adapter_callback at the default reflection level
    lands the union of the reflected table and the projection, filling every
    unselected column with NULL. A customer then maps a column that is always
    empty and nothing reports it.
    """
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    binding = make_binding(source="sql", config=sql_config(url))

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "orders", limit=10)
    assert business_columns(rows) == ["id", "total"]
    assert sorted(row["total"] for row in rows) == [10, 20, 30]


def test_sql_second_run_resumes_from_the_stored_cursor(
    source_settings, make_binding, tmp_path
):
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    binding = make_binding(
        source="sql",
        config=sql_config(
            url,
            query="SELECT id, total, updated_at FROM orders",
            incremental={"cursor_path": "updated_at"},
        ),
    )

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message

    connection = sqlite3.connect(tmp_path / "warehouse.db")
    with connection:
        connection.execute(
            "INSERT INTO orders (id, total, note, updated_at) VALUES "
            "('4', 40, 'fourth', '2024-01-05T00:00:00Z')"
        )
    connection.close()

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "orders", limit=20)
    by_id = {row["id"]: row for row in rows}
    assert sorted(by_id) == ["1", "2", "3", "4"], "rows duplicated across runs"
    # Rows below the stored cursor were not re-landed; the boundary row was.
    assert by_id["1"][RUN_ID_COLUMN] == str(first.id)
    assert by_id["3"][RUN_ID_COLUMN] == str(second.id)
    assert by_id["4"][RUN_ID_COLUMN] == str(second.id)


def test_sql_cursor_pushdown_keeps_the_boundary_row():
    """``>`` here would lose a record with no error anywhere.

    dlt's Incremental uses a closed lower bound, so a row written at exactly
    the stored cursor value is still wanted. A strict inequality in the
    pushed-down predicate drops it below dlt's notice.
    """
    config = {
        "query": "SELECT id FROM orders ORDER BY id",
        "incremental": {"cursor_path": "updated_at"},
    }
    statement, params = sql_source.build_query(config, "2024-01-03T00:00:00Z")

    assert "updated_at >= :dc_cursor_value" in statement
    assert "updated_at >" in statement and "updated_at > " not in statement
    assert params == {"dc_cursor_value": "2024-01-03T00:00:00Z"}
    # Wrapped, not appended: the customer's own ORDER BY must survive.
    assert "SELECT id FROM orders ORDER BY id" in statement
    assert statement.startswith("SELECT * FROM (")


def test_sql_first_run_scans_without_a_predicate():
    statement, params = sql_source.build_query(
        {"table": "orders", "incremental": {"cursor_path": "updated_at"}}, None
    )
    assert statement == "SELECT * FROM orders"
    assert params == {}


def test_sql_arrow_backend_is_rejected_at_build(
    source_settings, make_binding, tmp_path
):
    """Arrow batches cannot carry tenant metadata, so they must never be built."""
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    binding = make_binding(source="sql", config=sql_config(url, backend="pyarrow"))

    with pytest.raises(ConfigurationError, match="sqlalchemy"):
        sql_source.SqlSource().build_source(binding=binding, credentials=None, run=None)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.FAILED
    assert "backend" in run.error_message


def test_sql_takes_its_dsn_from_the_auth_backend(
    source_settings, make_binding, make_connection, tmp_path
):
    """A DSN in Binding.config is a plaintext credential; the backend route wins."""
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    connection = make_connection(
        auth_backend="static", metadata={"connection_params": {"url": url}}
    )
    config = sql_config(url)
    config.pop("url")
    binding = make_binding(source="sql", connection=connection, config=config)

    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert len(access.sample_rows(binding, "orders", limit=10)) == 3


def test_sql_refuses_a_cursor_that_is_not_a_plain_column():
    """The cursor is interpolated into SQL, so an expression is refused."""
    with pytest.raises(ConfigurationError, match="plain identifier"):
        sql_source.SqlSource().validate_config(
            {
                "query": "SELECT 1",
                "primary_key": "id",
                "incremental": {"cursor_path": "updated_at) OR (1=1"},
            }
        )


def test_sql_missing_dsn_is_a_configuration_error():
    with pytest.raises(ConfigurationError, match="DSN"):
        sql_source.connection_url({}, None)


def test_sql_unknown_driver_names_the_missing_module():
    with pytest.raises(ConfigurationError, match="driver"):
        sql_source.create_engine("nosuchdialect://user@host/db")


# --- filesystem ------------------------------------------------------------


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return path


def files_config(directory, *, file_format="jsonl", glob="*.jsonl", **overrides):
    spec = {
        "path": f"{directory}/{glob}",
        "format": file_format,
        "primary_key": "id",
        **overrides,
    }
    return {"resources": {"events": spec}}


def test_filesystem_reads_jsonl_with_no_extras(source_settings, make_binding, tmp_path):
    directory = tmp_path / "drop"
    write_jsonl(directory / "a.jsonl", [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}])
    write_jsonl(directory / "b.jsonl", [{"id": "3", "v": "c"}])

    binding = make_binding(source="files", config=files_config(directory))
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message

    rows = access.sample_rows(binding, "events", limit=10)
    assert sorted(row["id"] for row in rows) == ["1", "2", "3"]
    assert business_columns(rows) == ["id", "v"]


def test_filesystem_merges_on_the_declared_key_instead_of_duplicating(
    source_settings, make_binding, tmp_path
):
    """Re-reading a file must update rows, which is what primary_key buys."""
    directory = tmp_path / "drop"
    target = directory / "a.jsonl"
    write_jsonl(target, [{"id": "1", "v": "original"}])

    binding = make_binding(source="files", config=files_config(directory))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    write_jsonl(target, [{"id": "1", "v": "UPDATED"}])
    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    rows = access.sample_rows(binding, "events", limit=10)
    assert [row["v"] for row in rows] == ["UPDATED"]


def test_filesystem_csv_lands_records(source_settings, make_binding, tmp_path):
    pytest.importorskip("pandas")
    directory = tmp_path / "drop"
    directory.mkdir()
    (directory / "a.csv").write_text("id,v\n1,a\n2,b\n")

    binding = make_binding(
        source="files",
        config=files_config(directory, file_format="csv", glob="*.csv"),
    )
    run = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert run.status == RunStatus.SUCCEEDED, run.error_message
    assert len(access.sample_rows(binding, "events", limit=10)) == 2


@pytest.mark.parametrize(
    ("file_format", "module", "extra"),
    [("csv", "pandas", "csv"), ("parquet", "pyarrow", "parquet")],
)
def test_filesystem_rejects_a_format_whose_extra_is_missing_at_save_time(
    source_settings, make_binding, tmp_path, monkeypatch, file_format, module, extra
):
    """dlt imports these lazily, so the failure otherwise lands inside a Run.

    Absence is simulated rather than installed-around: the test environment
    installs every extra, so the path that matters is invisible without this.
    """
    monkeypatch.setattr(
        filesystem_source,
        "module_available",
        lambda name: name != module,
    )

    directory = tmp_path / "drop"
    directory.mkdir()
    binding = make_binding(
        source="files",
        config=files_config(
            directory, file_format=file_format, glob=f"*.{file_format}"
        ),
    )

    with pytest.raises(ConfigurationError) as caught:
        binding_services.validate_binding(binding)

    message = str(caught.value)
    assert module in message
    assert f"django-connectors[{extra}]" in message


def test_filesystem_refuses_a_cloud_bucket_for_now(source_settings, make_binding):
    binding = make_binding(
        source="files",
        config={
            "resources": {
                "events": {"bucket_url": "s3://bucket/prefix", "primary_key": "id"}
            }
        },
    )
    with pytest.raises(ConfigurationError, match="not supported in"):
        binding_services.validate_binding(binding)


def test_filesystem_refuses_a_relative_path(source_settings, make_binding):
    binding = make_binding(
        source="files",
        config={"resources": {"events": {"path": "data/*.jsonl", "primary_key": "id"}}},
    )
    with pytest.raises(ConfigurationError, match="absolute"):
        binding_services.validate_binding(binding)


def test_filesystem_splits_a_glob_from_its_directory(tmp_path):
    bucket_url, file_glob = filesystem_source.resolve_location(
        "events", {"path": f"{tmp_path}/nested/*.jsonl"}
    )
    assert bucket_url == f"file://{tmp_path}/nested"
    assert file_glob == "*.jsonl"

    bucket_url, file_glob = filesystem_source.resolve_location(
        "events", {"path": str(tmp_path)}
    )
    assert bucket_url == f"file://{tmp_path}"
    assert file_glob == "*"


def test_filesystem_merge_without_a_key_is_refused():
    with pytest.raises(ConfigurationError, match="primary_key"):
        filesystem_source.FilesystemSource().validate_config(
            {"resources": {"events": {"path": os.sep + "tmp"}}}
        )


# --- connector conformance ---------------------------------------------------
#
# The shared suite from `django_connectors.testing.conformance`, run against
# these three sources with their own fakes. `tests/test_source_conformance.py`
# holds the credential-free half and asserts that each `test_<key>_conformance`
# below exists — a new connector cannot quietly skip the contract.


def _assert_conformant(source_key, binding, resources):
    from django_connectors.models import Run
    from django_connectors.registry import sources as source_registry
    from django_connectors.testing import conformance

    definition = source_registry.get(source_key)
    credentials = None
    if binding.connection.auth_backend:
        from django_connectors.registry import auth_backends

        credentials = auth_backends.get(
            binding.connection.auth_backend
        ).get_credentials(binding.connection)

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
    return binding


def test_rest_conformance(source_settings, make_binding, serve):
    binding = make_binding(
        source="rest", config=rest_config(serve(FakeApi(ROWS, page_size=2)))
    )
    _assert_conformant("rest", binding, ["items"])
    rows = access.sample_rows(binding, "items", limit=10)
    assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}


def test_sql_conformance(source_settings, make_binding, tmp_path):
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    binding = make_binding(source="sql", config=sql_config(url))
    _assert_conformant("sql", binding, ["orders"])
    assert access.sample_rows(binding, "orders", limit=10)


def test_filesystem_conformance(source_settings, make_binding, tmp_path):
    directory = tmp_path / "drop"
    write_jsonl(directory / "a.jsonl", [{"id": "1", "v": "a"}, {"id": "2", "v": "b"}])
    binding = make_binding(source="files", config=files_config(directory))
    _assert_conformant("files", binding, ["events"])
    assert len(access.sample_rows(binding, "events", limit=10)) == 2
