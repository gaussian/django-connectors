"""Recorded provider exchanges, replayed offline.

The provider connectors' own tests run against in-process fakes, and the
conformance suite holds every connector to the same contract — but both verify
*our* logic against *our* belief about the provider, which is exactly the
belief most likely to be wrong. This tier records one real exchange per
connector against a sandbox, commits it redacted, and replays it on every pull
request. It is the only tier that can say "the connector still parses what the
provider actually returns".

Three properties, each enforced by a test in this module rather than by
convention:

**Nothing secret reaches a cassette.** Every value that came from a recording
environment variable is replaced by a fixed placeholder before it is written —
in the URI, in headers, in request and response bodies — and everything is
then run through ``errors.redact_secrets``, the same rules the scrubber applies
to error messages. :func:`test_committed_cassettes_carry_no_credentials` reads
every committed cassette back and refuses one where those rules would still
change anything. Redaction at record time is a fixture someone can forget;
the scanner is not.

**Replay is deterministic.** The placeholders are used *as the real values*
on replay, so the URIs the connector builds match the URIs in the cassette.
Pagination cursors are not masked — they are what the second run sends back.
Anything *named* like a credential is masked (that includes Graph's ``token=``
delta cursor), but on both sides at once: the response body that carries it
and the request URI built from it get the same mask, so the replay still
matches.

**Two sources run with their address guard lifted.** ``SalesforceSource`` and
``RestSource`` resolve the host by DNS before every request and pin each
socket to the address it resolved — neither of which a replay against a
placeholder host can satisfy, and neither of which is what a cassette
verifies. The recorded tier registers ``RecordedSalesforceSource`` with the
guard lifted, exactly as the loopback tests do. The guard has its own tests.

**The landed schema is snapshotted.** A provider renaming a field does not
fail a replay — the connector lands a NULL where a value used to be. So each
recording also commits ``<key>.schema.json``, the reduced landing schema the
library already writes after a Run, and the replay compares. A rename is a
diff in code review rather than a column nobody notices is empty.

How to record — once per connector, with a sandbox, never with production::

    export DJANGO_CONNECTORS_RECORD_GOOGLE_ACCESS_TOKEN=ya29....
    export DJANGO_CONNECTORS_RECORD_GOOGLE_SPREADSHEET_ID=1BxiM...
    uv run --all-extras pytest tests/test_recorded.py -k "gmail or sheets" \\
        --record-mode=rewrite

    uv run --all-extras pytest tests/test_recorded.py   # replays, scans
    git add tests/cassettes/

``rewrite`` rather than ``once``: a stale cassette must be replaced whole, not
appended to. The environment variables each connector needs are listed on its
:class:`Target` below, and a recording with any of them missing **fails**
rather than skips — a recording that quietly recorded nothing is worse than
none. Use a bare access token, not a refreshable credential: a refresh is a
token-endpoint exchange, and this tier should record the data API only.

The cassettes hold whatever the sandbox held. Use one with synthetic data;
they are committed.
"""

import json
import os
import pathlib

import pytest

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.errors import MASK, redact_secrets
from django_connectors.landing import access
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    RUN_ID_COLUMN,
    landing_table_name,
)
from django_connectors.providers.salesforce.source import SalesforceSource
from django_connectors.services import runs as run_services
from django_connectors.testing import conformance
from tests.test_providers_salesforce import serve as loopback_server

#: pytest resolves fixtures by name, so the imported fixture needs its own.
serve = loopback_server

pytestmark = [pytest.mark.django_db, pytest.mark.recorded]

CASSETTE_DIR = pathlib.Path(__file__).resolve().parent / "cassettes" / "test_recorded"
ENV_PREFIX = "DJANGO_CONNECTORS_RECORD_"

#: What the recorder writes in place of a real value. Every one is lowercase
#: letters and hyphens only, so none of them can match the scrubber's
#: high-entropy rule and none needs URL escaping.
PLACEHOLDERS = {
    "GOOGLE_ACCESS_TOKEN": "recorded-google-access-token",
    "GOOGLE_SPREADSHEET_ID": "recorded-spreadsheet-id",
    "MICROSOFT_ACCESS_TOKEN": "recorded-microsoft-access-token",
    "MICROSOFT_DRIVE_ID": "recorded-drive-id",
    "MICROSOFT_WORKBOOK_ITEM_ID": "recorded-workbook-item-id",
    "SALESFORCE_ACCESS_TOKEN": "recorded-salesforce-access-token",
    "SALESFORCE_INSTANCE_URL": "https://recorded.my.salesforce.com",
}

#: Response headers that survive recording. Everything else — request ids,
#: cookies, tracing headers, the whole `x-ms-*` and `x-goog-*` families — is
#: dropped: none of it is read by a connector, and each is a place for a
#: tenant identifier or a session token to hide.
KEPT_RESPONSE_HEADERS = frozenset(
    {"content-type", "content-length", "content-encoding", "location", "retry-after"}
)


class Target:
    """One connector: what it needs to record, and what it uses to replay."""

    def __init__(self, key, *, source, needs, build, resources):
        self.key = key
        self.source = source
        #: Names in :data:`PLACEHOLDERS`; the env var is ``ENV_PREFIX + name``.
        self.needs = needs
        #: ``values -> (credentials, binding config)``.
        self.build = build
        self.resources = resources

    def env_var(self, name):
        return ENV_PREFIX + name


TARGETS = {
    target.key: target
    for target in (
        Target(
            "gmail",
            source="django_connectors.providers.google.gmail.GmailSource",
            needs=("GOOGLE_ACCESS_TOKEN",),
            build=lambda v: (v["GOOGLE_ACCESS_TOKEN"], {"page_size": 5}),
            resources=("messages", "labels"),
        ),
        Target(
            "google_sheets",
            source="django_connectors.providers.google.sheets.GoogleSheetsSource",
            needs=("GOOGLE_ACCESS_TOKEN", "GOOGLE_SPREADSHEET_ID"),
            build=lambda v: (
                v["GOOGLE_ACCESS_TOKEN"],
                {
                    "spreadsheet_id": v["GOOGLE_SPREADSHEET_ID"],
                    # The first sheet, whole. A sandbox sheet with a header row
                    # and an `id` column; keyless is fine too, it then replaces.
                    "ranges": {"rows": {"range": "Sheet1!A:Z", "missing_key": "skip"}},
                    "key_column": "id",
                },
            ),
            resources=("rows",),
        ),
        Target(
            "entra_files",
            source="django_connectors.providers.microsoft.files.EntraFilesSource",
            needs=("MICROSOFT_ACCESS_TOKEN", "MICROSOFT_DRIVE_ID"),
            build=lambda v: (
                v["MICROSOFT_ACCESS_TOKEN"],
                {"drive_id": v["MICROSOFT_DRIVE_ID"], "page_size": 5},
            ),
            resources=("drive_items",),
        ),
        Target(
            "entra_excel",
            source="django_connectors.providers.microsoft.excel.EntraExcelSource",
            needs=(
                "MICROSOFT_ACCESS_TOKEN",
                "MICROSOFT_DRIVE_ID",
                "MICROSOFT_WORKBOOK_ITEM_ID",
            ),
            build=lambda v: (
                v["MICROSOFT_ACCESS_TOKEN"],
                {
                    "drive_id": v["MICROSOFT_DRIVE_ID"],
                    "item_id": v["MICROSOFT_WORKBOOK_ITEM_ID"],
                },
            ),
            resources=("worksheet_rows",),
        ),
        Target(
            "salesforce",
            source="tests.test_recorded.RecordedSalesforceSource",
            needs=("SALESFORCE_ACCESS_TOKEN", "SALESFORCE_INSTANCE_URL"),
            build=lambda v: (
                {
                    "access_token": v["SALESFORCE_ACCESS_TOKEN"],
                    "instance_url": v["SALESFORCE_INSTANCE_URL"],
                },
                {
                    "objects": {
                        "Account": {"fields": ["Id", "Name", "SystemModstamp"]}
                    },
                },
            ),
            resources=("Account",),
        ),
    )
}


class RecordedSalesforceSource(SalesforceSource):
    """The real source with the address guard lifted. See the module docstring."""

    allow_private_addresses = True


# --- credentials never touch the ORM ------------------------------------------

#: The credentials for the test in progress. Module state rather than a row:
#: a real access token has no business in even an in-memory database.
ACTIVE = {}


class RecordedCredentialsBackend:
    key = "recorded"

    def get_credentials(self, connection):
        return ACTIVE["credentials"]


@pytest.fixture
def recorded_settings(connectors_settings, settings):
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {
            **connectors_settings["SOURCES"],
            **{target.key: target.source for target in TARGETS.values()},
        },
        "AUTH_BACKENDS": {"recorded": "tests.test_recorded.RecordedCredentialsBackend"},
    }
    yield settings.DJANGO_CONNECTORS
    ACTIVE.clear()


# --- record or replay -----------------------------------------------------------


def _target_for(request):
    marker = request.node.get_closest_marker("default_cassette")
    assert marker, "recorded tests name their cassette with @default_cassette"
    return TARGETS[pathlib.Path(marker.args[0]).stem]


def _values(target, record_mode):
    """``{name: value}`` — placeholders on replay, the environment on record."""
    if record_mode == "none":
        return {name: PLACEHOLDERS[name] for name in target.needs}
    missing = [
        target.env_var(name)
        for name in target.needs
        if not os.environ.get(target.env_var(name))
    ]
    if missing:
        pytest.fail(
            f"recording {target.key!r} needs {missing} in the environment. A "
            f"recording that silently recorded nothing is worse than none."
        )
    return {name: os.environ[target.env_var(name)] for name in target.needs}


@pytest.fixture
def vcr_config(request, record_mode):
    target = _target_for(request)
    return _vcr_config_for(target, _values(target, record_mode))


def _vcr_config_for(target, values):
    """Redaction, configured per target.

    Two layers, both applied to URIs, headers and bodies of both directions.
    First every real value from the environment becomes its placeholder — that
    is what makes replay deterministic as well as safe. Then
    ``redact_secrets`` masks anything credential-shaped that was not one of
    ours: a refreshed token in a body, a JWT in a redirect, a ``tempauth`` on a
    download URL.
    """
    substitutions = {real: PLACEHOLDERS[name] for name, real in values.items() if real}

    def clean(text):
        if not isinstance(text, str):
            return text
        for real, placeholder in substitutions.items():
            text = text.replace(real, placeholder)
        return redact_secrets(text)

    def clean_bytes(body):
        if isinstance(body, bytes):
            try:
                return clean(body.decode("utf-8")).encode("utf-8")
            except UnicodeDecodeError:
                # A workbook download. Nothing textual to redact, and the
                # placeholders are not going to appear in a zip stream.
                return body
        return clean(body)

    def before_record_request(vcr_request):
        vcr_request.uri = clean(vcr_request.uri)
        vcr_request.body = clean_bytes(vcr_request.body)
        vcr_request.headers = {
            name: clean(value) for name, value in vcr_request.headers.items()
        }
        return vcr_request

    def before_record_response(response):
        headers = response.get("headers") or {}
        response["headers"] = {
            name: [clean(item) for item in items]
            for name, items in headers.items()
            if name.lower() in KEPT_RESPONSE_HEADERS
        }
        body = response.get("body") or {}
        if "string" in body:
            body["string"] = clean_bytes(body["string"])
        return response

    return {
        "filter_headers": [
            ("authorization", f"Bearer {PLACEHOLDERS[target.needs[0]]}"),
            ("cookie", None),
            ("user-agent", None),
        ],
        "before_record_request": before_record_request,
        "before_record_response": before_record_response,
        "decode_compressed_response": True,
        "match_on": ["method", "scheme", "host", "port", "path", "query", "body"],
    }


def _replay(target, record_mode, make_connection, make_binding):
    """Two Runs against the cassette: a first load and an incremental second."""
    cassette = CASSETTE_DIR / f"{target.key}.yaml"
    if record_mode == "none" and not cassette.exists():
        pytest.skip(
            f"no cassette for {target.key!r}. Record one with "
            f"`pytest {__file__} -k {target.key} --record-mode=rewrite` and the "
            f"{[target.env_var(name) for name in target.needs]} variables set."
        )

    values = _values(target, record_mode)
    credentials, config = target.build(values)
    ACTIVE["credentials"] = credentials

    connection = make_connection(provider=target.key, auth_backend="recorded")
    binding = make_binding(
        source=target.key,
        connection=connection,
        config=config,
        resources=list(target.resources),
    )

    first = run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    assert first.status == RunStatus.SUCCEEDED, first.error_message
    assert first.dlt_load_ids, "the first run landed nothing"

    second = run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert second.status == RunStatus.SUCCEEDED, second.error_message

    failures = conformance.check_landing_invariants(
        binding, expected_resources=target.resources
    )
    assert failures == [], "\n".join(failures)

    landed = {}
    for resource in target.resources:
        rows = access.sample_rows(binding, resource, limit=100)
        if not rows:
            continue
        assert {row[BINDING_ID_COLUMN] for row in rows} == {str(binding.id)}
        assert all(row[RUN_ID_COLUMN] for row in rows)
        landed[resource] = rows
    assert landed, f"{target.key} landed no rows in any resource"

    _check_schema_snapshot(target, binding, record_mode)
    return binding, landed


def _check_schema_snapshot(target, binding, record_mode):
    """Compare — or on record, write — the reduced landed schema."""
    tables = (binding.landing_schema or {}).get("tables") or {}
    by_resource = {
        resource: tables[name]
        for resource in target.resources
        if (name := landing_table_name(target.key, resource, binding.landing_key))
        in tables
    }
    path = CASSETTE_DIR / f"{target.key}.schema.json"

    if record_mode != "none":
        path.write_text(json.dumps(by_resource, indent=2, sort_keys=True) + "\n")
        return

    assert path.exists(), (
        f"{path.name} is missing. It is written by the recording; commit it "
        f"with the cassette."
    )
    expected = json.loads(path.read_text())
    assert by_resource == expected, (
        f"the landed schema for {target.key!r} differs from the snapshot taken "
        f"when the cassette was recorded. A column that vanished lands NULL "
        f"from now on; a column that appeared is unmapped. Re-record if the "
        f"provider changed, or fix the connector if it did not.\n"
        f"expected: {json.dumps(expected, sort_keys=True)}\n"
        f"actual:   {json.dumps(by_resource, sort_keys=True)}"
    )


# --- one test per connector ---------------------------------------------------------


@pytest.mark.vcr
@pytest.mark.default_cassette("gmail.yaml")
def test_gmail_replays(recorded_settings, record_mode, make_connection, make_binding):
    _, landed = _replay(TARGETS["gmail"], record_mode, make_connection, make_binding)
    message_ids = [row["id"] for row in landed["messages"]]
    assert len(message_ids) == len(set(message_ids)), "a second run duplicated rows"


@pytest.mark.vcr
@pytest.mark.default_cassette("google_sheets.yaml")
def test_google_sheets_replays(
    recorded_settings, record_mode, make_connection, make_binding
):
    _replay(TARGETS["google_sheets"], record_mode, make_connection, make_binding)


@pytest.mark.vcr
@pytest.mark.default_cassette("entra_files.yaml")
def test_entra_files_replays(
    recorded_settings, record_mode, make_connection, make_binding
):
    _, landed = _replay(
        TARGETS["entra_files"], record_mode, make_connection, make_binding
    )
    item_ids = [row["id"] for row in landed["drive_items"]]
    assert len(item_ids) == len(set(item_ids)), "a second run duplicated rows"


@pytest.mark.vcr
@pytest.mark.default_cassette("entra_excel.yaml")
def test_entra_excel_replays(
    recorded_settings, record_mode, make_connection, make_binding
):
    _replay(TARGETS["entra_excel"], record_mode, make_connection, make_binding)


@pytest.mark.vcr
@pytest.mark.default_cassette("salesforce.yaml")
def test_salesforce_replays(
    recorded_settings, record_mode, make_connection, make_binding
):
    _, landed = _replay(
        TARGETS["salesforce"], record_mode, make_connection, make_binding
    )
    ids = [row["id"] for row in landed["Account"]]
    assert len(ids) == len(set(ids)), "a second run duplicated rows"


# --- the cassettes themselves ---------------------------------------------------


def _committed_cassettes():
    return sorted(CASSETTE_DIR.glob("*.yaml"))


def test_every_target_has_a_test_named_for_its_cassette():
    """The gate that keeps `TARGETS` and the tests above in step."""
    source = pathlib.Path(__file__).read_text()
    for key in TARGETS:
        assert f'@pytest.mark.default_cassette("{key}.yaml")' in source, key
        assert f"def test_{key}_replays(" in source, key


def test_committed_cassettes_carry_no_credentials():
    """Every committed cassette, read back and held to the recorder's rules.

    Redaction at record time is a fixture; someone can record without it, or
    with an older version of it, or paste an exchange in by hand. This runs on
    what is actually in the repository, so it passes trivially until the first
    cassette is committed — the round-trip test at the bottom of this module is
    what proves the scanner can fail. A cassette that landed anywhere other
    than :data:`CASSETTE_DIR` is reported rather than ignored.
    """
    import yaml

    cassettes = _committed_cassettes()
    stray = [
        path
        for path in CASSETTE_DIR.parent.rglob("*.yaml")
        if path.parent != CASSETTE_DIR
    ]
    assert not stray, f"cassettes outside {CASSETTE_DIR}: {stray}"

    problems = []
    for path in cassettes:
        text = path.read_text()
        if redact_secrets(text) != text:
            problems.append(f"{path.name}: redact_secrets would still change it")

        for index, interaction in enumerate(yaml.safe_load(text)["interactions"]):
            request_headers = {
                name.lower(): value
                for name, value in (interaction["request"].get("headers") or {}).items()
            }
            authorization = request_headers.get("authorization")
            if authorization and not str(authorization).rstrip("']").endswith(MASK):
                problems.append(
                    f"{path.name}[{index}]: Authorization header is not masked"
                )
            if "cookie" in request_headers:
                problems.append(f"{path.name}[{index}]: request carries a cookie")
            response_headers = {
                name.lower() for name in (interaction["response"].get("headers") or {})
            }
            unexpected = sorted(response_headers - KEPT_RESPONSE_HEADERS)
            if unexpected:
                problems.append(
                    f"{path.name}[{index}]: response headers {unexpected} were "
                    f"not dropped"
                )
    assert not problems, "\n".join(problems)


def test_every_cassette_has_a_schema_snapshot():
    """A cassette without its snapshot cannot report a renamed field."""
    missing = [
        path.name
        for path in _committed_cassettes()
        if not path.with_suffix(".schema.json").exists()
    ]
    assert not missing, f"cassettes with no schema snapshot: {missing}"


def test_the_scanner_rejects_a_credential(tmp_path, monkeypatch):
    """A scanner nothing can fail is a scanner that proves nothing."""
    import yaml

    leaked = tmp_path / "cassettes" / "test_recorded"
    leaked.mkdir(parents=True)
    (leaked / "bad.yaml").write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "interactions": [
                    {
                        "request": {
                            "uri": "https://api.example/x",
                            "method": "GET",
                            "body": None,
                            "headers": {"Authorization": ["Bearer ya29.real-token"]},
                        },
                        "response": {
                            "status": {"code": 200, "message": "OK"},
                            "headers": {"Content-Type": ["application/json"]},
                            "body": {"string": '{"access_token": "sk-live-1234"}'},
                        },
                    }
                ],
            }
        )
    )
    monkeypatch.setattr("tests.test_recorded.CASSETTE_DIR", leaked)

    with pytest.raises(AssertionError) as excinfo:
        test_committed_cassettes_carry_no_credentials()
    report = str(excinfo.value)
    assert "redact_secrets would still change it" in report
    assert "Authorization header is not masked" in report


def test_redaction_is_two_sided_and_deterministic():
    """The recorder must rewrite bodies, not just headers.

    A token refresh response *is* a credential, and an error body routinely
    quotes the request that caused it. And a real identifier must become the
    same placeholder everywhere, or the second run's URI (built from a first
    response's body) will not match the recording.
    """
    body = '{"@odata.nextLink": "https://graph/drives/DRIVE-9/root/delta?token=abc"}'
    substituted = body.replace("DRIVE-9", PLACEHOLDERS["MICROSOFT_DRIVE_ID"])
    assert PLACEHOLDERS["MICROSOFT_DRIVE_ID"] in redact_secrets(substituted)
    assert (
        redact_secrets('{"refresh_token": "1//0abc", "expires_in": 3599}')
        == f'{{"refresh_token": "{MASK}", "expires_in": 3599}}'
    )
    assert redact_secrets("eyJhbGciOi.eyJzdWIiOi.abc-def") == MASK
    assert redact_secrets("?tempauth=v1.eyJ.x&id=1") == f"?tempauth={MASK}&id=1"


# --- the recorder itself, proven without a credential -----------------------


def test_the_recorder_round_trips_through_a_redacted_cassette(
    tmp_path, serve, connectors_settings, settings, make_connection, make_binding
):
    """Record against an in-process org, then replay with the org unreachable.

    This is the one test that exercises the *machinery* end to end, and it
    does not need a sandbox: the fake Salesforce plays the provider, a made-up
    token plays the credential, and the loopback origin plays the tenant
    identifier. What it proves:

    * the credential and the origin do not survive into the cassette;
    * the placeholders do, consistently, in URIs and bodies alike;
    * a replay against the placeholder host — which resolves to nothing — lands
      the same rows the recording did, across a first and a second Run;
    * the schema snapshot written by the recording matches on replay.
    """
    import vcr

    from tests.test_providers_salesforce import (
        FakeSalesforce,
        LoopbackSalesforceSource,
        sf_config,
    )

    secret = "sf-secret-token-000111"
    api = FakeSalesforce(token=secret, page_size=2)
    origin = serve(api)
    api.base_url = origin

    target = Target(
        "salesforce",
        source=f"{LoopbackSalesforceSource.__module__}.LoopbackSalesforceSource",
        needs=TARGETS["salesforce"].needs,
        build=lambda v: (
            {
                "access_token": v["SALESFORCE_ACCESS_TOKEN"],
                "instance_url": v["SALESFORCE_INSTANCE_URL"],
            },
            sf_config(),
        ),
        resources=("Account",),
    )
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "SOURCES": {**connectors_settings["SOURCES"], "salesforce": target.source},
        "AUTH_BACKENDS": {"recorded": "tests.test_recorded.RecordedCredentialsBackend"},
    }
    cassette_dir = tmp_path / "cassettes" / "test_recorded"
    cassette_dir.mkdir(parents=True)
    cassette = cassette_dir / "salesforce.yaml"

    # -- record --------------------------------------------------------------
    real = {"SALESFORCE_ACCESS_TOKEN": secret, "SALESFORCE_INSTANCE_URL": origin}
    recorder = vcr.VCR(**_vcr_config_for(target, real))
    global CASSETTE_DIR
    original_dir, CASSETTE_DIR = CASSETTE_DIR, cassette_dir
    try:
        with recorder.use_cassette(str(cassette), record_mode="all"):
            ACTIVE["credentials"], config = target.build(real)
            recorded_binding = make_binding(
                source="salesforce",
                connection=make_connection(provider="x", auth_backend="recorded"),
                config=config,
                resources=["Account"],
            )
            for trigger in (RunTrigger.INITIAL, RunTrigger.SCHEDULED):
                run = run_services.run_binding(recorded_binding, trigger=trigger)
                assert run.status == RunStatus.SUCCEEDED, run.error_message
            _check_schema_snapshot(target, recorded_binding, "all")
        recorded_rows = access.sample_rows(recorded_binding, "Account", limit=50)
        assert recorded_rows

        text = cassette.read_text()
        assert secret not in text, "the access token reached the cassette"
        assert origin not in text, "the loopback origin reached the cassette"
        assert PLACEHOLDERS["SALESFORCE_INSTANCE_URL"] in text
        assert f"Bearer {MASK}" in text, "the bearer scheme was not masked"
        assert (cassette_dir / "salesforce.schema.json").exists()
        # And the scanner, run on what was actually written.
        test_committed_cassettes_carry_no_credentials()

        # -- replay, with the org gone as far as the connector can tell -----
        ACTIVE["credentials"], config = target.build(
            {name: PLACEHOLDERS[name] for name in target.needs}
        )
        replayed_binding = make_binding(
            source="salesforce",
            connection=make_connection(provider="y", auth_backend="recorded"),
            config=config,
            resources=["Account"],
        )
        api.requests.clear()
        with recorder.use_cassette(str(cassette), record_mode="none"):
            for trigger in (RunTrigger.INITIAL, RunTrigger.SCHEDULED):
                run = run_services.run_binding(replayed_binding, trigger=trigger)
                assert run.status == RunStatus.SUCCEEDED, run.error_message
            _check_schema_snapshot(target, replayed_binding, "none")
        assert api.requests == [], "the replay reached the fake org"
    finally:
        CASSETTE_DIR = original_dir

    replayed_rows = access.sample_rows(replayed_binding, "Account", limit=50)
    assert sorted(row["id"] for row in replayed_rows) == sorted(
        row["id"] for row in recorded_rows
    )
