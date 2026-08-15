"""Salesforce SObjects, landed through the REST query API.

Written against the REST API directly rather than around dlt's "verified source"
for Salesforce, because that one is not a package at all: it is a ``dlt init``
code generator that clones a repo and copies files carrying a hardcoded SObject
list and no configuration surface. A library published to PyPI cannot depend on
a code generator, and a hardcoded object list is the opposite of what a
multi-tenant connector needs — so this is config-driven and needs no new
dependency beyond core dlt's own ``requests``.

Four things here are load-bearing.

**SOQL has no bind parameters, so validation is the entire defence.** Object
names, field names and the cursor column all arrive from customer-editable
Binding config and all end up interpolated into a query string. Every one of
them is checked against a strict Salesforce API-name pattern *at the point of
interpolation* (:func:`build_soql`), not only at save time — a validator that
can be skipped by calling a different entry point is not a validator. The
cursor *value* is normalised into a SOQL datetime literal by
:func:`soql_literal`, which refuses anything that is not one. The only raw
fragment is ``where``, which is customer-authored SOQL by design and runs
against the customer's own org under the customer's own credentials — exactly
the position ``sources/sql.py`` takes about its ``query``. It is wrapped in
parentheses, because ``WHERE A OR B AND cursor >= …`` binds as
``A OR (B AND …)`` and would quietly re-download the whole object every run.

**Deletion is genuinely detectable, so tombstones are emitted.** The
``queryAll`` endpoint returns soft-deleted rows with ``IsDeleted = true``, and
deleting a record bumps ``SystemModstamp``, so a deletion arrives through the
same incremental cursor as an update. The limit is the Recycle Bin: a record
hard-deleted, or purged before the next run, vanishes from ``queryAll`` and no
tombstone is ever emitted for it. That is a real gap and it is why
``detect_deletes`` reads as best-effort rather than as a guarantee.

**The cursor field must be spelled the way Salesforce spells it.** SOQL matches
field names case-insensitively but answers with the object's canonical API
casing, so ``cursor: "systemmodstamp"`` produces records with no
``systemmodstamp`` key. The library sets ``on_cursor_value_missing="include"``
(it must — tombstones carry no cursor), which turns that typo into a silent
full refresh on every run forever rather than an error. So the first record of
every resource is checked for the cursor key and the Run fails loudly if it is
absent.

**REQUEST_LIMIT_EXCEEDED is not retried.** Salesforce reuses that code for two
different things: HTTP 403 is the org's 24-hour API allocation, and HTTP 503 is
the concurrent long-running-request cap. Retrying the second is right; retrying
the first spends what is left of the org's allocation on nothing and breaks
every *other* integration in that org too. The status codes separate them
cleanly — dlt's retrying session retries 429 and 5xx and never 403 — so the 403
surfaces immediately as a SourceError that says the allocation is gone.

Unverified against a live provider
----------------------------------
No Salesforce org, sandbox or credential was available while this was written;
every test drives an in-process HTTP server replaying documented payload
shapes. Specifically **not** exercised: real ``nextRecordsUrl`` query locators
and their 2-minute expiry, real ``SystemModstamp`` semantics under bulk updates
and cascade deletes, the Recycle Bin's actual retention behaviour, real daily
API-allocation throttling, ``Sforce-Query-Options`` batch sizing against a large
object, the describe payload of objects with unusual field types, and whether
any given org's Connected App is permitted the objects a Binding names.
"""

import datetime as dt
import re
from typing import ClassVar
from urllib.parse import urlsplit

from django_connectors.errors import scrub
from django_connectors.exceptions import (
    ConfigurationError,
    ConnectorError,
    SourceError,
)
from django_connectors.providers.salesforce.auth import (
    REQUEST_LIMIT_ERROR_CODE,
    assert_salesforce_host,
    bearer_headers,
    credential_error_for,
    describe_errors,
    error_codes,
    parse_api_errors,
)
from django_connectors.sources.base import SourceDefinition
from django_connectors.sources.memory import tombstone

DEFAULT_API_VERSION = "60.0"
API_VERSION_RE = re.compile(r"^\d{2,3}\.\d$")

# Salesforce API names: a letter, then letters, digits and underscores. Custom
# objects end in `__c` and namespaced ones carry a second `__`, so consecutive
# underscores are legal here and the pattern must not forbid them. Anchored and
# length-bounded because this is what stands between customer config and a
# query string.
SOBJECT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
# A selected field may walk relationships — `Owner.Profile.Name`. Depth is
# capped at Salesforce's own limit of five levels.
FIELD_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_]{0,79}(\.[A-Za-z][A-Za-z0-9_]{0,79}){0,4}$"
)

DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

ID_FIELD = "Id"
DELETED_FIELD = "IsDeleted"

# Spec keys a Binding may declare per object. Restricted rather than passed
# through: an unknown key is nearly always a typo for one of these, and silently
# ignoring `filter` when the customer meant `where` means a Run that quietly
# downloads the entire object.
OBJECT_KEYS = frozenset(
    {"fields", "where", "cursor", "initial_value", "detect_deletes", "batch_size"}
)

# describe() field types that cannot appear in a plain SELECT list. `address`
# and `location` are compound wrappers (their components are separate,
# selectable fields); `base64` is the blob body of an Attachment or
# ContentVersion and would pull megabytes per row into a landing column.
UNSELECTABLE_FIELD_TYPES = frozenset({"address", "location", "base64"})

# Salesforce's own bounds for `Sforce-Query-Options: batchSize`.
MIN_BATCH_SIZE = 200
MAX_BATCH_SIZE = 2000

REQUEST_TIMEOUT_SECONDS = 60
CHECK_TIMEOUT_SECONDS = 30


class SalesforceSource(SourceDefinition):
    """A configured set of SObjects, each landed as one merge-keyed resource.

    Configuration::

        {
          "api_version": "60.0",
          "objects": {
            "Account": {
              "fields": ["Id", "Name", "SystemModstamp"],
              "where": "Type = 'Customer'",
              "cursor": "SystemModstamp",
              "initial_value": "2024-01-01T00:00:00Z",
              "detect_deletes": true,
              "batch_size": 2000
            },
            "Opportunity": {}
          }
        }

    ``objects`` may also be a plain list of names — ``["Account", "Contact"]``
    — which means "every field, no filter, no cursor". Omitting ``fields``
    costs one ``describe`` call per object per run and selects everything
    selectable, which is usually what a first sync wants and always what a
    ``FIELDS(ALL)`` query cannot give you (Salesforce caps that at 200 rows).

    The resource name is the SObject's API name, so ``binding.resources`` and
    the landing table both read as ``Account`` / ``account``.
    """

    key = "salesforce"
    provider = "salesforce"
    supported_auth_backends = ("salesforce", "static", "secret_reference")
    # Nothing optional: the REST API is reached through core dlt's requests.
    # Signing the JWT needs `cryptography`, and that is the *auth backend's*
    # dependency — declared there, so a host using a refresh-token grant is not
    # warned about an extra it does not need.
    required_extras: ClassVar[dict[str, str]] = {}

    #: ``queryAll`` surfaces soft-deleted rows, so remote deletions really are
    #: observable here — unlike every purely cursor-based source. Bounded by the
    #: Recycle Bin; see the module docstring.
    emits_tombstones = True

    #: Salesforce has no general-purpose HTTP webhook. Outbound Messages are
    #: SOAP with no shared-secret signature, and Platform Events need a CometD
    #: or gRPC subscriber held open — neither fits the WebhookAdapter contract,
    #: and a half-adapter that could not verify a delivery would be an
    #: unauthenticated trigger. Polling only, deliberately.
    webhook = None

    #: Opt-in escape hatch for an ``instance_url`` outside the Salesforce host
    #: allow-list, matching ``SalesforceBackend.allow_custom_login_host``. A
    #: class attribute for the same reason: the value it unlocks decides where
    #: the org's session bearer is sent.
    allow_custom_login_host = False

    @property
    def allow_private_addresses(self):
        """Whether a private/loopback instance host may be reached.

        Mirrors ``RestSource.allow_private_addresses``: a class attribute, so
        lifting it takes a deliberate registration in settings rather than a
        Binding edit by the party the guard exists to constrain.
        """
        from django_connectors.conf import conf

        return conf.REST_ALLOW_PRIVATE_ADDRESSES

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        config = config or {}

        version = api_version(config)
        if not API_VERSION_RE.match(version):
            raise ConfigurationError(
                f"'api_version' must look like '60.0', got {version!r}. It is "
                f"interpolated into every request path."
            )

        specs = object_specs(config)
        if not specs:
            raise ConfigurationError(
                "salesforce source config needs a non-empty 'objects' — either "
                "a list of SObject API names, or a mapping of "
                "{SObject: {fields, where, cursor, …}}."
            )
        for name, spec in specs.items():
            _validate_object(name, spec)

        instance_url = config.get("instance_url")
        if instance_url:
            self._assert_instance_url(str(instance_url))
        return None

    # --- extraction --------------------------------------------------------

    def incremental_for(self, resource_name, binding):
        """Cursor kwargs only. See ``SourceDefinition.incremental_for``.

        No ``last_value_func``: the default ``max`` is right for a monotonically
        increasing modification stamp, and Salesforce renders those in a fixed
        width (``2024-01-03T12:34:56.000+0000``) so the lexicographic comparison
        dlt performs on the raw strings agrees with chronological order.
        """
        spec = object_specs(binding.config or {}).get(resource_name)
        if not spec:
            return None
        cursor = spec.get("cursor")
        if not cursor:
            return None
        kwargs = {"cursor_path": cursor}
        if spec.get("initial_value"):
            kwargs["initial_value"] = spec["initial_value"]
        return kwargs

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)

        client = self.client_for(
            config, credentials, connection=getattr(binding, "connection", None)
        )
        resources = [
            self._build_resource(dlt, client, name, spec)
            for name, spec in object_specs(config).items()
        ]

        # dlt.source() called as a function, not used as a decorator: the
        # decorator takes the source name from the function's __name__ and so
        # cannot produce a runtime-chosen one.
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _build_resource(self, dlt, client, sobject, spec):
        def emit():
            yield from self._records(dlt, client, sobject, spec)

        return dlt.resource(
            emit,
            name=sobject,
            # Id is the SObject primary key, always. The landing layer prefixes
            # the binding id onto it to form the merge identity.
            primary_key=ID_FIELD,
            # Stated, never inherited. dlt defaults this hint to "append", not
            # to None, so omitting it appends a fresh copy of every re-fetched
            # record on every run — with the merge key correctly configured and
            # nothing raised anywhere.
            write_disposition="merge",
        )()

    def _records(self, dlt, client, sobject, spec):
        """Yield landing records for one SObject, tombstones included."""
        cursor = spec.get("cursor")
        detect_deletes = _detect_deletes(spec)
        fields = self._select_fields(client, sobject, spec)
        statement = build_soql(sobject, fields, spec, _stored_cursor_value(dlt, spec))

        keep_deleted_flag = not spec.get("fields") or _contains(
            spec.get("fields"), DELETED_FIELD
        )
        checked_cursor = False

        for raw in client.iter_query(
            statement,
            query_all=detect_deletes,
            batch_size=spec.get("batch_size"),
            sobject=sobject,
            detect_deletes=detect_deletes,
        ):
            record = strip_attributes(raw)

            if cursor and not checked_cursor:
                checked_cursor = True
                if cursor not in record:
                    raise SourceError(
                        f"{sobject} records carry no {cursor!r} field, so the "
                        f"incremental cursor would never find a value. SOQL "
                        f"matches field names case-insensitively but answers "
                        f"with the object's canonical API casing, so a cursor "
                        f"spelled differently from Salesforce's own spelling "
                        f"(e.g. 'systemmodstamp' for 'SystemModstamp') is "
                        f"accepted by the query and then never matches. This "
                        f"would otherwise re-download the whole object on every "
                        f"run forever, silently, because the library sets "
                        f"on_cursor_value_missing='include'. Record fields: "
                        f"{sorted(record)[:20]}."
                    )

            is_deleted = bool(record.get(DELETED_FIELD))
            if not keep_deleted_flag:
                record.pop(DELETED_FIELD, None)
            if detect_deletes and is_deleted:
                yield _tombstone_for(record, cursor)
                continue
            yield record

    def _select_fields(self, client, sobject, spec):
        """The SELECT list, with the columns the pipeline depends on forced in.

        A configured field list that omits the cursor is the interesting case:
        the query succeeds, records land, and the incremental never advances —
        so the cursor is appended rather than trusted to be there. Same for
        ``Id`` (the merge key) and ``IsDeleted`` (the tombstone signal): without
        them merge would have nothing to key on and deletions would be
        invisible.
        """
        fields = list(spec.get("fields") or [])
        if not fields:
            fields = client.selectable_fields(sobject)
            if not fields:
                raise SourceError(
                    f"describe returned no selectable fields for {sobject!r}; "
                    f"list them explicitly in the binding config."
                )

        required = [ID_FIELD]
        if spec.get("cursor"):
            required.append(spec["cursor"])
        if _detect_deletes(spec):
            required.append(DELETED_FIELD)

        ordered = []
        seen = set()
        for name in [*required, *fields]:
            lowered = name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(name)
        return ordered

    # --- provider surface --------------------------------------------------

    def check_connection(self, *, connection, credentials, binding=None):
        """One request. A SOQL probe when a Binding names an object, else a ping.

        The probe is worth the extra specificity: a token that lists API
        versions proves the org accepted the credential, while
        ``SELECT Id FROM Account LIMIT 1`` proves this Connected App can
        actually read the object the Binding was configured for, which is the
        question the operator is really asking.
        """
        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        client = self.client_for(config, credentials, connection=connection)

        specs = object_specs(config)
        if not specs:
            payload = client.get_path("/services/data/", timeout=CHECK_TIMEOUT_SECONDS)
            count = len(payload) if isinstance(payload, list) else 0
            return f"ok ({count} API versions)"

        sobject = next(iter(specs))
        _validate_identifier(sobject, SOBJECT_RE, "SObject name")
        payload = client.get_path(
            f"{client.base_path}/query/",
            params={"q": f"SELECT {ID_FIELD} FROM {sobject} LIMIT 1"},
            timeout=CHECK_TIMEOUT_SECONDS,
        )
        return f"ok ({sobject}: {payload.get('totalSize', 0)} matching records)"

    def discover(self, *, connection, credentials, query=None):
        """List the SObjects this org exposes, and optionally one object's fields.

        Cheap and genuinely useful: without it the person configuring a Binding
        has to know the API name of every custom object in the org, and custom
        objects are precisely the ones nobody can guess.
        """
        config = connection.metadata or {}
        client = self.client_for(config, credentials, connection=connection)
        payload = client.get_path(f"{client.base_path}/sobjects/")

        resources = []
        for entry in payload.get("sobjects") or []:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name") or ""
            if not entry.get("queryable", True):
                # Not synchronizable at all; offering it would only produce a
                # Binding that fails on its first run.
                continue
            resources.append(
                {
                    "name": name,
                    "label": entry.get("label") or name,
                    "custom": bool(entry.get("custom")),
                    "createable": bool(entry.get("createable")),
                    "deletable": bool(entry.get("deletable")),
                }
            )

        if query:
            needle = str(query).strip().lower()
            resources = [
                entry
                for entry in resources
                if needle in entry["name"].lower() or needle in entry["label"].lower()
            ]
            exact = [entry for entry in resources if entry["name"].lower() == needle]
            if exact:
                exact[0]["fields"] = client.describe_fields(exact[0]["name"])

        resources.sort(key=lambda entry: entry["name"])
        return {"resources": resources}

    # --- client construction ------------------------------------------------

    def client_for(self, config, credentials, *, connection=None):
        """A :class:`SalesforceClient` for this org, or explain what is missing."""
        access_token = _credential_value(credentials, "access_token")
        if not access_token:
            raise ConfigurationError(
                "no Salesforce access token: the Connection's auth backend "
                "returned nothing usable. Point Connection.auth_backend at the "
                "'salesforce' backend, or store a mapping with 'access_token' "
                "and 'instance_url'."
            )

        instance_url = (
            _credential_value(credentials, "instance_url")
            or (config or {}).get("instance_url")
            or ((connection.metadata or {}) if connection is not None else {}).get(
                "instance_url"
            )
        )
        if not instance_url:
            raise ConfigurationError(
                "no Salesforce instance_url. It normally arrives in the token "
                "response; set it in Binding.config or Connection.metadata only "
                "when the credential comes from somewhere that does not supply "
                "one. It cannot be guessed — every org has its own host."
            )

        from django_connectors.sources.rest import guarded_session

        return SalesforceClient(
            instance_url=self._assert_instance_url(str(instance_url)),
            access_token=access_token,
            api_version=api_version(config),
            session=guarded_session(allow_private=self.allow_private_addresses),
        )

    def _assert_instance_url(self, url):
        from django_connectors.sources.rest import assert_safe_url

        # instance_url can arrive from Binding.config or Connection.metadata,
        # both editable, and it is where the org session bearer is sent — so it
        # is constrained to Salesforce hosts exactly like login_url.
        assert_salesforce_host(
            url,
            "instance_url",
            allow_custom=self.allow_custom_login_host,
            subject=type(self),
        )
        allow_private = self.allow_private_addresses
        # Scheme before resolution: a URL that is wrong on its face should not
        # need a working DNS answer to be told so.
        if not allow_private and urlsplit(url).scheme != "https":
            raise ConfigurationError(
                f"Salesforce instance_url must be https, got {url!r}. Every "
                f"request carries a bearer token."
            )
        assert_safe_url(url, allow_private=allow_private)
        return url.rstrip("/")


class SalesforceClient:
    """One org, one access token, one API version.

    Holds the describe cache for the lifetime of a Run: a Binding with eight
    objects and no explicit field lists would otherwise re-describe each of them
    once per resource build, and describe is one of the more expensive calls in
    the API.
    """

    def __init__(self, *, instance_url, access_token, api_version, session):
        self.instance_url = instance_url.rstrip("/")
        self.access_token = access_token
        self.api_version = api_version
        self.session = session
        self._described = {}

    @property
    def base_path(self):
        return f"/services/data/v{self.api_version}"

    def get_path(self, path, *, params=None, headers=None, timeout=None):
        """GET a path on *this org's* host and return the decoded JSON body.

        A path, never a URL. ``nextRecordsUrl`` is an opaque server-generated
        locator, and treating it as a URL would let a response choose which host
        the next request — carrying the org's bearer token — is sent to.
        """
        if urlsplit(path).scheme or path.startswith("//"):
            raise SourceError(
                f"refusing to follow {path!r}: Salesforce paths are relative to "
                f"the org's instance URL, and an absolute one would send this "
                f"org's bearer token somewhere else."
            )
        url = f"{self.instance_url}/{path.lstrip('/')}"
        try:
            response = self.session.get(
                url,
                params=params,
                headers=bearer_headers(self.access_token, headers),
                timeout=timeout or REQUEST_TIMEOUT_SECONDS,
            )
        except ConnectorError:
            # The address guard fires from inside `send`; its message already
            # says what is wrong and must not be relabelled.
            raise
        except Exception as exc:
            raise SourceError(
                f"the request to Salesforce failed: {scrub(exc)}"
            ) from exc

        raise_for_salesforce_error(response)
        try:
            return response.json()
        except ValueError as exc:
            raise SourceError(
                f"Salesforce answered HTTP {response.status_code} with a body "
                f"that is not JSON."
            ) from exc

    def iter_query(
        self,
        soql,
        *,
        query_all=False,
        batch_size=None,
        sobject="",
        detect_deletes=False,
    ):
        """Yield raw records for `soql`, following ``nextRecordsUrl`` to the end.

        A source that stopped at the first page would land a plausible-looking
        partial table: ``done`` is ``false`` and ``records`` is a full batch, so
        nothing about the response looks like an error.
        """
        endpoint = "queryAll" if query_all else "query"
        headers = (
            {"Sforce-Query-Options": f"batchSize={int(batch_size)}"}
            if batch_size
            else None
        )
        try:
            payload = self.get_path(
                f"{self.base_path}/{endpoint}/", params={"q": soql}, headers=headers
            )
        except SourceError as exc:
            hint = _query_hint(exc, sobject, detect_deletes)
            if hint is None:
                raise
            raise hint from exc

        while True:
            yield from payload.get("records") or []
            next_path = payload.get("nextRecordsUrl")
            if not next_path:
                # `done` is not consulted: the locator's absence is the real
                # end-of-stream signal, and a response with `done: false` and no
                # locator would otherwise loop forever.
                break
            payload = self.get_path(next_path, headers=headers)

    def describe_fields(self, sobject):
        """The object's field descriptors, cached per client."""
        _validate_identifier(sobject, SOBJECT_RE, "SObject name")
        if sobject not in self._described:
            payload = self.get_path(f"{self.base_path}/sobjects/{sobject}/describe/")
            self._described[sobject] = [
                field
                for field in (payload.get("fields") or [])
                if isinstance(field, dict) and field.get("name")
            ]
        return self._described[sobject]

    def selectable_fields(self, sobject):
        """Field names a plain ``SELECT`` may list for `sobject`."""
        return [
            field["name"]
            for field in self.describe_fields(sobject)
            if str(field.get("type") or "").lower() not in UNSELECTABLE_FIELD_TYPES
            and not field.get("deprecatedAndHidden")
        ]


# --- SOQL ------------------------------------------------------------------


def build_soql(sobject, fields, spec, cursor_value=None):
    """Compose one SOQL statement, validating every identifier it interpolates.

    Validation happens here rather than only in ``validate_config`` because this
    is the point of interpolation, and a check that lives somewhere else can be
    reached around by any caller that skips it. There is no bind-parameter
    mechanism in SOQL to fall back on.

    No ``ORDER BY``. dlt persists incremental state only after a *successful*
    load, so ordering buys no resumability, and forcing the org's query
    optimizer to sort a large object is a real cost paid for nothing.

    ``>=``, never ``>``: dlt's ``Incremental`` uses a closed lower bound, so a
    record stamped at exactly the stored cursor value is still wanted. A strict
    inequality here would drop it below dlt's notice, where no test of dlt's own
    behaviour could see it.
    """
    _validate_identifier(sobject, SOBJECT_RE, "SObject name")
    selected = [_validate_identifier(name, FIELD_RE, "field name") for name in fields]
    if not selected:
        raise ConfigurationError(f"no fields to select for SObject {sobject!r}")

    clauses = []
    where = (spec or {}).get("where")
    if where:
        # Parenthesised, always. `WHERE A OR B AND cursor >= x` binds as
        # `A OR (B AND cursor >= x)`, which silently drops the incremental
        # predicate for every record matching A — a full re-download every run,
        # with no error and correct-looking data.
        clauses.append(f"({_validate_where(where)})")

    cursor = (spec or {}).get("cursor")
    if cursor and cursor_value is not None:
        _validate_identifier(cursor, SOBJECT_RE, "cursor field")
        clauses.append(f"{cursor} >= {soql_literal(cursor_value)}")

    statement = f"SELECT {', '.join(selected)} FROM {sobject}"
    if clauses:
        statement = f"{statement} WHERE {' AND '.join(clauses)}"
    return statement


def soql_literal(value):
    """Render `value` as an unquoted SOQL date/datetime literal, or refuse.

    Two Salesforce-specific traps, both of which produce a wrong query rather
    than an obvious failure:

    *what Salesforce emits is not what SOQL accepts*
        records carry ``2024-01-03T12:34:56.000+0000`` — an offset with no
        colon — and SOQL rejects that form. Echoing the stored cursor straight
        back into the WHERE clause makes every run after the first die on
        MALFORMED_QUERY, which reads like a config error rather than a format
        mismatch.

    *sub-second precision is truncated down, never rounded*
        against the ``>=`` predicate a truncated bound merely re-fetches the
        boundary record, and merge reconciles it. Rounding up would step over
        that record and lose it with no error anywhere.

    Datetime literals are unquoted in SOQL, which is exactly why this refuses
    anything that is not one instead of trying to escape it.
    """
    if isinstance(value, dt.datetime):
        moment = value
    else:
        text = str(value).strip()
        if DATE_ONLY_RE.fullmatch(text):
            # A SOQL date literal already, and valid unquoted.
            return text
        try:
            moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise ConfigurationError(
                f"{value!r} is not a Salesforce date or datetime, so it cannot "
                f"be used as an incremental cursor bound. SOQL takes an "
                f"unquoted literal such as 2024-01-03T00:00:00Z or 2024-01-03, "
                f"and there is no bind parameter to fall back on."
            ) from None

    if moment.tzinfo is None:
        # A naive stamp is treated as UTC rather than as the worker's local
        # time: the worker's timezone is an accident of deployment, and getting
        # it wrong shifts the whole incremental window by hours.
        moment = moment.replace(tzinfo=dt.UTC)
    return moment.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def strip_attributes(record):
    """Drop Salesforce's ``attributes`` envelope, at every level.

    Every record — and every nested record inside a relationship subquery —
    carries ``{"attributes": {"type": …, "url": …}}``. Landed as-is it becomes a
    JSON column holding a record URL, which is neither data the customer asked
    for nor anything a mapping can use, and it changes whenever the API version
    does, which would show up as schema drift.
    """
    if isinstance(record, dict):
        return {
            key: strip_attributes(value)
            for key, value in record.items()
            if key != "attributes"
        }
    if isinstance(record, list):
        return [strip_attributes(entry) for entry in record]
    return record


def _tombstone_for(record, cursor):
    """A deletion marker for one soft-deleted SObject row.

    Identity plus the cursor, and nothing else. The identity-only shape comes
    from ``sources/memory.py::tombstone`` and is deliberately lossy: delete-insert
    merge replaces the whole row, so every column not supplied here lands NULL —
    which is why Projection refuses to map a target identity field from outside
    the merge key.

    The cursor is carried anyway, against that grain, because dropping it means
    the high-water mark never advances past the deletion and the same tombstone
    is re-emitted on every subsequent run until some *other* record moves the
    cursor beyond it. In a quiet org that is unbounded.
    """
    marker = tombstone({ID_FIELD: record.get(ID_FIELD)})
    if cursor and record.get(cursor) is not None:
        marker[cursor] = record[cursor]
    return marker


def raise_for_salesforce_error(response):
    """Turn a failing Salesforce response into the right exception, or return.

    The ordering matters. ``REQUEST_LIMIT_EXCEEDED`` is checked first because
    :func:`credential_error_for` would otherwise classify the 403 it arrives on
    as a permission problem, and an operator told "this user is not permitted"
    when the org has simply spent its API allocation will go looking in entirely
    the wrong place.
    """
    if response.status_code < 400:
        return

    errors = parse_api_errors(response)
    codes = error_codes(errors)
    detail = describe_errors(errors)

    if REQUEST_LIMIT_ERROR_CODE in codes and response.status_code == 403:
        raise SourceError(
            f"Salesforce refused the request with REQUEST_LIMIT_EXCEEDED "
            f"(HTTP 403): this org has spent its 24-hour API request "
            f"allocation. This is not retried — the allocation refills on a "
            f"rolling window, and retrying spends what little is left and "
            f"breaks every other integration in the org too. Raise the org's "
            f"limit, lengthen this Binding's poll interval, or narrow its "
            f"field selection. Provider message: {detail}"
        )

    error = credential_error_for(response, errors)
    if error is not None:
        raise error

    raise SourceError(f"Salesforce answered HTTP {response.status_code}: {detail}")


def _query_hint(exc, sobject, detect_deletes):
    """The one hint a query failure on ``queryAll`` cannot give itself, or None.

    ``IsDeleted`` is a column *this source* added, not one the customer wrote,
    so "No such column 'IsDeleted'" names something they cannot find in their
    own configuration. Returns None when there is nothing to add, so the
    original exception is re-raised untouched rather than re-wrapped.
    """
    text = str(exc)
    if not (detect_deletes and DELETED_FIELD.lower() in text.lower()):
        return None
    return SourceError(
        f"{text} — {sobject!r} appears not to expose {DELETED_FIELD}, which "
        f"this source selects in order to detect deletions. Set "
        f"'detect_deletes': false for this object; tombstones will not be "
        f"emitted for it."
    )


# --- configuration helpers --------------------------------------------------


def api_version(config):
    return str((config or {}).get("api_version") or DEFAULT_API_VERSION)


def object_specs(config):
    """Normalise ``objects`` into ``{SObject: spec}``.

    Accepts a mapping, a list of names, or a list of ``{"name": …, …}`` objects,
    because "sync these three objects with default settings" should not require
    three empty dicts.
    """
    objects = (config or {}).get("objects")
    if objects is None:
        return {}
    if isinstance(objects, dict):
        return {
            str(name): dict(spec or {}) if isinstance(spec, dict) else {}
            for name, spec in objects.items()
        }
    if isinstance(objects, list | tuple):
        specs = {}
        for entry in objects:
            if isinstance(entry, str):
                specs[entry] = {}
            elif isinstance(entry, dict) and entry.get("name"):
                spec = {key: value for key, value in entry.items() if key != "name"}
                specs[str(entry["name"])] = spec
            else:
                raise ConfigurationError(
                    f"each entry of 'objects' must be an SObject name or an "
                    f"object with a 'name', got {entry!r}."
                )
        return specs
    raise ConfigurationError(
        f"'objects' must be a mapping or a list, got {type(objects).__name__}."
    )


def _validate_object(name, spec):
    _validate_identifier(name, SOBJECT_RE, "SObject name")

    unknown = set(spec) - OBJECT_KEYS
    if unknown:
        raise ConfigurationError(
            f"objects.{name} has unknown key(s) {sorted(unknown)}; supported: "
            f"{sorted(OBJECT_KEYS)}."
        )

    fields = spec.get("fields")
    if fields is not None:
        if not isinstance(fields, list | tuple) or not fields:
            raise ConfigurationError(
                f"objects.{name}.fields must be a non-empty list of field API "
                f"names, or omitted entirely to select everything."
            )
        for field in fields:
            _validate_identifier(field, FIELD_RE, f"objects.{name} field name")

    cursor = spec.get("cursor")
    if cursor is not None:
        # A plain field, not a relationship path: it is interpolated into the
        # WHERE clause *and* used as dlt's cursor_path, and a dotted path would
        # index into a nested dict that max() cannot order.
        _validate_identifier(cursor, SOBJECT_RE, f"objects.{name}.cursor")

    if spec.get("initial_value") is not None:
        if not cursor:
            raise ConfigurationError(
                f"objects.{name}.initial_value has no effect without a "
                f"'cursor' to apply it to."
            )
        # Fails here rather than on the first run, where the whole Run dies.
        soql_literal(spec["initial_value"])

    if spec.get("where") is not None:
        _validate_where(spec["where"], label=f"objects.{name}.where")

    batch_size = spec.get("batch_size")
    if batch_size is not None and (
        not isinstance(batch_size, int)
        or isinstance(batch_size, bool)
        or not MIN_BATCH_SIZE <= batch_size <= MAX_BATCH_SIZE
    ):
        raise ConfigurationError(
            f"objects.{name}.batch_size must be an integer between "
            f"{MIN_BATCH_SIZE} and {MAX_BATCH_SIZE} (Salesforce's own bounds "
            f"for Sforce-Query-Options), got {batch_size!r}."
        )

    detect = spec.get("detect_deletes")
    if detect is not None and not isinstance(detect, bool):
        raise ConfigurationError(
            f"objects.{name}.detect_deletes must be true or false, got {detect!r}."
        )


def _validate_identifier(value, pattern, label):
    """Refuse anything that is not a bare Salesforce API name.

    This is the whole injection defence. SOQL has no bind parameters, so the
    choice is between validating identifiers and escaping them, and there is no
    published escaping rule for a SOQL identifier to get right.
    """
    if not isinstance(value, str) or not pattern.match(value):
        raise ConfigurationError(
            f"{label} must be a Salesforce API name — a letter followed by "
            f"letters, digits and underscores — got {value!r}. It is "
            f"interpolated into a SOQL statement, and SOQL has no bind "
            f"parameters, so anything else is refused rather than escaped."
        )
    return value


def _validate_where(where, label="where"):
    """Sanity-check a customer-authored SOQL fragment.

    Deliberately *not* a parser. This fragment is customer-authored SOQL by
    design, running against the customer's own org under the customer's own
    credentials — the same position ``sources/sql.py`` takes about its ``query``
    — so the job here is to catch the two mistakes that produce a broken or
    silently-wrong query rather than to police what it may express.
    """
    if not isinstance(where, str) or not where.strip():
        raise ConfigurationError(f"{label} must be a non-empty string of SOQL.")
    if where.count("'") % 2:
        raise ConfigurationError(
            f"{label} has an odd number of quote characters, so a string "
            f"literal is unterminated: {where!r}."
        )
    if where.count("(") != where.count(")"):
        raise ConfigurationError(f"{label} has unbalanced parentheses: {where!r}.")
    return where


def _detect_deletes(spec):
    """Deletion detection is on unless the object cannot support it.

    Defaulting on because ``queryAll`` costs exactly what ``query`` costs and
    the alternative default is silent: without it a remote deletion emits
    nothing at all, the row stays in the landing table forever, and every
    projection downstream keeps a record the customer deleted.
    """
    value = (spec or {}).get("detect_deletes")
    return True if value is None else bool(value)


def _contains(names, target):
    return any(str(name).lower() == target.lower() for name in names or ())


def _stored_cursor_value(dlt, spec):
    """The incremental's last value, read out of dlt's own resource state.

    Read rather than received: the library builds the ``Incremental`` (see
    ``landing/instrument.py``), so the resource function is never handed one.
    Falling back to ``initial_value`` matters on the first run — otherwise the
    first extraction scans every record the org has ever had.
    """
    cursor = (spec or {}).get("cursor")
    if not cursor:
        return None
    try:
        stored = (dlt.current.resource_state().get("incremental") or {}).get(
            cursor
        ) or {}
    except Exception:
        # No pipeline state yet, or dlt moved where it keeps it. A full scan is
        # slower but never wrong; dlt still filters what it emits.
        stored = {}
    return stored.get("last_value") or spec.get("initial_value")


def _credential_value(credentials, key):
    """Read `key` out of whatever shape the auth backend returned."""
    if credentials is None:
        return None
    if isinstance(credentials, str):
        # A bare string can only be the token; instance_url then has to be
        # configured, and client_for says so.
        return credentials if key == "access_token" else None
    try:
        value = credentials[key]
    except (KeyError, TypeError, IndexError):
        value = getattr(credentials, key, None)
    return value or None
