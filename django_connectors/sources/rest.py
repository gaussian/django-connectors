"""A REST API source configured entirely by ``Binding.config``.

Wraps ``dlt.sources.rest_api``, which ships in dlt core — ``requests`` and
``jsonpath-ng`` are core dlt dependencies, so no extra is needed. The point of
this source is that a customer configures an API without anyone writing Python:
base URL, endpoint path, method, query parameters, pagination, the JSONPath to
the records, and the incremental cursor are all JSON.

Two things here are load-bearing and easy to get wrong:

**The cursor is declared twice, on purpose.** ``incremental_for`` returns cursor
*kwargs* so that the library constructs the ``Incremental`` (see
``landing/instrument.py``: it forces ``primary_key=()`` and
``on_cursor_value_missing="include"``, neither of which can be repaired after
the fact). Separately, ``incremental.start_param`` binds that same cursor to a
request parameter so the *provider* filters instead of us discarding records
after paying to download them. dlt resolves the conflict in our favour: an
incremental applied through ``apply_hints`` is marked ``from_hints`` and
``IncrementalResourceWrapper`` then refuses to replace it with the one
``rest_api`` built from the parameter declaration — verified against dlt 1.30,
where run 2's request carried the run-1 cursor value *and* the record sitting
exactly on the boundary was still re-emitted. The consequence is that
``start_param`` is mandatory whenever a cursor is declared: dlt injects the
hint-applied ``Incremental`` into ``paginate_resource`` regardless, and with no
parameter to bind it to the Run dies on ``'NoneType' object has no attribute
'start'``.

**base_url is customer-controlled, so this is an SSRF surface.** Checking
base_url once at save time is not a guard: ``RESTClient`` urljoins the endpoint
path onto it, so an absolute URL in ``path`` replaces the host outright, and
``requests`` follows redirects, so a 302 to ``169.254.169.254`` walks straight
past any check made at configuration time. Every request and every redirect hop
therefore goes through :func:`assert_safe_url` inside the session's ``send``.
That check alone is still not enough, because it validates a DNS answer that
urllib3 then throws away and looks up again at connect time — so the guarded
session also re-runs the check on the address the socket actually connected to
(see :func:`_pin_connections`).
"""

import ipaddress
import socket
from functools import cache
from typing import ClassVar
from urllib.parse import urljoin, urlsplit

from django_connectors.errors import scrub
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    ConnectorError,
    SourceError,
)
from django_connectors.sources.base import SourceDefinition

# Only these two reach a network the way a customer expects. `file:`, `ftp:` and
# friends turn a "call an API" feature into "read the worker's filesystem".
ALLOWED_SCHEMES = frozenset({"http", "https"})

# dlt's auth shorthands that make sense for a token handed over by an auth
# backend. `oauth2_client_credentials` is deliberately absent: acquiring tokens
# is the auth backend's job, not the source's.
AUTH_TYPES = frozenset({"bearer", "api_key", "http_basic"})

# Incremental keys a Binding may declare. Restricted rather than passed through
# because these end up as `Incremental(**kwargs)` and an unknown key would
# surface as a TypeError inside a customer's Run. `end_value` is absent
# deliberately: rest_api rejects it outright in a request-parameter binding,
# and a bounded backfill is not a v0.1 feature.
INCREMENTAL_KEYS = frozenset({"cursor_path", "initial_value"})

LAST_VALUE_FUNCS = {"max": max, "min": min}

CHECK_TIMEOUT_SECONDS = 10


def assert_safe_url(url, *, allow_private=False):
    """Raise ``ConfigurationError`` unless `url` points at a public http(s) host.

    Prevents the Binding editor from being turned into a request forwarder onto
    the network the worker sits in — cloud metadata (``169.254.169.254``),
    internal admin panels, ``localhost`` services. Resolution happens here
    rather than by pattern-matching the hostname, because ``evil.test`` with an
    A record of ``127.0.0.1`` looks perfectly public as a string.
    """
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise ConfigurationError(f"{url!r} is not a usable URL: {exc}") from exc

    scheme = (parts.scheme or "").lower()
    if scheme not in ALLOWED_SCHEMES:
        raise ConfigurationError(
            f"URL scheme {scheme or '(none)'!r} is not allowed for a rest "
            f"source; use http or https."
        )

    host = parts.hostname
    if not host:
        raise ConfigurationError(f"{url!r} has no host to call.")

    if allow_private:
        return

    for address in _resolve(host, parts.port or (443 if scheme == "https" else 80)):
        ip = ipaddress.ip_address(address)
        # ::ffff:127.0.0.1 is loopback wearing an IPv6 hat, and `is_global` on
        # the mapped form does not see through it.
        ip = getattr(ip, "ipv4_mapped", None) or ip
        if not ip.is_global:
            raise ConfigurationError(
                f"refusing to call host {host!r}: it resolves to {ip}, which is "
                f"a loopback, private, link-local or otherwise internal "
                f"address — cloud metadata services live at 169.254.169.254. "
                f"If a Binding genuinely must reach an internal host, register "
                f"a RestSource subclass with allow_private_addresses = True in "
                f"DJANGO_CONNECTORS['SOURCES']."
            )


def _resolve(host, port):
    """Every address `host` resolves to. A literal IP resolves without DNS."""
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise ConfigurationError(
            f"host {host!r} could not be resolved: {exc}. A base_url that does "
            f"not resolve cannot be synchronized."
        ) from exc
    return {info[4][0] for info in infos}


class RestSource(SourceDefinition):
    """A REST API described by JSON rather than by code."""

    key = "rest"
    provider = "rest"
    # Nothing optional: dlt's rest_api needs only core dlt.
    required_extras: ClassVar[dict[str, str]] = {}

    #: Opt-in escape hatch for internal APIs. Deliberately a class attribute
    #: rather than a Binding config key — the customer editing base_url is
    #: exactly the party an SSRF guard exists to constrain, so only a host
    #: registering a subclass in DJANGO_CONNECTORS["SOURCES"] can lift it.
    @property
    def allow_private_addresses(self):
        """Whether private/loopback/metadata addresses are reachable.

        Read from settings rather than hardcoded so a host with genuinely
        internal APIs can opt in globally; the default denies.
        """
        from django_connectors.conf import conf

        return conf.REST_ALLOW_PRIVATE_ADDRESSES

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration the Binding editor should never be able to save."""
        config = config or {}

        base_url = config.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            raise ConfigurationError(
                "rest source config needs a 'base_url' string, e.g. "
                "'https://api.example.com/v1/'."
            )
        assert_safe_url(base_url, allow_private=self.allow_private_addresses)

        resources = config.get("resources")
        if not isinstance(resources, dict) or not resources:
            raise ConfigurationError(
                "rest source config needs a non-empty 'resources' mapping of "
                "{resource_name: endpoint config}."
            )

        for name, spec in resources.items():
            self._validate_resource(name, spec)

        self._validate_auth(config.get("auth"))
        # Shape errors that only dlt can see (paginator kinds, HTTP methods,
        # unknown endpoint keys) — surfaced with the JSON path that is wrong.
        self._validate_against_dlt(config)
        return None

    def _validate_resource(self, name, spec):
        if not isinstance(spec, dict):
            raise ConfigurationError(f"resources.{name} must be an object")

        path = spec.get("path", name)
        if not isinstance(path, str) or urlsplit(path).scheme:
            # An absolute URL here would replace base_url entirely: urljoin
            # discards the base whenever the "path" carries its own scheme.
            raise ConfigurationError(
                f"resources.{name}.path must be a path relative to base_url, "
                f"not an absolute URL (got {path!r})."
            )

        disposition = spec.get("write_disposition", "merge")
        if disposition == "merge" and not spec.get("primary_key"):
            raise ConfigurationError(
                f"resources.{name} uses merge disposition and must declare "
                f"'primary_key' — merge without a key cannot identify the rows "
                f"it replaces."
            )

        incremental = spec.get("incremental")
        if incremental is None:
            return
        if not isinstance(incremental, dict):
            raise ConfigurationError(f"resources.{name}.incremental must be an object")
        if not incremental.get("cursor_path"):
            raise ConfigurationError(
                f"resources.{name}.incremental must declare 'cursor_path' — the "
                f"JSONPath to the field the provider orders records by."
            )
        if not incremental.get("start_param"):
            # Not a stylistic preference. dlt injects the library's Incremental
            # into `paginate_resource`'s `incremental_object` argument whether
            # or not a request parameter was declared, and with no parameter to
            # bind it to `_set_incremental_params` dereferences None: the Run
            # dies with "'NoneType' object has no attribute 'start'", naming
            # nothing the customer configured. Refusing here says what to fix.
            raise ConfigurationError(
                f"resources.{name}.incremental must declare 'start_param' — the "
                f"query parameter the provider filters on (e.g. 'since', "
                f"'updated_after'). Without it the cursor has nothing to bind "
                f"to and the whole collection is downloaded every run."
            )
        unknown = (
            set(incremental) - INCREMENTAL_KEYS - {"start_param", "last_value_func"}
        )
        if unknown:
            raise ConfigurationError(
                f"resources.{name}.incremental has unknown key(s) "
                f"{sorted(unknown)}; supported: "
                f"{sorted(INCREMENTAL_KEYS | {'start_param', 'last_value_func'})}."
            )
        func = incremental.get("last_value_func")
        if func is not None and func not in LAST_VALUE_FUNCS:
            raise ConfigurationError(
                f"resources.{name}.incremental.last_value_func must be one of "
                f"{sorted(LAST_VALUE_FUNCS)}, got {func!r}."
            )

    def _validate_auth(self, auth):
        if auth is None:
            return
        if not isinstance(auth, dict):
            raise ConfigurationError("'auth' must be an object")
        kind = auth.get("type")
        if kind not in AUTH_TYPES:
            raise ConfigurationError(
                f"auth.type must be one of {sorted(AUTH_TYPES)}, got {kind!r}. "
                f"The credential itself comes from the Connection's auth "
                f"backend and is never stored in Binding.config."
            )

    def _validate_against_dlt(self, config):
        from dlt.common.exceptions import DictValidationException
        from dlt.common.validation import validate_dict
        from dlt.sources.rest_api.typing import RESTAPIConfig

        # No credentials, no session, and no auth block: this runs at Binding
        # save time, where no credential exists yet and demanding one would
        # make a perfectly good configuration unsavable. Auth *shape* is
        # already checked by `_validate_auth`; none of it affects the rest.
        rest_config, order = _rest_api_config(
            config, credentials=None, include_auth=False
        )
        try:
            validate_dict(RESTAPIConfig, rest_config, path=".")
        except DictValidationException as exc:
            raise ConfigurationError(_readable_validation_error(exc, order)) from exc

    # --- extraction --------------------------------------------------------

    def incremental_for(self, resource_name, binding):
        """Cursor kwargs only. See ``SourceDefinition.incremental_for``."""
        spec = ((binding.config or {}).get("resources") or {}).get(resource_name) or {}
        incremental = spec.get("incremental")
        return _incremental_kwargs(incremental) if incremental else None

    def build_source(self, *, binding, credentials, run):
        config = binding.config or {}
        self.validate_config(config)

        session = guarded_session(allow_private=self.allow_private_addresses)
        rest_config, _ = _rest_api_config(config, credentials, session=session)

        from dlt.sources.rest_api import rest_api_source

        try:
            return rest_api_source(rest_config, name=self.key, section=self.key)
        except ConnectorError:
            raise
        except Exception as exc:
            raise SourceError(
                f"could not build the rest source for binding {binding.id}: "
                f"{scrub(exc)}"
            ) from exc

    def check_connection(self, *, connection, credentials, binding=None):
        """One cheap request against the configured API.

        `binding` is optional so that a Connection can be tested before any
        Binding exists; in that case ``Connection.metadata`` supplies base_url.
        """
        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        base_url = config.get("base_url")
        if not base_url:
            raise ConfigurationError(
                "no 'base_url' to test: put one in the Binding's config or in "
                "Connection.metadata."
            )
        assert_safe_url(base_url, allow_private=self.allow_private_addresses)

        session = guarded_session(allow_private=self.allow_private_addresses)
        auth = _auth_config(config.get("auth"), credentials)

        from dlt.sources.rest_api.config_setup import create_auth

        url = urljoin(base_url, _check_path(config))
        try:
            response = session.get(
                url,
                auth=create_auth(auth),
                headers=config.get("headers") or None,
                timeout=CHECK_TIMEOUT_SECONDS,
            )
        except ConnectorError:
            # The SSRF guard fires from inside `send`; its message already says
            # exactly what is wrong and must not be relabelled as a transport
            # failure.
            raise
        except Exception as exc:
            raise SourceError(f"request to the API failed: {scrub(exc)}") from exc

        if response.status_code in (401, 403):
            raise AuthError(
                f"the API rejected these credentials with HTTP {response.status_code}."
            )
        if response.status_code >= 400:
            raise SourceError(f"the API answered HTTP {response.status_code}.")
        return f"ok ({response.status_code})"


def _check_path(config):
    """The cheapest path to probe: an explicit one, else the first resource."""
    explicit = config.get("check_path")
    if explicit is not None:
        return explicit
    first = next(iter((config.get("resources") or {}).items()), None)
    if first is None:
        return ""
    name, spec = first
    return (spec or {}).get("path", name)


def guarded_session(*, allow_private):
    """A requests session that re-checks the destination of every hop.

    ``Session.send`` is the single funnel every request *and every redirect
    hop* passes through — ``resolve_redirects`` calls it again per hop — which
    makes it the only place a URL check cannot be routed around. On top of
    that, :func:`_pin_connections` re-checks each socket's peer, because the
    name check and the connection do not share a DNS lookup.
    """
    from dlt.sources.helpers.requests.retry import Client

    # dlt's own client, so retry/backoff behaviour matches every other request
    # dlt makes; raise_for_status stays off because rest_api's response hooks
    # depend on seeing error responses.
    session = Client(raise_for_status=False).session
    send = session.send

    def guarded_send(request, **kwargs):
        assert_safe_url(request.url, allow_private=allow_private)
        return send(request, **kwargs)

    session.send = guarded_send
    if not allow_private:
        _pin_connections(session)
    return session


def _pin_connections(session):
    """Re-run the address guard on the address each socket actually reached.

    The silent failure this exists for is DNS rebinding. ``assert_safe_url``
    resolves the hostname, approves those answers, and then hands the *name* to
    requests — which resolves it a second time inside urllib3 when the socket is
    opened. An attacker who controls the zone answers the first lookup with a
    public address and the second with ``169.254.169.254``, and every check in
    this module passes while the worker talks to the metadata service. Measured:
    a rebinding name reached a loopback server through a fully enabled guard.

    So each connection re-checks its own peer, which is by definition the
    resolution that was used, before any request bytes or TLS ClientHello go out.
    Hostname, SNI and certificate validation are untouched — only the sanity of
    the address is asserted a second time.

    Requests routed through a proxy are not pinned and do not need to be: the
    worker never resolves the name in that case, the proxy does, so the
    destination policy belongs there.
    """
    classes = _pinned_pool_classes()
    for adapter in session.adapters.values():
        manager = getattr(adapter, "poolmanager", None)
        if manager is not None:
            # Per-instance, so dlt's retry adapter keeps every other setting it
            # was built with; the pools themselves are created lazily, after this.
            manager.pool_classes_by_scheme = classes


@cache
def _pinned_pool_classes():
    """urllib3 pool classes whose connections assert their own peer address.

    Built lazily and once: urllib3 arrives with requests, which arrives with
    dlt, and this module keeps all three off the import path of a host that
    never runs a pipeline.
    """
    from urllib3 import HTTPConnectionPool, HTTPSConnectionPool
    from urllib3.connection import HTTPConnection, HTTPSConnection

    class PinnedHTTPConnection(HTTPConnection):
        def _new_conn(self):
            return _checked(super()._new_conn(), scheme="http", hostname=self.host)

    class PinnedHTTPSConnection(HTTPSConnection):
        def _new_conn(self):
            # Runs before `connect` wraps the socket in TLS, so a rebound
            # address never even sees a ClientHello.
            return _checked(super()._new_conn(), scheme="https", hostname=self.host)

    class PinnedHTTPConnectionPool(HTTPConnectionPool):
        ConnectionCls = PinnedHTTPConnection

    class PinnedHTTPSConnectionPool(HTTPSConnectionPool):
        ConnectionCls = PinnedHTTPSConnection

    return {"http": PinnedHTTPConnectionPool, "https": PinnedHTTPSConnectionPool}


def _checked(sock, *, scheme, hostname):
    """`sock`, or ``ConfigurationError`` if it landed on an internal address."""
    try:
        _assert_safe_peer(sock, scheme=scheme, hostname=hostname)
    except BaseException:
        # Nothing has been written yet; drop the socket rather than leaving a
        # half-open connection to whatever answered.
        sock.close()
        raise
    return sock


def _assert_safe_peer(sock, *, scheme, hostname):
    """Raise ``ConfigurationError`` if `sock` landed on an internal address."""
    peer = sock.getpeername()
    address, port = peer[0], peer[1]
    # An IPv6 literal has to go back into a URL bracketed, or urlsplit reads the
    # last group as a port.
    literal = f"[{address}]" if ":" in address else address
    try:
        # Deliberately the same entry point the hostname went through: one
        # decision about what "internal" means, and one place to relax it.
        assert_safe_url(f"{scheme}://{literal}:{port}/", allow_private=False)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"refusing to talk to {hostname!r}: the connection landed on "
            f"{address}, which is not what the name resolved to when it was "
            f"checked. A name that answers differently on the second lookup is "
            f"a DNS rebinding attempt. {exc}"
        ) from exc


# --- config translation ----------------------------------------------------


def _rest_api_config(config, credentials, *, session=None, include_auth=True):
    """Translate ``Binding.config`` into dlt's ``RESTAPIConfig``.

    Returns ``(rest_api_config, resource_order)``; the order maps dlt's
    positional ``resources[i]`` error paths back to the customer's own resource
    names.
    """
    client = {"base_url": config["base_url"]}
    if config.get("headers"):
        client["headers"] = dict(config["headers"])
    if config.get("paginator"):
        client["paginator"] = config["paginator"]
    auth = _auth_config(config.get("auth"), credentials) if include_auth else None
    if auth:
        client["auth"] = auth
    if session is not None:
        client["session"] = session

    entries = []
    order = []
    for name, spec in (config.get("resources") or {}).items():
        entries.append(_endpoint_resource(name, spec))
        order.append(name)
    return {"client": client, "resources": entries}, order


def _endpoint_resource(name, spec):
    endpoint = {"path": spec.get("path", name)}
    for key in ("method", "json", "data_selector", "paginator", "headers"):
        if spec.get(key) is not None:
            endpoint[key] = spec[key]

    params = dict(spec.get("params") or {})
    incremental = spec.get("incremental") or {}
    start_param = incremental.get("start_param")
    if start_param:
        # Push the cursor into the request so the provider filters. The
        # authoritative Incremental is still the one the library applies via
        # apply_hints; this declaration only supplies the parameter binding.
        params[start_param] = {
            "type": "incremental",
            **_incremental_kwargs(incremental),
        }
    if params:
        endpoint["params"] = params

    # `write_disposition` is stated, never left out. `dlt.resource` defaults the
    # hint to "append", not to nothing, so the landing layer's own "unset means
    # merge" rule never gets a chance to fire — measured as run 2 appending a
    # second copy of every record it re-fetched, with the merge key correctly
    # configured and no error anywhere.
    entry = {"name": name, "write_disposition": spec.get("write_disposition", "merge")}
    if spec.get("primary_key") is not None:
        entry["primary_key"] = spec["primary_key"]
    entry["endpoint"] = endpoint
    return entry


def _incremental_kwargs(incremental):
    kwargs = {key: incremental[key] for key in INCREMENTAL_KEYS if key in incremental}
    func = incremental.get("last_value_func")
    if func:
        kwargs["last_value_func"] = LAST_VALUE_FUNCS[func]
    return kwargs


def _auth_config(auth, credentials):
    """dlt auth config for a token the auth backend produced, or None."""
    token = _token(credentials)
    spec = dict(auth or {})
    kind = spec.get("type") or ("bearer" if token else None)
    if kind is None:
        return None
    if not token:
        raise AuthError(
            f"this binding declares {kind!r} auth but the Connection's auth "
            f"backend returned no credentials."
        )
    if kind == "api_key":
        return {
            "type": "api_key",
            "api_key": token,
            "name": spec.get("name", "Authorization"),
            "location": spec.get("location", "header"),
        }
    if kind == "http_basic":
        return {
            "type": "http_basic",
            "username": spec.get("username", ""),
            "password": token,
        }
    return {"type": "bearer", "token": token}


# Where a credential object may keep the token. Ordered: an OAuth backend
# returns `access_token`, a static one usually just `token`.
_TOKEN_KEYS = ("access_token", "token", "api_key")


def _token(credentials):
    if credentials is None:
        return None
    if isinstance(credentials, str):
        return credentials
    for key in _TOKEN_KEYS:
        value = (
            credentials.get(key)
            if isinstance(credentials, dict)
            else getattr(credentials, key, None)
        )
        if value:
            return value
    return None


# --- error readability -----------------------------------------------------


def _readable_validation_error(exc, order):
    """Name the JSON path that is wrong, not the union that failed to match.

    dlt reports the *outermost* failure — ``resources[0]`` "expects one of str,
    EndpointResource, DltResource" — and buries the actual problem several
    nested exceptions down. Unpicked, that message tells the person editing the
    Binding nothing at all.
    """
    leaf = _deepest(exc)
    return (
        f"rest source config is invalid at {_humanize_path(leaf.path, order)!r}: "
        f"{leaf.msg}"
    )


def _deepest(exc):
    """The nested validation failure with the most specific path."""
    best = exc
    for nested in exc.nested_exceptions or ():
        candidate = _deepest(nested)
        if len(candidate.path) > len(best.path):
            best = candidate
    return best


def _humanize_path(path, order):
    """``./resources[0]/endpoint`` -> ``resources.items.endpoint``."""
    text = (path or "").lstrip("./").replace("/", ".")
    for index, name in enumerate(order):
        text = text.replace(f"resources[{index}]", f"resources.{name}")
    return text or "(root)"
