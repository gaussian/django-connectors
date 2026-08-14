"""Salesforce credential acquisition, and the classification of its refusals.

Two server-to-server OAuth2 grants:

**JWT bearer** (``flow="jwt_bearer"``, the default) is the right shape for a
backend connector. There is no refresh token to rot, nothing to re-consent after
a password reset, and no browser round trip in a Celery worker: the Connected
App's certificate *is* the credential, and every Run mints a short-lived access
token from it. The private key is resolved through the configured SecretStore
under ``Connection.auth_reference`` — never from ``Connection.metadata``, which
is rendered in the admin and returned by the API.

**Refresh token** (``flow="refresh_token"``) exists for orgs that already
completed a user-delegated web-server flow elsewhere and only need the grant
exchanged. It is second-class on purpose: a refresh token dies when the user's
password changes, when an admin revokes the app, or when the org's session
policy expires it, and none of those are things a scheduled connector can fix.

Three things here are load-bearing:

``instance_url`` comes from the token response and is never derived
    every org lives on its own My Domain host, sandboxes on another, and
    Salesforce migrates orgs between instances without asking. A base URL
    guessed from the login host works right up until the org is moved, at which
    point every request 404s or — worse — reaches a *different* org's host and
    is refused as an invalid session, which reads exactly like revocation.

``aud`` is the login *service*, not the login *URL*
    Salesforce validates the assertion's audience against
    ``https://login.salesforce.com`` / ``https://test.salesforce.com``, so an
    org that signs in through its own My Domain still asserts the generic
    audience. Sending the My Domain URL as ``aud`` fails with ``invalid_grant``
    and an error description that names nothing useful.

the assertion is signed by ``cryptography``, never by hand
    an RS256 signer with the wrong padding produces assertions anybody holding
    the *public* certificate can forge, and nothing about that failure is
    visible until it is exploited. A missing ``cryptography`` is reported as a
    ConfigurationError naming the extra, so it surfaces at ``manage.py check``
    rather than as a bare ImportError inside a customer's Run.

This module also owns the classification of Salesforce's *API* errors
(:func:`parse_api_errors`, :func:`credential_error_for`), which the source
imports. Whether a 401 means "revoked" is a statement about the credential, not
about the query that happened to be in flight, so it belongs here.

Unverified against a live provider
----------------------------------
No Salesforce org, Connected App, certificate or sandbox was available while
this was written; every test drives an in-process HTTP server replaying the
documented payload shapes. Specifically **not** exercised: real JWT-bearer
pre-authorisation (the "user hasn't approved this consumer" state an admin
clears in Setup), a real refresh-token grant, the exact
``error``/``error_description`` pairs a live org returns for a revoked, locked
or expired grant, clock-skew rejection of an assertion, an org's own session
timeout policy, and ``INVALID_SESSION_ID`` arriving mid-run rather than on the
first request.
"""

import base64
import json
import time
from typing import ClassVar
from urllib.parse import urlsplit

from django_connectors.auth.base import AuthBackend, Credentials
from django_connectors.errors import scrub
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    ConnectorError,
    CredentialsExpired,
    CredentialsRevoked,
)

PRODUCTION_LOGIN_URL = "https://login.salesforce.com"
SANDBOX_LOGIN_URL = "https://test.salesforce.com"

TOKEN_PATH = "/services/oauth2/token"
#: Unversioned, so it works on every org and needs no API version to probe with.
VERSIONS_PATH = "/services/data/"

JWT_BEARER_GRANT = "urn:ietf:params:oauth:grant-type:jwt-bearer"
REFRESH_TOKEN_GRANT = "refresh_token"

FLOWS = frozenset({"jwt_bearer", "refresh_token"})

# Salesforce rejects an assertion whose `exp` is further out than a few minutes,
# and there is nothing to gain from a long-lived one: the assertion is exchanged
# immediately and never stored.
ASSERTION_LIFETIME_SECONDS = 180

REQUEST_TIMEOUT_SECONDS = 30

# OAuth `error` values meaning the grant is gone and no retry recovers it. The
# runner turns these into a blocked Binding rather than a schedule that spends
# the org's API allocation on a 400 every fifteen minutes.
#
# `invalid_grant` is the broad one: Salesforce returns it for an unapproved
# Connected App, a deactivated user, a revoked token *and* — the known false
# positive — an assertion rejected for clock skew. Treating skew as revocation
# takes the Connection out of service until a human looks, which is the right
# direction to be wrong in; the alternative retries a broken clock forever.
REVOKED_OAUTH_ERRORS = frozenset(
    {
        "invalid_grant",
        "inactive_user",
        "inactive_org",
        "access_denied",
    }
)

# Wrong Connected App, wrong secret, wrong grant type. Deliberately *not*
# CredentialsRevoked: nothing was ever granted, so "revoked" would mislead the
# operator into re-authorising instead of fixing the consumer key.
CONFIGURATION_OAUTH_ERRORS = frozenset(
    {
        "invalid_client",
        "invalid_client_id",
        "invalid_client_credentials",
        "unsupported_grant_type",
        "invalid_request",
    }
)

# Salesforce answers 401 with this the moment a session stops being usable —
# admin revoked the app, the user was deactivated, the org's session policy
# killed it. See `credential_error_for` for why it maps to revocation.
SESSION_ERROR_CODES = frozenset({"INVALID_SESSION_ID"})

# The daily API allocation. Lives here rather than in the source because it is
# part of the same status-code classification table, and the source imports it.
REQUEST_LIMIT_ERROR_CODE = "REQUEST_LIMIT_EXCEEDED"

# How much of an unparseable error body is worth keeping. Salesforce fronts orgs
# with proxies that answer HTML, and an entire error page in Run.error_message
# buries every other field in the admin.
ERROR_SNIPPET_LENGTH = 300


class SalesforceBackend(AuthBackend):
    """OAuth2 JWT-bearer (or refresh-token) credentials for one Salesforce org.

    Returns a :class:`~django_connectors.auth.base.Credentials` carrying
    ``access_token`` and ``instance_url`` — both are needed, and a source that
    got only the token would have nowhere to send it.

    Configuration is split by sensitivity, and the split is the point:

    ``Connection.metadata`` / ``Connection.auth_metadata``
        non-secret settings — ``flow``, ``client_id``, ``username``,
        ``sandbox``, ``login_url``, ``audience``, ``scope``.

    the SecretStore, under ``Connection.auth_reference``
        a dict (or a JSON string) holding ``private_key`` and optionally
        ``private_key_passphrase``, ``refresh_token``, ``client_secret``. It may
        also carry any of the non-secret settings, and those win — the store is
        what an operator rotates, so it must be able to correct a stale
        ``client_id`` without a second edit somewhere else.

    No handshake, hence no :meth:`begin_setup`: the JWT-bearer flow is
    authorised once in Salesforce Setup, by uploading the certificate to the
    Connected App and pre-authorising the profile. There is nothing for this
    library to redirect a browser to.

    Nothing is cached. A fresh assertion per Run costs one extra request and
    means no bearer token is ever written to the database; it also makes a
    mid-run ``INVALID_SESSION_ID`` genuinely mean "the session was killed"
    rather than "the token we cached went stale", which is what lets the
    classification below be as decisive as it is.
    """

    key = "salesforce"
    provider = "salesforce"
    required_extras: ClassVar[dict[str, str]] = {"cryptography": "secrets"}

    #: Opt-in escape hatch for reaching a private address, matching
    #: ``RestSource.allow_private_addresses``. Deliberately a class attribute
    #: rather than a Connection setting: the party editing a Connection is
    #: exactly the party the guard exists to constrain, so only a host
    #: registering a subclass can lift it.
    @property
    def allow_private_addresses(self):
        from django_connectors.conf import conf

        return conf.REST_ALLOW_PRIVATE_ADDRESSES

    # --- credentials -------------------------------------------------------

    def get_credentials(self, connection):
        settings = self.resolve_settings(connection)
        if settings["flow"] == REFRESH_TOKEN_GRANT:
            data = _refresh_token_request(settings)
        else:
            data = _jwt_bearer_request(settings)

        payload = self._post_token(settings, data)

        access_token = payload.get("access_token")
        if not access_token:
            raise AuthError(
                "the Salesforce token endpoint answered 200 with no "
                "access_token; the response was not a token grant."
            )
        instance_url = payload.get("instance_url") or settings.get("instance_url")
        if not instance_url:
            raise AuthError(
                "the Salesforce token response carried no instance_url, and "
                "none is configured. Every org has its own host and it cannot "
                "be derived from the login URL, so there is nowhere to send "
                "API requests."
            )

        return Credentials(
            provider=self.provider,
            access_token=access_token,
            instance_url=self._assert_url(str(instance_url), "instance_url"),
            token_type=payload.get("token_type") or "Bearer",
            issued_at=payload.get("issued_at") or "",
            scope=payload.get("scope") or "",
        )

    def resolve_settings(self, connection):
        """Merge the non-secret settings with the stored payload.

        The store wins. It is the copy an operator rotates, and a rotation that
        could be silently overridden by a stale value in a JSONField rendered in
        the admin is not a rotation.
        """
        metadata = {
            **(connection.auth_metadata or {}),
            **(connection.metadata or {}),
            **self._stored(connection),
        }

        flow = metadata.get("flow") or (
            REFRESH_TOKEN_GRANT if metadata.get("refresh_token") else "jwt_bearer"
        )
        if flow not in FLOWS:
            raise ConfigurationError(
                f"unknown Salesforce auth flow {flow!r}; supported: {sorted(FLOWS)}."
            )
        metadata["flow"] = flow

        sandbox = bool(metadata.get("sandbox"))
        login_url = metadata.get("login_url") or (
            SANDBOX_LOGIN_URL if sandbox else PRODUCTION_LOGIN_URL
        )
        metadata["login_url"] = self._assert_url(str(login_url), "login_url")
        # Not login_url: Salesforce validates `aud` against the login *service*,
        # so a My Domain sign-in host still asserts the generic audience.
        metadata["audience"] = metadata.get("audience") or (
            SANDBOX_LOGIN_URL if sandbox else PRODUCTION_LOGIN_URL
        )
        return metadata

    def _stored(self, connection):
        """The SecretStore payload for this Connection, as a dict."""
        reference = connection.auth_reference
        if not reference:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no auth_reference, so there is no key to look up. Set "
                f"it to the SecretStore key holding "
                f"{{'private_key': '-----BEGIN PRIVATE KEY-----…'}}."
            )

        value = self.store.get(connection, reference)
        if value is None or value == "":
            # Absent reads as revoked, exactly as StaticCredentialsBackend does:
            # the normal way a Salesforce integration ends is that someone
            # deleted the key. Retrying an empty credential only spends the
            # org's API allocation on 400s.
            raise CredentialsRevoked(
                f"no Salesforce credential is stored for connection "
                f"{connection.id} under auth_reference {reference!r} "
                f"({self.store})"
            )

        if isinstance(value, str):
            value = _parse_stored_string(value)
        if not isinstance(value, dict):
            raise ConfigurationError(
                f"the stored Salesforce credential for connection "
                f"{connection.id} is a {type(value).__name__}; it must be a "
                f"mapping (or a JSON object) with at least 'private_key' or "
                f"'refresh_token'."
            )
        return value

    # --- provider calls ----------------------------------------------------

    def _post_token(self, settings, data):
        session = self._session()
        url = settings["login_url"] + TOKEN_PATH
        try:
            response = session.post(
                url,
                data=data,
                headers={"Accept": "application/json"},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except ConnectorError:
            # The address guard fires from inside `send` and already says
            # exactly what is wrong; relabelling it as a transport failure
            # would hide an SSRF refusal behind "could not be reached".
            raise
        except Exception as exc:
            # A network blip must never read as revocation — the runner would
            # take a healthy Connection out of service for a DNS hiccup.
            raise AuthError(
                f"the Salesforce token endpoint could not be reached: {scrub(exc)}"
            ) from exc

        payload = _json_object(response)
        if response.status_code >= 400 or payload.get("error"):
            raise oauth_error_for(response.status_code, payload)
        return payload

    def test(self, connection):
        """Mint a token and spend one request proving the org accepts it.

        The default ``AuthBackend.test`` only proves a credential *resolves*,
        and for a JWT-bearer grant that is nearly the whole question — but not
        all of it: an org can hand back a token whose user has API access
        disabled, and "test connection" is read as a claim about the org.
        """
        credentials = self.get_credentials(connection)
        session = self._session()
        url = credentials["instance_url"] + VERSIONS_PATH
        try:
            response = session.get(
                url,
                headers=bearer_headers(credentials["access_token"]),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise AuthError(
                f"could not reach the Salesforce org: {scrub(exc)}"
            ) from exc

        error = credential_error_for(response)
        if error is not None:
            raise error
        if response.status_code >= 400:
            raise AuthError(
                f"the Salesforce org answered HTTP {response.status_code} to a "
                f"version listing; the token was minted but is not usable."
            )
        try:
            versions = response.json()
        except ValueError:
            versions = []
        count = len(versions) if isinstance(versions, list) else 0
        return f"ok ({count} API versions available)"

    def revoke(self, connection):
        """Mark the Connection revoked. Deliberately local.

        Nothing is called at Salesforce and nothing is deleted from the store.
        A JWT-bearer grant has no provider-side token to revoke — the trust is
        the certificate on the Connected App, which an admin removes in Setup —
        and the private key in the store is very often shared by every
        Connection into the same org, so deleting it here would revoke all of
        them. Saying so is better than doing half of it.
        """
        return super().revoke(connection)

    def health(self, connection):
        """Non-secret facts only: what is configured, never what it holds."""
        try:
            settings = self.resolve_settings(connection)
        except ConnectorError as exc:
            return {
                "configured": False,
                "problem": type(exc).__name__,
                "status": connection.status,
            }
        return {
            "configured": True,
            "flow": settings["flow"],
            "login_url": settings["login_url"],
            "audience": settings["audience"],
            "username": settings.get("username", ""),
            "client_id_set": bool(settings.get("client_id")),
            "private_key_present": bool(settings.get("private_key")),
            "refresh_token_present": bool(settings.get("refresh_token")),
            "status": connection.status,
        }

    # --- transport ---------------------------------------------------------

    def _session(self):
        """dlt's retrying session, with the address guard on every hop.

        Reused from the REST source rather than reimplemented: it is the same
        guard, already tested, and it re-checks redirects — which matters here
        because ``login_url`` is host-editable and a 302 defeats any check made
        once at configuration time.
        """
        from django_connectors.sources.rest import guarded_session

        return guarded_session(allow_private=self.allow_private_addresses)

    def _assert_url(self, url, label):
        from django_connectors.sources.rest import assert_safe_url

        allow_private = self.allow_private_addresses
        assert_safe_url(url, allow_private=allow_private)
        if not allow_private and urlsplit(url).scheme != "https":
            raise ConfigurationError(
                f"Salesforce {label} must be https, got {url!r}. Every request "
                f"carries a bearer token, and a bearer token sent over http is "
                f"a bearer token given away."
            )
        return url.rstrip("/")


# --- grant construction ----------------------------------------------------


def _jwt_bearer_request(settings):
    missing = [
        name
        for name in ("client_id", "username", "private_key")
        if not settings.get(name)
    ]
    if missing:
        raise ConfigurationError(
            f"the Salesforce jwt_bearer flow needs {missing} and they are not "
            f"set. 'client_id' is the Connected App's consumer key and "
            f"'username' the integration user it acts as; both may live in "
            f"Connection.metadata. 'private_key' is the PEM matching the "
            f"certificate uploaded to the Connected App and must come from the "
            f"SecretStore."
        )
    assertion = build_assertion(
        client_id=settings["client_id"],
        username=settings["username"],
        audience=settings["audience"],
        private_key=settings["private_key"],
        passphrase=settings.get("private_key_passphrase"),
    )
    return {"grant_type": JWT_BEARER_GRANT, "assertion": assertion}


def _refresh_token_request(settings):
    missing = [
        name for name in ("client_id", "refresh_token") if not settings.get(name)
    ]
    if missing:
        raise ConfigurationError(
            f"the Salesforce refresh_token flow needs {missing} and they are not set."
        )
    data = {
        "grant_type": REFRESH_TOKEN_GRANT,
        "client_id": settings["client_id"],
        "refresh_token": settings["refresh_token"],
    }
    # Absent for a Connected App configured as a public client (PKCE); sending
    # an empty one is rejected outright, so it is omitted rather than blanked.
    if settings.get("client_secret"):
        data["client_secret"] = settings["client_secret"]
    return data


def build_assertion(
    *,
    client_id,
    username,
    audience,
    private_key,
    passphrase=None,
    lifetime_seconds=ASSERTION_LIFETIME_SECONDS,
    issued_at=None,
):
    """Return the signed RS256 JWT for the ``urn:…:jwt-bearer`` grant.

    Claims are exactly the four Salesforce validates. Extra claims are not
    harmless: an ``nbf`` a second in the future, or a ``jti`` replayed inside
    the assertion window, both fail as ``invalid_grant`` — which this module
    classifies as revocation.
    """
    now = int(time.time() if issued_at is None else issued_at)
    header = {"alg": "RS256", "typ": "JWT"}
    claims = {
        "iss": client_id,
        "sub": username,
        "aud": audience,
        "exp": now + int(lifetime_seconds),
    }
    signing_input = b".".join((_segment(header), _segment(claims)))
    signature = _sign_rs256(signing_input, private_key, passphrase)
    return b".".join((signing_input, _b64url(signature))).decode("ascii")


def _segment(payload):
    # Compact separators and sorted keys: a JWT segment is signed bytes, so its
    # serialization has to be deterministic rather than merely equivalent.
    return _b64url(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )


def _b64url(data):
    """base64url with the padding stripped, as JWS requires."""
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def _sign_rs256(signing_input, private_key, passphrase):
    """Sign with ``cryptography``. Never reimplement this.

    RS256 is RSASSA-PKCS1-v1_5 over SHA-256. Getting the padding wrong yields
    signatures that verify against the *public* certificate under a different
    scheme, which is to say forgeable by anyone who can read the Connected
    App's certificate — and nothing about it fails visibly first.
    """
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding, rsa
    except ImportError as exc:
        raise ConfigurationError(
            "signing a Salesforce JWT assertion needs the 'cryptography' "
            "package, which is not installed: "
            "pip install 'django-connectors[secrets]'."
        ) from exc

    pem = private_key.encode("utf-8") if isinstance(private_key, str) else private_key
    password = passphrase.encode("utf-8") if isinstance(passphrase, str) else passphrase
    try:
        key = serialization.load_pem_private_key(pem, password=password or None)
    except (TypeError, ValueError):
        # `from None`, and no interpolation of the exception: the messages
        # cryptography raises here are generic, and chaining one into a stored
        # Run.error_message is a well-trodden route for key material to reach
        # the admin.
        raise ConfigurationError(
            "the stored Salesforce private key could not be loaded as PEM. It "
            "is not PEM, or it is encrypted and 'private_key_passphrase' is "
            "missing or wrong."
        ) from None

    if not isinstance(key, rsa.RSAPrivateKey):
        raise ConfigurationError(
            f"Salesforce JWT assertions are RS256 and must be signed with an "
            f"RSA key; the stored key is {type(key).__name__}."
        )
    return key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())


def _parse_stored_string(value):
    """Interpret a SecretStore value that came back as a string."""
    text = value.strip()
    if text.startswith("{"):
        try:
            return json.loads(text)
        except ValueError as exc:
            raise ConfigurationError(
                f"the stored Salesforce credential looks like JSON but does "
                f"not parse: {exc.__class__.__name__}. Store a mapping, or a "
                f"bare PEM private key."
            ) from None
    if "-----BEGIN" in text:
        return {"private_key": text}
    raise ConfigurationError(
        "the stored Salesforce credential is a bare string that is neither a "
        "JSON object nor a PEM private key. Store "
        "{'private_key': '-----BEGIN PRIVATE KEY-----…'} — a bare refresh "
        "token is refused because it is indistinguishable from a typo."
    )


# --- error classification --------------------------------------------------


def bearer_headers(access_token, extra=None):
    """Standard headers for a Salesforce REST call."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def oauth_error_for(status_code, payload):
    """Classify a token-endpoint refusal into the runner's vocabulary.

    Returns the exception to raise; the caller raises it so the traceback
    starts at the request rather than in here.
    """
    error = str(payload.get("error") or "").strip().lower()
    description = str(payload.get("error_description") or "").strip()
    detail = scrub(f"{error or status_code}: {description}".strip(": "))

    # Checked before the revoked set on purpose: Salesforce reports an expired
    # refresh token as `invalid_grant` with "expired access/refresh token" in
    # the description, and CredentialsExpired says something a human can act on
    # ("re-authorise") where CredentialsRevoked would not.
    if "expired" in error or "expired" in description.lower():
        return CredentialsExpired(
            f"Salesforce refused the grant as expired ({detail}). "
            f"Re-authorise the Connection."
        )
    if error in REVOKED_OAUTH_ERRORS:
        return CredentialsRevoked(
            f"Salesforce refused the grant ({detail}). For the jwt_bearer flow "
            f"this is usually the Connected App not being pre-authorised for "
            f"the integration user's profile, the user being deactivated, or "
            f"the certificate no longer matching the stored private key."
        )
    if error in CONFIGURATION_OAUTH_ERRORS:
        return ConfigurationError(
            f"Salesforce rejected the token request as malformed ({detail}). "
            f"Check the Connected App's consumer key, the flow, and whether "
            f"this app requires a client secret."
        )
    return AuthError(f"the Salesforce token endpoint answered {detail}.")


def parse_api_errors(response):
    """Salesforce's REST error body as ``[{"errorCode": …, "message": …}]``.

    Never raises. Salesforce answers almost every REST failure with a JSON
    *array* of error objects, but not all of them: OAuth endpoints answer with a
    single object, and a proxy in front of the org answers with HTML. A parser
    that raised on the last case would turn "the provider said no" into "the
    connector crashed", losing the status code that was the only usable fact.
    """
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        return [{"errorCode": "", "message": _snippet(response)}]

    errors = [
        {
            "errorCode": str(entry.get("errorCode") or entry.get("error") or ""),
            "message": str(
                entry.get("message") or entry.get("error_description") or ""
            ),
        }
        for entry in payload
        if isinstance(entry, dict)
    ]
    return errors or [{"errorCode": "", "message": _snippet(response)}]


def error_codes(errors):
    """Upper-cased ``errorCode`` values from :func:`parse_api_errors`."""
    return {str(entry.get("errorCode") or "").upper() for entry in errors} - {""}


def describe_errors(errors):
    """A scrubbed one-line rendering of a parsed error list."""
    parts = [
        f"{entry.get('errorCode', '')} {entry.get('message', '')}".strip()
        for entry in errors
    ]
    return scrub("; ".join(part for part in parts if part)) or "(no detail)"


def credential_error_for(response, errors=None):
    """A credential-shaped exception for `response`, or ``None``.

    ``None`` means "not a credential problem" — the caller then decides, and
    the source turns it into a SourceError. Only 401 and 403 are candidates:
    Salesforce reports a bad SOQL query as 400 and a throttle as 403/503, and
    misreading either as revocation would block the Connection for a typo.

    ``INVALID_SESSION_ID`` maps to :class:`CredentialsRevoked`, not
    :class:`CredentialsExpired`, and that is a deliberate consequence of this
    backend never caching a token: the access token in flight was minted at the
    start of this Run, so a session that is already invalid was killed at the
    provider — the app was revoked, the user deactivated, or the org's IP/session
    policy rejected it. The known false positive is a Run that outlives the
    org's session timeout, which for a multi-hour sync is possible; a blocked
    Binding and a visible error is the better failure of the two available.
    """
    status = response.status_code
    if status not in (401, 403):
        return None

    errors = parse_api_errors(response) if errors is None else errors
    codes = error_codes(errors)
    detail = describe_errors(errors)

    if codes & SESSION_ERROR_CODES:
        return CredentialsRevoked(
            f"Salesforce rejected the session with HTTP {status} "
            f"INVALID_SESSION_ID ({detail}). The token was minted for this run, "
            f"so the org refused it outright: the Connected App was revoked, "
            f"the integration user was deactivated, or an org policy blocked "
            f"the session."
        )
    if REQUEST_LIMIT_ERROR_CODE in codes:
        # The API allocation, not the credential. The source raises its own,
        # much more specific, non-retrying error for this.
        return None
    return AuthError(
        f"Salesforce refused the request with HTTP {status} ({detail}). The "
        f"credential is valid but this user or org is not permitted to do this."
    )


def _json_object(response):
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _snippet(response):
    try:
        text = response.text or ""
    except Exception:
        return ""
    return scrub(text[:ERROR_SNIPPET_LENGTH])
