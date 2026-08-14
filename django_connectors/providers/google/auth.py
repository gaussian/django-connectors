"""Google Workspace credentials, and the HTTP seam every Google source shares.

Two things live here because they are the same decision seen from two sides.
:class:`GoogleWorkspaceBackend` turns a service-account key into a bearer token
and classifies the failures of that exchange; :func:`raise_for_google_error`
classifies the failures of *using* that token. Both must sort provider
responses into the same three buckets, because the runner acts on them
differently:

``CredentialsRevoked`` / ``CredentialsExpired``
    ``services.runs`` marks the Connection revoked and blocks its Bindings.
    Correct for ``invalid_grant`` and for HTTP 401 — no retry recovers either,
    and retrying burns quota against an account that has already said no.

``AuthError`` / ``SourceError``
    retried on the Binding's normal schedule. Correct for HTTP 403
    ``insufficientPermissions``: a scope that has not been granted *yet* is a
    configuration problem an administrator fixes without re-authorising, and
    taking the whole Connection out of service for it would be an overreaction.

Throttling is the third bucket and is not an error at all. Google reports it as
HTTP 429 *or* as HTTP 403 with a rate-limit ``reason``, so a source that only
looks at the status code treats a quota blip as a permissions failure and stops
syncing. :func:`google_request` handles both, honours ``Retry-After``, and
retries with exponential backoff.

**Unverified against a live provider.** Everything here was exercised against
mocked HTTP only. Not covered: a real domain-wide-delegation consent grant, a
real ``invalid_grant`` from a deleted service account or a revoked delegation,
real quota exhaustion, and google-auth's own clock-skew handling against
Google's token endpoint.
"""

import json
import logging
import time
from typing import ClassVar

from django_connectors.auth.base import AuthBackend
from django_connectors.errors import scrub
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
    SourceError,
)

logger = logging.getLogger(__name__)

#: The extra that installs ``google-auth``.
GOOGLE_EXTRA = "google"

#: Read-only by default, and deliberately so: a connector that ingests data has
#: no reason to hold a scope that can send mail or rewrite a spreadsheet, and a
#: service account with domain-wide delegation applies these scopes to *every*
#: mailbox in the domain. A host that needs more must say so in
#: ``Connection.metadata["scopes"]`` or subclass the backend.
DEFAULT_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
)

# OAuth 2 error codes that mean "this grant is gone". `invalid_grant` covers the
# lot in practice: a deleted service account, a revoked domain-wide delegation,
# an impersonated user who no longer exists, and a refresh token the user
# withdrew all surface as exactly that.
REVOKED_OAUTH_ERRORS = frozenset(
    {
        "invalid_grant",
        "invalid_client",
        "unauthorized_client",
        "access_denied",
        "invalid_rapt",
    }
)

# `reason` values Google returns *inside a 403 body* for quota, not permission.
# Missing one of these turns a survivable throttle into a permanent stop.
# Both vocabularies appear in the wild: the classic per-error `reason`, and the
# newer top-level `status` enum that the same APIs use inconsistently.
RATE_LIMIT_REASONS = frozenset(
    {
        "rateLimitExceeded",
        "userRateLimitExceeded",
        "quotaExceeded",
        "dailyLimitExceeded",
        "backendError",
        "RESOURCE_EXHAUSTED",
        "UNAVAILABLE",
    }
)

# Statuses dlt's retry client may retry on its own. 429 is absent on purpose:
# it is handled here so that `Retry-After` and the 403-with-a-rate-limit-reason
# case go through one code path, and so a test can count attempts.
RETRYABLE_STATUS_CODES = (500, 502, 503, 504)

MAX_THROTTLE_ATTEMPTS = 5
#: Cap on a single honoured ``Retry-After``. Google occasionally answers a
#: daily-quota exhaustion with hours; sleeping that inside a Run holds the
#: Binding's lease until it expires and the Run is reaped as abandoned.
MAX_RETRY_AFTER_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 60


def wait_before_retry(seconds):
    """Pause between throttled attempts.

    A module-level function rather than an inline ``time.sleep`` so that tests
    can replace it and assert on the delays that *would* have been taken — a
    backoff test that actually sleeps is a test nobody runs.
    """
    time.sleep(seconds)


# --- credential shapes -----------------------------------------------------


def bearer_token(credentials):
    """The OAuth access token to send, from whatever the auth backend returned.

    Three shapes reach a Google source and all three are legitimate, which is
    why this is a function rather than an assumption:

    * a plain ``str`` — a static token from ``StaticCredentialsBackend``;
    * a ``Mapping`` — :class:`~django_connectors.auth.base.Credentials`, which
      is what ``AllauthBackend`` returns for a user-delegated grant;
    * a ``google.auth`` credentials object — what
      :class:`GoogleWorkspaceBackend` returns, and the only one that can renew
      itself mid-Run.

    The third case is the reason this is called per request rather than once at
    build time: a service-account token lives an hour, a full Gmail sync can
    outlast that, and a token captured at ``build_source`` time would start
    returning 401 halfway through — which the runner would then report as a
    revoked Connection.
    """
    if credentials is None:
        raise AuthError(
            "this Google source needs credentials, but the Connection's auth "
            "backend returned none. Set Connection.auth_backend to a backend "
            "that yields a Google access token (google_workspace, allauth, or "
            "static)."
        )

    if isinstance(credentials, str):
        return credentials

    # A live google-auth credential first: it is also a Mapping-free object, and
    # refreshing it is the whole point of accepting it.
    if hasattr(credentials, "refresh") and hasattr(credentials, "token"):
        _refresh_if_needed(credentials)
        token = credentials.token
        if not token:
            raise CredentialsExpired(
                "the Google credentials object holds no access token after a "
                "refresh attempt."
            )
        return token

    for key in ("access_token", "token", "api_key"):
        value = (
            credentials.get(key)
            if hasattr(credentials, "get")
            else getattr(credentials, key, None)
        )
        if value:
            return value

    raise AuthError(
        f"could not find an access token on the credentials the auth backend "
        f"returned ({type(credentials).__name__}). Expected a string, a mapping "
        f"with 'access_token', or a google.auth credentials object."
    )


def _refresh_if_needed(credentials):
    """Refresh a google-auth credential whose token is missing or stale."""
    if credentials.token and getattr(credentials, "valid", False):
        return credentials
    try:
        from google.auth.transport.requests import Request
    except ImportError as exc:
        raise _missing_google_auth() from exc

    try:
        credentials.refresh(Request())
    except Exception as exc:
        raise classify_refresh_error(exc) from exc
    return credentials


def classify_refresh_error(exc):
    """Map a google-auth failure onto this package's exception hierarchy.

    Returns the exception to raise; the caller raises it so the ``from exc``
    chain stays honest. The distinction is the entire contract of
    :mod:`django_connectors.auth.base`: report a network blip as
    ``CredentialsRevoked`` and a healthy Connection is taken out of service
    until a human notices, while reporting a genuine revocation as anything
    else gets the Binding retried against a 401 forever.
    """
    text = str(exc)
    lowered = text.lower()
    detail = scrub(text)

    # google-auth puts the parsed error body in args[1] when it has one.
    code = ""
    for arg in getattr(exc, "args", ()):
        if isinstance(arg, dict):
            code = str(arg.get("error") or "")
            break
    if not code:
        code = next((name for name in REVOKED_OAUTH_ERRORS if name in lowered), "")

    if code in REVOKED_OAUTH_ERRORS:
        return CredentialsRevoked(
            f"Google rejected this service account grant ({code}): {detail}. "
            f"The key was deleted, the service account was disabled, or "
            f"domain-wide delegation for the impersonated user was withdrawn. "
            f"No retry recovers this."
        )
    if "expired" in lowered or "stale" in lowered:
        return CredentialsExpired(
            f"the Google credential could not be renewed: {detail}"
        )

    transport = _google_transport_error()
    if transport is not None and isinstance(exc, transport):
        # A DNS failure or a 503 from oauth2.googleapis.com. Transient, and
        # emphatically not a reason to revoke a Connection.
        return AuthError(f"could not reach Google's token endpoint: {detail}")
    return AuthError(f"the Google token exchange failed: {detail}")


def _google_transport_error():
    try:
        from google.auth.exceptions import TransportError
    except ImportError:
        return None
    return TransportError


def _missing_google_auth():
    return ConfigurationError(
        f"google-auth is not installed, so a Google service-account token "
        f"cannot be signed. pip install 'django-connectors[{GOOGLE_EXTRA}]'. "
        f"Signing the JWT by hand is not an option: getting the assertion's "
        f"audience, expiry or signature subtly wrong fails as an opaque "
        f"'invalid_grant'."
    )


def service_account_info(value):
    """Normalize whatever the SecretStore returned into a key-file dict.

    Stores differ: a JSON column hands back a ``dict``, Vault and AWS Secrets
    Manager hand back the raw string that was written. Accepting only one of
    those makes the backend work in development and fail in production.
    """
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise ConfigurationError(
                f"the stored Google credential is not valid JSON: {exc}. It must "
                f"be the service-account key file, verbatim."
            ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"the stored Google credential must be the service-account key JSON "
            f"(an object), got {type(value).__name__}."
        )
    missing = sorted({"client_email", "private_key", "token_uri"} - set(value))
    if missing:
        raise ConfigurationError(
            f"the stored Google service-account key is missing {missing}. "
            f"Store the key file exactly as Google issued it."
        )
    return value


# --- the HTTP seam ---------------------------------------------------------


def google_client(*, base_url, credentials):
    """A ``RESTClient`` for one Google API host, authorized per request.

    ``base_url`` is a class attribute of the source, never a Binding config key.
    A customer-supplied base URL would not merely be an SSRF hole — it would
    forward a Workspace-wide OAuth bearer token to a host of the customer's
    choosing, which is credential exfiltration with extra steps. Redirecting a
    source at a test server is therefore a deliberate subclass registration,
    exactly as ``RestSource.allow_private_addresses`` is.
    """
    from dlt.sources.helpers.requests.retry import Client
    from dlt.sources.helpers.rest_client import RESTClient

    # dlt's retry client, so transport failures behave the way they do
    # everywhere else in this package. `raise_for_status=False` because every
    # status is classified here; 429 is excluded from its retry list so that
    # throttling goes through `google_request` alone.
    session = Client(
        raise_for_status=False,
        status_codes=RETRYABLE_STATUS_CODES,
        request_timeout=REQUEST_TIMEOUT_SECONDS,
    ).session

    def authorize(request):
        # Called by requests for every attempt, including retries, so a token
        # that expired mid-run is renewed rather than replayed.
        request.headers["Authorization"] = f"Bearer {bearer_token(credentials)}"
        return request

    return RESTClient(base_url=base_url, session=session, auth=authorize)


def google_request(client, path, *, params=None, method="GET", json_body=None):
    """One request, with throttling absorbed. Returns the raw ``Response``.

    Nothing is raised for an error status: callers need to see a 404 before it
    becomes an exception (Gmail's expired-history fallback depends on exactly
    that), so classification is a separate, explicit step.
    """
    last = None
    for attempt in range(1, MAX_THROTTLE_ATTEMPTS + 1):
        try:
            response = client.request(
                path, method=method, params=params, json=json_body
            )
        except Exception as exc:
            # dlt's session already retried transport failures; reaching here
            # means it gave up. Never a credential problem.
            raise SourceError(f"the request to Google failed: {scrub(exc)}") from exc

        last = response
        if not _is_throttled(response):
            return response
        if attempt == MAX_THROTTLE_ATTEMPTS:
            break
        delay = _retry_delay(response, attempt)
        logger.info(
            "Google throttled %s (HTTP %s); waiting %ss before attempt %s",
            path,
            response.status_code,
            delay,
            attempt + 1,
        )
        wait_before_retry(delay)

    return last


def _is_throttled(response):
    """Whether `response` is Google saying "slow down" rather than "no".

    403 is the trap. Google uses it for both "you may not do this" and "you
    have done this too often", and the two are distinguishable only by the
    ``reason`` buried in the error body.
    """
    if response.status_code == 429:
        return True
    if response.status_code != 403:
        return False
    return error_reason(response) in RATE_LIMIT_REASONS


def _retry_delay(response, attempt):
    """Seconds to wait: the provider's own figure when it gave one."""
    header = response.headers.get("Retry-After")
    if header:
        try:
            return min(max(float(header), 0.0), MAX_RETRY_AFTER_SECONDS)
        except ValueError:
            # Retry-After may be an HTTP-date. Falling back to the backoff is
            # better than parsing dates for a value we cap anyway.
            pass
    return min(2.0 ** (attempt - 1), MAX_RETRY_AFTER_SECONDS)


def error_payload(response):
    """The JSON error body, or ``{}``. Never raises."""
    try:
        body = response.json()
    except Exception:
        return {}
    if not isinstance(body, dict):
        return {}
    error = body.get("error")
    return error if isinstance(error, dict) else {}


def error_reason(response):
    """Google's machine-readable error reason, e.g. ``rateLimitExceeded``."""
    error = error_payload(response)
    for detail in error.get("errors") or ():
        if isinstance(detail, dict) and detail.get("reason"):
            return detail["reason"]
    status = error.get("status")
    return status if isinstance(status, str) else ""


def error_message(response):
    """A short, scrubbed description of a Google error response."""
    message = error_payload(response).get("message")
    if not message:
        # Truncated hard: an HTML error page from a proxy is not worth storing.
        message = (response.text or "")[:200]
    return scrub(message)


def raise_for_google_error(response, *, what):
    """Turn an error status into the right exception, or return `response`.

    `what` names the call in the message ("listing Gmail messages"), because
    the status code alone tells an operator nothing about which of a dozen
    requests in a Run failed.
    """
    status = response.status_code
    if status < 400:
        return response

    detail = error_message(response)
    reason = error_reason(response)

    if status == 401:
        # Terminal by construction: the token was rejected outright. Retrying
        # spends quota against a credential that has already been withdrawn.
        raise CredentialsRevoked(
            f"Google rejected the access token while {what} (HTTP 401): {detail}. "
            f"The grant was revoked, the service-account key was deleted, or the "
            f"impersonated user no longer exists."
        )
    if status in (403, 429) and (status == 429 or reason in RATE_LIMIT_REASONS):
        # Reached only after `google_request` exhausted its retries, so this is
        # sustained throttling rather than a permissions problem — and calling
        # it AuthError would put a quota outage in the same bucket as a missing
        # scope, which is where operators stop reading.
        raise SourceError(
            f"Google is still throttling after {MAX_THROTTLE_ATTEMPTS} attempts "
            f"while {what} (HTTP {status}, {reason or 'no reason'}): {detail}."
        )
    if status == 403:
        # Not CredentialsRevoked: a missing scope or a disabled API is fixed by
        # an administrator without the user re-authorising anything, so the
        # Connection stays in service and the Binding retries.
        raise AuthError(
            f"Google refused the request while {what} (HTTP 403, {reason or 'no reason'}): "
            f"{detail}. Usually a scope that was never granted, an API that is "
            f"not enabled on the project, or domain-wide delegation missing the "
            f"scope for the impersonated user."
        )
    raise SourceError(f"Google answered HTTP {status} while {what}: {detail}")


def google_json(client, path, *, params=None, method="GET", json_body=None, what):
    """Request, classify, and decode. The normal way to call a Google API."""
    response = google_request(
        client, path, params=params, method=method, json_body=json_body
    )
    raise_for_google_error(response, what=what)
    try:
        payload = response.json()
    except ValueError as exc:
        raise SourceError(
            f"Google returned a non-JSON body while {what} "
            f"(HTTP {response.status_code})."
        ) from exc
    return payload if isinstance(payload, dict) else {}


def paginate(client, path, *, params=None, what, max_pages=10_000):
    """Yield every page of a ``pageToken``-paginated Google collection.

    Google's list methods all share this shape, and every one of them will hand
    back a first page and a ``nextPageToken`` forever if you ignore it. A
    connector that stops after page one lands a plausible-looking partial table
    that nothing reports as wrong.

    ``max_pages`` is a circuit breaker, not a limit anyone should hit: a
    provider bug that echoes the same token back would otherwise loop until the
    Binding's lease expires.
    """
    query = dict(params or {})
    seen_tokens = set()
    for page_number in range(max_pages):
        payload = google_json(client, path, params=query, what=what)
        yield payload

        token = payload.get("nextPageToken")
        if not token:
            return
        if token in seen_tokens:
            raise SourceError(
                f"Google repeated pageToken {token[:12]}… while {what} at page "
                f"{page_number + 1}; refusing to loop."
            )
        seen_tokens.add(token)
        query["pageToken"] = token

    raise SourceError(
        f"stopped after {max_pages} pages while {what}; the collection did not end."
    )


# --- the auth backend ------------------------------------------------------


class GoogleWorkspaceBackend(AuthBackend):
    """A Google service account, optionally impersonating a Workspace user.

    The key file is resolved through the configured SecretStore under
    ``Connection.auth_reference`` and never appears in ``auth_metadata`` —
    ``Connection.clean()`` refuses credential-shaped keys there precisely
    because that field is rendered in the admin and returned by the API, and a
    service-account private key is the most dangerous single string in a
    Workspace deployment: with domain-wide delegation it opens every mailbox in
    the domain.

    Configuration lives in ``Connection.metadata``, which holds no secrets::

        {"subject": "analytics@example.com",
         "scopes": ["https://www.googleapis.com/auth/gmail.readonly"]}

    ``subject`` is the user to impersonate. Without it the credential acts as
    the service account itself, which has no mailbox and no Drive — so a Gmail
    Binding with no ``subject`` fails with a 400 from Gmail rather than
    returning an empty inbox, and that is the better of the two outcomes.

    :meth:`get_credentials` returns the live ``google.auth`` credentials object
    rather than a token string, so that a Run outlasting the token's hour can
    renew it in place. ``repr`` on that object holds no token, and neither the
    key nor the token is ever interpolated into an error message.
    """

    key = "google_workspace"
    provider = "google"
    required_extras: ClassVar[dict[str, str]] = {"google.oauth2": GOOGLE_EXTRA}

    #: Fallback when ``Connection.metadata["scopes"]`` is unset. A subclass
    #: registered by the host is the supported way to narrow these globally.
    default_scopes = DEFAULT_SCOPES

    # --- use ---------------------------------------------------------------

    def get_credentials(self, connection):
        info = service_account_info(self._stored_key(connection))
        scopes = self.scopes_for(connection)
        subject = self.subject_for(connection)
        credentials = self._build(info, scopes, subject)

        try:
            from google.auth.transport.requests import Request
        except ImportError as exc:
            raise _missing_google_auth() from exc

        try:
            # Refreshed here, not lazily: `complete_setup` and the admin's
            # "test connection" both promise that the credential *works*, and a
            # credential that has never been exchanged proves only that a file
            # parsed. This is also where domain-wide delegation for `subject`
            # is first checked by Google.
            credentials.refresh(Request())
        except Exception as exc:
            raise classify_refresh_error(exc) from exc

        if not credentials.token:
            raise CredentialsExpired(
                f"Google returned no access token for connection "
                f"{connection.id} (subject={subject or 'none'!r})."
            )
        return credentials

    def _build(self, info, scopes, subject):
        try:
            from google.oauth2 import service_account
        except ImportError as exc:
            raise _missing_google_auth() from exc

        try:
            return service_account.Credentials.from_service_account_info(
                info, scopes=list(scopes), subject=subject or None
            )
        except (ValueError, TypeError) as exc:
            # A malformed private key raises here, before any network call. It
            # is a configuration fault, not a revocation: reporting it as the
            # latter would take the Connection out of service for a typo.
            raise ConfigurationError(
                f"the stored Google service-account key could not be loaded: "
                f"{scrub(exc)}"
            ) from exc

    # --- setup -------------------------------------------------------------

    def store_credentials(self, connection, key_json, *, reference=None):
        """Save the service-account key file and point `connection` at it.

        Leaves ``Connection.status`` alone. Activating is ``complete_setup``'s
        job and that one exchanges the key for a token first — "a key is
        stored" and "the key works" are different claims, and only the second
        one is worth showing an operator.
        """
        reference = reference or connection.auth_reference or "google_service_account"
        # Validated before it is stored, so a paste-o is caught while the
        # person who made it is still looking at the form.
        info = service_account_info(key_json)
        # Serialised here rather than handed over as a dict. ``SecretStore``
        # implementations store text — ``ModelSecretStore`` refuses anything
        # else outright, and one that did not would write ``str(dict)``, a
        # Python repr that nothing can parse back and that nobody discovers
        # until a Run tries to use it.
        self.store.set(connection, reference, json.dumps(info))
        if connection.auth_reference != reference:
            connection.auth_reference = reference
            connection.save(update_fields=["auth_reference"])
        return reference

    def revoke(self, connection):
        """Delete the stored key, then mark the Connection revoked.

        Deletion first: if the store raises, the Connection stays as it was and
        the operator sees it, rather than ending up labelled "revoked" while a
        working service-account key is still sitting in the store.

        This cannot revoke the *key* at Google — only the Cloud console can —
        so it does not claim to.
        """
        if connection.auth_reference:
            self.store.delete(connection, connection.auth_reference)
        return super().revoke(connection)

    # --- reporting ---------------------------------------------------------

    def test(self, connection):
        credentials = self.get_credentials(connection)
        expiry = getattr(credentials, "expiry", None)
        subject = self.subject_for(connection)
        return (
            f"token issued for {subject or 'the service account itself'} "
            f"(expires {expiry.isoformat() if expiry else 'unknown'})"
        )

    def health(self, connection):
        """Non-secret facts only: never the key, never the token."""
        try:
            credentials = self.get_credentials(connection)
        except AuthError as exc:
            return {"usable": False, "reason": type(exc).__name__}
        except ConfigurationError:
            return {"usable": False, "reason": "ConfigurationError"}
        expiry = getattr(credentials, "expiry", None)
        return {
            "usable": True,
            "expires_at": expiry.isoformat() if expiry else None,
            "subject": self.subject_for(connection),
            "scopes": list(self.scopes_for(connection)),
        }

    # --- configuration -----------------------------------------------------

    def scopes_for(self, connection):
        """Scopes to request, from ``Connection.metadata`` or the default set."""
        scopes = (connection.metadata or {}).get("scopes")
        if scopes is None:
            return tuple(self.default_scopes)
        if isinstance(scopes, str):
            scopes = [scopes]
        if not isinstance(scopes, list | tuple) or not all(
            isinstance(scope, str) and scope for scope in scopes
        ):
            raise ConfigurationError(
                f"connection {connection.id} has metadata['scopes'] that is not "
                f"a list of scope URLs."
            )
        if not scopes:
            raise ConfigurationError(
                f"connection {connection.id} declares an empty metadata['scopes']. "
                f"A token with no scopes is accepted by Google and then refused "
                f"by every API, which reads as a permissions bug for weeks."
            )
        return tuple(scopes)

    def subject_for(self, connection):
        """The Workspace user to impersonate, or ``""`` for none."""
        metadata = connection.metadata or {}
        subject = metadata.get("subject") or metadata.get("impersonate_user") or ""
        if subject and not isinstance(subject, str):
            raise ConfigurationError(
                f"connection {connection.id} has a non-string metadata['subject']."
            )
        return subject

    def _stored_key(self, connection):
        if not connection.auth_reference:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no auth_reference, so there is no key to look up. Set "
                f"it to the SecretStore key holding the service-account key JSON "
                f"(store_credentials() does this for you). The key itself must "
                f"never go in auth_metadata — that field is public to the API."
            )
        value = self.store.get(connection, connection.auth_reference)
        if value is None or value == "":
            # Absent is revocation, not misconfiguration: deleting the stored
            # key is how an operator withdraws this Connection's access, and the
            # runner must block its Bindings rather than retry an empty key.
            raise CredentialsRevoked(
                f"no Google service-account key is stored for connection "
                f"{connection.id} under auth_reference "
                f"{connection.auth_reference!r} ({self.store})."
            )
        return value
