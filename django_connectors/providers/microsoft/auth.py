"""App-only (client-credentials) authentication against Microsoft Entra ID.

**Why app-only, and not a delegated user token.** The thing a customer actually
asks for is "sync our SharePoint site" or "sync the Finance document library" —
an *organisation-scoped* claim. A delegated token cannot express it: it carries
one person's permissions, so the sync silently narrows to whatever that person
can see, changes shape when their group membership changes, and stops entirely
when they leave the company, go on holiday with MFA re-prompts pending, or have
their refresh token invalidated by a Conditional Access policy. None of those
produce an error that says "your data is now incomplete" — they produce a
smaller result set. An app-only credential belongs to the *tenant*: consent is
granted once by an administrator, the permission set is fixed and auditable, and
nothing about it depends on an individual continuing to exist. That is the whole
reason this backend exists, and it is why the sources in this package never fall
back to a user token.

**The client assertion is never hand-rolled.** ``azure-identity`` builds and
signs it (a certificate credential means an RS256 JWT with an ``x5t`` header, a
correct ``aud`` for the tenant's token endpoint, and a fresh ``jti`` per
request), and it is also the only party that gets the authority discovery,
instance validation, regional endpoints and token caching right. Signing a JWT
by hand is a small amount of code with a large number of ways to be quietly
wrong — a stale ``aud`` after a tenant moves clouds, a reused ``jti``, a
thumbprint encoded as hex rather than base64url — and every one of them presents
as an opaque AADSTS error months later.

**Credential material comes from the SecretStore, never from the model.** The
tenant id is on ``Connection.external_tenant_id`` (it is not a secret; it is
routing), and ``Connection.auth_reference`` is a handle the configured store
resolves into the client id + secret, or the client id + certificate.

Errors are classified into exactly the three the runner acts on. Getting that
wrong is expensive in both directions, so the mapping is table-driven off the
AADSTS code — the one genuinely diagnostic token in an Entra failure — and the
code is repeated into the message, because without it "Authentication failed" is
indistinguishable between a rotated secret, a deleted app registration and a
tenant that never consented.

**Unverified against a live provider.** Token acquisition here was exercised
against azure-identity's real code path with a stubbed HTTP transport: the OIDC
discovery document and the ``/oauth2/v2.0/token`` response are ours, but the
request, the signed client assertion and the error handling are azure-identity's
own. Not exercised: a real administrator walking the multi-tenant admin-consent
screen and the callback it produces; real AADSTS error bodies (the codes below
come from Microsoft's published list, not from a failing tenant); Conditional
Access policies that block a service principal; token acquisition against any
national-cloud authority other than the global one; and certificate credentials
using ``send_certificate_chain`` (subject-name/issuer auth).
"""

import datetime as dt
import hmac
import json
import re
import uuid
from collections.abc import Mapping
from typing import ClassVar
from urllib.parse import urlencode, urlsplit

from django_connectors.auth.base import AuthBackend, Credentials
from django_connectors.enums import ConnectionStatus
from django_connectors.errors import scrub
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
)

#: App-only tokens are only ever issued for a ``/.default`` scope: the client
#: credentials grant has no user to consent on the fly, so Entra hands back the
#: application permissions an administrator already approved. Asking for a
#: granular scope instead does not narrow the token, it fails the request.
GRAPH_SCOPE = "https://graph.microsoft.com/.default"

DEFAULT_AUTHORITY = "login.microsoftonline.com"

#: Sovereign-cloud login hosts. The authority is where the client secret (or the
#: signed assertion) is POSTed, and it is reachable from ``Connection.metadata``,
#: which an administrator can edit — so an unconstrained value would turn a
#: Connection edit into credential exfiltration. Lifting this takes a subclass
#: registered in ``DJANGO_CONNECTORS["AUTH_BACKENDS"]``, exactly like the REST
#: source's private-address guard.
ALLOWED_AUTHORITY_HOSTS = frozenset(
    {
        "login.microsoftonline.com",
        "login.microsoftonline.us",
        "login.partner.microsoftonline.cn",
        "login.microsoftonline.de",
    }
)

AADSTS_RE = re.compile(r"\bAADSTS\d{4,7}\b")

#: The credential itself lapsed and a human has to mint a new one. Reported as
#: CredentialsExpired: the runner blocks the Bindings instead of retrying, but
#: the wording tells the operator to *rotate*, not to re-consent.
EXPIRED_AADSTS_CODES = {
    # "The provided client secret keys for app ... are expired."
    "AADSTS7000222": "the client secret has expired; rotate it in the app registration",
    # "Client assertion is not within its valid time range." — an expired
    # signing certificate, or badly skewed clocks on the worker.
    "AADSTS700024": (
        "the signed client assertion was outside its validity window; the "
        "signing certificate has expired or this host's clock is skewed"
    ),
    # "The certificate ... has expired."
    "AADSTS700027": "the client certificate has expired or is not trusted",
    "AADSTS50173": "the grant was invalidated; a fresh credential is required",
}

#: Access is gone and no retry recovers it. Reported as CredentialsRevoked, which
#: takes the Connection out of service — correct here, because every one of these
#: means an administrator changed something in the directory.
REVOKED_AADSTS_CODES = {
    "AADSTS7000215": "the client secret is not valid (wrong value, or rotated)",
    "AADSTS700016": (
        "the application was not found in this tenant; it was deleted, or the "
        "tenant id does not match the app registration"
    ),
    "AADSTS7000112": "the application is disabled in the directory",
    "AADSTS7000229": (
        "the application has no service principal in this tenant — admin "
        "consent was never granted, or the enterprise application was deleted"
    ),
    "AADSTS700213": "no service principal for this application in the tenant",
}

#: The tenant/resource routing is wrong rather than the credential. Raised as
#: ConfigurationError so the runner does *not* mark the Connection revoked: no
#: amount of re-consenting fixes a typo in a tenant id.
CONFIGURATION_AADSTS_CODES = {
    "AADSTS90002": "no such tenant; check Connection.external_tenant_id",
    "AADSTS900023": "the tenant identifier is not a valid tenant name or id",
    "AADSTS500011": (
        "the requested resource has no service principal in this tenant; check "
        "the scope"
    ),
}

#: OAuth 2.0 ``error`` values, matched only as a fallback. azure-identity's
#: message usually carries the AADSTS code and not the error name, but other
#: callers (and other clouds) surface the raw token-endpoint body.
REVOKED_OAUTH_ERRORS = ("invalid_client", "unauthorized_client")
EXPIRED_OAUTH_ERRORS = ("invalid_grant",)


class EntraBackend(AuthBackend):
    """Acquire an app-only Microsoft Graph token for a Connection's tenant.

    Expected stored payload (whatever the configured SecretStore returns for
    ``Connection.auth_reference``), as a mapping or a JSON object::

        {"client_id": "<app id>", "client_secret": "<secret value>"}
        {"client_id": "<app id>", "certificate_data": "<PEM key + cert>"}
        {"client_id": "<app id>", "certificate_path": "/run/secrets/app.pem"}

    A bare string is also accepted and treated as the client secret, so that a
    platform injecting one environment variable works with
    ``SettingsSecretStore``; the client id then comes from
    ``Connection.auth_metadata["client_id"]``, which is fine because a client id
    is public.
    """

    key = "entra"
    provider = "microsoft"
    required_extras: ClassVar[dict[str, str]] = {"azure.identity": "microsoft"}

    #: Opt-in escape hatch for a login host outside :data:`ALLOWED_AUTHORITY_HOSTS`
    #: — an on-premises ADFS federation endpoint, say. Deliberately a class
    #: attribute and not a config key: the administrator editing
    #: ``Connection.metadata`` is exactly the party this guard constrains.
    allow_custom_authority = False

    # --- token acquisition -------------------------------------------------

    def get_credentials(self, connection):
        """Return a freshly minted app-only access token for `connection`.

        No cross-call caching. azure-identity caches inside the credential
        object, and this builds one per call, so a Run costs one extra token
        request — about 200ms once, against a sync that is about to make
        hundreds of Graph calls. The alternative, a process-wide cache keyed by
        tenant, would keep another tenant's bearer token alive in the memory of
        a worker that has moved on to a different customer, which is not a
        trade this library is willing to make for 200ms.
        """
        tenant_id = self.tenant_id_for(connection)
        payload = self.credential_payload(connection)
        scope = self.scope_for(connection)

        credential = self.build_credential(
            connection, tenant_id=tenant_id, payload=payload
        )
        try:
            token = credential.get_token(scope)
        except Exception as exc:
            raise self.map_error(
                exc, connection=connection, tenant_id=tenant_id
            ) from exc
        finally:
            # Releases the underlying HTTP session. A credential per call would
            # otherwise leak one connection pool per Run.
            close = getattr(credential, "close", None)
            if close is not None:
                close()

        return Credentials(
            provider=self.provider,
            access_token=token.token,
            expires_at=dt.datetime.fromtimestamp(token.expires_on, tz=dt.UTC),
            tenant_id=tenant_id,
            client_id=payload.get("client_id", ""),
            scope=scope,
        )

    def build_credential(self, connection, *, tenant_id, payload):
        """Construct the azure-identity credential this payload describes.

        Certificate first: a payload carrying both a certificate and a secret is
        a mid-rotation state, and the certificate is the stronger credential.
        """
        identity = self._identity_module()
        options = {
            "authority": self.authority_for(connection, payload),
            **self.credential_options(connection),
        }
        client_id = payload.get("client_id")
        if not client_id:
            raise ConfigurationError(
                f"connection {connection.id} has no client id: put 'client_id' "
                f"in the stored credential payload, or in "
                f"Connection.auth_metadata['client_id'] (a client id is public, "
                f"so it may live on the model)."
            )

        certificate = payload.get("certificate_data") or payload.get("certificate_path")
        if certificate:
            kwargs = dict(options)
            if payload.get("certificate_data"):
                data = payload["certificate_data"]
                kwargs["certificate_data"] = (
                    data.encode() if isinstance(data, str) else data
                )
            else:
                kwargs["certificate_path"] = payload["certificate_path"]
            password = payload.get("certificate_password")
            if password:
                kwargs["password"] = password
            if payload.get("send_certificate_chain"):
                # Subject-name/issuer authentication. Only some tenants accept
                # it, so it is opt-in rather than always on.
                kwargs["send_certificate_chain"] = True
            return identity.CertificateCredential(tenant_id, client_id, **kwargs)

        secret = payload.get("client_secret")
        if not secret:
            raise ConfigurationError(
                f"the stored credential for connection {connection.id} has "
                f"neither 'client_secret' nor 'certificate_data'/"
                f"'certificate_path'. App-only authentication needs one of them."
            )
        return identity.ClientSecretCredential(tenant_id, client_id, secret, **options)

    def credential_options(self, connection):
        """Extra keyword arguments for the azure-identity credential.

        The extension point for everything azure-identity supports that this
        library does not model: ``disable_instance_discovery`` for a sovereign
        or air-gapped cloud, ``connection_verify`` for a corporate TLS
        intercepting proxy, ``transport`` for a host that funnels all outbound
        traffic through its own pipeline. Returns nothing by default.
        """
        return {}

    # --- setup -------------------------------------------------------------

    def begin_setup(self, connection, request=None):
        """Return the administrator-consent URL for this Connection.

        The multi-tenant flow, deliberately: a customer's administrator opens
        this once, approves the application permissions for their whole tenant,
        and the grant then belongs to the tenant rather than to whoever happened
        to click. ``state`` is the Connection's single-use ``setup_token``, so a
        callback cannot be pointed at a different Connection.
        """
        redirect_uri = self._redirect_uri(connection)
        payload = self._optional_payload(connection)
        client_id = payload.get("client_id") or (connection.auth_metadata or {}).get(
            "client_id"
        )
        if not client_id:
            raise ConfigurationError(
                f"connection {connection.id} needs a client id before consent "
                f"can be requested; set Connection.auth_metadata['client_id'] "
                f"or store it with the credential."
            )

        connection.setup_token = uuid.uuid4()
        connection.save(update_fields=["setup_token"])

        # "organizations", not "common": personal Microsoft accounts have no
        # administrator and cannot grant application permissions at all, so
        # offering them the screen only produces a confusing failure.
        tenant_segment = connection.external_tenant_id or "organizations"
        authority = self.authority_for(connection, payload)
        query = urlencode(
            {
                "client_id": client_id,
                "scope": self.scope_for(connection),
                "redirect_uri": redirect_uri,
                "state": str(connection.setup_token),
            }
        )
        return {
            "authorization_url": (
                f"https://{authority}/{tenant_segment}/v2.0/adminconsent?{query}"
            ),
            "state": str(connection.setup_token),
        }

    def complete_setup(self, connection, request=None):
        """Record the consenting tenant, then prove a token can be obtained.

        Both halves matter. Without the first, a multi-tenant application has no
        idea *which* tenant just consented — the consent callback is the only
        place that is reported. Without the second, the Connection goes active
        on the strength of a redirect the browser was told to follow, and every
        Binding under it fails at the first Run.
        """
        params = self._callback_params(request)
        if params:
            error = params.get("error")
            if error:
                raise self.map_error(
                    f"{error}: {params.get('error_description', '')}",
                    connection=connection,
                    tenant_id=connection.external_tenant_id,
                )
            self._assert_state_matches(connection, params.get("state"))
            tenant = params.get("tenant")
            if tenant and tenant != connection.external_tenant_id:
                connection.external_tenant_id = tenant
                connection.save(update_fields=["external_tenant_id"])

        # Raises rather than returning a bad Connection to an "active" state.
        self.get_credentials(connection)

        connection.status = ConnectionStatus.ACTIVE
        connection.setup_token = None
        connection.save(update_fields=["status", "setup_token"])
        return connection

    def revoke(self, connection):
        """Mark the Connection revoked; leave the stored credential alone.

        Unlike a per-user token, an Entra application secret is usually shared
        by every Connection this host has to every customer tenant. Deleting it
        because one tenant withdrew consent would break all of the others. The
        tenant-side grant is removed by an administrator in the Enterprise
        Applications blade, which is not something this package can do or claim.
        """
        return super().revoke(connection)

    def test(self, connection):
        credentials = self.get_credentials(connection)
        expires_at = credentials["expires_at"]
        return (
            f"app-only token acquired for tenant {credentials['tenant_id']} "
            f"(expires {expires_at.isoformat()})"
        )

    def health(self, connection):
        """Non-secret facts only: never the secret, never the token."""
        try:
            payload = self.credential_payload(connection)
        except (AuthError, ConfigurationError) as exc:
            return {"usable": False, "reason": type(exc).__name__}
        kind = (
            "certificate"
            if payload.get("certificate_data") or payload.get("certificate_path")
            else "client_secret"
        )
        return {
            "usable": True,
            "tenant_id": connection.external_tenant_id,
            "client_id": payload.get("client_id", ""),
            "credential_kind": kind,
            "status": connection.status,
        }

    # --- pieces ------------------------------------------------------------

    def tenant_id_for(self, connection):
        """The directory to authenticate against.

        Required, and never defaulted to ``common``/``organizations``: the
        client-credentials grant has no user whose home tenant could be
        inferred, so a missing tenant id is a configuration error and not
        something to guess at.
        """
        tenant_id = (connection.external_tenant_id or "").strip()
        if not tenant_id:
            raise ConfigurationError(
                f"connection {connection.id} has no external_tenant_id. An "
                f"app-only token is issued by a specific directory, so the "
                f"tenant id (a GUID, or the tenant's domain) must be recorded "
                f"before a Run can authenticate. complete_setup() records it "
                f"from the admin-consent callback."
            )
        return tenant_id

    def scope_for(self, connection):
        """The ``.default`` scope to request, Graph unless overridden."""
        scope = (connection.auth_metadata or {}).get("scope") or GRAPH_SCOPE
        if not str(scope).endswith("/.default"):
            raise ConfigurationError(
                f"auth_metadata['scope'] must end with '/.default' (got "
                f"{scope!r}). The client-credentials grant issues the "
                f"application permissions an administrator already consented "
                f"to; a granular scope is rejected by Entra rather than "
                f"narrowing the token."
            )
        return str(scope)

    def authority_for(self, connection, payload=None):
        """The login host, validated against the sovereign-cloud allow-list."""
        raw = (
            (payload or {}).get("authority")
            or (connection.metadata or {}).get("authority")
            or DEFAULT_AUTHORITY
        )
        host = urlsplit(raw if "//" in str(raw) else f"https://{raw}").hostname or ""
        host = host.lower()
        if not self.allow_custom_authority and host not in ALLOWED_AUTHORITY_HOSTS:
            raise ConfigurationError(
                f"refusing to send this Connection's client credential to "
                f"{host!r}: it is not a Microsoft login host. Allowed: "
                f"{sorted(ALLOWED_AUTHORITY_HOSTS)}. A deployment that must "
                f"reach a different authority (ADFS, a private cloud) should "
                f"register an EntraBackend subclass with "
                f"allow_custom_authority = True."
            )
        return host

    def credential_payload(self, connection):
        """The stored credential, normalized into a mapping.

        A missing entry is reported as revocation rather than as a
        configuration error, matching ``StaticCredentialsBackend``: the ordinary
        way a stored credential ends is that somebody deleted it, and the runner
        must stop the Bindings rather than retry an empty secret against Entra
        once a minute.
        """
        reference = (connection.auth_reference or "").strip()
        if not reference:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no auth_reference, so there is no key to resolve "
                f"against the SecretStore."
            )
        raw = self.store.get(connection, reference)
        if raw is None or raw == "":
            raise CredentialsRevoked(
                f"no credential is stored for connection {connection.id} under "
                f"auth_reference {reference!r} ({self.store})."
            )
        payload = normalize_payload(raw)
        # A client id is public routing information, so allowing it on the model
        # keeps single-secret deployments (one env var) workable.
        if not payload.get("client_id"):
            fallback = (connection.auth_metadata or {}).get("client_id")
            if fallback:
                payload["client_id"] = fallback
        return payload

    def map_error(self, exc, *, connection=None, tenant_id=""):
        """Classify an Entra failure into what the runner acts on.

        Returns the exception to raise rather than raising, so callers keep the
        original as ``__cause__``.
        """
        return map_entra_error(exc, connection=connection, tenant_id=tenant_id)

    # --- internals ---------------------------------------------------------

    def _identity_module(self):
        try:
            from azure import identity
        except ImportError as exc:
            raise ConfigurationError(
                f"the {self.key!r} auth backend needs azure-identity: "
                f"pip install 'django-connectors[microsoft]'. It is imported "
                f"here rather than at module scope so that a host which never "
                f"connects to Microsoft never pays for it — and so that a "
                f"missing extra is this message rather than an ImportError "
                f"inside a customer's Run."
            ) from exc
        return identity

    def _optional_payload(self, connection):
        """The stored payload if there is one; ``{}`` before anything is stored.

        ``begin_setup`` runs before consent, and frequently before a credential
        has been saved at all, so a missing one must not stop the operator from
        getting the consent URL.
        """
        try:
            return self.credential_payload(connection)
        except (AuthError, ConfigurationError):
            return {}

    def _redirect_uri(self, connection):
        uri = (connection.metadata or {}).get("redirect_uri")
        if not uri:
            raise ConfigurationError(
                f"connection {connection.id} needs "
                f"Connection.metadata['redirect_uri'] before consent can be "
                f"requested. It must exactly match a redirect URI registered on "
                f"the Entra application; Entra compares them literally, "
                f"including the trailing slash."
            )
        return uri

    def _callback_params(self, request):
        params = getattr(request, "GET", None) if request is not None else None
        if params is None:
            return {}
        return {
            key: params.get(key)
            for key in ("state", "tenant", "error", "error_description")
        }

    def _assert_state_matches(self, connection, state):
        """Constant-time comparison against the Connection's own setup token.

        The callback is an unauthenticated GET carrying an attacker-influencable
        ``tenant`` parameter, which this method's caller writes onto the model.
        Without this check, anyone who can make the operator's browser follow a
        URL can repoint a Connection at their own tenant.
        """
        expected = str(connection.setup_token or "")
        if not expected or not state or not hmac.compare_digest(expected, str(state)):
            raise AuthError(
                f"the admin-consent callback for connection {connection.id} did "
                f"not carry this Connection's setup token, so it cannot be "
                f"trusted to say which tenant consented. Start the flow again "
                f"with begin_setup()."
            )


# --- error classification --------------------------------------------------


def map_entra_error(exc, *, connection=None, tenant_id=""):
    """Return the exception to raise for an Entra token failure.

    Table-driven on the AADSTS code because that is the only part of an Entra
    error with stable meaning — the prose around it is localized and rewritten
    between releases. The code is repeated into our own message because
    ``errors.scrub`` runs over everything that is persisted, and a support
    conversation with Microsoft is impossible without it.

    Unknown failures become a plain ``AuthError`` on purpose. ``AuthError`` is
    retried; ``CredentialsRevoked`` takes the whole Connection out of service.
    Guessing "revoked" for an error nobody has classified yet would let one
    Entra outage disable every Microsoft Connection a host has.
    """
    raw = str(exc)
    codes = AADSTS_RE.findall(raw)
    lowered = raw.lower()
    detail = scrub(exc)
    where = f" for tenant {tenant_id!r}" if tenant_id else ""
    which = f" (connection {connection.id})" if connection is not None else ""

    for code in codes:
        if code in CONFIGURATION_AADSTS_CODES:
            return ConfigurationError(
                f"Entra refused the app-only token request{where}{which}: "
                f"{code} — {CONFIGURATION_AADSTS_CODES[code]}. Provider detail: "
                f"{detail}"
            )
        if code in EXPIRED_AADSTS_CODES:
            return CredentialsExpired(
                f"the app-only credential{where}{which} has expired: {code} — "
                f"{EXPIRED_AADSTS_CODES[code]}. Provider detail: {detail}"
            )
        if code in REVOKED_AADSTS_CODES:
            return CredentialsRevoked(
                f"Entra rejected the app-only credential{where}{which}: {code} "
                f"— {REVOKED_AADSTS_CODES[code]}. Provider detail: {detail}"
            )

    if any(name in lowered for name in EXPIRED_OAUTH_ERRORS):
        return CredentialsExpired(
            f"the app-only credential{where}{which} lapsed: {detail}"
        )
    if any(name in lowered for name in REVOKED_OAUTH_ERRORS):
        return CredentialsRevoked(
            f"Entra rejected the app-only credential{where}{which}: {detail}"
        )

    seen = f" (AADSTS code(s): {', '.join(sorted(set(codes)))})" if codes else ""
    return AuthError(
        f"could not obtain an app-only token{where}{which}{seen}: {detail}"
    )


def normalize_payload(raw):
    """Coerce whatever the SecretStore returned into a credential mapping.

    Never interpolates the value into an error message — only its type. A store
    misconfiguration must not be the thing that prints a client secret into a
    Run's ``error_message``.
    """
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        text = raw.strip()
        if text.startswith("{"):
            try:
                loaded = json.loads(text)
            except ValueError as exc:
                raise ConfigurationError(
                    f"the stored Entra credential looks like JSON but did not "
                    f"parse: {exc.__class__.__name__} at position "
                    f"{getattr(exc, 'pos', '?')}."
                ) from exc
            if not isinstance(loaded, Mapping):
                raise ConfigurationError(
                    f"the stored Entra credential parsed as "
                    f"{type(loaded).__name__}; it must be a JSON object with "
                    f"'client_id' and 'client_secret'."
                )
            return dict(loaded)
        # A bare string is the client secret. The client id then has to come
        # from auth_metadata, which is fine: it is public.
        return {"client_secret": text}
    raise ConfigurationError(
        f"the stored Entra credential is a {type(raw).__name__}; it must be a "
        f"mapping, a JSON object, or the client secret as a string."
    )
