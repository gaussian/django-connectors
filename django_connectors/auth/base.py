"""The AuthBackend contract.

An AuthBackend answers one question for the runner — *what credential should
this Connection use right now* — and answers it in a shape the runner can act
on. That shape is the whole point of the base class:

``CredentialsRevoked`` / ``CredentialsExpired`` are load-bearing
    ``services.runs`` catches exactly these two, flips
    ``Connection.status`` to ``revoked`` and blocks dependent Bindings instead
    of retrying. A backend that reports revocation as a generic exception gets
    the Binding retried on a schedule forever, burning provider quota against a
    401 and inviting rate limiting; one that reports a transient network blip
    as ``CredentialsRevoked`` takes a healthy Connection out of service until a
    human notices. Getting this distinction right is the contract.

``get_credentials`` never returns something falsy for "no credential"
    it raises. A ``None`` would travel all the way into a source and surface as
    an unexplained provider error.

The payload's *shape* is a contract between a backend and the sources that
declare it in ``SourceDefinition.supported_auth_backends``, not something this
package normalises. A static API key is a string; an OAuth grant is several
fields. Forcing both through one envelope would only mean every source unwraps
it again.
"""

from collections.abc import Mapping
from typing import ClassVar

from django_connectors.enums import ConnectionStatus


class AuthBackend:
    """Base class for every auth backend.

    Registered under ``DJANGO_CONNECTORS["AUTH_BACKENDS"]`` and resolved lazily,
    so a backend module is free to import a provider SDK at its own module
    scope — nothing imports it until a Connection actually names it.
    """

    #: Registry key, matching ``Connection.auth_backend``.
    key = ""
    #: Human-facing provider grouping, e.g. "google", "microsoft".
    provider = ""
    #: ``{module_name: extra_name}`` — surfaced by the W005 system check.
    required_extras: ClassVar[dict[str, str]] = {}

    @property
    def store(self):
        """The configured SecretStore, resolved on use."""
        from django_connectors.secrets import get_secret_store

        return get_secret_store()

    # --- setup -------------------------------------------------------------

    def begin_setup(self, connection, request=None):
        """Start an out-of-band authorisation handshake.

        Returns whatever the caller needs to continue — typically
        ``{"authorization_url": ...}`` — or ``None`` when the backend needs no
        handshake at all, which is the default.

        ``request`` is optional and exists only because an OAuth redirect
        genuinely needs the session it will be correlated against. Nothing in
        this package may *require* one: the service layer must stay callable
        from a Celery task, a management command and a test.
        """
        return None

    def complete_setup(self, connection, request=None):
        """Finish the handshake and activate the Connection.

        The default proves the credential resolves *before* marking the
        Connection active. Activating first and validating later produces the
        worst failure mode available: a Connection that looks healthy in the
        UI, schedules Bindings, and fails every one of them.
        """
        self.get_credentials(connection)
        connection.status = ConnectionStatus.ACTIVE
        connection.setup_token = None
        connection.save(update_fields=["status", "setup_token"])
        return connection

    # --- use ---------------------------------------------------------------

    def get_credentials(self, connection):
        """Return usable credentials for `connection`.

        Raise :class:`~django_connectors.exceptions.CredentialsRevoked` when the
        credential is gone and no retry can recover it, and
        :class:`~django_connectors.exceptions.CredentialsExpired` when it lapsed
        and could not be refreshed. Anything else — a provider outage, a
        malformed configuration — must be some other exception, or the runner
        will take the Connection out of service for it.
        """
        raise NotImplementedError

    def test(self, connection):
        """Cheaply verify the credentials, returning a short status string.

        The default only proves a credential can be *resolved*. A backend that
        can afford a cheap provider call should override this — resolvable and
        accepted are different claims, and the admin's "test connection" action
        is read as the second one.
        """
        self.get_credentials(connection)
        return "credentials resolved"

    def revoke(self, connection):
        """Stop using this Connection's credentials.

        Local by construction: this marks the Connection revoked, and a backend
        that owns the stored material also removes it. It cannot promise the
        *provider* forgot anything — only the provider's own consent screen
        does that — so it never claims to.
        """
        connection.status = ConnectionStatus.REVOKED
        connection.save(update_fields=["status"])
        return connection

    def health(self, connection):
        """Optional: non-secret facts about the credential's state.

        Returns ``None`` when the backend has nothing to report. Whatever it
        returns is rendered in the admin and returned by the API, so it must
        never contain credential material — expiry timestamps and booleans, not
        tokens.
        """
        return None

    def __str__(self):
        return self.key or type(self).__name__


class Credentials(Mapping):
    """A multi-field credential payload whose ``repr`` holds no credential.

    Tokens escape through repr, not through deliberate printing: a failing
    request logs its arguments, a traceback renders every local variable, and
    ``logger.exception`` in a host's task wrapper writes the lot to disk. A
    plain dict cooperates with all three. This does not.

    It is a Mapping, so ``credentials["access_token"]`` and ``**credentials``
    work as usual — reading a value stays deliberate and explicit.
    """

    __slots__ = ("_provider", "_values")

    def __init__(self, provider="", **values):
        self._provider = provider
        self._values = dict(values)

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        # Key names only. They are configuration, not secrets, and naming them
        # is what makes a "wrong credential shape" bug debuggable at all.
        return (
            f"{type(self).__name__}(provider={self._provider!r}, "
            f"keys={sorted(self._values)!r})"
        )

    @property
    def provider(self):
        return self._provider
