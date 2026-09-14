"""Credentials this library stores and owns.

The API-key case: a value handed over once, kept in the configured SecretStore
under ``Connection.auth_reference``, and used unchanged until someone replaces
it. No handshake, no refresh, no expiry the library can observe — so the only
way this backend learns a credential stopped working is that it is no longer in
the store, which is exactly what ``revoke()`` arranges.
"""

from django_connectors.auth.base import AuthBackend
from django_connectors.enums import ConnectionStatus
from django_connectors.exceptions import ConfigurationError, CredentialsRevoked


class StaticCredentialsBackend(AuthBackend):
    """Read the credential the SecretStore holds for ``auth_reference``.

    Returns the stored value unchanged — a string for an API key, whatever the
    store yields for a richer payload. Sources that declare this backend in
    ``supported_auth_backends`` are declaring that they can consume that.
    """

    key = "static"

    def get_credentials(self, connection):
        reference = self._reference(connection)
        value = self.store.get(connection, reference)
        if value is None or value == "":
            # Absent is treated as revoked, not as a configuration error: the
            # normal way a static credential ends is that someone deleted it.
            # The runner then blocks the Bindings instead of retrying an empty
            # credential against the provider once a minute.
            raise CredentialsRevoked(
                f"no credential is stored for connection {connection.id} under "
                f"auth_reference {reference!r} ({self.store})"
            )
        return value

    def store_credentials(self, connection, value, *, reference=None):
        """Save `value` and point `connection` at it.

        The Connection is left in whatever status it had: activating it is
        ``complete_setup``'s job, and that one verifies the credential resolves
        first. Saving a credential and asserting the Connection works are
        different claims.
        """
        reference = reference or connection.auth_reference or "credential"
        self.store.set(connection, reference, value)
        if connection.auth_reference != reference:
            connection.auth_reference = reference
            connection.save(update_fields=["auth_reference"])
        return reference

    def revoke(self, connection):
        """Delete the stored credential, then mark the Connection revoked.

        Deletion first, deliberately. If the store raises, the Connection stays
        as it was and the operator sees the failure — the alternative leaves a
        Connection labelled "revoked" whose credential is still live and still
        usable by anything holding a reference to it.
        """
        reference = connection.auth_reference
        if reference:
            self.store.delete(connection, reference)
        return super().revoke(connection)

    def health(self, connection):
        """Whether a credential is present. Never what it is."""
        try:
            present = (
                self.store.get(connection, self._reference(connection)) is not None
            )
        except ConfigurationError:
            present = False
        return {"credential_present": present, "status": connection.status}

    def _reference(self, connection):
        if not connection.auth_reference:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no auth_reference, so there is no key to look up. Set "
                f"it to the SecretStore key holding the credential "
                f"(store_credentials() does this for you)."
            )
        return connection.auth_reference

    def complete_setup(self, connection, request=None):
        """Activate once the stored credential resolves."""
        if connection.status == ConnectionStatus.REVOKED:
            # A revoked Connection that still resolves a credential is a
            # contradiction worth surfacing rather than quietly un-revoking.
            raise ConfigurationError(
                f"connection {connection.id} is revoked; store a fresh "
                f"credential before completing setup."
            )
        return super().complete_setup(connection, request=request)
