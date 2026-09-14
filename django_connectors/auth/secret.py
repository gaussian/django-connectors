"""Credentials someone else owns, reached through an opaque reference.

For deployments where the credential lives in Vault, AWS Secrets Manager, GCP
Secret Manager or an internal service, fronted by a host-written
:class:`~django_connectors.secrets.SecretStore`. ``Connection.auth_reference``
is then a *pointer* — a Vault path, an ARN, a version alias — and this backend's
job is to hand it to the store and get out of the way.

Two things follow from the material not being ours, and they are the whole
difference between this backend and ``StaticCredentialsBackend``:

**Nothing is written.** There is no ``store_credentials``; the external manager
is the system of record and a write from here would fork it.

**Revocation is local.** ``revoke()`` marks the Connection revoked and leaves
the external secret alone. Deleting a secret out of a shared manager because
one Connection stopped using it is not a decision this library gets to take —
other systems may hold the same reference.

Every lookup is a fresh call to the store. External managers rotate values in
place behind a stable reference, and that is the *point* of using one; caching
here would keep serving a superseded credential until the process restarted.
"""

from django_connectors.auth.base import AuthBackend
from django_connectors.exceptions import ConfigurationError, CredentialsRevoked


class SecretReferenceBackend(AuthBackend):
    """Resolve ``auth_reference`` through the SecretStore and return the result."""

    key = "secret_reference"

    def get_credentials(self, connection):
        reference = self._reference(connection)
        value = self.store.get(connection, reference)
        if value is None:
            # Only None means "not there". Anything else — an empty dict, a
            # zero-length string — is a value the external manager chose to
            # return, and reinterpreting it would be this library second-
            # guessing the system of record.
            raise CredentialsRevoked(
                f"the secret store ({self.store}) has nothing at reference "
                f"{reference!r} for connection {connection.id}. The secret was "
                f"deleted, the reference is stale, or this deployment cannot "
                f"read it."
            )
        return value

    def health(self, connection):
        return {
            "reference_set": bool(connection.auth_reference),
            "store": str(self.store),
            "status": connection.status,
        }

    def _reference(self, connection):
        if not connection.auth_reference:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no auth_reference. Set it to the handle your "
                f"SecretStore resolves — a Vault path, a secret ARN, an id."
            )
        return connection.auth_reference
