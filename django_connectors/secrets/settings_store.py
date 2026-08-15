"""A read-only store backed by environment variables and Django settings.

For deployments whose credentials arrive as injected environment variables —
one service account, one API key, rotated by the platform rather than by this
library. Zero dependencies, nothing written to the database, and nothing this
library could leak that the process environment did not already hold.

The prefix is the security boundary. ``Connection.auth_reference`` is
host-supplied data, editable in the admin, so an unprefixed lookup would turn
"which credential does this Connection use" into "read any Django setting",
handing ``SECRET_KEY`` or ``DATABASES`` to whatever source the Binding runs.
Only names under :attr:`SettingsSecretStore.prefix` are reachable.
"""

import os
import re

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured

from django_connectors.secrets.base import SecretStore

# Django's own Settings object only exposes upper-case names, so a key has to
# be normalised into that shape to be resolvable at all.
KEY_RE = re.compile(r"[A-Za-z0-9_]+")


class SettingsSecretStore(SecretStore):
    """Resolve a key against ``os.environ`` first, then ``django.conf.settings``.

    ``auth_reference = "acme_token"`` reads ``DJANGO_CONNECTORS_SECRET_ACME_TOKEN``.
    Environment wins over settings so that a container can override a checked-in
    default without a settings module change.
    """

    prefix = "DJANGO_CONNECTORS_SECRET_"

    def get(self, connection, key):
        name = self.name_for(key)
        # An unset variable is frequently materialised as an empty string by
        # container tooling; treating "" as a value would hand a source a blank
        # credential and turn a config error into a provider 401.
        value = os.environ.get(name) or getattr(django_settings, name, None)
        return value or None

    def set(self, connection, key, value):
        raise ImproperlyConfigured(
            f"{type(self).__name__} is read-only: the credential for key {key!r} "
            f"was not saved. Set {self.name_for(key)} in the environment (or in "
            f"the settings module), or configure a writable SecretStore."
        )

    def delete(self, connection, key):
        raise ImproperlyConfigured(
            f"{type(self).__name__} is read-only and cannot delete {key!r}. "
            f"Unset {self.name_for(key)} where it is defined."
        )

    def name_for(self, key):
        """Return the environment/settings name `key` resolves to.

        Rejects anything that is not a bare identifier: a key containing a dot
        or a dash cannot name an environment variable portably, and allowing it
        would make the prefix guard depend on the shell.
        """
        if not key or not KEY_RE.fullmatch(str(key)):
            raise ImproperlyConfigured(
                f"{type(self).__name__} needs auth_reference to be a bare "
                f"identifier ([A-Za-z0-9_]), got {key!r}. It is turned into the "
                f"environment variable {self.prefix}<KEY>."
            )
        return f"{self.prefix}{key.upper()}"
