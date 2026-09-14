"""The default store: reads nothing, writes nothing, and says so.

This is the default so that storing a credential is always a decision someone
took. The alternative — defaulting to a working store — means every host stores
credentials wherever the library happened to choose, in whatever form it
happened to choose, and nobody ever reads the setting.
"""

from django.core.exceptions import ImproperlyConfigured

from django_connectors.conf import SETTING_NAME
from django_connectors.secrets.base import SETTING_KEY, SecretStore


class NullSecretStore(SecretStore):
    """Stores nothing. ``set()`` raises rather than silently dropping a value."""

    def get(self, connection, key):
        """Always ``None`` — nothing was ever stored here."""
        return None

    def set(self, connection, key, value):
        """Refuse, naming the setting that has to change.

        Not a silent no-op and not a plaintext fallback: both would leave a
        host believing a credential was saved. The next Run would then fail
        with a puzzling "no credential stored" instead of with the real cause,
        which is that no store was ever configured.
        """
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['{SETTING_KEY}'] is the default NullSecretStore, "
            f"which stores nothing, so the credential for key {key!r} was not "
            f"saved. Point it at a store that can hold credentials — e.g. "
            f"'django_connectors.secrets.ModelSecretStore' together with "
            f"{SETTING_NAME}['SECRET_ENCRYPTION'], or your own SecretStore "
            f"subclass fronting an external secret manager."
        )

    def delete(self, connection, key):
        """Nothing to delete, and honest about it: always ``False``."""
        return False
