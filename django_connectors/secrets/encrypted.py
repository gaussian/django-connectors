"""A store backed by the ``ConnectionSecret`` table.

Encryption is chosen per deployment and stated explicitly. There is no default,
because both answers are defensible and only one of them is safe to guess
wrong:

``"none"``
    django-allauth's trust model — ``SocialToken.token`` is a plaintext column
    — and entirely reasonable when the database is already the trust boundary
    and the alternative is a key sitting in the same settings file.

``"fernet"``
    encrypts at rest with a key that is *not* ``settings.SECRET_KEY``. Django
    documents rotating ``SECRET_KEY`` as a routine operation, and hosts do it;
    keying credentials off it would silently convert a routine rotation into
    the permanent loss of every stored credential.

Which scheme was used is recorded on the row, and reads honour the *row*, not
the current setting. That is what makes a migration from ``none`` to ``fernet``
possible at all: rows already written stay readable while new writes encrypt.
"""

from typing import ClassVar

from django.core.exceptions import ImproperlyConfigured

from django_connectors.conf import SETTING_NAME, conf
from django_connectors.enums import SecretEncryption
from django_connectors.exceptions import ConfigurationError
from django_connectors.secrets.base import SecretStore

ENCRYPTION_SETTING = "SECRET_ENCRYPTION"
KEY_SETTING = "SECRET_KEY"


class ModelSecretStore(SecretStore):
    """Store credentials in ``django_connectors.ConnectionSecret``."""

    # Only the Fernet path needs it, so this is a warning (W005), not a hard
    # requirement: a deployment on SECRET_ENCRYPTION="none" is fully functional
    # without cryptography installed.
    required_extras: ClassVar[dict[str, str]] = {"cryptography": "secrets"}

    def get(self, connection, key):
        model = self._checked_model(key)
        row = model.objects.filter(connection=connection, key=key).first()
        if row is None:
            return None
        return self._decrypt(row.value, row.encryption, key)

    def set(self, connection, key, value):
        """Write `value`, or raise before touching the database.

        Order matters: everything that can refuse does so first, so a deployment
        that never chose a scheme ends up with no row at all rather than a row
        it believes is encrypted.
        """
        model = self._checked_model(key)
        encryption = self._encryption()
        stored = self._encrypt(value, encryption)
        model.objects.update_or_create(
            connection=connection,
            key=key,
            defaults={"value": stored, "encryption": encryption},
        )
        return None

    def delete(self, connection, key):
        model = self._checked_model(key)
        deleted, _ = model.objects.filter(connection=connection, key=key).delete()
        return bool(deleted)

    # --- internals ---------------------------------------------------------

    def _model(self):
        # Imported per call rather than at module scope: this module is
        # reachable from `get_secret_store()`, which a host could reasonably
        # call from somewhere the app registry is not ready yet.
        from django_connectors.models import ConnectionSecret

        return ConnectionSecret

    def _checked_model(self, key):
        """Return the model, refusing a key the column cannot hold.

        MySQL in strict mode raises "Data too long for column" while sqlite
        stores the whole thing regardless of the declared width, so an
        over-long ``auth_reference`` is a defect that development never sees
        and production hits on the first write.
        """
        model = self._model()
        if not key:
            raise ImproperlyConfigured("a secret key may not be empty.")
        limit = model._meta.get_field("key").max_length
        if len(key) > limit:
            raise ImproperlyConfigured(
                f"secret key {key[:20]!r}... is {len(key)} characters; "
                f"ConnectionSecret.key holds {limit}. Use a shorter "
                f"auth_reference, or a SecretStore whose backing store can "
                f"address longer names."
            )
        return model

    def _encryption(self):
        """Return the configured scheme, or explain that one must be chosen."""
        value = conf.SECRET_ENCRYPTION
        if value in (None, ""):
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{ENCRYPTION_SETTING}'] must be set to "
                f"{SecretEncryption.NONE.value!r} or {SecretEncryption.FERNET.value!r} "
                f"before {type(self).__name__} will store anything. There is "
                f"deliberately no default, so that storing credentials in "
                f"plaintext is always a decision someone took."
            )
        if value not in SecretEncryption.values:
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{ENCRYPTION_SETTING}'] = {value!r} is not a "
                f"known scheme. Valid values: {', '.join(SecretEncryption.values)}."
            )
        return value

    def _encrypt(self, value, encryption):
        # str only, and deliberately no str() fallback: `str({"token": ...})`
        # writes a Python repr that comes back out as a string nothing can
        # parse, and the caller does not find out until a Run uses it. A
        # credential with structure is the caller's to serialise.
        if not isinstance(value, str):
            raise TypeError(
                f"{type(self).__name__} stores text, got {type(value).__name__}. "
                f"Serialise structured credentials yourself (json.dumps) so that "
                f"what comes back out is exactly what went in."
            )
        if encryption == SecretEncryption.NONE:
            return value
        return self._fernet().encrypt(value.encode()).decode()

    def _decrypt(self, stored, encryption, key):
        if encryption == SecretEncryption.NONE:
            return stored
        if encryption != SecretEncryption.FERNET:
            raise ImproperlyConfigured(
                f"ConnectionSecret {key!r} was written with encryption "
                f"{encryption!r}, which this version does not know how to read."
            )

        # _fernet() first: it is what turns a missing extra into a
        # ConfigurationError naming it, and importing InvalidToken above that
        # would beat it to the punch with a bare ImportError.
        fernet = self._fernet()
        from cryptography.fernet import InvalidToken

        try:
            return fernet.decrypt(stored.encode()).decode()
        except InvalidToken as exc:
            # Never re-raise the original: its repr carries the ciphertext.
            raise ImproperlyConfigured(
                f"ConnectionSecret {key!r} could not be decrypted with "
                f"{SETTING_NAME}['{KEY_SETTING}']. The key has changed since the "
                f"secret was written; restore the previous key or re-authorise "
                f"the connection."
            ) from exc

    def _fernet(self):
        """Build the Fernet, naming the extra rather than leaking an ImportError."""
        try:
            from cryptography.fernet import Fernet
        except ImportError as exc:
            raise ConfigurationError(
                f"{SETTING_NAME}['{ENCRYPTION_SETTING}'] = "
                f"{SecretEncryption.FERNET.value!r} needs the cryptography "
                f"package: pip install 'django-connectors[secrets]'"
            ) from exc

        key = conf.require(KEY_SETTING)
        if isinstance(key, str):
            key = key.encode()
        try:
            return Fernet(key)
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{KEY_SETTING}'] is not a valid Fernet key. "
                f'Generate one with `python -c "from cryptography.fernet import '
                f'Fernet; print(Fernet.generate_key().decode())"`. It must not be '
                f"settings.SECRET_KEY: rotating that would make every stored "
                f"credential unreadable."
            ) from exc
