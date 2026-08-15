"""Where credential material lives, and how it is reached.

The store is deliberately pluggable and deliberately *empty* by default. A
library that ships a working plaintext store makes plaintext the path of least
resistance: everything works in development, nothing warns, and the decision to
store credentials unencrypted is never actually taken by anyone. So
``DJANGO_CONNECTORS["SECRET_STORE"]`` defaults to :class:`NullSecretStore`,
whose ``set()`` refuses, and :class:`ModelSecretStore` refuses to write until
``SECRET_ENCRYPTION`` names a scheme.

Two error types, split on who has to fix it:

``ImproperlyConfigured``
    a settings problem — the same class ``conf`` and ``registry`` raise, so
    ``manage.py check`` and startup surface it in one voice.
``ConfigurationError``
    a missing optional dependency, naming the extra to install. It is a
    ``ConnectorError``, so a Run records it as one rather than as a bare
    ``ImportError`` from inside a customer's pipeline.
"""

from django_connectors.secrets.base import SecretStore, get_secret_store
from django_connectors.secrets.encrypted import ModelSecretStore
from django_connectors.secrets.null import NullSecretStore
from django_connectors.secrets.settings_store import SettingsSecretStore

__all__ = [
    "ModelSecretStore",
    "NullSecretStore",
    "SecretStore",
    "SettingsSecretStore",
    "get_secret_store",
]
