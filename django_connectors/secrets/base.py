"""The SecretStore contract and its resolver.

A store is addressed by a ``(connection, key)`` pair rather than by a global
name so that no store implementation can be asked for "the token" without also
being told whose it is. Multi-tenancy is then a property of the interface
instead of a convention each implementation has to remember.

Imports Django and the standard library only, and resolves the configured
dotted path on first *use*: a host that never reaches a credential never pays
for whatever SDK its store is built on.
"""

from typing import Any, ClassVar

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_connectors.conf import SETTING_NAME, conf

SETTING_KEY = "SECRET_STORE"


class SecretStore:
    """Where a Connection's credential material actually lives.

    Implementations must hold to three rules, because callers depend on them:

    ``get`` returns ``None`` for "not stored"
        never a placeholder, and never a partial value. Auth backends turn that
        ``None`` into :class:`~django_connectors.exceptions.CredentialsRevoked`,
        which is what stops a Run from calling a provider with an empty token
        and burning quota against a 401.

    ``set`` either stores the value safely or raises
        silently downgrading to plaintext is the failure this whole package
        exists to prevent.

    nothing is ever logged, repr'd or interpolated into an exception message
        including on the failure paths. An error message says which key failed,
        never what it held.
    """

    #: ``{module_name: extra_name}`` — surfaced by the W005 system check.
    required_extras: ClassVar[dict[str, str]] = {}

    def get(self, connection, key: str) -> Any:
        """Return the value stored for `key`, or ``None`` if there is none."""
        raise NotImplementedError

    def set(self, connection, key: str, value: Any) -> None:
        """Store `value` under `key`, replacing any existing value."""
        raise NotImplementedError

    def delete(self, connection, key: str) -> bool:
        """Remove `key`. Returns whether anything was actually removed."""
        raise NotImplementedError

    def __str__(self) -> str:
        return type(self).__name__


_store: SecretStore | None = None


def get_secret_store() -> SecretStore:
    """Return the configured store, resolving the dotted path once.

    Cached rather than rebuilt per call: a store fronting an external secret
    manager typically holds a client with its own connection pool and auth
    handshake, and rebuilding it per credential lookup would turn one Run into
    one handshake per Binding.
    """
    global _store
    if _store is not None:
        return _store

    path = conf.require(SETTING_KEY)
    if not isinstance(path, str):
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['{SETTING_KEY}'] must be a dotted path string, "
            f"got {type(path).__name__}."
        )
    try:
        obj = import_string(path)
    except ImportError as exc:
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['{SETTING_KEY}'] = {path!r} could not be imported: {exc}"
        ) from exc

    # A path may name a class (instantiated once and reused) or an already
    # constructed object, matching how the source and auth registries resolve.
    if isinstance(obj, type):
        try:
            obj = obj()
        except Exception as exc:
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{SETTING_KEY}'] = {path!r} could not be "
                f"instantiated: {exc}"
            ) from exc

    if not isinstance(obj, SecretStore):
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['{SETTING_KEY}'] = {path!r} is not a SecretStore. "
            f"Subclass django_connectors.secrets.SecretStore."
        )

    _store = obj
    return _store


@receiver(setting_changed)
def _reset_store(*, setting: str, **kwargs: Any) -> None:
    """Keep `override_settings` honest, exactly as ``conf`` and ``registry`` do."""
    if setting == SETTING_NAME:
        global _store
        _store = None
