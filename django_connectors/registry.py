"""Settings-driven registries for sources and auth backends.

Two rules make these safe inside Django's app loading:

**Nothing resolves at import time or during ``ready()``.** A dotted path is
turned into an object on first *use*. Reading a registry during ``ready()``
would make behaviour depend on ``INSTALLED_APPS`` ordering — the library would
see an empty registry while a system check running later saw a full one — and
nobody treats that ordering as semantic.

**Import cost is deferred.** A source module is free to ``import dlt`` at its own
module scope precisely because it is not imported until a Run needs it.

The target registry is deliberately *not* here: targets are host-owned and are
registered imperatively (see :mod:`django_connectors.projections.targets`).
"""

from typing import Any

from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils.module_loading import import_string

from django_connectors.conf import SETTING_NAME, conf


class LazyRegistry:
    """A ``{key: dotted_path}`` setting, resolved on first use and cached."""

    def __init__(self, setting_key: str, label: str) -> None:
        self.setting_key = setting_key
        self.label = label
        self._resolved: dict[str, Any] = {}

    @property
    def paths(self) -> dict[str, str]:
        value = getattr(conf, self.setting_key)
        if not isinstance(value, dict):
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{self.setting_key}'] must be a dict of "
                f"{{key: dotted_path}}, got {type(value).__name__}."
            )
        return value

    def keys(self) -> list[str]:
        return sorted(self.paths)

    def __contains__(self, key: object) -> bool:
        return key in self.paths

    def get(self, key: str) -> Any:
        """Resolve `key`, or raise ImproperlyConfigured naming what is wrong."""
        if key in self._resolved:
            return self._resolved[key]

        try:
            path = self.paths[key]
        except KeyError:
            known = ", ".join(self.keys()) or "(none registered)"
            raise ImproperlyConfigured(
                f"Unknown {self.label} {key!r}. "
                f"Add it to {SETTING_NAME}['{self.setting_key}']. Registered: {known}."
            ) from None

        try:
            obj = import_string(path)
        except ImportError as exc:
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{self.setting_key}'][{key!r}] = {path!r} "
                f"could not be imported: {exc}"
            ) from exc

        # A path may name a class (instantiated once and reused) or an already
        # constructed object. Classes are the common case and cheaper to write.
        if isinstance(obj, type):
            try:
                obj = obj()
            except Exception as exc:
                raise ImproperlyConfigured(
                    f"{SETTING_NAME}['{self.setting_key}'][{key!r}] = {path!r} "
                    f"could not be instantiated: {exc}"
                ) from exc

        self._resolved[key] = obj
        return obj

    def all(self) -> dict[str, Any]:
        return {key: self.get(key) for key in self.keys()}

    def _reset(self) -> None:
        self._resolved.clear()


sources = LazyRegistry("SOURCES", "source")
auth_backends = LazyRegistry("AUTH_BACKENDS", "auth backend")


@receiver(setting_changed)
def _reset_registries(*, setting: str, **kwargs: Any) -> None:
    if setting == SETTING_NAME:
        sources._reset()
        auth_backends._reset()
