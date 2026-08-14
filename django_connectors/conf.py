"""Typed access to the ``DJANGO_CONNECTORS`` setting.

Every key has a default and every key is validated, because this library's whole
integration surface is host-supplied strings — dotted paths, a landing DSN,
registry keys — which means misconfiguration is the dominant failure mode. A
typo in a setting name must not silently take the default forever.

Imports Django and the standard library only: this module is reachable from
``AppConfig.ready()`` and from system checks, both of which run before the app
registry is usable and long before anyone wants to pay for ``import dlt``.
"""

import datetime as dt
from typing import Any

from django.conf import settings as django_settings
from django.core.exceptions import ImproperlyConfigured
from django.core.signals import setting_changed
from django.dispatch import receiver

SETTING_NAME = "DJANGO_CONNECTORS"

DEFAULTS: dict[str, Any] = {
    # --- landing -----------------------------------------------------------
    # SQLAlchemy DSN for the landing database. MySQL and PostgreSQL are both
    # supported and both covered by the `serverdb` test tier:
    #   "mysql+pymysql://user:pw@host:3306/connectors_landing"
    #   "postgresql+psycopg2://user:pw@host:5432/connectors_landing"
    # Reached only through dlt; deliberately NOT a Django DATABASES alias, so
    # that no ORM model can ever be routed there.
    "LANDING_URL": None,
    "LANDING_DATASET": "connectors_landing",
    # Where dlt keeps pipeline working state. Must be durable across a Run for
    # pending-package recovery to work; an ephemeral worker filesystem loses
    # in-flight load packages (incremental cursors survive — they restore from
    # the destination — but a partially loaded package does not).
    "PIPELINES_DIR": None,
    # --- registries (key -> dotted path), resolved lazily on first use ------
    "SOURCES": {},
    "AUTH_BACKENDS": {},
    # --- secrets -----------------------------------------------------------
    # Default refuses to store anything rather than silently writing plaintext.
    "SECRET_STORE": "django_connectors.secrets.NullSecretStore",
    # "none" mirrors django-allauth's trust model (SocialToken is a plaintext
    # column); "fernet" encrypts at rest. Must be chosen explicitly — there is
    # no default, so nobody stores plaintext by accident.
    "SECRET_ENCRYPTION": None,
    # Fernet key for SECRET_ENCRYPTION="fernet". Deliberately NOT
    # settings.SECRET_KEY: rotating that would destroy every stored credential.
    "SECRET_KEY": None,
    # --- execution ---------------------------------------------------------
    "DEFAULT_RUN_TIMEOUT": dt.timedelta(hours=6),
    "DEFAULT_POLL_INTERVAL": dt.timedelta(minutes=15),
    # --- projection --------------------------------------------------------
    "PROJECTION_BATCH_SIZE": 1000,
    # How far back the projection sweeper looks for un-projected loads. This is
    # the auto-heal horizon: a ProjectionRun that fails and is never retried
    # within this window is never picked up again.
    "PROJECTION_SWEEP_LOOKBACK": dt.timedelta(days=7),
    "PREVIEW_MAX_ROWS": 50,
    "SAMPLE_MAX_ROWS": 100,
    "PREVIEW_MAX_BYTES": 1024 * 1024,
    # --- webhooks ----------------------------------------------------------
    "WEBHOOK_MAX_BODY_BYTES": 64 * 1024,
    "WEBHOOK_DEDUPE_TTL": dt.timedelta(minutes=10),
    "WEBHOOK_DEBOUNCE": dt.timedelta(seconds=30),
    "WEBHOOK_RATE_LIMIT_PER_MINUTE": 120,
    # --- sources -----------------------------------------------------------
    # Lift the REST source's SSRF guard, which otherwise refuses any host
    # resolving to a private, loopback, link-local or metadata address. Hosts
    # with genuinely internal APIs need this; the default denies.
    "REST_ALLOW_PRIVATE_ADDRESSES": False,
    # --- optional DRF API --------------------------------------------------
    # Dotted path to callable(request) -> the owning object, or
    # (content_type, object_id). Only the host knows what owns a Connection.
    # Unset means every API request is denied — the safe default.
    "API_OWNER_RESOLVER": None,
    # Dotted paths to DRF permission classes. Unset means DenyAll: DRF's own
    # default is AllowAny, so a host following install instructions verbatim
    # would otherwise publish connector data unauthenticated.
    "API_PERMISSION_CLASSES": (),
    # --- admin -------------------------------------------------------------
    "ADMIN_ACTION_MAX_SELECTION": 50,
    # --- errors ------------------------------------------------------------
    "ERROR_MESSAGE_MAX_LENGTH": 4096,
}


class Settings:
    """Lazy, validated, cached view over ``settings.DJANGO_CONNECTORS``."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in DEFAULTS:
            raise AttributeError(
                f"{name!r} is not a django-connectors setting. "
                f"Valid settings: {', '.join(sorted(DEFAULTS))}."
            )
        value = self._user_settings.get(name, DEFAULTS[name])
        # Cache on the instance; _reset() clears it when settings change.
        self.__dict__[name] = value
        return value

    @property
    def _user_settings(self) -> dict[str, Any]:
        cached = self.__dict__.get("_user_settings_cache")
        if cached is not None:
            return cached
        user = getattr(django_settings, SETTING_NAME, None) or {}
        if not isinstance(user, dict):
            raise ImproperlyConfigured(
                f"settings.{SETTING_NAME} must be a dict, got {type(user).__name__}."
            )
        unknown = set(user) - set(DEFAULTS)
        if unknown:
            raise ImproperlyConfigured(
                f"Unknown {SETTING_NAME} setting(s): {', '.join(sorted(unknown))}. "
                f"Valid settings: {', '.join(sorted(DEFAULTS))}."
            )
        self.__dict__["_user_settings_cache"] = user
        return user

    def require(self, name: str) -> Any:
        """Return a setting that has no usable default, or explain what to set."""
        value = getattr(self, name)
        if value in (None, ""):
            raise ImproperlyConfigured(
                f"{SETTING_NAME}['{name}'] must be set. "
                f"Add it to settings.{SETTING_NAME}."
            )
        return value

    def is_set(self, name: str) -> bool:
        return name in self._user_settings

    def _reset(self) -> None:
        self.__dict__.clear()


conf = Settings()


@receiver(setting_changed)
def _reset_conf(*, setting: str, **kwargs: Any) -> None:
    """Keep `override_settings` honest.

    Without this a test that overrides DJANGO_CONNECTORS would read whatever the
    first test to touch the setting happened to cache.
    """
    if setting == SETTING_NAME:
        conf._reset()
