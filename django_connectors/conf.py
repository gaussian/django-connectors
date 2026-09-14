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
    # dlt creates no index on a landing table and merge is `DELETE ... WHERE
    # EXISTS`, so merge cost is linear in table size without one — measured at
    # 179s for 10,000 rows into a 60,000-row table on MySQL. The library issues
    # the missing CREATE INDEX itself after a successful load. Set False only
    # if the landing role has no DDL rights or the indexes are managed
    # elsewhere; a Binding then stays fast only for as long as it stays small.
    "PROVISION_LANDING_INDEXES": True,
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
    # How a successful Run hands its ProjectionRuns on. None runs them inline,
    # in the ingestion worker, while the Binding's lease is still held — so a
    # slow writer counts against run_timeout. A dotted path to a
    # callable(projection_run) enqueues instead; the celery extra ships
    # "django_connectors.scheduler.celery.enqueue_projection_run". The sweeper
    # heals whatever a queue drops, exactly as it heals a dropped inline call.
    "PROJECTION_DISPATCH": None,
    # A dispatched ProjectionRun still QUEUED after this long is one the queue
    # dropped; the sweeper executes it itself. Long enough that a busy queue is
    # not second-guessed, short enough that a lost message is not a day.
    "PROJECTION_QUEUE_GRACE": dt.timedelta(minutes=10),
    # How far back the projection sweeper looks for un-projected loads. This is
    # the auto-heal horizon: a ProjectionRun that fails and is never retried
    # within this window is never picked up again.
    "PROJECTION_SWEEP_LOOKBACK": dt.timedelta(days=7),
    # How many per-row reasons a ProjectionRun keeps in `warnings`. The count
    # in `records_skipped` is always exact; only the explanations are capped.
    "PROJECTION_MAX_WARNINGS": 100,
    # Items per page from SourceDefinition.discover(); also the ceiling a
    # client may ask for, so one request cannot pull a whole drive listing.
    "DISCOVERY_PAGE_SIZE": 100,
    "PREVIEW_MAX_ROWS": 50,
    "SAMPLE_MAX_ROWS": 100,
    "PREVIEW_MAX_BYTES": 1024 * 1024,
    # --- content -----------------------------------------------------------
    # Ceiling on one fetch_record_content() call. A worker's memory is finite
    # and "the customer uploaded a 4GB video into the reports folder" is a
    # Tuesday; a host asking for more than this gets the ceiling.
    "CONTENT_MAX_BYTES": 32 * 1024 * 1024,
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
