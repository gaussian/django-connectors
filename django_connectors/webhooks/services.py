"""WebhookSubscription lifecycle.

Every function takes explicit objects plus an optional `actor` — never a
``request`` — so the same call works from a view, a Celery task, a management
command or a test.

The renewal rule is the reason this module exists at all. Provider
subscriptions are short (Microsoft Graph maxes out at ~3 days, Google Drive
channels at 24h) and expiry is **silent**: no error, no final delivery, the
push traffic simply stops and the Binding quietly degrades to whatever polling
interval it happens to have. So renewal is driven from a sweep over
``(status, renew_at)`` — the index the model already declares — and ``renew_at``
is always strictly before ``expires_at``, never equal to it.
"""

import datetime as dt
import logging
from urllib.parse import urlsplit

from django.urls import NoReverseMatch, reverse
from django.utils import timezone

from django_connectors.enums import WebhookStatus
from django_connectors.errors import scrub
from django_connectors.exceptions import ConfigurationError
from django_connectors.models import WebhookSubscription
from django_connectors.registry import sources
from django_connectors.webhooks.base import WebhookRegistration, adapter_for

logger = logging.getLogger(__name__)

#: Reversed to build the callback URL. Matches ``webhooks/urls.py``.
CALLBACK_URL_NAME = "django_connectors_webhooks:receive"

# Renewal happens with a quarter of the remaining lifetime to spare, floored at
# five minutes. A fraction rather than a fixed lead because provider lifetimes
# span 24h to 3 days; the floor because a fraction of a very short lifetime is
# not enough time for one failed attempt plus one retry.
RENEW_LEAD_FRACTION = 0.25
MIN_RENEW_LEAD = dt.timedelta(minutes=5)
# How long to wait after a failed renewal before trying again. One transient
# 503 must not retire a subscription that is still hours from expiring.
RENEW_RETRY_INTERVAL = dt.timedelta(minutes=5)

# Statuses whose rows still have (or may still have) a live provider-side
# subscription behind them.
LIVE_STATUSES = (WebhookStatus.PENDING, WebhookStatus.ACTIVE)


def adapter_for_binding(binding):
    """Return this Binding's webhook adapter, or explain what is missing."""
    source_definition = sources.get(binding.source)
    adapter = adapter_for(source_definition)
    if adapter is None:
        raise ConfigurationError(
            f"source {binding.source!r} declares no webhook adapter, so it "
            f"cannot receive push notifications. Set `webhook = "
            f"MyAdapter()` on its SourceDefinition, or drive this binding by "
            f"polling only."
        )
    return adapter


def callback_path(public_id):
    """The path component of the callback URL for `public_id`."""
    try:
        return reverse(CALLBACK_URL_NAME, kwargs={"public_id": public_id})
    except NoReverseMatch as exc:
        raise ConfigurationError(
            f"could not reverse {CALLBACK_URL_NAME!r}. Include the webhook "
            f"URLs in your URLconf — e.g. path('connectors/webhooks/', "
            f"include('django_connectors.webhooks.urls')) — or pass an "
            f"explicit callback_url."
        ) from exc


def build_callback_url(subscription, base_url):
    """Absolute callback URL for `subscription` under `base_url`.

    The host supplies the origin rather than the library deriving it from a
    request: subscriptions are created from tasks and management commands where
    there is no request, and ``request.get_host()`` is attacker-influenced —
    a spoofed Host header would hand the provider someone else's callback URL.
    """
    if not base_url:
        raise ConfigurationError(
            "base_url is required to build a webhook callback URL, e.g. "
            "'https://app.example.com'. Providers require an absolute, "
            "publicly reachable URL."
        )
    parts = urlsplit(base_url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ConfigurationError(
            f"base_url must be an absolute http(s) origin, got {base_url!r}."
        )
    if parts.scheme != "https":
        # Not fatal — local development goes through http tunnels — but every
        # major provider refuses a plain-http callback outright.
        logger.warning(
            "webhook callback base_url is not https; most providers reject it"
        )
    return base_url.rstrip("/") + callback_path(subscription.public_id)


def renew_at_for(expires_at, *, now=None):
    """When to renew a subscription expiring at `expires_at`.

    Always strictly before expiry, and never in the past relative to `now`, so
    that a subscription created with a very short lifetime is renewed on the
    very next sweep instead of being scheduled for a moment already gone.
    """
    if expires_at is None:
        return None
    now = now or timezone.now()
    lifetime = expires_at - now
    if lifetime <= dt.timedelta(0):
        return now
    lead = min(max(lifetime * RENEW_LEAD_FRACTION, MIN_RENEW_LEAD), lifetime)
    return expires_at - lead


def create_subscription(
    binding, *, base_url=None, callback_url=None, resource="", metadata=None, actor=None
):
    """Create the local row, register it with the provider, return the row.

    The row is saved **before** the provider is called, because the callback URL
    embeds its ``public_id`` and the provider needs that URL as an argument. The
    row is also kept when registration fails, rather than rolled back: a
    provider may well have created its side before failing to answer us, and
    deleting the row would strand that subscription with nothing to attribute
    it to and no record of why.
    """
    adapter = adapter_for_binding(binding)

    subscription = WebhookSubscription.objects.create(
        binding=binding,
        resource=resource,
        metadata=dict(metadata or {}),
        status=WebhookStatus.PENDING,
    )
    url = callback_url or build_callback_url(subscription, base_url)

    # Deliberately not inside a transaction: this is an outbound HTTP call, and
    # holding a row lock across it would tie a database connection to a remote
    # provider's response time.
    try:
        registration = adapter.create(binding, url)
    except Exception as exc:
        logger.warning(
            "webhook registration failed for binding %s: %s",
            binding.id,
            scrub(exc),
        )
        _record_error(subscription, exc, status=WebhookStatus.FAILED)
        raise

    _apply_registration(subscription, registration, status=WebhookStatus.ACTIVE)

    if not binding.webhook_enabled:
        binding.webhook_enabled = True
        binding.save(update_fields=["webhook_enabled"])
    logger.info(
        "webhook subscription %s created for binding %s by %s",
        subscription.id,
        binding.id,
        actor or "system",
    )
    return subscription


def renew_subscription(subscription, *, actor=None, now=None):
    """Renew one subscription with its provider, now. Returns the row.

    The single implementation of what "renew" means: the sweep below is a loop
    over this, and so are the admin action and the API endpoint. Two code paths
    is how the admin's "renew" came to mean *make due* while the sweep meant
    *actually renew* — one of which is not renewal at all.

    **Raises on provider failure, after recording it.** The back-off in
    :func:`_record_renew_failure` has already been applied by the time the
    exception reaches the caller, so a caller that swallows it still leaves the
    subscription in a correct state, and a caller that reports it (the admin,
    the API) can say which subscription failed and why.
    """
    now = now or timezone.now()

    if subscription.status not in LIVE_STATUSES:
        # There is nothing on the provider's side left to extend: an expired,
        # failed or deleted subscription has to be created again. Renewing one
        # would write a fresh expiry onto a row no delivery will ever match.
        raise ConfigurationError(
            f"subscription {subscription.id} is {subscription.status}, so there "
            f"is no live provider subscription to renew. Create a new one."
        )

    try:
        adapter = adapter_for_binding(subscription.binding)
        registration = adapter.renew(subscription)
    except Exception as exc:
        logger.warning(
            "webhook renewal failed for subscription %s: %s",
            subscription.id,
            scrub(exc),
        )
        _record_renew_failure(subscription, exc, now=now)
        raise

    _apply_registration(
        subscription, registration, status=WebhookStatus.ACTIVE, now=now
    )
    logger.info(
        "webhook subscription %s renewed by %s", subscription.id, actor or "system"
    )
    return subscription


def renew_due_webhooks(*, now=None, limit=None):
    """Renew every active subscription whose `renew_at` has arrived.

    Returns ``{"renewed": [...], "failed": [...]}``. A failure does not retire
    the subscription unless it has already expired — one transient provider
    error must not turn a subscription that is still hours from expiry into a
    permanently dead one. That rule lives in :func:`renew_subscription`, which
    this is a loop over.
    """
    now = now or timezone.now()
    due = WebhookSubscription.objects.filter(
        status=WebhookStatus.ACTIVE,
        renew_at__isnull=False,
        renew_at__lte=now,
    ).select_related("binding")
    if limit:
        due = due[:limit]

    renewed, failed = [], []
    for subscription in due:
        try:
            renew_subscription(subscription, now=now)
        except Exception:
            # Already logged and backed off inside. One unreachable provider
            # must not abandon the rest of the sweep.
            failed.append(subscription)
        else:
            renewed.append(subscription)
    return {"renewed": renewed, "failed": failed}


def expire_subscriptions(*, now=None):
    """Mark every subscription past its expiry as expired. Returns how many.

    No provider call: an expired subscription is already gone on their side.
    Doing this in one UPDATE matters because the alternative — treating a stale
    row as live — leaves the receive view accepting deliveries for a
    subscription the provider has forgotten.
    """
    now = now or timezone.now()
    return WebhookSubscription.objects.filter(
        status__in=LIVE_STATUSES,
        expires_at__isnull=False,
        expires_at__lte=now,
    ).update(status=WebhookStatus.EXPIRED, updated_at=now)


def delete_subscription(subscription, *, actor=None):
    """Tear the subscription down, locally for certain and remotely if possible.

    The local row is marked deleted even when the provider call fails. The row
    is what authorises deliveries, so flipping it is the safe direction: the
    endpoint stops accepting immediately, and the orphaned provider-side
    subscription expires on its own. The error is kept in ``metadata`` so an
    operator can clean up rather than having to discover the leak.
    """
    error = None
    try:
        adapter = adapter_for_binding(subscription.binding)
        adapter.delete(subscription)
    except Exception as exc:
        error = exc
        logger.warning(
            "webhook teardown failed for subscription %s, marking deleted "
            "locally anyway: %s",
            subscription.id,
            scrub(exc),
        )

    _record_error(subscription, error, status=WebhookStatus.DELETED)

    binding = subscription.binding
    if binding.webhook_enabled and not (
        WebhookSubscription.objects.filter(binding=binding, status__in=LIVE_STATUSES)
        .exclude(pk=subscription.pk)
        .exists()
    ):
        binding.webhook_enabled = False
        binding.save(update_fields=["webhook_enabled"])
    logger.info(
        "webhook subscription %s deleted by %s", subscription.id, actor or "system"
    )
    return subscription


# --- internals -------------------------------------------------------------


def _apply_registration(subscription, registration, *, status, now=None):
    """Copy what the provider told us onto the row."""
    if registration is None:
        # Legitimate: a provider with no ids and no expiry has nothing to say.
        registration = WebhookRegistration()
    if not isinstance(registration, WebhookRegistration):
        raise ConfigurationError(
            f"webhook adapter returned {type(registration).__name__}; it must "
            f"return a WebhookRegistration (or None)."
        )

    now = now or timezone.now()
    if registration.external_id:
        subscription.external_id = registration.external_id
    if registration.external_resource_id:
        subscription.external_resource_id = registration.external_resource_id
    if registration.secret_reference:
        subscription.secret_reference = registration.secret_reference
    if registration.expires_at is not None:
        subscription.expires_at = registration.expires_at
    subscription.renew_at = registration.renew_at or renew_at_for(
        subscription.expires_at, now=now
    )

    metadata = dict(subscription.metadata or {})
    metadata.update(registration.metadata or {})
    # A successful round trip clears the previous failure; leaving it would
    # make a healthy subscription look broken forever in the admin.
    metadata.pop("last_error", None)
    metadata.pop("last_error_at", None)
    subscription.metadata = metadata

    subscription.status = status
    subscription.save(
        update_fields=[
            "external_id",
            "external_resource_id",
            "secret_reference",
            "expires_at",
            "renew_at",
            "metadata",
            "status",
            "updated_at",
        ]
    )
    return subscription


def _record_renew_failure(subscription, exc, *, now):
    """Back off and retry, unless the subscription is already past expiry."""
    subscription.metadata = _with_error(subscription.metadata, exc, now=now)
    fields = ["metadata", "updated_at"]

    expires_at = subscription.expires_at
    if expires_at is not None and expires_at <= now:
        subscription.status = WebhookStatus.FAILED
        fields.append("status")
    else:
        retry_at = now + RENEW_RETRY_INTERVAL
        subscription.renew_at = (
            min(retry_at, expires_at) if expires_at is not None else retry_at
        )
        fields.append("renew_at")
    subscription.save(update_fields=fields)
    return subscription


def _record_error(subscription, exc, *, status):
    subscription.metadata = _with_error(subscription.metadata, exc)
    subscription.status = status
    subscription.save(update_fields=["metadata", "status", "updated_at"])
    return subscription


def _with_error(metadata, exc, *, now=None):
    """Merge a scrubbed error into `metadata`.

    Scrubbed because ``metadata`` is rendered in the admin and returned by the
    API, and a provider's error body routinely quotes the request — including
    the bearer token that was on it.
    """
    merged = dict(metadata or {})
    if exc is None:
        merged.pop("last_error", None)
        merged.pop("last_error_at", None)
        return merged
    merged["last_error"] = scrub(exc)
    merged["last_error_at"] = (now or timezone.now()).isoformat()
    return merged
