"""The webhook receive endpoint.

Plain Django, no DRF: this endpoint must work in hosts that do not install DRF,
and it deliberately shares nothing with the host's API — no session auth, no
throttle classes, no content negotiation, no versioning. The provider proves
itself with a signature; everything else is attack surface.

**A delivery never carries data into the landing tables.** It marks the Binding
changed and schedules exactly the dlt source that polling would run. Two
consequences make the whole design work: webhooks and polling cannot disagree,
because there is only one ingestion path; and a dropped delivery loses nothing,
because the next poll reconciles. That is why every rejection below is safe —
the worst case is latency, never data loss.

The order of operations is load-bearing:

1. **Resolve the subscription** — an unknown or non-active ``public_id`` is a
   flat 404 with a fixed body, identical whether the id never existed or was
   deleted this morning. Anything else is an enumeration oracle.
2. **Rate limit** — before the body is touched, so a flood costs one cache
   round trip rather than a 64KB read plus an HMAC.
3. **Cap the body** — before parsing *or* verification. Verifying first would
   mean HMAC-ing attacker-chosen megabytes; parsing first would mean handing
   them to a JSON decoder. The declared ``Content-Length`` is rejected first so
   an oversized body is refused without being read at all.
4. **Verify** — the endpoint is ``csrf_exempt`` (a provider cannot send a CSRF
   token), which makes verification the only authentication there is. A missing
   adapter, a False return and a raised exception are all the same 401.
5. **Replay-protect** — a cache-backed, TTL-bounded record of delivery ids.
   Providers retry aggressively on any non-2xx and some retry on 2xx as well.
6. **Debounce and schedule** — one Run per burst, with ``Binding.dirty``
   carrying the tail.

No response and no stored field ever contains anything from the request. Echoing
a header or a payload fragment back turns this endpoint into a reflector, and
storing one puts attacker text into the admin.
"""

import hashlib
import logging
import time

from django.core.cache import cache
from django.core.exceptions import RequestDataTooBig, TooManyFieldsSent
from django.http import HttpResponseNotAllowed, JsonResponse, UnreadablePostError
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt

from django_connectors.conf import conf
from django_connectors.enums import RunTrigger, WebhookStatus
from django_connectors.errors import scrub
from django_connectors.models import Binding, WebhookSubscription

logger = logging.getLogger(__name__)

CACHE_PREFIX = "django_connectors:webhook"

# Fixed response bodies. Never interpolate anything from the request into these.
_NOT_FOUND = {"detail": "not found"}
_UNAUTHORIZED = {"detail": "verification failed"}
_TOO_LARGE = {"detail": "payload too large"}
_TOO_MANY = {"detail": "too many requests"}
_MALFORMED = {"detail": "malformed request"}


@csrf_exempt
def receive(request, public_id):
    """Accept one provider delivery for the subscription at `public_id`."""
    if request.method != "POST":
        # Providers POST. Anything else — including a browser GET probing the
        # URL — gets a bare 405 and never reaches verification.
        return HttpResponseNotAllowed(["POST"])

    subscription = _active_subscription(public_id)
    if subscription is None:
        return JsonResponse(_NOT_FOUND, status=404)

    if not _within_rate_limit(subscription):
        response = JsonResponse(_TOO_MANY, status=429)
        response["Retry-After"] = "60"
        return response

    body, oversized = _read_body(request)
    if oversized:
        return JsonResponse(_TOO_LARGE, status=413)
    if body is None:
        return JsonResponse(_MALFORMED, status=400)

    adapter = _adapter_or_none(subscription)
    if adapter is None or not _verify(adapter, subscription, request):
        return JsonResponse(_UNAUTHORIZED, status=401)

    if _is_replay(adapter, subscription, request, body):
        # 200, not 4xx: the delivery was genuine and already accounted for, and
        # a non-2xx would make the provider retry it forever.
        return JsonResponse({"status": "duplicate"}, status=200)

    _touch(subscription)
    return JsonResponse({"status": _schedule(subscription.binding)}, status=202)


# --- steps -----------------------------------------------------------------


def _active_subscription(public_id):
    """The subscription for `public_id`, iff it may currently accept deliveries.

    Only ``active`` qualifies. A pending row has not finished registering, and
    expired/failed/deleted rows must stop being an entry point the moment they
    leave service — that is the whole value of tearing one down.
    """
    return (
        WebhookSubscription.objects.filter(
            public_id=public_id, status=WebhookStatus.ACTIVE
        )
        .select_related("binding", "binding__connection")
        .first()
    )


def _within_rate_limit(subscription):
    """Fixed-window counter, per subscription, per minute.

    Best-effort by construction: with a per-process cache this bounds one
    worker, not the fleet. That is acceptable precisely because rejecting a
    delivery is not lossy — but it is why production deployments want a shared
    cache backend, and why the limit is a floor on cost rather than a security
    control.
    """
    limit = conf.WEBHOOK_RATE_LIMIT_PER_MINUTE
    if not limit:
        return True
    window = int(time.time()) // 60
    key = f"{CACHE_PREFIX}:rate:{subscription.public_id}:{window}"
    # add() then incr(): add is atomic, so two workers racing on the first
    # request of a window cannot both start the counter at 1.
    if cache.add(key, 1, timeout=120):
        return True
    try:
        count = cache.incr(key)
    except ValueError:
        # The key expired between add() and incr(). Re-seed rather than
        # treating an expiry as a limit breach.
        cache.add(key, 1, timeout=120)
        return True
    return count <= limit


def _read_body(request):
    """Return ``(body, oversized)``, refusing anything over the configured cap.

    ``Content-Length`` is checked first so an oversized body is rejected before
    it is read; the actual length is checked afterwards because the header is
    attacker-supplied and chunked requests do not send one at all.
    """
    cap = conf.WEBHOOK_MAX_BODY_BYTES
    declared = request.META.get("CONTENT_LENGTH")
    if declared:
        try:
            if int(declared) > cap:
                return None, True
        except (TypeError, ValueError):
            return None, True

    try:
        body = request.body
    except (RequestDataTooBig, TooManyFieldsSent):
        # Django's own DATA_UPLOAD_MAX_* guard fired first.
        return None, True
    except UnreadablePostError:
        # Client hung up mid-body. Nothing to verify and nothing to schedule.
        return None, False

    if len(body) > cap:
        return None, True
    return body, False


def _adapter_or_none(subscription):
    """The Binding's adapter, or None — which the caller turns into a 401.

    A misconfigured source must not be able to make an unauthenticated delivery
    succeed, so "no adapter" is failed closed and logged, never skipped.
    """
    from django_connectors.webhooks.services import adapter_for_binding

    try:
        return adapter_for_binding(subscription.binding)
    except Exception as exc:
        logger.warning(
            "webhook delivery for subscription %s has no usable adapter: %s",
            subscription.id,
            scrub(exc),
        )
        return None


def _verify(adapter, subscription, request):
    """True iff the adapter vouched for this request.

    A raised exception is a rejection, not a 500: an adapter that cannot resolve
    its signing secret, or that trips over a malformed header, has by definition
    not authenticated the caller.
    """
    try:
        verified = adapter.verify(subscription, request)
    except Exception as exc:
        logger.warning(
            "webhook verification raised for subscription %s: %s",
            subscription.id,
            scrub(exc),
        )
        return False
    if not verified:
        logger.warning(
            "webhook verification rejected a delivery for subscription %s",
            subscription.id,
        )
        return False
    return True


def _is_replay(adapter, subscription, request, body):
    """True iff this exact delivery has already been accepted recently.

    The key is scoped to the subscription so one tenant's delivery id can never
    suppress another's, and hashed so that a provider id — which can be long,
    can contain anything, and is attacker-influenced in the general case —
    never reaches a cache key verbatim.
    """
    ttl = conf.WEBHOOK_DEDUPE_TTL.total_seconds()
    if ttl <= 0:
        return False

    fingerprint = _delivery_fingerprint(adapter, subscription, request, body)
    if fingerprint is None:
        # Nothing identifies this delivery: no provider id and an empty body.
        # Deduplicating on "nothing" would collapse every distinct delivery in
        # the window, so this falls through to the debounce instead.
        return False

    key = f"{CACHE_PREFIX}:seen:{subscription.public_id}:{fingerprint}"
    return not cache.add(key, 1, timeout=int(ttl))


def _delivery_fingerprint(adapter, subscription, request, body):
    try:
        delivery = adapter.parse(subscription, request)
    except Exception as exc:
        logger.warning(
            "webhook parse raised for subscription %s: %s",
            subscription.id,
            scrub(exc),
        )
        delivery = None

    delivery_id = getattr(delivery, "delivery_id", "") or ""
    if delivery_id:
        return hashlib.sha256(delivery_id.encode("utf-8", "replace")).hexdigest()
    if body:
        return hashlib.sha256(body).hexdigest()
    return None


def _touch(subscription):
    """Record that a delivery arrived, without racing a concurrent one."""
    WebhookSubscription.objects.filter(pk=subscription.pk).update(
        last_notification_at=timezone.now()
    )


def _schedule(binding):
    """Coalesce this delivery into at most one Run, and say what happened.

    Two mechanisms, and both are needed:

    ``cache.add`` debounce
        The first delivery in a ``WEBHOOK_DEBOUNCE`` window queues a Run; the
        rest of the burst does not. ``add`` is atomic, so two workers handling
        two deliveries of the same burst cannot both win.

    ``Binding.dirty``
        A debounced delivery still happened. Without this flag, a change
        arriving one second after the window opened — or while the Run it
        opened is already extracting — would simply be forgotten until the next
        poll. ``dirty`` is set instead, and the scheduler clears it as it claims
        the follow-up Run, so a whole burst tail produces exactly one more Run
        rather than one per delivery.

    ``enqueue_run`` itself is the third layer: its unique ``dedupe_key`` means
    at most one queued Run exists per Binding, so even a debounce miss cannot
    produce a queue pile-up.
    """
    from django_connectors.services.runs import enqueue_run

    if not (binding.is_runnable and binding.connection.is_usable):
        # Blocked, disabled or revoked. Remember that something changed so the
        # Binding syncs as soon as it is usable again, but do not queue a Run
        # that is guaranteed to fail.
        _mark_dirty(binding)
        return "deferred"

    debounce = conf.WEBHOOK_DEBOUNCE.total_seconds()
    key = f"{CACHE_PREFIX}:debounce:{binding.id}"
    if debounce > 0 and not cache.add(key, 1, timeout=int(debounce)):
        _mark_dirty(binding)
        return "debounced"

    enqueue_run(binding, trigger=RunTrigger.WEBHOOK)
    return "accepted"


def _mark_dirty(binding):
    # Queryset update, not instance save: this races with the scheduler
    # clearing the flag, and it must not clobber any other column.
    Binding.objects.filter(pk=binding.pk).update(dirty=True)
