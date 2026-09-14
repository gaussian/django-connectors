"""The WebhookAdapter contract.

An adapter owns everything provider-specific about push delivery: creating the
subscription, keeping it alive, tearing it down, and — the part that matters —
proving that a request on the callback URL really came from the provider.

``verify()`` is abstract, and deliberately has **no** base implementation. The
receive view is ``csrf_exempt`` (a provider cannot send a CSRF token), so
``verify()`` is the *only* thing standing between an unauthenticated POST and a
Run being scheduled. A base class that returned ``True`` by default, or that
quietly skipped verification when ``secret_reference`` was blank, would turn
every adapter that forgot to override it into an open trigger. Because
``verify`` is declared with ``@abstractmethod``, a subclass that omits it cannot
be *instantiated* at all — which for the intended usage (``webhook =
MyAdapter()`` in a source module) fails loudly at import time, long before a
delivery arrives.

Adapters hang off the SourceDefinition::

    class MyGraphSource(SourceDefinition):
        key = "graph"
        webhook = MyGraphWebhookAdapter()

There is deliberately no webhook registry: an adapter is meaningless without
the source whose data it announces, and a fourth registry would be a fourth
place for a key to be misspelt.
"""

import abc
import datetime as dt
import hashlib
import hmac
from dataclasses import dataclass, field
from typing import ClassVar

#: The attribute a SourceDefinition exposes its adapter on.
ADAPTER_ATTRIBUTE = "webhook"


@dataclass(frozen=True)
class WebhookRegistration:
    """What a provider told us after we created or renewed a subscription.

    Returned by :meth:`WebhookAdapter.create` and :meth:`WebhookAdapter.renew`
    and copied onto the ``WebhookSubscription`` row by the lifecycle services.
    Every field is optional because providers differ wildly in what they hand
    back — Graph returns an id and an expiry, a plain HTTP callback returns
    nothing at all.

    ``secret_reference`` is a *handle* resolved through the host's SecretStore,
    never the shared secret itself: the row it lands on is rendered in the admin
    and returned by the API.
    """

    external_id: str = ""
    external_resource_id: str = ""
    expires_at: dt.datetime | None = None
    #: When to renew. Left None, the services derive it from ``expires_at``.
    renew_at: dt.datetime | None = None
    secret_reference: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class WebhookDelivery:
    """What :meth:`WebhookAdapter.parse` made of one delivery.

    Only ``delivery_id`` is load-bearing: it is the replay-protection key. Give
    it the provider's own per-delivery identifier (Graph's ``subscriptionId`` +
    ``changeType`` is *not* one; GitHub's ``X-GitHub-Delivery`` is). Leave it
    blank and the view falls back to hashing the raw body, which is correct but
    coarser — two genuinely distinct events with byte-identical bodies inside
    the dedupe window collapse into one, and only the next poll separates them.
    """

    delivery_id: str = ""
    #: Provider-side ids the delivery mentioned. Informational in v0.1: the
    #: whole point of the design is that a delivery schedules a full source run
    #: rather than trying to ingest the payload it carries.
    external_ids: tuple[str, ...] = ()


class WebhookAdapter(abc.ABC):
    """Provider-side subscription lifecycle plus delivery authentication."""

    #: ``{module_name: extra_name}`` — surfaced by the W005 system check, the
    #: same contract SourceDefinition uses. An optional dependency must be
    #: imported inside a method so a missing one is a ConfigurationError naming
    #: the extra rather than a bare ImportError inside a customer's Run.
    required_extras: ClassVar[dict[str, str]] = {}

    def create(self, binding, callback_url):
        """Register `callback_url` with the provider. Return a WebhookRegistration.

        Called with the ``WebhookSubscription`` row already saved, because
        ``callback_url`` embeds its unguessable ``public_id`` — the row has to
        exist before the URL can be computed.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support creating subscriptions"
        )

    def renew(self, subscription):
        """Extend the provider-side subscription. Return a WebhookRegistration.

        Providers expire these fast — Graph maxes out at ~3 days — and a missed
        renewal stops delivery *silently*, so this must not be optional for any
        adapter whose provider sets an expiry.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support renewing subscriptions"
        )

    def delete(self, subscription):
        """Tear the subscription down at the provider. Return None."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support deleting subscriptions"
        )

    @abc.abstractmethod
    def verify(self, subscription, request):
        """Return True iff `request` genuinely came from the provider.

        Abstract on purpose — see the module docstring. Return False or raise
        to reject; either way the view answers 401 with a fixed body and never
        reflects anything from the request.

        Implementations must compare secrets with :meth:`secrets_equal` (or
        ``hmac.compare_digest`` directly). ``==`` on a signature leaks its
        prefix through timing, and the attacker here controls both the body and
        the number of attempts.
        """
        raise NotImplementedError

    def parse(self, subscription, request):
        """Return a :class:`WebhookDelivery` describing this request.

        The default extracts nothing, which makes the view fall back to hashing
        the body for replay protection. Override to supply the provider's own
        delivery id, which is both cheaper and exact.
        """
        return WebhookDelivery()

    # --- helpers available to every adapter --------------------------------

    @staticmethod
    def secrets_equal(expected, provided):
        """Constant-time comparison of two secrets or signatures.

        Returns False for a missing value rather than True: an adapter whose
        secret failed to resolve must reject the delivery, not accept every
        delivery.
        """
        if not expected or not provided:
            return False
        if isinstance(expected, str):
            expected = expected.encode()
        if isinstance(provided, str):
            provided = provided.encode()
        return hmac.compare_digest(expected, provided)

    @staticmethod
    def hmac_hexdigest(secret, body, *, algorithm="sha256"):
        """HMAC of `body` under `secret`, as hex — the common signature shape."""
        if isinstance(secret, str):
            secret = secret.encode()
        if isinstance(body, str):
            body = body.encode()
        return hmac.new(secret, body, getattr(hashlib, algorithm)).hexdigest()


def adapter_for(source_definition):
    """Return the adapter hanging off `source_definition`, or None.

    ``getattr`` rather than an attribute access so that a SourceDefinition
    written before webhooks existed keeps working; "this source has no adapter"
    is a legitimate, common state, not an error.
    """
    return getattr(source_definition, ADAPTER_ATTRIBUTE, None)
