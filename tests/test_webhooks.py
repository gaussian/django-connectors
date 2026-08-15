"""Webhook lifecycle and the receive endpoint.

The adapter used throughout signs deliveries with a real HMAC over the raw body,
because the properties under test — constant-time comparison, "verification
happens after the size cap", "a raising verifier is a 401" — are meaningless
against a stub that returns True.

This module doubles as the URLconf for its own tests (``urlpatterns`` at the
bottom), so the callback URL the services hand a provider is the same one the
client posts to, reversed the same way.
"""

import datetime as dt
import json
import sys
import types
import uuid

import pytest
from django.core.cache import cache
from django.test import Client
from django.urls import include, path, reverse
from django.utils import timezone

from django_connectors.enums import (
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
    WebhookStatus,
)
from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.models import Run, WebhookSubscription
from django_connectors.sources.base import SourceDefinition
from django_connectors.webhooks import services as webhook_services
from django_connectors.webhooks.base import (
    WebhookAdapter,
    WebhookDelivery,
    WebhookRegistration,
)

pytestmark = pytest.mark.django_db

SECRET = "shared-signing-secret"
SIGNATURE_HEADER = "X-Signature"
DELIVERY_HEADER = "X-Delivery-Id"

#: Mutable per-test control/recording surface for the adapter below. A module
#: global rather than instance state because the registry instantiates the
#: SourceDefinition itself and caches it — the test never holds the instance.
STATE: dict = {}


class RecordingAdapter(WebhookAdapter):
    """A realistic HMAC-over-body adapter that records what it was asked."""

    def create(self, binding, callback_url):
        STATE["created"].append(callback_url)
        if STATE.get("create_raises"):
            raise SourceError("provider refused the subscription")
        return STATE.get("registration", WebhookRegistration(external_id="sub-1"))

    def renew(self, subscription):
        STATE["renewed"].append(subscription.id)
        if STATE.get("renew_raises"):
            raise SourceError("provider refused the renewal")
        return STATE.get("renewal", WebhookRegistration())

    def delete(self, subscription):
        STATE["deleted"].append(subscription.id)
        if STATE.get("delete_raises"):
            raise SourceError("provider refused the teardown")

    def verify(self, subscription, request):
        STATE["verified"].append(subscription.id)
        if STATE.get("verify_raises"):
            raise RuntimeError("secret store unreachable")
        expected = self.hmac_hexdigest(SECRET, request.body)
        return self.secrets_equal(expected, request.headers.get(SIGNATURE_HEADER, ""))

    def parse(self, subscription, request):
        STATE["parsed"].append(subscription.id)
        return WebhookDelivery(delivery_id=request.headers.get(DELIVERY_HEADER, ""))


class HookedSource(SourceDefinition):
    key = "hooked"
    provider = "hooked"
    # The whole registration contract: the adapter hangs off the source.
    webhook = RecordingAdapter()


class BareSource(SourceDefinition):
    """A source with no webhook adapter at all — the fail-closed case."""

    key = "bare"
    provider = "bare"


HOOKED_PATH = "tests.test_webhooks.HookedSource"
BARE_PATH = "tests.test_webhooks.BareSource"


@pytest.fixture(autouse=True)
def clean_state():
    STATE.clear()
    STATE.update(created=[], renewed=[], deleted=[], verified=[], parsed=[])
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def webhook_settings(settings):
    """Configure the sources and this module as the URLconf.

    Returns a callable so a test can override one connectors setting without
    restating the whole dict — assigning to ``settings.DJANGO_CONNECTORS``
    fires ``setting_changed``, which resets both the conf cache and the
    registries.
    """

    def configure(**overrides):
        settings.ROOT_URLCONF = "tests.test_webhooks"
        settings.DJANGO_CONNECTORS = {
            "SOURCES": {"hooked": HOOKED_PATH, "bare": BARE_PATH},
            **overrides,
        }
        return settings.DJANGO_CONNECTORS

    configure()
    return configure


@pytest.fixture
def binding(make_binding):
    return make_binding(source="hooked")


@pytest.fixture
def subscription(binding):
    return WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.ACTIVE, resource="messages"
    )


def url_for(subscription):
    return reverse(
        "django_connectors_webhooks:receive",
        kwargs={"public_id": subscription.public_id},
    )


def deliver(
    client, subscription, body=b'{"ok":true}', *, delivery_id="d1", sign=True, **extra
):
    headers = {DELIVERY_HEADER: delivery_id}
    if sign:
        headers[SIGNATURE_HEADER] = WebhookAdapter.hmac_hexdigest(SECRET, body)
    elif sign is not None:
        headers[SIGNATURE_HEADER] = "0" * 64
    return client.post(
        url_for(subscription),
        data=body,
        content_type="application/json",
        headers=headers,
        **extra,
    )


# --- the adapter contract --------------------------------------------------


def test_an_adapter_that_forgets_verify_cannot_be_instantiated():
    """The endpoint is csrf_exempt, so a default-True verify would be an open door."""

    class Forgetful(WebhookAdapter):
        def create(self, binding, callback_url):
            return WebhookRegistration()

    with pytest.raises(TypeError, match="verify"):
        Forgetful()


def test_unsupported_lifecycle_operations_name_the_adapter():
    """A provider that needs no renewal is normal; a silent no-op is not."""

    class MinimalAdapter(WebhookAdapter):
        def verify(self, subscription, request):
            return True

    minimal = MinimalAdapter()
    for call in (
        lambda: minimal.create(None, "https://example.test/x"),
        lambda: minimal.renew(None),
        lambda: minimal.delete(None),
    ):
        with pytest.raises(NotImplementedError, match="MinimalAdapter"):
            call()


def test_secrets_equal_never_accepts_a_missing_value():
    """An adapter whose secret failed to resolve must reject, not accept all."""
    assert WebhookAdapter.secrets_equal("abc", "abc") is True
    assert WebhookAdapter.secrets_equal("abc", "abd") is False
    assert WebhookAdapter.secrets_equal("", "") is False
    assert WebhookAdapter.secrets_equal(None, "abc") is False
    assert WebhookAdapter.secrets_equal("abc", None) is False
    assert WebhookAdapter.secrets_equal(b"abc", "abc") is True


# --- the receive view ------------------------------------------------------


def test_unknown_public_id_is_indistinguishable_from_a_retired_one(
    webhook_settings, subscription
):
    client = Client()
    unknown = types.SimpleNamespace(public_id=uuid.uuid4())
    missing = client.post(url_for(unknown), data=b"{}", content_type="application/json")

    subscription.status = WebhookStatus.DELETED
    subscription.save(update_fields=["status"])
    retired = deliver(client, subscription)

    assert missing.status_code == retired.status_code == 404
    assert missing.content == retired.content
    assert not STATE["verified"]


def test_a_signed_delivery_queues_exactly_one_webhook_run(
    webhook_settings, subscription
):
    response = deliver(Client(), subscription)

    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}

    run = Run.objects.get()
    assert run.binding_id == subscription.binding_id
    assert run.trigger == RunTrigger.WEBHOOK
    assert run.status == RunStatus.QUEUED
    # The queue slot is claimed, which is what collapses the rest of the burst.
    assert run.dedupe_key == subscription.binding_id

    subscription.refresh_from_db()
    assert subscription.last_notification_at is not None


def test_an_unsigned_delivery_is_rejected_and_schedules_nothing(
    webhook_settings, subscription
):
    response = deliver(Client(), subscription, sign=False)

    assert response.status_code == 401
    assert not Run.objects.exists()


def test_a_verifier_that_raises_is_a_401_not_a_500(webhook_settings, subscription):
    STATE["verify_raises"] = True

    response = deliver(Client(), subscription)

    assert response.status_code == 401
    assert not Run.objects.exists()


def test_a_source_with_no_adapter_fails_closed(webhook_settings, make_binding):
    """A misconfigured source must not become an unauthenticated trigger."""
    bare = make_binding(source="bare")
    subscription = WebhookSubscription.objects.create(
        binding=bare, status=WebhookStatus.ACTIVE
    )

    response = deliver(Client(), subscription)

    assert response.status_code == 401
    assert not Run.objects.exists()


def test_the_body_cap_is_applied_before_verification(webhook_settings, subscription):
    """Verifying first would mean HMAC-ing attacker-chosen megabytes."""
    webhook_settings(WEBHOOK_MAX_BODY_BYTES=32)

    response = deliver(Client(), subscription, body=b"x" * 4096)

    assert response.status_code == 413
    assert STATE["verified"] == []
    assert not Run.objects.exists()


def test_an_oversized_content_length_is_refused_without_reading_the_body(
    webhook_settings, subscription
):
    webhook_settings(WEBHOOK_MAX_BODY_BYTES=64)

    response = deliver(Client(), subscription, CONTENT_LENGTH="999999")

    assert response.status_code == 413
    assert STATE["verified"] == []


def test_a_replayed_delivery_id_is_accepted_once(webhook_settings, subscription):
    client = Client()
    first = deliver(client, subscription, delivery_id="delivery-a")
    second = deliver(client, subscription, delivery_id="delivery-a")

    assert first.json() == {"status": "accepted"}
    # 200 rather than 4xx: providers retry every non-2xx, forever.
    assert second.status_code == 200
    assert second.json() == {"status": "duplicate"}
    assert Run.objects.count() == 1


def test_the_dedupe_key_is_scoped_to_one_subscription(webhook_settings, binding):
    """One tenant's delivery id must never suppress another's."""
    first = WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.ACTIVE
    )
    second = WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.ACTIVE
    )
    client = Client()

    assert deliver(client, first, delivery_id="same").json()["status"] == "accepted"
    assert deliver(client, second, delivery_id="same").json()["status"] != "duplicate"


def test_a_burst_produces_one_run_and_leaves_the_binding_dirty(
    webhook_settings, subscription
):
    client = Client()
    statuses = [
        deliver(client, subscription, delivery_id=f"d{index}").json()["status"]
        for index in range(4)
    ]

    assert statuses == ["accepted", "debounced", "debounced", "debounced"]
    assert Run.objects.count() == 1
    subscription.binding.refresh_from_db()
    # The tail of the burst is real change; `dirty` is what stops it being lost.
    assert subscription.binding.dirty is True


def test_a_delivery_mid_run_produces_exactly_one_follow_up_run(
    webhook_settings, subscription
):
    """A Run is already extracting; the burst must earn one follow-up, not four."""
    binding = subscription.binding
    Run.objects.create(
        binding=binding, trigger=RunTrigger.SCHEDULED, status=RunStatus.RUNNING
    )
    client = Client()
    for index in range(4):
        deliver(client, subscription, delivery_id=f"m{index}")

    assert Run.objects.filter(status=RunStatus.QUEUED).count() == 1
    binding.refresh_from_db()
    assert binding.dirty is True


def test_queue_coalescing_still_holds_when_the_debounce_is_disabled(
    webhook_settings, subscription
):
    """`dedupe_key` is the third layer, and it does not depend on the cache."""
    webhook_settings(WEBHOOK_DEBOUNCE=dt.timedelta(0))
    client = Client()

    for index in range(3):
        assert (
            deliver(client, subscription, delivery_id=f"n{index}").json()["status"]
            == "accepted"
        )

    assert Run.objects.filter(status=RunStatus.QUEUED).count() == 1


def test_the_rate_limit_returns_429(webhook_settings, subscription):
    webhook_settings(WEBHOOK_RATE_LIMIT_PER_MINUTE=2)
    client = Client()

    codes = [
        deliver(client, subscription, delivery_id=f"r{index}").status_code
        for index in range(4)
    ]

    assert codes == [202, 202, 429, 429]
    limited = deliver(client, subscription, delivery_id="r9")
    assert limited["Retry-After"] == "60"


def test_a_delivery_for_an_unrunnable_binding_defers_instead_of_queueing(
    webhook_settings, subscription
):
    binding = subscription.binding
    binding.status = BindingStatus.BLOCKED
    binding.save(update_fields=["status"])

    response = deliver(Client(), subscription)

    assert response.json() == {"status": "deferred"}
    assert not Run.objects.exists()
    binding.refresh_from_db()
    # Remembered, so it syncs the moment the block clears.
    assert binding.dirty is True


def test_a_delivery_for_a_revoked_connection_defers(webhook_settings, subscription):
    connection = subscription.binding.connection
    connection.status = ConnectionStatus.REVOKED
    connection.save(update_fields=["status"])

    assert deliver(Client(), subscription).json() == {"status": "deferred"}
    assert not Run.objects.exists()


def test_the_endpoint_is_csrf_exempt(webhook_settings, subscription):
    """Providers cannot send a CSRF token — which is why verify() is mandatory."""
    client = Client(enforce_csrf_checks=True)

    assert deliver(client, subscription).status_code == 202


def test_only_post_is_accepted(webhook_settings, subscription):
    response = Client().get(url_for(subscription))

    assert response.status_code == 405
    assert STATE["verified"] == []


def test_nothing_from_the_request_is_ever_echoed_or_stored(
    webhook_settings, subscription
):
    marker = "canary-4f2a-must-not-be-reflected"
    body = json.dumps({"note": marker}).encode()
    client = Client()

    rejected = deliver(client, subscription, body=body, sign=False)
    accepted = deliver(client, subscription, body=body, delivery_id=marker)

    for response in (rejected, accepted):
        assert marker not in response.content.decode()
        assert marker not in "".join(f"{k}{v}" for k, v in response.items())

    subscription.refresh_from_db()
    assert marker not in json.dumps(subscription.metadata)
    assert marker not in json.dumps(list(Run.objects.values("note", "error_message")))


# --- lifecycle services ----------------------------------------------------


def test_create_subscription_registers_the_reversible_callback_url(
    webhook_settings, binding
):
    expires_at = timezone.now() + dt.timedelta(days=3)
    STATE["registration"] = WebhookRegistration(
        external_id="graph-123",
        external_resource_id="/me/messages",
        expires_at=expires_at,
        secret_reference="secret://hook/1",
        metadata={"changeType": "updated"},
    )

    subscription = webhook_services.create_subscription(
        binding, base_url="https://app.example.test/", resource="messages"
    )

    assert STATE["created"] == [f"https://app.example.test{url_for(subscription)}"]
    assert str(subscription.public_id) in STATE["created"][0]
    assert subscription.status == WebhookStatus.ACTIVE
    assert subscription.external_id == "graph-123"
    assert subscription.secret_reference == "secret://hook/1"
    assert subscription.metadata["changeType"] == "updated"
    binding.refresh_from_db()
    assert binding.webhook_enabled is True


def test_renew_at_is_strictly_before_expiry(webhook_settings, binding):
    expires_at = timezone.now() + dt.timedelta(days=3)
    STATE["registration"] = WebhookRegistration(expires_at=expires_at)

    subscription = webhook_services.create_subscription(
        binding, base_url="https://app.example.test"
    )

    assert subscription.renew_at is not None
    assert subscription.renew_at < subscription.expires_at
    # A missed renewal stops delivery silently, so the lead must be real.
    assert subscription.expires_at - subscription.renew_at >= dt.timedelta(minutes=5)


def test_a_very_short_lifetime_is_renewed_immediately_rather_than_in_the_past(
    webhook_settings,
):
    now = timezone.now()
    assert webhook_services.renew_at_for(now + dt.timedelta(minutes=1), now=now) == now
    assert webhook_services.renew_at_for(None) is None


def test_a_failed_registration_keeps_the_row_so_the_provider_side_is_traceable(
    webhook_settings, binding
):
    STATE["create_raises"] = True

    with pytest.raises(SourceError):
        webhook_services.create_subscription(binding, base_url="https://app.test")

    subscription = WebhookSubscription.objects.get()
    assert subscription.status == WebhookStatus.FAILED
    assert "provider refused" in subscription.metadata["last_error"]
    binding.refresh_from_db()
    assert binding.webhook_enabled is False


def test_create_subscription_refuses_a_relative_or_missing_base_url(
    webhook_settings, binding
):
    with pytest.raises(ConfigurationError, match="base_url"):
        webhook_services.create_subscription(binding)
    with pytest.raises(ConfigurationError, match="absolute"):
        webhook_services.create_subscription(binding, base_url="app.example.test")


def test_callback_path_explains_an_unincluded_urlconf(
    webhook_settings, settings, monkeypatch
):
    empty = types.ModuleType("tests_empty_urlconf")
    empty.urlpatterns = []
    monkeypatch.setitem(sys.modules, "tests_empty_urlconf", empty)
    settings.ROOT_URLCONF = "tests_empty_urlconf"

    with pytest.raises(ConfigurationError, match="include"):
        webhook_services.callback_path(uuid.uuid4())


def test_a_source_without_an_adapter_cannot_subscribe(webhook_settings, make_binding):
    with pytest.raises(ConfigurationError, match="no webhook adapter"):
        webhook_services.create_subscription(
            make_binding(source="bare"), base_url="https://app.test"
        )


def test_renew_due_webhooks_touches_only_due_active_rows(webhook_settings, binding):
    now = timezone.now()
    due = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now + dt.timedelta(hours=2),
        renew_at=now - dt.timedelta(minutes=1),
    )
    WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now + dt.timedelta(days=2),
        renew_at=now + dt.timedelta(days=1),
    )
    WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.EXPIRED,
        renew_at=now - dt.timedelta(hours=1),
    )
    STATE["renewal"] = WebhookRegistration(expires_at=now + dt.timedelta(days=3))

    result = webhook_services.renew_due_webhooks(now=now)

    assert STATE["renewed"] == [due.id]
    assert [item.id for item in result["renewed"]] == [due.id]
    due.refresh_from_db()
    assert due.expires_at > now + dt.timedelta(days=2)
    assert due.renew_at < due.expires_at


def test_a_transient_renewal_failure_backs_off_instead_of_retiring(
    webhook_settings, binding
):
    now = timezone.now()
    subscription = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now + dt.timedelta(days=1),
        renew_at=now - dt.timedelta(minutes=1),
    )
    STATE["renew_raises"] = True

    result = webhook_services.renew_due_webhooks(now=now)

    assert [item.id for item in result["failed"]] == [subscription.id]
    subscription.refresh_from_db()
    # Still live: one 503 must not kill a subscription with a day left on it.
    assert subscription.status == WebhookStatus.ACTIVE
    assert now < subscription.renew_at <= now + dt.timedelta(minutes=5)
    assert "provider refused" in subscription.metadata["last_error"]


def test_a_renewal_failure_past_expiry_retires_the_subscription(
    webhook_settings, binding
):
    now = timezone.now()
    subscription = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now - dt.timedelta(minutes=1),
        renew_at=now - dt.timedelta(minutes=30),
    )
    STATE["renew_raises"] = True

    webhook_services.renew_due_webhooks(now=now)

    subscription.refresh_from_db()
    assert subscription.status == WebhookStatus.FAILED


def test_a_successful_renewal_clears_the_previous_error(webhook_settings, binding):
    now = timezone.now()
    subscription = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now + dt.timedelta(days=1),
        renew_at=now - dt.timedelta(minutes=1),
        metadata={"last_error": "an old failure"},
    )
    STATE["renewal"] = WebhookRegistration(expires_at=now + dt.timedelta(days=3))

    webhook_services.renew_due_webhooks(now=now)

    subscription.refresh_from_db()
    assert "last_error" not in subscription.metadata


def test_expire_subscriptions_retires_everything_past_its_expiry(
    webhook_settings, binding
):
    now = timezone.now()
    stale = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now - dt.timedelta(seconds=1),
    )
    live = WebhookSubscription.objects.create(
        binding=binding,
        status=WebhookStatus.ACTIVE,
        expires_at=now + dt.timedelta(days=1),
    )
    no_expiry = WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.ACTIVE
    )

    assert webhook_services.expire_subscriptions(now=now) == 1

    stale.refresh_from_db()
    live.refresh_from_db()
    no_expiry.refresh_from_db()
    assert stale.status == WebhookStatus.EXPIRED
    assert live.status == WebhookStatus.ACTIVE
    assert no_expiry.status == WebhookStatus.ACTIVE


def test_an_expired_subscription_stops_accepting_deliveries(
    webhook_settings, subscription
):
    subscription.expires_at = timezone.now() - dt.timedelta(seconds=1)
    subscription.save(update_fields=["expires_at"])
    webhook_services.expire_subscriptions()

    assert deliver(Client(), subscription).status_code == 404


def test_delete_subscription_tears_down_locally_even_when_the_provider_fails(
    webhook_settings, subscription
):
    binding = subscription.binding
    binding.webhook_enabled = True
    binding.save(update_fields=["webhook_enabled"])
    STATE["delete_raises"] = True

    webhook_services.delete_subscription(subscription)

    subscription.refresh_from_db()
    assert subscription.status == WebhookStatus.DELETED
    assert "provider refused" in subscription.metadata["last_error"]
    binding.refresh_from_db()
    assert binding.webhook_enabled is False
    # The endpoint stops accepting immediately, which is the safe direction.
    assert deliver(Client(), subscription).status_code == 404


def test_delete_keeps_webhook_enabled_while_another_subscription_lives(
    webhook_settings, binding, subscription
):
    binding.webhook_enabled = True
    binding.save(update_fields=["webhook_enabled"])
    WebhookSubscription.objects.create(binding=binding, status=WebhookStatus.ACTIVE)

    webhook_services.delete_subscription(subscription)

    binding.refresh_from_db()
    assert binding.webhook_enabled is True


def test_an_adapter_returning_the_wrong_type_is_a_configuration_error(
    webhook_settings, binding
):
    STATE["registration"] = {"external_id": "not-a-registration"}

    with pytest.raises(ConfigurationError, match="WebhookRegistration"):
        webhook_services.create_subscription(binding, base_url="https://app.test")


# --- URLconf for the view tests --------------------------------------------

urlpatterns = [
    path("connectors/webhooks/", include("django_connectors.webhooks.urls")),
]
