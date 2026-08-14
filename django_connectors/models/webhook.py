"""WebhookSubscription — how a provider tells us a Binding may have changed."""

import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _

from django_connectors.enums import WebhookStatus
from django_connectors.models.binding import Binding


class WebhookSubscription(models.Model):
    """The provider-side subscription, channel or watch backing a Binding.

    A delivery never carries data into the landing tables. It marks the Binding
    as possibly-changed and schedules the same dlt source that polling would
    run, so webhooks and polling do not create two ingestion paths that can
    disagree. It also means a delivery can be dropped without losing data — the
    next poll reconciles.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    binding = models.ForeignKey(
        Binding, related_name="webhook_subscriptions", on_delete=models.CASCADE
    )

    # The unguessable component of the callback URL. Random and separate from
    # `id` so that the endpoint cannot be derived from anything the API exposes.
    public_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)

    resource = models.CharField(max_length=150, blank=True)
    external_id = models.CharField(max_length=500, blank=True)
    external_resource_id = models.CharField(max_length=500, blank=True)

    status = models.CharField(
        max_length=32, choices=WebhookStatus, default=WebhookStatus.PENDING
    )

    expires_at = models.DateTimeField(null=True, blank=True)
    # Renew before expiry rather than at it; provider subscriptions are short
    # (Graph maxes out at ~3 days) and a missed renewal silently stops delivery.
    renew_at = models.DateTimeField(null=True, blank=True)

    # Handle for the shared signing secret, resolved through the SecretStore.
    # Never the secret itself.
    secret_reference = models.CharField(max_length=500, blank=True)

    metadata = models.JSONField(default=dict, blank=True)
    last_notification_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("webhook subscription")
        verbose_name_plural = _("webhook subscriptions")
        ordering = ("-created_at",)
        permissions = (
            ("renew_webhooksubscription", _("Can renew a webhook subscription")),
        )
        indexes = (
            models.Index(fields=("binding", "status"), name="dc_hook_binding_idx"),
            models.Index(fields=("status", "renew_at"), name="dc_hook_renew_idx"),
        )

    def __str__(self):
        return f"{self.resource or 'subscription'} ({self.status})"
