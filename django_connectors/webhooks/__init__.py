"""Provider push notifications.

A delivery never carries data into the landing tables. It marks the Binding as
possibly-changed and schedules the same dlt source that polling would run, so
webhooks and polling cannot become two ingestion paths that disagree — and a
dropped delivery loses nothing, because the next poll reconciles.

Nothing here is imported during app loading: the receive view is reached only
through the host's URLconf, and the lifecycle services only from a task or a
management command.
"""

from django_connectors.webhooks.base import (
    WebhookAdapter,
    WebhookDelivery,
    WebhookRegistration,
)

__all__ = [
    "WebhookAdapter",
    "WebhookDelivery",
    "WebhookRegistration",
]
