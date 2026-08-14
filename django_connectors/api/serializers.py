"""API serializers.

Two rules run through all of these:

**No credential is ever serialized.** ``Connection.auth_metadata`` and
``auth_reference`` are write-only or absent; ``ConnectionSecret`` has no
serializer at all.

**Error text is scrubbed again on the way out.** It is already scrubbed when
written, and that is tested — but re-scrubbing on read is nearly free and covers
rows written before the scrubber existed, or by a code path that bypassed it.
Defence in depth is worth it for the one field capable of carrying a landing DSN.
"""

from rest_framework import serializers

from django_connectors.errors import scrub
from django_connectors.models import (
    Binding,
    Connection,
    Projection,
    ProjectionRun,
    Run,
    WebhookSubscription,
)


class ScrubbedCharField(serializers.CharField):
    """A text field re-scrubbed at serialization time."""

    def to_representation(self, value):
        return scrub(super().to_representation(value))


class ConnectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Connection
        fields = (
            "id",
            "provider",
            "auth_backend",
            "external_account_id",
            "external_tenant_id",
            "status",
            "metadata",
            "created_at",
            "updated_at",
        )
        # `auth_metadata`, `auth_reference` and `setup_token` are deliberately
        # absent rather than read-only: a read-only field is still rendered.
        read_only_fields = ("id", "status", "created_at", "updated_at")


class BindingSerializer(serializers.ModelSerializer):
    last_error = ScrubbedCharField(read_only=True, allow_blank=True)

    class Meta:
        model = Binding
        fields = (
            "id",
            "connection",
            "source",
            "resources",
            "config",
            "enabled",
            "poll_interval",
            "next_run_at",
            "min_run_interval",
            "webhook_enabled",
            "landing_retention",
            "status",
            "landing_schema_at",
            "last_success_at",
            "last_failure_at",
            "last_error",
            "created_at",
            "updated_at",
        )
        read_only_fields = (
            "id",
            "status",
            "next_run_at",
            "landing_schema_at",
            "last_success_at",
            "last_failure_at",
            "last_error",
            "created_at",
            "updated_at",
        )


class RunSerializer(serializers.ModelSerializer):
    error_message = ScrubbedCharField(read_only=True, allow_blank=True)

    class Meta:
        model = Run
        fields = (
            "id",
            "binding",
            "trigger",
            "status",
            "started_at",
            "finished_at",
            "dlt_load_ids",
            "metrics",
            "error_type",
            "error_message",
            "note",
            "created_at",
        )
        read_only_fields = fields


class ProjectionSerializer(serializers.ModelSerializer):
    last_error = ScrubbedCharField(read_only=True, allow_blank=True)

    class Meta:
        model = Projection
        fields = (
            "id",
            "binding",
            "resource",
            "target",
            "name",
            "mapping",
            "filters",
            "enabled",
            "version",
            "status",
            "last_success_at",
            "last_failure_at",
            "last_error",
            "created_at",
            "updated_at",
        )
        # `version` is bumped by the service on a mapping change, never set by
        # a client — a client-chosen version would break the superseded check.
        read_only_fields = (
            "id",
            "version",
            "status",
            "last_success_at",
            "last_failure_at",
            "last_error",
            "created_at",
            "updated_at",
        )


class ProjectionRunSerializer(serializers.ModelSerializer):
    error_message = ScrubbedCharField(read_only=True, allow_blank=True)

    class Meta:
        model = ProjectionRun
        fields = (
            "id",
            "projection",
            "source_run",
            "mode",
            "projection_version",
            "status",
            "load_ids",
            "records_seen",
            "records_written",
            "records_deleted",
            "started_at",
            "finished_at",
            "error_type",
            "error_message",
            "created_at",
        )
        read_only_fields = fields


class WebhookSubscriptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = WebhookSubscription
        # `public_id` is the unguessable component of the callback URL and
        # `secret_reference` resolves to a signing secret; neither is exposed.
        fields = (
            "id",
            "binding",
            "resource",
            "status",
            "expires_at",
            "renew_at",
            "last_notification_at",
            "created_at",
        )
        read_only_fields = fields


class MappingUpdateSerializer(serializers.Serializer):
    """Payload for changing a mapping.

    ``acknowledge_identity_change`` is required when identity derivation
    changes: it orphans everything already written under the old identity, and
    this library cannot clean that up because it never learns what the writer
    wrote.
    """

    mapping = serializers.JSONField(required=False)
    filters = serializers.JSONField(required=False)
    acknowledge_identity_change = serializers.BooleanField(default=False)
