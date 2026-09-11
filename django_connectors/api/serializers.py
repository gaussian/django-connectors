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

from django_connectors.api.scoping import resolve_owner
from django_connectors.errors import scrub
from django_connectors.models import (
    Binding,
    Connection,
    Projection,
    ProjectionRun,
    Run,
    WebhookSubscription,
)


class OwnerScopedPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    """A writable foreign key that only accepts objects the caller owns.

    Scoping a viewset's *queryset* protects reads only. DRF builds a writable
    relation from the related model's unfiltered default manager, so a plain
    ModelSerializer happily accepts another tenant's primary key — and for
    ``Binding.connection`` that is a credential exfiltration primitive, not
    merely a data-integrity problem: the attacker points the Binding at a server
    they control and the runner sends the victim's provider token to it in an
    Authorization header.

    Fails closed: no request, or an owner that does not resolve, yields an empty
    queryset rather than an unfiltered one.
    """

    def __init__(self, *, owner_lookup="", **kwargs):
        #: ORM path from the related model to ``Connection.owner_*``.
        self.owner_lookup = owner_lookup
        super().__init__(**kwargs)

    def get_queryset(self):
        queryset = super().get_queryset()
        request = self.context.get("request")
        if request is None:
            return queryset.none()
        owner = resolve_owner(request)
        if owner is None:
            return queryset.none()
        content_type_id, object_id = owner
        prefix = f"{self.owner_lookup}__" if self.owner_lookup else ""
        return queryset.filter(
            **{
                f"{prefix}owner_content_type_id": content_type_id,
                f"{prefix}owner_object_id": object_id,
            }
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
    connection = OwnerScopedPrimaryKeyRelatedField(
        queryset=Connection.objects.all(), owner_lookup=""
    )

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

    def validate(self, attrs):
        """Run the same checks ``Binding.clean()`` runs, on the merged result.

        DRF does not call ``full_clean()``, so without this the API is the one
        way into the database that accepts a Binding guaranteed to fail its
        first Run. Merged with ``self.instance`` because a PATCH carries only
        the fields it changes, and validating those alone would pass a config
        that is invalid *for the source already stored on the row*.

        A throwaway unsaved ``Binding`` is used as the subject rather than a
        dict: the service takes a Binding, and building one here keeps a single
        implementation of what "valid" means. It is never saved.
        """
        from django_connectors.services.bindings import binding_field_errors

        instance = self.instance
        probe = Binding(
            source=attrs.get("source", getattr(instance, "source", "")),
            config=attrs.get("config", getattr(instance, "config", None) or {}),
            resources=attrs.get(
                "resources", getattr(instance, "resources", None) or []
            ),
            landing_key=getattr(instance, "landing_key", ""),
        )
        errors = binding_field_errors(probe)
        if errors:
            raise serializers.ValidationError(errors)
        return attrs


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
    binding = OwnerScopedPrimaryKeyRelatedField(
        queryset=Binding.objects.all(), owner_lookup="connection"
    )

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


def _assert_writable_relations_are_owner_scoped():
    """Guard against a serializer exposing an unscoped writable FK.

    Read scoping lives on the viewset; write scoping lives here, and the two are
    easy to confuse. This asserts the second at import time so a serializer added
    later cannot quietly accept another tenant's primary key.
    """
    from django.core.exceptions import ImproperlyConfigured

    offenders = []
    for name, obj in list(globals().items()):
        if not isinstance(obj, type) or not issubclass(
            obj, serializers.ModelSerializer
        ):
            continue
        if obj.__module__ != __name__:
            continue
        model = getattr(getattr(obj, "Meta", None), "model", None)
        if model is None:
            continue
        declared = obj().fields
        for field_name, field in declared.items():
            if field.read_only or not isinstance(
                field, serializers.PrimaryKeyRelatedField
            ):
                continue
            related = model._meta.get_field(field_name).related_model
            if related._meta.app_label != "django_connectors":
                continue
            if not isinstance(field, OwnerScopedPrimaryKeyRelatedField):
                offenders.append(f"{name}.{field_name}")
    if offenders:
        raise ImproperlyConfigured(
            f"writable relations {offenders} are not owner-scoped, so a caller "
            f"could attach their object to another tenant's record."
        )


_assert_writable_relations_are_owner_scoped()
