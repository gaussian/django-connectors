"""Django admin.

Two properties matter more than convenience here:

**No credential is ever rendered.** ``Connection.auth_metadata`` is excluded
from the form entirely (a JSONField textarea is exactly how a credential ends up
pasted somewhere it is readable), and ``ConnectionSecret.value`` is never
displayed, searchable, or editable.

**Every action is permission-gated.** Django's ``_filter_actions_by_permissions``
appends any action *lacking* ``allowed_permissions`` unconditionally, so an
action declared without ``@admin.action(permissions=[...])`` is executable by
view-only staff. Django then resolves ``has_<verb>_permission`` by name and
silently offers the action if that method is missing. Both mistakes are silent,
so ``tests/test_admin.py`` asserts against them by introspection.

Imports Django and the standard library only: this module is eagerly
autodiscovered whenever ``django.contrib.admin`` is installed.
"""

from django.contrib import admin

from django_connectors.models import (
    Binding,
    BindingLock,
    Connection,
    ConnectionSecret,
    Projection,
    ProjectionRun,
    Run,
    WebhookSubscription,
)

TIMESTAMPS = ("created_at", "updated_at")


@admin.register(Connection)
class ConnectionAdmin(admin.ModelAdmin):
    # auth_metadata is deliberately absent from every one of these.
    list_display = (
        "__str__",
        "provider",
        "auth_backend",
        "status",
        "external_tenant_id",
        "created_at",
    )
    list_filter = ("status", "provider", "auth_backend")
    search_fields = ("external_account_id", "external_tenant_id", "provider")
    readonly_fields = (*TIMESTAMPS, "setup_token")
    # Excluding rather than making read-only: a read-only JSONField is still
    # rendered, and rendering is the leak.
    exclude = ("auth_metadata",)
    date_hierarchy = "created_at"

    def has_test_connection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.test_connection")


@admin.register(Binding)
class BindingAdmin(admin.ModelAdmin):
    list_display = (
        "__str__",
        "connection",
        "status",
        "enabled",
        "poll_interval",
        "next_run_at",
        "last_success_at",
    )
    list_filter = ("status", "enabled", "webhook_enabled", "source")
    search_fields = ("source", "landing_key")
    readonly_fields = (
        *TIMESTAMPS,
        "landing_key",
        "landing_schema",
        "landing_schema_version_hash",
        "landing_schema_at",
        "landing_purged_at",
        "last_error",
    )
    raw_id_fields = ("connection",)
    date_hierarchy = "created_at"

    def has_run_binding_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.run_binding")


@admin.register(Run)
class RunAdmin(admin.ModelAdmin):
    # error_message is safe to display: it is written only through
    # errors.scrub(), which is asserted by tests/test_errors.py.
    list_display = ("id", "binding", "trigger", "status", "started_at", "finished_at")
    list_filter = ("status", "trigger")
    search_fields = ("id", "dlt_pipeline_name", "error_type")
    date_hierarchy = "created_at"
    raw_id_fields = ("binding",)

    def get_readonly_fields(self, request, obj=None):
        # A Run is a record of something that already happened; editing one
        # falsifies history rather than fixing anything.
        return tuple(field.name for field in self.model._meta.fields)

    def has_add_permission(self, request):
        return False


@admin.register(Projection)
class ProjectionAdmin(admin.ModelAdmin):
    list_display = ("__str__", "binding", "resource", "target", "status", "version")
    list_filter = ("status", "enabled", "target")
    search_fields = ("name", "resource", "target")
    readonly_fields = (*TIMESTAMPS, "version", "last_error")
    raw_id_fields = ("binding",)

    def has_preview_projection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.preview_projection")

    def has_replay_projection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.replay_projection")


@admin.register(ProjectionRun)
class ProjectionRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "projection",
        "mode",
        "status",
        "projection_version",
        "records_written",
        "started_at",
    )
    list_filter = ("status", "mode")
    search_fields = ("id", "error_type")
    date_hierarchy = "created_at"
    raw_id_fields = ("projection", "source_run")

    def get_readonly_fields(self, request, obj=None):
        return tuple(field.name for field in self.model._meta.fields)

    def has_add_permission(self, request):
        return False


@admin.register(WebhookSubscription)
class WebhookSubscriptionAdmin(admin.ModelAdmin):
    list_display = ("__str__", "binding", "status", "expires_at", "renew_at")
    list_filter = ("status",)
    search_fields = ("external_id", "external_resource_id")
    # public_id is the unguessable part of the callback URL; secret_reference
    # is a handle, but neither should be casually editable.
    readonly_fields = (*TIMESTAMPS, "public_id", "last_notification_at")
    raw_id_fields = ("binding",)

    def has_renew_webhooksubscription_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.renew_webhooksubscription")


@admin.register(BindingLock)
class BindingLockAdmin(admin.ModelAdmin):
    """Visible so an operator can see why a Binding is not running."""

    list_display = ("binding", "acquired_at", "expires_at")
    readonly_fields = ("binding", "token", "acquired_at", "expires_at")

    def has_add_permission(self, request):
        return False


@admin.register(ConnectionSecret)
class ConnectionSecretAdmin(admin.ModelAdmin):
    """Registered so operators can audit *which* secrets exist — never their values."""

    list_display = ("key", "connection", "encryption", "updated_at")
    list_filter = ("encryption",)
    # `value` appears in no list, no search, no form, and no readonly set.
    fields = ("connection", "key", "encryption", *TIMESTAMPS)
    readonly_fields = ("connection", "key", "encryption", *TIMESTAMPS)
    raw_id_fields = ("connection",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
