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

**Every action goes through the service layer**, calls it once per selected
object, and refuses a selection larger than ``ADMIN_ACTION_MAX_SELECTION``.
Duplicating a service's rules here is how the admin and the API start
disagreeing about what "replay" or "renew" means.

At module scope this imports Django, the standard library, and this package's
Django-only modules (``conf``, ``enums``, ``models``) — nothing that resolves a
registry or reaches a provider SDK. This module is eagerly autodiscovered
whenever ``django.contrib.admin`` is installed, so those imports happen inside
the actions that need them.
"""

from django.contrib import admin, messages

from django_connectors.conf import conf
from django_connectors.enums import RunTrigger
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


def _selection_is_within_limit(model_admin, request, queryset):
    """Refuse a selection larger than ``ADMIN_ACTION_MAX_SELECTION``.

    Every action below does real work per object — a provider round trip, a
    queued Run, a full re-projection. Django's changelist offers "select all
    N", so without a cap one click can turn the whole table into that many
    provider calls inside a single request, which is how an operator takes
    their own integration down while trying to fix it.
    """
    limit = conf.ADMIN_ACTION_MAX_SELECTION
    count = queryset.count()
    if count <= limit:
        return True
    model_admin.message_user(
        request,
        f"{count} selected, which is more than "
        f"DJANGO_CONNECTORS['ADMIN_ACTION_MAX_SELECTION'] ({limit}). "
        f"Nothing was done. Select fewer, or raise the setting.",
        level=messages.ERROR,
    )
    return False


def _report_failure(model_admin, request, obj, exc):
    """Report one object's failure without aborting the batch, and scrub it.

    ``errors.scrub`` because a provider exception routinely carries the token
    or DSN it was built from, and an admin message is rendered straight back
    into a page — the one place this package is most careful never to render a
    credential.
    """
    from django_connectors.errors import scrub

    model_admin.message_user(request, f"{obj}: {scrub(exc)}", level=messages.ERROR)


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
    actions = ("test_connection",)

    def has_test_connection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.test_connection")

    @admin.action(
        permissions=["test_connection"],
        description="Test the credential behind each connection",
    )
    def test_connection(self, request, queryset):
        """Ask each Connection's auth backend whether its credential still works.

        The same call the API's ``connections/<id>/test`` endpoint makes. It
        reaches the provider, so it is capped like every other action here.
        """
        if not _selection_is_within_limit(self, request, queryset):
            return
        # Imported here, not at module scope: this module is autodiscovered on
        # every startup, and resolving the registry drags in whichever SDK the
        # configured backends need.
        from django_connectors.registry import auth_backends

        for connection in queryset:
            try:
                result = auth_backends.get(connection.auth_backend).test(connection)
            except Exception as exc:
                # One unreachable provider must not abandon the rest of the
                # batch half-tested, with no report of which half.
                _report_failure(self, request, connection, exc)
            else:
                self.message_user(
                    request, f"{connection}: {result}", level=messages.SUCCESS
                )


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
    actions = ("run_binding",)

    def has_run_binding_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.run_binding")

    @admin.action(
        permissions=["run_binding"], description="Queue a run for each binding"
    )
    def run_binding(self, request, queryset):
        """Queue a Run per Binding rather than executing one inline.

        ``services.runs.run_binding`` would extract, normalize and load inside
        this request: a web worker held for the whole ingestion, behind a proxy
        that will time out long before it finishes, times however many rows
        were selected. ``enqueue_run`` records the same Run — visible in the
        Run changelist immediately — and lets the scheduler execute it, which
        is where every other trigger in this package sends its work.
        """
        if not _selection_is_within_limit(self, request, queryset):
            return
        from django_connectors.services.runs import enqueue_run

        queued = 0
        for binding in queryset.select_related("connection"):
            if not (binding.is_runnable and binding.connection.is_usable):
                # Queueing here would produce a Run that is guaranteed to fail
                # and would hide the actual reason behind a run failure.
                self.message_user(
                    request,
                    f"{binding}: not runnable (binding {binding.status}, "
                    f"connection {binding.connection.status}); not queued.",
                    level=messages.WARNING,
                )
                continue
            try:
                enqueue_run(binding, trigger=RunTrigger.MANUAL)
            except Exception as exc:
                _report_failure(self, request, binding, exc)
            else:
                queued += 1
        if queued:
            self.message_user(
                request,
                f"Queued {queued} run(s). The scheduler executes them on its "
                f"next tick.",
                level=messages.SUCCESS,
            )


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
    actions = ("preview_projection", "replay_projection")

    def has_preview_projection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.preview_projection")

    def has_replay_projection_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.replay_projection")

    @admin.action(
        permissions=["preview_projection"],
        description="Preview what each projection would write",
    )
    def preview_projection(self, request, queryset):
        """A dry run: nothing is written, and the row counts say what would be.

        Separately permissioned from replay because it reads customer data —
        that is the whole point of a preview — while changing nothing.
        """
        if not _selection_is_within_limit(self, request, queryset):
            return
        from django_connectors.services import projections as projection_services

        for projection in queryset.select_related("binding"):
            try:
                result = projection_services.preview_projection(projection)
            except Exception as exc:
                _report_failure(self, request, projection, exc)
            else:
                level = messages.WARNING if result["error_count"] else messages.SUCCESS
                self.message_user(
                    request,
                    f"{projection}: {result['row_count']} row(s) sampled, "
                    f"{result['ok_count']} ok, {result['error_count']} error(s), "
                    f"{result['filtered_count']} filtered.",
                    level=level,
                )

    @admin.action(
        permissions=["replay_projection"], description="Replay every landed row"
    )
    def replay_projection(self, request, queryset):
        """Re-project everything that has landed, through the service layer.

        Executed inline, unlike the Binding action above: there is no queue for
        projection work, and `replay_projection` is the only thing that refuses
        a target which cannot have a scope replaced. Reimplementing that check
        here to gain a queue would be duplicating the one rule that keeps a
        replay from orphaning records the host wrote under an old mapping.
        """
        if not _selection_is_within_limit(self, request, queryset):
            return
        from django_connectors.services import projections as projection_services

        for projection in queryset.select_related("binding"):
            try:
                projection_run = projection_services.replay_projection(projection)
            except Exception as exc:
                _report_failure(self, request, projection, exc)
            else:
                self.message_user(
                    request,
                    f"{projection}: replay {projection_run.status}, "
                    f"{projection_run.records_written} written, "
                    f"{projection_run.records_deleted} deleted.",
                    level=messages.SUCCESS,
                )


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
    actions = ("renew_webhooksubscription",)

    def has_renew_webhooksubscription_permission(self, request, obj=None):
        return request.user.has_perm("django_connectors.renew_webhooksubscription")

    @admin.action(
        permissions=["renew_webhooksubscription"],
        description="Renew each subscription with its provider",
    )
    def renew_webhooksubscription(self, request, queryset):
        """Renew each selection now, through the service layer.

        This used to only move ``renew_at`` forward and leave the work to the
        sweep, because the sweep was the only code that knew how to record a
        failed renewal without retiring a subscription still hours from expiry.
        An action labelled "renew" that in fact meant "become due" is a
        different operation with a different failure mode — nothing happens at
        all if the sweep is not scheduled — so the per-row body of the sweep is
        now ``webhooks.services.renew_subscription`` and both call it.
        """
        if not _selection_is_within_limit(self, request, queryset):
            return
        from django_connectors.webhooks.services import renew_subscription

        renewed = 0
        for subscription in queryset.select_related("binding"):
            try:
                renew_subscription(subscription, actor=request.user)
            except Exception as exc:
                # Includes the refusal to renew a non-live subscription, which
                # names the status and says to create a new one.
                _report_failure(self, request, subscription, exc)
            else:
                renewed += 1
        if renewed:
            self.message_user(
                request,
                f"Renewed {renewed} subscription(s) with their provider.",
                level=messages.SUCCESS,
            )


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
