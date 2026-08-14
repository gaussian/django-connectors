"""Admin safety properties, asserted by introspection.

Both failure modes guarded here are silent in Django:

* an action declared without ``permissions=[...]`` is appended unconditionally
  by ``_filter_actions_by_permissions``, so view-only staff can run it;
* Django resolves ``has_<verb>_permission`` by name, and silently offers the
  action when that method does not exist.

They are asserted generically so that actions added in later phases inherit the
guarantee instead of having to remember it.
"""

import pytest
from django.contrib import admin
from django.contrib.auth.models import Permission, User
from django.contrib.contenttypes.models import ContentType

from django_connectors.models import (
    Binding,
    Connection,
    ConnectionSecret,
    Projection,
    Run,
    WebhookSubscription,
)

CONNECTOR_ADMINS = [
    (model, admin_instance)
    for model, admin_instance in admin.site._registry.items()
    if model._meta.app_label == "django_connectors"
]


def test_every_model_is_registered():
    registered = {model.__name__ for model, _ in CONNECTOR_ADMINS}
    assert registered == {
        "Binding",
        "BindingLock",
        "Connection",
        "ConnectionSecret",
        "Projection",
        "ProjectionRun",
        "Run",
        "WebhookSubscription",
    }


def _assert_actions_are_gated(admin_instance):
    """Introspects declared actions only — no DB, no deprecated internals.

    Returns how many actions it inspected, because an assertion loop over an
    empty sequence is a test that cannot fail.
    """
    inspected = 0
    for entry in admin_instance.actions or ():
        function = (
            getattr(admin_instance, entry, None) if isinstance(entry, str) else entry
        )
        assert function is not None, f"declared action {entry!r} does not resolve"
        name = getattr(function, "__name__", str(entry))
        permissions = getattr(function, "allowed_permissions", ())
        assert permissions, (
            f"{admin_instance.__class__.__name__}.{name} has no "
            f"allowed_permissions; view-only staff could run it"
        )
        for permission in permissions:
            method = f"has_{permission}_permission"
            assert hasattr(admin_instance, method), (
                f"{admin_instance.__class__.__name__}.{name} requires "
                f"{permission!r} but defines no {method}(); Django would offer "
                f"the action anyway"
            )
        inspected += 1
    return inspected


@pytest.mark.parametrize(
    ("model", "admin_instance"),
    CONNECTOR_ADMINS,
    ids=lambda arg: getattr(arg, "__name__", ""),
)
def test_every_action_is_permission_gated(model, admin_instance):
    _assert_actions_are_gated(admin_instance)


def test_the_gating_assertion_actually_inspects_something():
    """Otherwise the loop above is vacuous and passes whatever admin.py says.

    Named rather than counted: an action that quietly disappears takes its
    permission's only enforcement with it, and the custom permissions stay in
    the database advertising something nothing can do.
    """
    declared = {
        (admin_instance.__class__.__name__, entry)
        for _, admin_instance in CONNECTOR_ADMINS
        for entry in admin_instance.actions or ()
    }
    assert declared == {
        ("ConnectionAdmin", "test_connection"),
        ("BindingAdmin", "run_binding"),
        ("ProjectionAdmin", "preview_projection"),
        ("ProjectionAdmin", "replay_projection"),
        ("WebhookSubscriptionAdmin", "renew_webhooksubscription"),
    }


def test_the_gating_assertion_fires_on_an_unguarded_action():
    """Proves the guard bites, against an admin written the wrong way on purpose."""

    class UngatedAdmin(admin.ModelAdmin):
        actions = ("do_something",)

        def do_something(self, request, queryset):
            """Declared without @admin.action(permissions=[...])."""

    with pytest.raises(AssertionError, match="allowed_permissions"):
        _assert_actions_are_gated(UngatedAdmin(Binding, admin.site))


def test_the_gating_assertion_fires_when_the_permission_method_is_missing():
    class MisspeltAdmin(admin.ModelAdmin):
        actions = ("do_something",)

        @admin.action(permissions=["run_bindings"])  # note the typo
        def do_something(self, request, queryset):
            """Django resolves has_run_bindings_permission, which does not exist."""

    with pytest.raises(AssertionError, match="has_run_bindings_permission"):
        _assert_actions_are_gated(MisspeltAdmin(Binding, admin.site))


def test_auth_metadata_is_never_rendered():
    """A JSONField textarea is how a credential ends up somewhere readable."""
    connection_admin = admin.site._registry[Connection]
    assert "auth_metadata" in connection_admin.exclude
    assert "auth_metadata" not in connection_admin.list_display
    assert "auth_metadata" not in connection_admin.search_fields
    assert "auth_metadata" not in (connection_admin.readonly_fields or ())


def test_connection_secret_value_is_never_exposed():
    secret_admin = admin.site._registry[ConnectionSecret]
    for attribute in ("list_display", "search_fields", "fields", "readonly_fields"):
        assert "value" not in (getattr(secret_admin, attribute) or ())


@pytest.mark.django_db
def test_run_history_is_not_editable():
    run_admin = admin.site._registry[Run]
    request = _request_for(_staff_user())
    assert run_admin.has_add_permission(request) is False
    readonly = run_admin.get_readonly_fields(request)
    assert "status" in readonly
    assert "error_message" in readonly


@pytest.mark.django_db
def test_view_only_staff_cannot_run_a_binding():
    """The permission method must actually deny, not merely exist."""
    user = _staff_user()
    user.user_permissions.add(
        Permission.objects.get(
            codename="view_binding",
            content_type=ContentType.objects.get_for_model(Binding),
        )
    )
    user = User.objects.get(pk=user.pk)  # reset the permission cache

    binding_admin = admin.site._registry[Binding]
    request = _request_for(user)
    assert binding_admin.has_run_binding_permission(request) is False
    assert "run_binding" not in binding_admin.get_actions(request)


@pytest.mark.django_db
def test_staff_with_the_permission_may_run_a_binding():
    user = _staff_user()
    user.user_permissions.add(
        Permission.objects.get(
            codename="run_binding",
            content_type=ContentType.objects.get_for_model(Binding),
        )
    )
    user = User.objects.get(pk=user.pk)

    binding_admin = admin.site._registry[Binding]
    assert binding_admin.has_run_binding_permission(_request_for(user)) is True


# --- helpers ---------------------------------------------------------------


def _staff_user():
    return User.objects.create_user(
        username=f"staff{User.objects.count()}", password="x", is_staff=True
    )


def _request_for(user):
    from django.contrib.messages.storage.fallback import FallbackStorage
    from django.test import RequestFactory

    request = RequestFactory().get("/admin/")
    request.user = user
    # Actions report per-object outcomes through message_user, which raises
    # MessageFailure without storage on the request.
    request.session = {}
    request._messages = FallbackStorage(request)
    return request


# --- the actions themselves ---------------------------------------------------
#
# The custom permissions in migration 0001 exist for these five actions. Until
# they were declared, `has_*_permission` was unreachable — Django only resolves
# it for an action declaring `permissions=[...]` — so the gating above was a
# guarantee about nothing.

ACTION_PERMISSIONS = [
    (Connection, "test_connection", "test_connection"),
    (Binding, "run_binding", "run_binding"),
    (Projection, "preview_projection", "preview_projection"),
    (Projection, "replay_projection", "replay_projection"),
    (WebhookSubscription, "renew_webhooksubscription", "renew_webhooksubscription"),
]


@pytest.mark.django_db
@pytest.mark.parametrize(("model", "action", "codename"), ACTION_PERMISSIONS)
def test_an_action_is_offered_only_to_staff_holding_its_permission(
    model, action, codename
):
    """Both halves matter: withheld without the permission, offered with it."""
    model_admin = admin.site._registry[model]

    without = _staff_user()
    assert action not in model_admin.get_actions(_request_for(without))

    with_permission = _staff_user()
    with_permission.user_permissions.add(
        Permission.objects.get(
            codename=codename, content_type=ContentType.objects.get_for_model(model)
        )
    )
    with_permission = User.objects.get(pk=with_permission.pk)
    assert action in model_admin.get_actions(_request_for(with_permission))


@pytest.mark.django_db
def test_the_run_action_queues_a_run_instead_of_executing_one(make_binding):
    """A Run inside the admin request would hold a web worker for the ingestion.

    So the action must produce a queued Run for the scheduler — and it must
    produce one at all, which is what makes the action more than decoration.
    """
    from django_connectors.enums import RunStatus, RunTrigger
    from django_connectors.models import Run

    binding = make_binding()
    binding_admin = admin.site._registry[Binding]
    request = _request_for(_staff_user())

    binding_admin.run_binding(request, Binding.objects.filter(pk=binding.pk))

    run = Run.objects.get(binding=binding)
    assert run.status == RunStatus.QUEUED
    assert run.trigger == RunTrigger.MANUAL


@pytest.mark.django_db
def test_the_run_action_does_nothing_for_a_binding_that_cannot_run(make_binding):
    """A queued Run for a blocked Binding fails later and hides the real cause."""
    from django_connectors.enums import ConnectionStatus
    from django_connectors.models import Run

    binding = make_binding()
    binding.connection.status = ConnectionStatus.REVOKED
    binding.connection.save(update_fields=["status"])

    binding_admin = admin.site._registry[Binding]
    binding_admin.run_binding(
        _request_for(_staff_user()), Binding.objects.filter(pk=binding.pk)
    )

    assert not Run.objects.exists()


@pytest.mark.django_db
def test_an_action_refuses_a_selection_over_the_configured_maximum(make_binding):
    """A "select all" on a large changelist is one click from every action."""
    from django.test import override_settings

    from django_connectors.models import Run

    make_binding()
    make_binding()
    binding_admin = admin.site._registry[Binding]

    with override_settings(DJANGO_CONNECTORS={"ADMIN_ACTION_MAX_SELECTION": 1}):
        binding_admin.run_binding(_request_for(_staff_user()), Binding.objects.all())

    assert not Run.objects.exists(), "the cap did not stop the action"


@pytest.mark.django_db
def test_the_renew_action_only_makes_active_subscriptions_due(make_binding):
    """The sweep reads (status, renew_at); moving renew_at elsewhere is a no-op."""
    import datetime as dt

    from django.utils import timezone

    from django_connectors.enums import WebhookStatus

    binding = make_binding()
    later = timezone.now() + dt.timedelta(days=2)
    active = WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.ACTIVE, renew_at=later
    )
    pending = WebhookSubscription.objects.create(
        binding=binding, status=WebhookStatus.PENDING, renew_at=later
    )

    hook_admin = admin.site._registry[WebhookSubscription]
    hook_admin.renew_webhooksubscription(
        _request_for(_staff_user()), WebhookSubscription.objects.all()
    )

    active.refresh_from_db()
    pending.refresh_from_db()
    assert active.renew_at < later, "the active subscription was not made due"
    assert pending.renew_at == later, "a pending subscription was touched"
