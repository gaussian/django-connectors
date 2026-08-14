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

from django_connectors.models import Binding, Connection, ConnectionSecret, Run

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


@pytest.mark.parametrize(
    ("model", "admin_instance"),
    CONNECTOR_ADMINS,
    ids=lambda arg: getattr(arg, "__name__", ""),
)
def test_every_action_is_permission_gated(model, admin_instance):
    """Introspects declared actions only — no DB, no deprecated internals."""
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
    from django.test import RequestFactory

    request = RequestFactory().get("/admin/")
    request.user = user
    return request
