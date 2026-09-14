"""Composing the API viewsets with a host's own authentication.

A host that already has an API has an authentication scheme, a permission
model and a notion of "the current tenant". It should not need to install the
library's ``API_OWNER_RESOLVER`` and ``API_PERMISSION_CLASSES`` settings — it
subclasses the viewsets, sets ``permission_classes`` the ordinary DRF way, and
overrides ``get_owner()``. Every test here runs with **both settings unset**:
if any code path fell back to them, ``resolve_owner`` would raise
``ImproperlyConfigured`` and the request would 500, which is what makes the
status codes below a proof rather than a demonstration.

The property that matters most is the last one: the owner a subclass resolves
scopes reads, scopes writable relations, and stamps new Connections. One
answer, three uses. A host could not otherwise scope its list view by one
tenancy and its writable foreign keys by another — the latter being the field
that guards against credential exfiltration.
"""

import pytest
from django.urls import include, path, reverse

pytest.importorskip("rest_framework")

from rest_framework.permissions import AllowAny, BasePermission, IsAuthenticated
from rest_framework.routers import DefaultRouter

from django_connectors.api import views as api_views
from django_connectors.models import Binding, Connection
from tests.conftest import memory_config

#: Save-time validation refuses an empty config, so a creatable Binding needs a real one.
VALID_CONFIG = memory_config(batches=[[{"id": "1"}]])

pytestmark = pytest.mark.django_db


# --- a host's composed viewsets --------------------------------------------


class HostTenancyMixin:
    """The host's answer to "which tenant is this?": the username, here."""

    permission_classes = (IsAuthenticated,)

    def get_owner(self):
        from django.contrib.contenttypes.models import ContentType

        user = self.request.user
        if not user.is_authenticated or user.username == "nobody":
            return None
        return ContentType.objects.get_for_model(ContentType).id, user.username


class HostConnectionViewSet(HostTenancyMixin, api_views.ConnectionViewSet):
    pass


class HostBindingViewSet(HostTenancyMixin, api_views.BindingViewSet):
    pass


class HostProjectionViewSet(HostTenancyMixin, api_views.ProjectionViewSet):
    pass


class OnlyUserOne(BasePermission):
    def has_permission(self, request, view):
        return request.user.username == "1"


class GatedConnectionViewSet(HostTenancyMixin, api_views.ConnectionViewSet):
    permission_classes = (OnlyUserOne,)


class OpenTargetViewSet(api_views.TargetViewSet):
    permission_classes = (AllowAny,)


router = DefaultRouter()
router.register("connections", HostConnectionViewSet, basename="connection")
router.register("bindings", HostBindingViewSet, basename="binding")
router.register("projections", HostProjectionViewSet, basename="projection")
router.register("gated", GatedConnectionViewSet, basename="gated")
router.register("targets", OpenTargetViewSet, basename="target")

urlpatterns = [path("host/", include((router.urls, "host"), namespace="host"))]


@pytest.fixture
def composed_settings(connectors_settings, settings):
    settings.ROOT_URLCONF = "tests.test_api_composed"
    # Deliberately: no API_OWNER_RESOLVER, no API_PERMISSION_CLASSES.
    assert "API_OWNER_RESOLVER" not in settings.DJANGO_CONNECTORS
    assert "API_PERMISSION_CLASSES" not in settings.DJANGO_CONNECTORS
    return settings


def _url(name, **kwargs):
    return reverse(f"host:{name}", kwargs=kwargs)


def _create_connection(client, **extra):
    return client.post(
        _url("connection-list"),
        {"provider": "memory", "auth_backend": "static", "metadata": {}, **extra},
        format="json",
    )


# --- reads -----------------------------------------------------------------


def test_host_owner_scopes_reads_with_no_setting_configured(
    composed_settings, make_connection, client_for
):
    mine = make_connection(owner_id="1")
    make_connection(owner_id="2")

    response = client_for("1").get(_url("connection-list"))

    assert response.status_code == 200
    assert [row["id"] for row in response.data] == [str(mine.id)]


def test_a_host_owner_of_none_sees_nothing(
    composed_settings, make_connection, client_for
):
    make_connection(owner_id="1")
    response = client_for("nobody").get(_url("connection-list"))
    assert response.status_code == 200
    assert response.data == []


# --- create ----------------------------------------------------------------


def test_created_connection_is_stamped_with_the_host_owner(
    composed_settings, client_for
):
    """Before this, POST /connections/ was an IntegrityError — a 500."""
    response = _create_connection(client_for("2"))

    assert response.status_code == 201, response.data
    connection = Connection.objects.get(pk=response.data["id"])
    assert connection.owner_object_id == "2"


def test_a_client_cannot_choose_its_own_owner(composed_settings, client_for):
    """The owner columns are absent from the serializer, so they are ignored."""
    response = _create_connection(client_for("2"), owner_object_id="1")
    assert response.status_code == 201
    assert Connection.objects.get(pk=response.data["id"]).owner_object_id == "2"


def test_no_owner_means_no_create_not_a_500(composed_settings, client_for):
    response = _create_connection(client_for("nobody"))
    assert response.status_code == 403
    assert Connection.objects.count() == 0


# --- writes scope on the same owner as reads --------------------------------


def test_writable_relations_scope_on_the_host_owner(
    composed_settings, make_connection, client_for
):
    """User 2 cannot attach a Binding to user 1's Connection, and can to their own."""
    victim = make_connection(owner_id="1")
    own = make_connection(owner_id="2")
    client = client_for("2")

    stolen = client.post(
        _url("binding-list"),
        {"connection": str(victim.id), "source": "memory", "config": VALID_CONFIG},
        format="json",
    )
    assert stolen.status_code == 400
    assert "connection" in stolen.data
    assert Binding.objects.filter(connection=victim).count() == 0

    legitimate = client.post(
        _url("binding-list"),
        {"connection": str(own.id), "source": "memory", "config": VALID_CONFIG},
        format="json",
    )
    assert legitimate.status_code == 201, legitimate.data
    assert Binding.objects.get(pk=legitimate.data["id"]).connection == own


def test_reads_and_writes_agree_when_the_host_owner_is_none(
    composed_settings, make_connection, client_for
):
    own = make_connection(owner_id="nobody")
    response = client_for("nobody").post(
        _url("binding-list"),
        {"connection": str(own.id), "source": "memory", "config": VALID_CONFIG},
        format="json",
    )
    # None fails closed on the writable relation too: not "nobody's
    # connection matches", but "no connection matches".
    assert response.status_code == 400


# --- permissions -----------------------------------------------------------


def test_subclass_permission_classes_win_over_the_unset_setting(
    composed_settings, make_connection, client_for
):
    """With the setting unset the library default is DenyAll; the subclass wins."""
    make_connection(owner_id="1")
    make_connection(owner_id="2")

    assert client_for("1").get(_url("gated-list")).status_code == 200
    assert client_for("2").get(_url("gated-list")).status_code == 403


def test_metadata_viewsets_honour_subclass_permissions(composed_settings):
    from rest_framework.test import APIClient

    response = APIClient().get(_url("target-list"))
    assert response.status_code == 200
    assert isinstance(response.data, list)


def test_the_library_default_is_still_deny_all(connectors_settings, settings):
    """A subclass that declares nothing inherits the sentinel, and the sentinel
    denies. This is what keeps an unconfigured host closed."""
    from django_connectors.api.views import BaseViewSet, ConfiguredPermissionsMixin

    assert BaseViewSet.permission_classes is None
    assert ConfiguredPermissionsMixin.permission_classes is None
    permissions = api_views.ConnectionViewSet().get_permissions()
    assert [type(p).__name__ for p in permissions] == ["DenyAll"]


# --- owner shapes ----------------------------------------------------------


@pytest.mark.parametrize(
    "shape",
    ["content_type_instance", "model_class", "model_instance", "bare_instance"],
)
def test_every_natural_owner_shape_normalises_to_the_same_answer(shape, db):
    """A ContentType *instance* used to resolve to the content type of the
    ContentType model itself, so every owner model collapsed onto one tenant."""
    from django.contrib.auth.models import Group
    from django.contrib.contenttypes.models import ContentType

    from django_connectors.api.scoping import normalize_owner

    group = Group.objects.create(name="g")
    expected = (ContentType.objects.get_for_model(Group).id, str(group.pk))
    owner = {
        "content_type_instance": (ContentType.objects.get_for_model(Group), group.pk),
        "model_class": (Group, group.pk),
        "model_instance": (group, group.pk),
        "bare_instance": group,
    }[shape]
    assert normalize_owner(owner) == expected


@pytest.mark.parametrize("owner", [("Group", 1), (True, 1), "1", (1,), None])
def test_an_owner_shape_that_cannot_be_trusted_is_refused(owner, db):
    from django.core.exceptions import ImproperlyConfigured

    from django_connectors.api.scoping import normalize_owner

    with pytest.raises(ImproperlyConfigured):
        normalize_owner(owner)
