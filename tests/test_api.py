"""API hardening.

The API is the part of this library most likely to leak data, for reasons that
are entirely mundane: DRF defaults to ``AllowAny``, the natural queryset is
``Model.objects.all()``, and the natural route is a flat collection. Each test
here pins one of those doors shut.
"""

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.urls import include, path, reverse

# The API is behind the `drf` extra, so the whole module skips without it.
# The `test-minimal` CI tier runs with zero extras installed and would
# otherwise fail at collection rather than skipping.
pytest.importorskip("rest_framework")

from rest_framework import viewsets
from rest_framework.generics import GenericAPIView
from rest_framework.test import APIClient

from django_connectors.api import views as api_views
from django_connectors.api.scoping import OwnerScopedQuerysetMixin
from django_connectors.enums import RunTrigger
from django_connectors.models import Binding, Run
from django_connectors.services import runs as run_services
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db

urlpatterns = [path("api/connectors/", include("django_connectors.api.urls"))]


@pytest.fixture
def api_settings(connectors_settings, settings):
    settings.ROOT_URLCONF = "tests.test_api"
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "API_OWNER_RESOLVER": "tests.test_api.resolve_owner",
        "API_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    }
    return settings


#: Set by tests to control which owner the resolver reports.
CURRENT_OWNER = {"object_id": "1"}


def resolve_owner(request):
    from django.contrib.contenttypes.models import ContentType

    if CURRENT_OWNER.get("object_id") is None:
        return None
    return ContentType.objects.get_for_model(ContentType).id, CURRENT_OWNER["object_id"]


@pytest.fixture(autouse=True)
def reset_owner():
    CURRENT_OWNER["object_id"] = "1"
    yield
    CURRENT_OWNER["object_id"] = "1"


@pytest.fixture
def client_for():
    from django.contrib.auth.models import User

    def factory(username=None):
        username = username or f"apiuser{User.objects.count()}"
        user = User.objects.create_user(username=username, password="x")
        client = APIClient()
        client.force_authenticate(user=user)
        return client

    return factory


# --- structural guarantees -------------------------------------------------


def test_every_viewset_is_owner_scoped():
    """Asserted by introspection so a viewset added later inherits the rule."""
    unscoped = [
        name
        for name, obj in vars(api_views).items()
        if isinstance(obj, type)
        and issubclass(obj, GenericAPIView)
        and obj.__module__ == api_views.__name__
        and not getattr(obj, "abstract_scope", False)
        and not issubclass(obj, OwnerScopedQuerysetMixin)
    ]
    assert unscoped == []


def test_a_viewset_without_an_owner_lookup_fails_at_class_definition():
    """The mistake must not be able to reach production behind an untested path."""
    with pytest.raises(ImproperlyConfigured, match="owner_lookup"):

        class Broken(OwnerScopedQuerysetMixin, viewsets.ModelViewSet):
            queryset = Binding.objects.all()


def test_there_are_no_flat_run_collections():
    """A flat /runs/ has no owner in the path and no safe default queryset."""
    from django_connectors.api.urls import router

    registered = {prefix for prefix, _, _ in router.registry}
    assert "runs" not in registered
    assert "projection-runs" not in registered


def test_default_permission_denies_everyone(connectors_settings, settings, client_for):
    """DRF's own default is AllowAny; ours must not be."""
    settings.ROOT_URLCONF = "tests.test_api"
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "API_OWNER_RESOLVER": "tests.test_api.resolve_owner",
    }
    response = client_for().get(reverse("django_connectors:connection-list"))
    assert response.status_code == 403


def test_requests_are_denied_when_no_owner_resolver_is_configured(
    connectors_settings, settings, client_for
):
    settings.ROOT_URLCONF = "tests.test_api"
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "API_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    }
    with pytest.raises(ImproperlyConfigured, match="API_OWNER_RESOLVER"):
        client_for().get(reverse("django_connectors:connection-list"))


# --- tenant isolation ------------------------------------------------------


def test_a_caller_sees_only_their_own_connections(
    api_settings, make_connection, client_for
):
    make_connection(owner_id="1", provider="mine")
    make_connection(owner_id="2", provider="theirs")

    response = client_for().get(reverse("django_connectors:connection-list"))
    assert response.status_code == 200
    providers = {item["provider"] for item in response.json()}
    assert providers == {"mine"}


def test_another_tenants_binding_is_not_retrievable(
    api_settings, make_binding, client_for
):
    other = make_binding(owner_id="2")
    response = client_for().get(
        reverse("django_connectors:binding-detail", args=[other.id])
    )
    assert response.status_code == 404


def test_runs_are_scoped_through_their_binding(api_settings, make_binding, client_for):
    mine = make_binding(owner_id="1")
    theirs = make_binding(owner_id="2")
    Run.objects.create(binding=mine, trigger=RunTrigger.MANUAL)
    Run.objects.create(binding=theirs, trigger=RunTrigger.MANUAL)

    response = client_for().get(
        reverse("django_connectors:binding-runs", args=[mine.id])
    )
    assert response.status_code == 200
    assert len(response.json()) == 1

    denied = client_for("other").get(
        reverse("django_connectors:binding-runs", args=[theirs.id])
    )
    assert denied.status_code == 404


def test_an_unresolvable_owner_sees_nothing(api_settings, make_connection, client_for):
    """Fails closed: no owner means an empty queryset, not an unfiltered one."""
    make_connection(owner_id="1")
    CURRENT_OWNER["object_id"] = None
    response = client_for().get(reverse("django_connectors:connection-list"))
    assert response.json() == []


# --- credential exposure ---------------------------------------------------


def test_credentials_are_never_serialized(api_settings, make_connection, client_for):
    connection = make_connection(owner_id="1")
    connection.auth_reference = "vault://super-secret-handle"
    connection.auth_metadata = {"scopes": ["mail.read"]}
    connection.save()

    payload = (
        client_for()
        .get(reverse("django_connectors:connection-detail", args=[connection.id]))
        .json()
    )

    assert "auth_metadata" not in payload
    assert "auth_reference" not in payload
    assert "setup_token" not in payload
    assert "super-secret-handle" not in str(payload)


def test_error_messages_are_scrubbed_again_on_the_way_out(
    api_settings, make_binding, client_for
):
    """Defence in depth: covers rows written before the scrubber existed."""
    binding = make_binding(owner_id="1")
    Run.objects.create(
        binding=binding,
        trigger=RunTrigger.MANUAL,
        status="failed",
        error_type="OperationalError",
        # Simulates a row that bypassed scrub() at write time.
        error_message=(
            "could not connect using "
            "mysql+pymysql://root:sup3rSekritPassw0rd@db.internal:3306/landing"
        ),
    )
    payload = (
        client_for()
        .get(reverse("django_connectors:binding-runs", args=[binding.id]))
        .json()
    )
    assert "sup3rSekritPassw0rd" not in str(payload)


def test_webhook_public_id_and_secret_reference_are_not_exposed(
    api_settings, make_binding, client_for
):
    from django_connectors.models import WebhookSubscription

    binding = make_binding(owner_id="1")
    subscription = WebhookSubscription.objects.create(
        binding=binding, secret_reference="vault://hmac-key"
    )
    payload = (
        client_for()
        .get(
            reverse(
                "django_connectors:webhooksubscription-detail", args=[subscription.id]
            )
        )
        .json()
    )

    assert "public_id" not in payload
    assert "secret_reference" not in payload
    assert str(subscription.public_id) not in str(payload)


# --- behaviour -------------------------------------------------------------


def test_landing_schema_and_sample_are_owner_scoped_and_bounded(
    api_settings, make_binding, client_for
):
    binding = make_binding(
        owner_id="1", config=memory_config(batches=[[{"id": "e1", "v": "a"}]])
    )
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)

    schema = (
        client_for()
        .get(reverse("django_connectors:binding-landing-schema", args=[binding.id]))
        .json()
    )
    assert schema["status"] == "ready"

    sample = (
        client_for()
        .get(f"/api/connectors/bindings/{binding.id}/resources/events/sample/")
        .json()
    )
    assert sample["rows"]
    assert not any(
        key.startswith(("_connector_", "_dlt_")) for key in sample["rows"][0]
    )


def test_projection_version_cannot_be_set_by_a_client(
    api_settings, make_binding, client_for
):
    """A client-chosen version would break the superseded-run check."""
    from django_connectors.api.serializers import ProjectionSerializer

    assert "version" in ProjectionSerializer.Meta.read_only_fields


def test_targets_endpoint_exposes_shape_not_tenant_data(api_settings, client_for):
    from django_connectors.projections.fields import StringField
    from django_connectors.projections.targets import (
        TargetDefinition,
        register_target,
        unregister_all,
    )

    unregister_all()
    register_target(
        TargetDefinition(
            key="events",
            fields={"external_id": StringField(required=True)},
            identity_fields=("external_id",),
            identity_scope="owner",
            writer=lambda records, context: len(records),
        )
    )
    try:
        payload = client_for().get(reverse("django_connectors:target-list")).json()
        assert payload[0]["key"] == "events"
        assert payload[0]["fields"]["external_id"]["required"] is True
    finally:
        unregister_all()


# --- cross-tenant writes ---------------------------------------------------


def test_a_binding_cannot_be_attached_to_another_tenants_connection(
    api_settings, make_connection, client_for
):
    """The attack this closes is credential exfiltration, not untidiness.

    Scoping a viewset's queryset protects reads only. DRF builds a writable
    relation from the related model's *unfiltered* manager, so a plain
    ModelSerializer accepted another tenant's Connection id — and the attacker
    then points the Binding at a server they control, whereupon the runner sends
    the victim's provider token there in an Authorization header.
    """
    victim = make_connection(owner_id="2", provider="victim")
    response = client_for().post(
        reverse("django_connectors:binding-list"),
        {"connection": str(victim.id), "source": "memory", "config": {}},
        format="json",
    )
    assert response.status_code == 400
    assert "connection" in response.json()
    assert not Binding.objects.filter(connection=victim).exists()


def test_a_binding_cannot_be_repointed_at_another_tenants_connection(
    api_settings, make_binding, make_connection, client_for
):
    """PATCH is the same hole as POST and was equally open."""
    mine = make_binding(owner_id="1")
    victim = make_connection(owner_id="2", provider="victim")

    response = client_for().patch(
        reverse("django_connectors:binding-detail", args=[mine.id]),
        {"connection": str(victim.id)},
        format="json",
    )
    assert response.status_code == 400

    mine.refresh_from_db()
    assert mine.connection.owner_object_id == "1"


def test_a_projection_cannot_be_attached_to_another_tenants_binding(
    api_settings, make_binding, client_for
):
    from django_connectors.models import Projection

    victim = make_binding(owner_id="2")
    response = client_for().post(
        reverse("django_connectors:projection-list"),
        {
            "binding": str(victim.id),
            "resource": "events",
            "target": "events",
            "name": "stolen",
            "mapping": {"external_id": {"source": "id"}},
        },
        format="json",
    )
    assert response.status_code == 400
    assert not Projection.objects.filter(binding=victim).exists()


def test_a_binding_can_still_be_created_against_your_own_connection(
    api_settings, make_connection, client_for
):
    """The guard must not break the legitimate path.

    The config has to be a real one: the serializer now runs the source's own
    ``validate_config``, and the memory source refuses an empty mapping.
    """
    mine = make_connection(owner_id="1", provider="mine")
    response = client_for().post(
        reverse("django_connectors:binding-list"),
        {
            "connection": str(mine.id),
            "source": "memory",
            "config": {"resources": {"events": {"primary_key": "id"}}},
        },
        format="json",
    )
    assert response.status_code == 201, response.json()
    assert Binding.objects.filter(connection=mine).count() == 1


def test_every_writable_relation_is_owner_scoped():
    """Asserted structurally so a serializer added later inherits the rule."""
    from django_connectors.api import serializers as api_serializers

    # Raises ImproperlyConfigured if any writable FK is unscoped.
    api_serializers._assert_writable_relations_are_owner_scoped()


# --- Binding configuration is validated here too ---------------------------
#
# DRF never calls `full_clean()`, so without a `validate()` of its own the API
# is the one way into the database that accepts a Binding guaranteed to fail
# its first Run.


def test_the_api_refuses_a_malformed_binding_config(
    api_settings, make_connection, client_for
):
    mine = make_connection(owner_id="1", provider="mine")
    response = client_for().post(
        reverse("django_connectors:binding-list"),
        {"connection": str(mine.id), "source": "memory", "config": {}},
        format="json",
    )
    assert response.status_code == 400, response.json()
    assert "config" in response.json()
    assert not Binding.objects.exists()


def test_the_api_refuses_an_unregistered_source(
    api_settings, make_connection, client_for
):
    mine = make_connection(owner_id="1", provider="mine")
    response = client_for().post(
        reverse("django_connectors:binding-list"),
        {"connection": str(mine.id), "source": "not-registered", "config": {}},
        format="json",
    )
    assert response.status_code == 400, response.json()
    assert "source" in response.json()


def test_the_api_refuses_a_landing_table_name_that_would_not_fit(
    api_settings, make_connection, client_for
):
    """63 characters, checked before the row exists — it has no landing_key yet."""
    mine = make_connection(owner_id="1", provider="mine")
    response = client_for().post(
        reverse("django_connectors:binding-list"),
        {
            "connection": str(mine.id),
            "source": "memory",
            "resources": ["r" * 60],
            "config": {"resources": {"r" * 60: {"primary_key": "id"}}},
        },
        format="json",
    )
    assert response.status_code == 400, response.json()
    assert "resources" in response.json()


def test_a_patch_is_validated_against_the_source_already_on_the_row(
    api_settings, make_binding, client_for
):
    """A PATCH carries only what changed.

    Validating the payload alone would accept a config that is invalid for the
    source already stored — which is the whole failure this closes.
    """
    binding = make_binding(owner_id="1", config=memory_config(batches=[[{"id": "1"}]]))
    response = client_for().patch(
        reverse("django_connectors:binding-detail", args=[binding.id]),
        {"config": {"resources": {}}},
        format="json",
    )
    assert response.status_code == 400, response.json()
    assert "config" in response.json()

    binding.refresh_from_db()
    assert binding.config["resources"], "the invalid config was written anyway"


# --- webhook subscriptions -------------------------------------------------


def _subscription_for(binding, **kwargs):
    from django_connectors.enums import WebhookStatus
    from django_connectors.models import WebhookSubscription

    return WebhookSubscription.objects.create(
        binding=binding, status=kwargs.pop("status", WebhookStatus.ACTIVE), **kwargs
    )


def test_the_api_can_renew_a_subscription(api_settings, make_binding, client_for):
    """The API used to have no renewal at all, so the only way to renew one
    subscription was to wait for the sweep — or to move ``renew_at`` by hand and
    hope the sweep was scheduled."""
    from django_connectors.webhooks import services as webhook_services

    subscription = _subscription_for(make_binding(owner_id="1"))

    renewed = []
    original = webhook_services.renew_subscription
    webhook_services.renew_subscription = lambda row, *, actor=None, now=None: (
        renewed.append(row.id) or row
    )
    try:
        response = client_for().post(
            reverse(
                "django_connectors:webhooksubscription-renew", args=[subscription.id]
            )
        )
    finally:
        webhook_services.renew_subscription = original

    assert response.status_code == 200, response.json()
    assert renewed == [subscription.id]


def test_renewing_reports_a_provider_refusal_as_a_conflict(
    api_settings, make_binding, client_for
):
    """The memory source declares no webhook adapter, which is the fail-closed
    case: there is nothing to renew, and that is a 409 rather than a 500."""
    subscription = _subscription_for(make_binding(owner_id="1"))

    response = client_for().post(
        reverse("django_connectors:webhooksubscription-renew", args=[subscription.id])
    )

    assert response.status_code == 409, response.json()
    assert "webhook adapter" in response.json()["detail"]


def test_renewing_another_tenants_subscription_is_a_404(
    api_settings, make_binding, client_for
):
    victim = _subscription_for(make_binding(owner_id="2"))

    response = client_for().post(
        reverse("django_connectors:webhooksubscription-renew", args=[victim.id])
    )

    assert response.status_code == 404


def test_subscriptions_still_cannot_be_created_through_the_api(
    api_settings, make_binding, client_for
):
    """POST had to be allowed for `renew`; `create` must stay closed.

    Creating one needs a callback origin the host supplies — deriving it from
    ``request.get_host()`` would let a spoofed Host header hand the provider
    someone else's callback URL.
    """
    binding = make_binding(owner_id="1")
    response = client_for().post(
        reverse("django_connectors:webhooksubscription-list"),
        {"binding": str(binding.id)},
        format="json",
    )
    assert response.status_code == 405, response.json()
