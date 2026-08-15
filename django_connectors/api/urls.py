"""URL routing for the optional DRF API.

The host includes these explicitly at a path of their choosing::

    path("api/connectors/", include("django_connectors.api.urls")),

Webhook endpoints are deliberately NOT here — they are plain Django views with
their own URLconf, because a provider calling back is not an API client and must
not inherit the API's authentication.
"""

from django.urls import include, path
from rest_framework.routers import DefaultRouter

from django_connectors.api.views import (
    BindingViewSet,
    ConnectionViewSet,
    ProjectionViewSet,
    SourceViewSet,
    TargetViewSet,
    WebhookSubscriptionViewSet,
)

app_name = "django_connectors"

router = DefaultRouter()
router.register("connections", ConnectionViewSet, basename="connection")
router.register("bindings", BindingViewSet, basename="binding")
router.register("projections", ProjectionViewSet, basename="projection")
router.register(
    "webhook-subscriptions", WebhookSubscriptionViewSet, basename="webhooksubscription"
)
router.register("targets", TargetViewSet, basename="target")
router.register("sources", SourceViewSet, basename="source")

# Note the absence of flat "runs" and "projection-runs" routes. They nest under
# their parent instead, so no route exists whose default queryset spans tenants.
urlpatterns = [path("", include(router.urls))]
