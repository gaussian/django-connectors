"""URL configuration.

Webhook URLs are mounted separately from the API on purpose: a provider calling
back is not an API client and must not inherit the API's authentication.
"""

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("connectors/hooks/", include("django_connectors.webhooks.urls")),
    # The DRF API is optional and denies every request until
    # API_OWNER_RESOLVER and API_PERMISSION_CLASSES are configured.
    # path("api/connectors/", include("django_connectors.api.urls")),
]
