"""Webhook callback routes, for the host to include at a path of its choosing::

    path("connectors/webhooks/", include("django_connectors.webhooks.urls")),

Deliberately separate from any API URLconf. These endpoints are unauthenticated
in the session/token sense — the provider proves itself with a signature, not a
login — so they must not sit behind the same middleware, throttles or
permission classes as the host's own API, and must not inherit its versioning.

There is no trailing slash. With one, a provider that posts to the path without
it gets Django's ``APPEND_SLASH`` 301, and a 301 makes most HTTP clients re-issue
the request as a GET with no body: the delivery is silently lost. The URL handed
to the provider always comes from ``reverse()`` on this pattern, so the exact
form is ours to fix.
"""

from django.urls import path

from django_connectors.webhooks import views

app_name = "django_connectors_webhooks"

urlpatterns = [
    path("<uuid:public_id>", views.receive, name="receive"),
]
