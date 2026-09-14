"""Model package.

Every model module must be imported here. A model whose module is never
imported is invisible to Django: ``makemigrations`` silently omits it, with no
error and no warning — which is why ``tests/test_models.py`` asserts the exact
set of registered models rather than trusting this file to be complete.
"""

from django_connectors.models.binding import Binding, BindingLock
from django_connectors.models.connection import Connection, ConnectionSecret
from django_connectors.models.projection import Projection, ProjectionRun
from django_connectors.models.run import Run
from django_connectors.models.webhook import WebhookSubscription

__all__ = [
    "Binding",
    "BindingLock",
    "Connection",
    "ConnectionSecret",
    "Projection",
    "ProjectionRun",
    "Run",
    "WebhookSubscription",
]
