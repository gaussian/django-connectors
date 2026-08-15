"""API permissions.

The default is :class:`DenyAll`, not DRF's ``AllowAny``. A connector API exposes
customer credentials metadata, landed sample rows and run diagnostics; shipping
it open by default and relying on hosts to notice is the wrong way round.
"""

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_connectors.conf import conf


def _permission_base():
    from rest_framework.permissions import BasePermission

    return BasePermission


def deny_all_class():
    """Refuse every request. The default until a host configures a policy."""

    class DenyAll(_permission_base()):
        message = (
            "django-connectors' API denies all requests until "
            "DJANGO_CONNECTORS['API_PERMISSION_CLASSES'] is configured."
        )

        def has_permission(self, request, view):
            return False

        def has_object_permission(self, request, view, obj):
            return False

    return DenyAll


def configured_permission_classes():
    """Resolve the host's permission classes, or fall back to DenyAll."""
    configured = conf.API_PERMISSION_CLASSES
    if not configured:
        return [deny_all_class()]

    classes = []
    for entry in configured:
        try:
            classes.append(import_string(entry) if isinstance(entry, str) else entry)
        except ImportError as exc:
            raise ImproperlyConfigured(
                f"API_PERMISSION_CLASSES entry {entry!r} could not be imported: {exc}"
            ) from exc
    return classes
