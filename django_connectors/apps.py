"""Django application configuration."""

from django.apps import AppConfig
from django.utils.module_loading import autodiscover_modules


class DjangoConnectorsConfig(AppConfig):
    # `label` is baked into every migration's FK targets and every table name,
    # so it is immutable once released. The default is what we ship.
    name = "django_connectors"
    verbose_name = "Connectors"

    def ready(self) -> None:
        # Importing registers the check functions with Django's registry.
        from django_connectors import checks  # noqa: F401

        # Connects the pre_delete guard that stops a Binding being deleted while
        # its landing tables still hold customer rows.
        from django_connectors.services import retention  # noqa: F401

        # Import each installed app's `connectors` module so that host calls to
        # register_target() have run before anything reads the target registry.
        # This is the contract django.contrib.admin uses for `admin.py`, and it
        # is why the target registry is imperative while sources/auth backends
        # are settings-driven: targets carry a `writer` callable, which cannot
        # be expressed as a dotted path without making every host write a shim.
        #
        # Note this *populates* registries; it must never *read* one, or
        # behaviour would depend on INSTALLED_APPS ordering.
        autodiscover_modules("connectors")
