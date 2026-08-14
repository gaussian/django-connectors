"""django-connectors — connect Django applications to third-party systems.

The public API is exposed lazily (PEP 562). Two reasons:

* ``from .models import Connection`` at package scope raises
  ``AppRegistryNotReady``, because this module is imported while Django is still
  building the app registry.
* ``import dlt`` costs ~0.6s — 25x a bare interpreter start and 20x
  ``import django`` — and is paid by every ``manage.py`` invocation, autoreload
  cycle, worker fork and test collection. Nothing here may pull it in; a Run
  imports it when a Run actually needs it.

Both properties are enforced by tests, not convention.
"""

from typing import Any

__version__ = "0.0.1"

# name -> module it lives in. Kept explicit so that `import django_connectors`
# stays cheap and so that the public surface is a single readable list.
_EXPORTS = {
    # Errors
    "ConnectorError": "django_connectors.exceptions",
    "ConfigurationError": "django_connectors.exceptions",
    "AuthError": "django_connectors.exceptions",
    "CredentialsRevoked": "django_connectors.exceptions",
    "CredentialsExpired": "django_connectors.exceptions",
    "SourceError": "django_connectors.exceptions",
    "LandingError": "django_connectors.exceptions",
    "LandingSchemaError": "django_connectors.exceptions",
    "LandingWedgedError": "django_connectors.exceptions",
    "ProjectionError": "django_connectors.exceptions",
    "MappingValidationError": "django_connectors.exceptions",
    "CastError": "django_connectors.exceptions",
    "SchemaDriftError": "django_connectors.exceptions",
    "TargetWriteError": "django_connectors.exceptions",
    "LockNotAcquired": "django_connectors.exceptions",
    # Targets — what a host registers and what its writer receives
    "TargetDefinition": "django_connectors.projections.targets",
    "register_target": "django_connectors.projections.targets",
    "ProjectedRecord": "django_connectors.projections.targets",
    "WriterContext": "django_connectors.projections.targets",
    # Target field types
    "BooleanField": "django_connectors.projections.fields",
    "DateField": "django_connectors.projections.fields",
    "DateTimeField": "django_connectors.projections.fields",
    "DecimalField": "django_connectors.projections.fields",
    "FloatField": "django_connectors.projections.fields",
    "IntegerField": "django_connectors.projections.fields",
    "JSONField": "django_connectors.projections.fields",
    "StringField": "django_connectors.projections.fields",
    # Sources
    "SourceDefinition": "django_connectors.sources.base",
}

__all__ = ["__version__", *sorted(_EXPORTS)]


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    value = getattr(import_module(module_path), name)
    globals()[name] = value  # cache, so this costs once
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
