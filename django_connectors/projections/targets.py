"""Host-registered targets.

A target is a *shape* the host accepts plus a callable that persists it. This is
the boundary: django-connectors validates records against the declared shape and
hands them over, and never learns what the writer does with them — which Django
model, which table, which side effects.

The registry is imperative rather than settings-driven because a
``TargetDefinition`` carries a ``writer`` callable, which cannot be expressed as
a dotted path without making every host write a shim. It is populated by
``autodiscover_modules("connectors")`` from this package's ``AppConfig.ready()``
— the same contract ``django.contrib.admin`` uses for ``admin.py`` — so hosts
register targets in ``<app>/connectors.py``.
"""

from dataclasses import dataclass, field
from typing import Any, Literal

from django_connectors.enums import IdentityScope
from django_connectors.exceptions import ConfigurationError
from django_connectors.projections.fields import Field, JSONField


@dataclass(frozen=True)
class ProjectedRecord:
    """One mapped record handed to a target writer.

    ``identity`` is never mutated by the library: it is the host's join key, and
    rewriting it would silently orphan whatever the host wrote before.
    """

    operation: Literal["upsert", "delete"]
    identity: dict
    values: dict = field(default_factory=dict)


@dataclass(frozen=True)
class WriterContext:
    """Everything a writer needs to scope a batch, passed once per batch.

    Carries the owner explicitly. Without it a writer receiving records from two
    tenants' Bindings has no way to tell them apart, and the multi-tenant
    guarantee the landing layer builds would be discarded at the last step.
    """

    owner_content_type_id: int | None
    owner_object_id: str
    connection_id: Any
    binding_id: Any
    projection_id: Any
    projection_version: int
    projection_run_id: Any
    mode: str
    batch_index: int
    load_ids: tuple = ()
    replace_scope: bool = False


class TargetDefinition:
    """The contract between the host and this library."""

    def __init__(
        self,
        *,
        key,
        fields,
        identity_fields,
        writer,
        identity_scope,
        label="",
        supports_scope_replace=False,
        description="",
    ):
        if not key:
            raise ConfigurationError("TargetDefinition needs a key")
        if not fields:
            raise ConfigurationError(f"target {key!r} declares no fields")
        for name, declared in fields.items():
            if not isinstance(declared, Field):
                raise ConfigurationError(
                    f"target {key!r} field {name!r} must be a "
                    f"django_connectors.projections.fields.Field, got "
                    f"{type(declared).__name__}"
                )
        if not identity_fields:
            raise ConfigurationError(
                f"target {key!r} declares no identity_fields; without them a "
                f"record cannot be matched to an existing one and every run "
                f"would insert duplicates"
            )
        missing = [name for name in identity_fields if name not in fields]
        if missing:
            raise ConfigurationError(
                f"target {key!r} identity_fields {missing} are not declared in fields"
            )
        # An identity is used as a dict key when records are collapsed, so an
        # object-valued one is unhashable and fails every run with a raw
        # TypeError naming neither the field nor the cause. It is also a poor
        # join key for the host, which is the only thing identity is for.
        unhashable = [
            name for name in identity_fields if isinstance(fields[name], JSONField)
        ]
        if unhashable:
            raise ConfigurationError(
                f"target {key!r} identity_fields {unhashable} are JSONField; "
                f"identity is a join key and must be a scalar field"
            )
        if not callable(writer):
            raise ConfigurationError(f"target {key!r} writer is not callable")
        # No default: whether identities are unique per owner or globally
        # decides whether two tenants can collide, and a wrong guess is a
        # cross-tenant data leak. The host must state it.
        if identity_scope not in set(IdentityScope.values):
            raise ConfigurationError(
                f"target {key!r} needs identity_scope="
                f"{' or '.join(repr(v) for v in IdentityScope.values)}; it "
                f"decides whether two owners may share an identity value."
            )

        self.key = key
        self.fields = dict(fields)
        self.identity_fields = tuple(identity_fields)
        self.writer = writer
        self.identity_scope = identity_scope
        self.label = label or key
        self.description = description
        # Replay rewrites the host's records. Only the host can enumerate what
        # it previously wrote, so only the host can declare replay safe.
        self.supports_scope_replace = supports_scope_replace

    @property
    def required_fields(self):
        return tuple(
            name for name, declared in self.fields.items() if declared.required
        )

    def describe(self):
        return {
            "key": self.key,
            "label": self.label,
            "description": self.description,
            "identity_fields": list(self.identity_fields),
            "identity_scope": self.identity_scope,
            "supports_scope_replace": self.supports_scope_replace,
            "fields": {
                name: declared.describe() for name, declared in self.fields.items()
            },
        }

    def __str__(self):
        return self.key


_registry: dict[str, TargetDefinition] = {}


def register_target(definition, *, override=False):
    """Register a target. Raises on a duplicate key unless `override`.

    Equality-based deduplication is not an option: a ``TargetDefinition``
    carries a ``writer`` callable, so a module genuinely re-executed produces a
    new function object and would compare unequal to itself.
    """
    if not isinstance(definition, TargetDefinition):
        raise ConfigurationError(
            f"register_target expects a TargetDefinition, got "
            f"{type(definition).__name__}"
        )
    existing = _registry.get(definition.key)
    if existing is not None and not override:
        if existing is definition:
            return definition
        raise ConfigurationError(
            f"target {definition.key!r} is already registered. Pass "
            f"override=True if replacing it is intended."
        )
    _registry[definition.key] = definition
    return definition


def get_target(key):
    try:
        return _registry[key]
    except KeyError:
        known = ", ".join(sorted(_registry)) or "(none registered)"
        raise ConfigurationError(
            f"unknown target {key!r}. Register it with register_target() in an "
            f"app's connectors.py. Registered: {known}"
        ) from None


def all_targets():
    return dict(_registry)


def target_keys():
    return sorted(_registry)


def is_registered(key):
    return key in _registry


def unregister_all():
    """Empty the registry. For tests only."""
    _registry.clear()
