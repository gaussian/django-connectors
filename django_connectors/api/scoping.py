"""Owner scoping for the API.

This is the module that decides which tenant a request may see, and it is
written to fail closed at every step.

The reason it is this defensive: the natural way to write these viewsets is
``queryset = Run.objects.all()`` with a flat ``/runs/`` route. That returns
every tenant's rows — including ``error_message``, which is a field capable of
carrying provider detail — to any authenticated caller. DRF then compounds it,
because ``DEFAULT_PERMISSION_CLASSES`` is ``AllowAny``, so a host following
install instructions verbatim publishes the whole thing unauthenticated.

So: there are no flat collections (runs nest under their binding), every viewset
must mix in :class:`OwnerScopedQuerysetMixin` — asserted at class-definition
time, not by review — and the default permission denies everyone until the host
configures a policy.
"""

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from django_connectors.conf import SETTING_NAME, conf


def resolve_owner(request):
    """Return ``(content_type_id, object_id)`` for the caller, or None.

    Delegated to the host, because only the host knows what owns a Connection —
    a Team, an Organisation, a User. Configure with
    ``DJANGO_CONNECTORS["API_OWNER_RESOLVER"]``, a dotted path to a callable
    taking the request.
    """
    path = conf.API_OWNER_RESOLVER
    if not path:
        raise ImproperlyConfigured(
            f"{SETTING_NAME}['API_OWNER_RESOLVER'] is not set, so the API "
            f"cannot tell which tenant a request belongs to. Set it to a dotted "
            f"path to a callable(request) returning the owning object (or "
            f"(content_type, object_id)). Until then every API request is "
            f"denied — which is the safe default, not a bug."
        )
    resolver = import_string(path) if isinstance(path, str) else path
    owner = resolver(request)
    if owner is None:
        return None
    return normalize_owner(owner)


def normalize_owner(owner):
    """``(content_type_id, object_id)`` from any shape a host naturally returns.

    A model instance; or a pair whose first slot is a content type id, a
    ``ContentType`` instance, a model class or a model instance. Anything else
    is refused: guessing here is how two owner models that happen to share a
    primary key collapse onto one tenant.
    """
    from django.contrib.contenttypes.models import ContentType
    from django.db import models

    if isinstance(owner, tuple | list) and len(owner) == 2:
        content_type, object_id = owner
        if isinstance(content_type, ContentType):
            content_type = content_type.id
        elif isinstance(content_type, type) and issubclass(content_type, models.Model):
            content_type = ContentType.objects.get_for_model(content_type).id
        elif isinstance(content_type, models.Model):
            content_type = ContentType.objects.get_for_model(type(content_type)).id
        elif not isinstance(content_type, int) or isinstance(content_type, bool):
            raise ImproperlyConfigured(
                f"owner content type must be a ContentType, a model class, a "
                f"model instance or a content type id, got {content_type!r}"
            )
        return content_type, str(object_id)

    if isinstance(owner, models.Model):
        return ContentType.objects.get_for_model(type(owner)).id, str(owner.pk)

    raise ImproperlyConfigured(
        f"owner must be a model instance or a (content_type, object_id) pair, "
        f"got {type(owner).__name__}"
    )


class OwnerScopedQuerysetMixin:
    """Restricts every queryset to the caller's owner. Fails closed.

    ``owner_lookup`` is the ORM path from this viewset's model to
    ``Connection.owner_*``. A subclass that does not declare one is a
    programming error and raises at class-definition time rather than quietly
    returning everything.

    **Composing with your own API.** A host that already has an authentication
    scheme overrides :meth:`get_owner` and sets ``permission_classes`` on a
    subclass; ``API_OWNER_RESOLVER`` and ``API_PERMISSION_CLASSES`` then never
    need to be set. The owner this method returns is what scopes reads, what
    scopes writable relations on the serializer (via the serializer context),
    and what a newly created Connection is stamped with — one answer, three
    uses, so a host cannot scope reads by one tenancy and writes by another.
    """

    #: e.g. "" on Connection, "connection" on Binding, "binding__connection" on Run.
    owner_lookup = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Checked here rather than at request time so the mistake cannot reach
        # production behind an untested code path.
        if getattr(cls, "owner_lookup", None) is None and not getattr(
            cls, "abstract_scope", False
        ):
            raise ImproperlyConfigured(
                f"{cls.__name__} mixes in OwnerScopedQuerysetMixin but declares "
                f"no owner_lookup, so it cannot scope its queryset to a tenant."
            )

    def get_owner(self):
        """``(content_type_id, object_id)`` for the caller, or None for nobody.

        The default delegates to ``API_OWNER_RESOLVER``. Override it to answer
        from your own request state — ``self.request.team``, a session, a token
        claim. Return a model instance or an explicit ``(content_type,
        object_id)`` pair; both are normalised. Return None to show nothing.
        """
        return resolve_owner(self.request)

    def resolved_owner(self):
        """:meth:`get_owner`, normalised. Cached per request so the three
        consumers (queryset, serializer context, create) agree on one answer."""
        if not hasattr(self, "_resolved_owner"):
            owner = self.get_owner()
            self._resolved_owner = None if owner is None else normalize_owner(owner)
        return self._resolved_owner

    def get_queryset(self):
        return scope_to_owner(
            super().get_queryset(), self.resolved_owner(), self.owner_lookup
        )

    def get_serializer_context(self):
        # The serializer's writable relations scope on this, not on the setting,
        # so an overridden get_owner() governs writes as well as reads.
        context = super().get_serializer_context()
        context[OWNER_CONTEXT_KEY] = self.resolved_owner()
        return context


#: Serializer context key carrying the viewset's resolved owner.
OWNER_CONTEXT_KEY = "connectors_owner"


def scope_to_owner(queryset, owner, lookup=""):
    """`queryset` restricted to `owner`, or empty when there is no owner.

    The one expression that decides which tenant's rows a caller sees — used
    for reads and for writable relations alike, so the two cannot drift.
    """
    if owner is None:
        return queryset.none()
    content_type_id, object_id = owner
    prefix = f"{lookup}__" if lookup else ""
    return queryset.filter(
        **{
            f"{prefix}owner_content_type_id": content_type_id,
            f"{prefix}owner_object_id": object_id,
        }
    )
