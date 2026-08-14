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
    """Accept a model instance or an explicit ``(content_type, object_id)``."""
    from django.contrib.contenttypes.models import ContentType

    if isinstance(owner, tuple | list):
        content_type, object_id = owner
        if not isinstance(content_type, int):
            content_type = ContentType.objects.get_for_model(
                content_type if isinstance(content_type, type) else type(content_type)
            ).id
        return content_type, str(object_id)

    return ContentType.objects.get_for_model(type(owner)).id, str(owner.pk)


class OwnerScopedQuerysetMixin:
    """Restricts every queryset to the caller's owner. Fails closed.

    ``owner_lookup`` is the ORM path from this viewset's model to
    ``Connection.owner_*``. A subclass that does not declare one is a
    programming error and raises at class-definition time rather than quietly
    returning everything.
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

    def get_queryset(self):
        queryset = super().get_queryset()
        owner = resolve_owner(self.request)
        if owner is None:
            return queryset.none()
        content_type_id, object_id = owner

        prefix = f"{self.owner_lookup}__" if self.owner_lookup else ""
        return queryset.filter(
            **{
                f"{prefix}owner_content_type_id": content_type_id,
                f"{prefix}owner_object_id": object_id,
            }
        )
