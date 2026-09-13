# The optional REST API

Behind the `drf` extra. Off until you mount it, and denies every request until
you say who may call it.

```python
# urls.py
path("api/connectors/", include("django_connectors.api.urls")),
```

Routes: `connections/`, `bindings/`, `projections/`, `webhook-subscriptions/`,
plus `targets/` and `sources/` (registered shapes, no customer data). Runs nest
under their Binding and projection runs under their Projection — there is no
flat collection whose default queryset spans tenants.

Actions: `connections/<id>/test` and `/discover`; `bindings/<id>/run`,
`/landing-schema`, `/resources/<name>/sample`, `/runs`;
`projections/<id>/validate`, `/preview`, `/mapping` (PATCH), `/run`,
`/replay`, `/runs`. Each one is a thin call into `django_connectors.services`.

## Two ways to authorise it

**Settings-driven** — for a host with no API of its own:

```python
DJANGO_CONNECTORS = {
    "API_OWNER_RESOLVER": "myapp.tenancy.owner_for_request",   # callable(request) -> owner
    "API_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
}
```

**Composed** — for a host that already has authentication, permission classes
and a notion of the current tenant. Leave both settings unset. Subclass the
viewsets, set `permission_classes` the ordinary DRF way, and override
`get_owner()`:

```python
from django_connectors.api import views as connector_views

class TenancyMixin:
    authentication_classes = MyAuthClasses
    permission_classes = (MyPermission,)

    def get_owner(self):
        return self.request.team          # a model instance, or (content_type, object_id)

class ConnectionViewSet(TenancyMixin, connector_views.ConnectionViewSet): ...
class BindingViewSet(TenancyMixin, connector_views.BindingViewSet): ...
class ProjectionViewSet(TenancyMixin, connector_views.ProjectionViewSet): ...
```

Register those on your own router, under your own URL prefix, with your own
throttles and schema generation. You get every route and action above.

## One owner, three uses

Whatever `get_owner()` returns (or the resolver setting, in the settings-driven
case) is the single answer used for:

1. **reads** — every queryset is filtered to that owner;
2. **writable relations** — `Binding.connection` and `Projection.binding` only
   accept objects that owner holds, which is what stops a caller attaching a
   Binding to another tenant's Connection and receiving its provider token;
3. **create** — a new Connection is stamped with that owner. The owner columns
   are not in the serializer, so a client cannot choose its tenant.

`None` means "nobody": reads return nothing, writable relations match nothing,
and create is refused with 403. A host cannot end up scoping its list view by
one tenancy and its foreign keys by another.

## What stays closed

- `permission_classes` unset on a subclass means the setting applies, and the
  setting unset means `DenyAll`. Nothing is open by accident.
- `auth_metadata`, `auth_reference`, `setup_token`, `ConnectionSecret` and a
  webhook's `public_id`/`secret_reference` are never serialized.
- Error text is scrubbed on the way out as well as on the way in.
- Every viewset must mix in `OwnerScopedQuerysetMixin`, and every writable
  relation must be an `OwnerScopedPrimaryKeyRelatedField`; both are asserted
  at import time.
