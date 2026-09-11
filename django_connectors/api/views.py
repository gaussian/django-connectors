"""API viewsets.

Deliberately **no flat ``/runs/`` or ``/projection-runs/`` collections.** Runs
nest under their Binding and projection runs under their Projection, so there is
no route whose default queryset spans tenants — the shape of the URL makes the
scoping mistake unavailable rather than merely discouraged.

Every viewset mixes in :class:`OwnerScopedQuerysetMixin`, which is asserted at
class-definition time, and the module-level check below asserts it again across
the whole module so a viewset added later cannot skip it.
"""

from django.core.exceptions import ImproperlyConfigured
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

from django_connectors.api.permissions import configured_permission_classes
from django_connectors.api.scoping import OwnerScopedQuerysetMixin
from django_connectors.api.serializers import (
    BindingSerializer,
    ConnectionSerializer,
    MappingUpdateSerializer,
    ProjectionRunSerializer,
    ProjectionSerializer,
    RunSerializer,
    WebhookSubscriptionSerializer,
)
from django_connectors.enums import RunTrigger
from django_connectors.exceptions import ConnectorError
from django_connectors.models import (
    Binding,
    Connection,
    Projection,
    ProjectionRun,
    Run,
    WebhookSubscription,
)
from django_connectors.projections.targets import all_targets
from django_connectors.services import discovery
from django_connectors.services import projections as projection_services
from django_connectors.services import runs as run_services


class BaseViewSet(OwnerScopedQuerysetMixin, viewsets.ModelViewSet):
    abstract_scope = True

    def get_permissions(self):
        return [permission() for permission in configured_permission_classes()]


class ConnectionViewSet(BaseViewSet):
    owner_lookup = ""
    queryset = Connection.objects.all()
    serializer_class = ConnectionSerializer

    @action(detail=True, methods=["post"])
    def test(self, request, pk=None):
        connection = self.get_object()
        try:
            from django_connectors.registry import auth_backends

            backend = auth_backends.get(connection.auth_backend)
            result = backend.test(connection)
        except ConnectorError as exc:
            return _error(exc)
        return Response({"ok": True, "detail": str(result)})

    @action(detail=True, methods=["get"])
    def discover(self, request, pk=None):
        connection = self.get_object()
        try:
            return Response(
                discovery.discover_remote(
                    connection, query=request.query_params.get("q")
                )
            )
        except ConnectorError as exc:
            return _error(exc)


class BindingViewSet(BaseViewSet):
    owner_lookup = "connection"
    queryset = Binding.objects.select_related("connection")
    serializer_class = BindingSerializer

    @action(detail=True, methods=["post"])
    def run(self, request, pk=None):
        binding = self.get_object()
        run = run_services.run_binding(
            binding, trigger=RunTrigger.MANUAL, actor=request.user
        )
        return Response(RunSerializer(run).data, status=status.HTTP_202_ACCEPTED)

    @action(detail=True, methods=["get"], url_path="landing-schema")
    def landing_schema(self, request, pk=None):
        return Response(discovery.get_landing_schema(self.get_object()))

    @action(
        detail=True, methods=["get"], url_path=r"resources/(?P<resource>[^/.]+)/sample"
    )
    def sample(self, request, pk=None, resource=None):
        binding = self.get_object()
        try:
            rows = discovery.sample_resource(binding, resource)
        except ConnectorError as exc:
            return _error(exc)
        return Response({"resource": resource, "rows": rows})

    @action(detail=True, methods=["get"])
    def runs(self, request, pk=None):
        binding = self.get_object()
        queryset = Run.objects.filter(binding=binding)[:100]
        return Response(RunSerializer(queryset, many=True).data)


class ProjectionViewSet(BaseViewSet):
    owner_lookup = "binding__connection"
    queryset = Projection.objects.select_related("binding__connection")
    serializer_class = ProjectionSerializer

    @action(detail=True, methods=["post"])
    def validate(self, request, pk=None):
        result = projection_services.validate_projection(self.get_object())
        return Response(result.as_dict())

    @action(detail=True, methods=["post"])
    def preview(self, request, pk=None):
        try:
            return Response(
                projection_services.preview_projection(
                    self.get_object(), limit=request.data.get("limit")
                )
            )
        except ConnectorError as exc:
            return _error(exc)

    @action(detail=True, methods=["patch"], url_path="mapping")
    def update_mapping(self, request, pk=None):
        serializer = MappingUpdateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            projection = projection_services.update_mapping(
                self.get_object(), **serializer.validated_data
            )
        except ConnectorError as exc:
            return _error(exc, status.HTTP_409_CONFLICT)
        return Response(ProjectionSerializer(projection).data)

    @action(detail=True, methods=["post"])
    def run(self, request, pk=None):
        projection_run = projection_services.run_projection(self.get_object())
        return Response(
            ProjectionRunSerializer(projection_run).data,
            status=status.HTTP_202_ACCEPTED,
        )

    @action(detail=True, methods=["post"])
    def replay(self, request, pk=None):
        try:
            projection_run = projection_services.replay_projection(self.get_object())
        except ConnectorError as exc:
            # 409, not 400: the request is well-formed, the target simply does
            # not support having a scope replaced.
            return _error(exc, status.HTTP_409_CONFLICT)
        return Response(
            ProjectionRunSerializer(projection_run).data,
            status=status.HTTP_202_ACCEPTED,
        )

    @action(detail=True, methods=["get"], url_path="runs")
    def projection_runs(self, request, pk=None):
        queryset = ProjectionRun.objects.filter(projection=self.get_object())[:100]
        return Response(ProjectionRunSerializer(queryset, many=True).data)


class WebhookSubscriptionViewSet(BaseViewSet):
    owner_lookup = "binding__connection"
    queryset = WebhookSubscription.objects.select_related("binding__connection")
    serializer_class = WebhookSubscriptionSerializer
    # POST is allowed only for the `renew` detail route below; `create` is
    # overridden to refuse. Creating a subscription needs a callback origin the
    # host supplies (`request.get_host()` is attacker-influenced), so it stays
    # in the service layer rather than being derivable from a request.
    http_method_names = ("get", "post", "delete", "head", "options")

    def create(self, request, *args, **kwargs):
        return Response(
            {
                "detail": "subscriptions are created by "
                "django_connectors.webhooks.services.create_subscription, which "
                "needs an explicit callback origin."
            },
            status=status.HTTP_405_METHOD_NOT_ALLOWED,
        )

    @action(detail=True, methods=["post"])
    def renew(self, request, pk=None):
        """Renew this subscription with its provider, now.

        The same service call the admin action and the sweep make. Renewal is
        not a schedule nudge: a subscription whose provider-side expiry passes
        stops delivering silently, with no error and no final delivery.
        """
        from django_connectors.webhooks.services import renew_subscription

        subscription = self.get_object()
        try:
            renew_subscription(subscription, actor=request.user)
        except Exception as exc:
            # Broader than the other endpoints here, deliberately: a webhook
            # adapter is host-written and may raise a bare `requests` error, and
            # the failure is already logged and backed off on the row by the
            # time it arrives. A 500 would add a traceback and no information.
            return _error(exc, status.HTTP_409_CONFLICT)
        return Response(WebhookSubscriptionSerializer(subscription).data)


class TargetViewSet(viewsets.ViewSet):
    """Registered target shapes. Host-declared metadata, not tenant data.

    The only unscoped viewset in the API, and safe precisely because it contains
    no customer data — just the schema a mapping UI needs to render.
    """

    def get_permissions(self):
        return [permission() for permission in configured_permission_classes()]

    def list(self, request):
        return Response([target.describe() for target in all_targets().values()])

    def retrieve(self, request, pk=None):
        targets = all_targets()
        if pk not in targets:
            return Response(
                {"detail": f"unknown target {pk!r}"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(targets[pk].describe())


class SourceViewSet(viewsets.ViewSet):
    """Registered source definitions. Also host/library metadata, not tenant data."""

    def get_permissions(self):
        return [permission() for permission in configured_permission_classes()]

    def list(self, request):
        from django_connectors.registry import sources

        return Response(
            [
                {
                    "key": key,
                    "provider": getattr(definition, "provider", ""),
                    "supported_auth_backends": list(
                        getattr(definition, "supported_auth_backends", ())
                    ),
                    "emits_tombstones": getattr(definition, "emits_tombstones", False),
                }
                for key, definition in sources.all().items()
            ]
        )


def _error(exc, status_code=status.HTTP_400_BAD_REQUEST):
    from django_connectors.errors import scrub

    return Response({"detail": scrub(exc)}, status=status_code)


def _assert_every_viewset_is_scoped():
    """Guard against a viewset added later forgetting to scope its queryset."""
    unscoped = []
    for name, obj in list(globals().items()):
        if not isinstance(obj, type) or not issubclass(obj, GenericAPIView):
            continue
        # Only viewsets defined here — imported base classes are not ours.
        if obj.__module__ != __name__:
            continue
        if getattr(obj, "abstract_scope", False):
            continue
        if not issubclass(obj, OwnerScopedQuerysetMixin):
            unscoped.append(name)
    if unscoped:
        raise ImproperlyConfigured(
            f"API viewsets {unscoped} do not mix in OwnerScopedQuerysetMixin, "
            f"so their querysets span every tenant."
        )


_assert_every_viewset_is_scoped()
