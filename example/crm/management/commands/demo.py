"""End-to-end demonstration: source -> landing -> projection -> host records.

Run with ``python manage.py demo``. Idempotent — safe to run repeatedly.
"""

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

from crm.models import CrmEvent, Team
from django_connectors.enums import ConnectionStatus, ProjectionStatus, RunTrigger
from django_connectors.models import Binding, Connection, Projection
from django_connectors.services import projections as projection_services
from django_connectors.services import runs as run_services

# Two runs of a source that reports one new event, then updates it and deletes
# another — enough to show merge and tombstone behaviour.
BATCHES = [
    [
        {
            "id": "evt-1",
            "ts": "2024-05-01T09:00:00Z",
            "kind": "signup",
            "meta": {"plan": "pro"},
        },
        {"id": "evt-2", "ts": "2024-05-01T10:00:00Z", "kind": "login", "meta": {}},
    ],
    [
        {
            "id": "evt-1",
            "ts": "2024-05-01T09:00:00Z",
            "kind": "signup",
            "meta": {"plan": "enterprise"},
        },
        {"id": "evt-2", "_connector_deleted": True},
    ],
]

MAPPING = {
    "external_id": {"source": "id"},
    "occurred_at": {"source": "ts", "cast": "datetime"},
    "type": {"source": "kind"},
    "payload": {
        "object": {
            "detail": {"source": "meta"},
            "source_system": {"constant": "demo"},
        }
    },
}


class Command(BaseCommand):
    help = "Run the full connector -> projection flow against the memory source."

    def handle(self, *args, **options):
        team, _ = Team.objects.get_or_create(name="Acme")

        connection, _ = Connection.objects.get_or_create(
            owner_content_type=ContentType.objects.get_for_model(Team),
            owner_object_id=str(team.pk),
            provider="memory",
            defaults={"status": ConnectionStatus.ACTIVE},
        )

        binding, created = Binding.objects.get_or_create(
            connection=connection,
            source="memory",
            defaults={
                "resources": ["events"],
                "config": {
                    "resources": {"events": {"primary_key": "id", "batches": BATCHES}}
                },
            },
        )
        if created:
            self.stdout.write(f"created binding {binding.landing_key}")

        projection, _ = Projection.objects.get_or_create(
            binding=binding,
            resource="events",
            target="events",
            defaults={
                "name": "Demo events",
                "mapping": MAPPING,
                "status": ProjectionStatus.ACTIVE,
            },
        )

        for index in (1, 2):
            run = run_services.run_binding(binding, trigger=RunTrigger.MANUAL)
            self.stdout.write(f"run {index}: {run.status} loads={run.dlt_load_ids}")
            if run.status != "succeeded":
                self.stderr.write(run.error_message)
                return

        # A successful Run dispatches projections automatically; replay here so
        # the demo shows the full-scope path too.
        projection_run = projection_services.replay_projection(projection)
        self.stdout.write(
            f"projection: {projection_run.status} "
            f"seen={projection_run.records_seen} "
            f"written={projection_run.records_written} "
            f"deleted={projection_run.records_deleted}"
        )

        self.stdout.write("")
        self.stdout.write("host records:")
        for event in CrmEvent.objects.filter(team=team):
            state = "deleted" if event.deleted_at else "live"
            self.stdout.write(
                f"  {event.external_id}  {event.type:8} {state:8} {event.payload}"
            )
