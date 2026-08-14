"""End-to-end demonstration: source -> landing -> projection -> host records.

Run with ``python manage.py demo``. Idempotent — safe to run repeatedly.

It is also the only end-to-end gate in CI, which is why every step is asserted
rather than merely printed. ``run_binding`` records a failure as a Run *status*
instead of raising — that is the library's contract, so a caller can decide
what a failed Run means — and a demo that only prints the status exits 0 on
essentially every ingestion regression there is.
"""

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand, CommandError

from crm.models import CrmEvent, Team
from django_connectors.enums import (
    ConnectionStatus,
    ProjectionRunStatus,
    ProjectionStatus,
    RunStatus,
    RunTrigger,
)
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
            if run.status != RunStatus.SUCCEEDED:
                raise CommandError(f"run {index} {run.status}: {run.error_message}")

        # A successful Run dispatches projections automatically; replay here so
        # the demo shows the full-scope path too.
        projection_run = projection_services.replay_projection(projection)
        self.stdout.write(
            f"projection: {projection_run.status} "
            f"seen={projection_run.records_seen} "
            f"written={projection_run.records_written} "
            f"deleted={projection_run.records_deleted}"
        )
        if projection_run.status != ProjectionRunStatus.SUCCEEDED:
            raise CommandError(
                f"projection {projection_run.status}: {projection_run.error_message}"
            )

        self.stdout.write("")
        self.stdout.write("host records:")
        events = {}
        for event in CrmEvent.objects.filter(team=team):
            state = "deleted" if event.deleted_at else "live"
            events[event.external_id] = event
            self.stdout.write(
                f"  {event.external_id}  {event.type:8} {state:8} {event.payload}"
            )

        self._check_host_records(events)
        self.stdout.write("")
        self.stdout.write("demo ok")

    def _check_host_records(self, events):
        """Refuse to exit 0 unless the host models say what README.md says.

        A green Run and a green ProjectionRun still leave two silent outcomes:
        a load that lands zero rows, and a projection that writes nothing. Both
        print a plausible summary and neither reaches the host's tables, so the
        rows themselves are what this checks.
        """
        problems = []
        if set(events) != {"evt-1", "evt-2"}:
            problems.append(f"expected evt-1 and evt-2, got {sorted(events)}")
        else:
            live, deleted = events["evt-1"], events["evt-2"]
            if live.deleted_at is not None:
                problems.append("evt-1 is soft-deleted; only evt-2 should be")
            if deleted.deleted_at is None:
                problems.append(
                    "evt-2 was reported gone by the source but is not soft-deleted"
                )
            if live.type != "signup":
                problems.append(f"evt-1.type is {live.type!r}, expected 'signup'")
            # Run 2 updates evt-1 in place. Still 'pro' means the merge landed a
            # second copy or the projection never re-read it.
            expected = {"detail": {"plan": "enterprise"}, "source_system": "demo"}
            if live.payload != expected:
                problems.append(
                    f"evt-1.payload is {live.payload!r}, expected {expected!r}"
                )
        if problems:
            raise CommandError("; ".join(problems))
