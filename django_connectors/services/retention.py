"""Landing data lifecycle: purge and reset.

There is deliberately **no time-based retention sweeper**. Pruning "old" rows
from a merge resource does not delete history, it deletes *current state*: the
incremental cursor has already advanced past those records, so the source will
never re-emit them and a full replay cannot reconstruct the target. That is why
``LandingRetention`` offers only ``current_state`` and ``permanent``, both
no-ops. :func:`reset_binding_state` is the escape hatch when a cursor needs to
go backwards.

Deletion is two-phase for a reason: deleting a Binding row cascades through the
control plane but leaves its landing rows in the landing database forever, with
``_connector_binding_id`` pointing at a row that no longer exists. That is a
data-retention compliance failure, not untidiness — so a Binding that has ever
landed data cannot be deleted until its landing tables are dropped.
"""

import logging

from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.utils import timezone

from django_connectors.enums import BindingStatus
from django_connectors.exceptions import ConnectorError
from django_connectors.landing import access
from django_connectors.landing.destination import build_pipeline

logger = logging.getLogger(__name__)


class LandingNotPurged(ConnectorError):
    """A Binding with landed data was deleted before its tables were dropped."""


def purge_binding_landing(binding):
    """Drop every landing table for `binding` and forget its dlt state.

    Cheap and complete because each Binding owns its tables: a DROP, rather
    than a tenant-filtered DELETE that would leave other tenants' rows
    interleaved in the same table.
    """
    previous_status = binding.status
    binding.status = BindingStatus.PURGING
    binding.save(update_fields=["status"])

    dropped = []
    try:
        pipeline = build_pipeline(binding)
        dropped = access.drop_landing_tables(binding, pipeline=pipeline)
        # Forget cursors and schema too, or a later Binding reusing the
        # pipeline name would restore state describing tables that are gone.
        try:
            pipeline.drop()
        except Exception as exc:
            logger.warning(
                "dropped landing tables for %s but could not drop pipeline state: %s",
                binding.id,
                type(exc).__name__,
            )
    except Exception:
        binding.status = previous_status
        binding.save(update_fields=["status"])
        raise

    binding.landing_purged_at = timezone.now()
    binding.landing_schema = {}
    binding.landing_schema_version_hash = ""
    binding.landing_schema_at = None
    binding.status = BindingStatus.DISABLED
    binding.enabled = False
    binding.save(
        update_fields=[
            "landing_purged_at",
            "landing_schema",
            "landing_schema_version_hash",
            "landing_schema_at",
            "status",
            "enabled",
        ]
    )
    return {"dropped_tables": dropped}


def reset_binding_state(binding, *, backfill=True):
    """Forget the incremental cursor so the next Run re-extracts from scratch.

    The escape hatch from a poisoned cursor — one that advanced past records
    that never landed, which no amount of retrying can fix because the source
    will not re-emit them.

    With `backfill` the landed rows are kept and re-merged over; without it the
    landing tables are dropped first, which is the only way to lose rows that
    the source will never send again.
    """
    if not backfill:
        purge_binding_landing(binding)
        binding.status = BindingStatus.PENDING
        binding.enabled = True
        binding.landing_purged_at = None
        binding.save(update_fields=["status", "enabled", "landing_purged_at"])
        return {"dropped": True, "cursor_reset": True}

    pipeline = build_pipeline(binding)
    try:
        pipeline.drop()
    except Exception as exc:
        raise ConnectorError(
            f"could not reset dlt state for binding {binding.id}: {exc}"
        ) from exc

    binding.status = BindingStatus.PENDING
    binding.save(update_fields=["status"])
    return {"dropped": False, "cursor_reset": True}


def delete_binding(binding, *, purge=True):
    """Delete a Binding, refusing to strand its landing data."""
    if purge and _has_landed(binding):
        purge_binding_landing(binding)
    binding.refresh_from_db()
    return binding.delete()


def _has_landed(binding):
    return binding.last_success_at is not None and binding.landing_purged_at is None


@receiver(pre_delete, sender="django_connectors.Binding")
def protect_unpurged_binding(sender, instance, **kwargs):
    """Refuse to delete a Binding whose landing tables still exist.

    A guard rather than an automatic purge: dropping customer data as a side
    effect of a cascade — or of an admin clicking delete — should not be
    something this library does silently.
    """
    if _has_landed(instance):
        raise LandingNotPurged(
            f"binding {instance.id} has landed data that has not been purged. "
            f"Call django_connectors.services.retention.purge_binding_landing() "
            f"first, or delete_binding(binding, purge=True). Deleting it now "
            f"would strand its rows in the landing database with no row to "
            f"attribute them to."
        )
