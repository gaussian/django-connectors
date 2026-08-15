"""Binding — which source data from a Connection to keep synchronized."""

import uuid

from django.db import IntegrityError, models, transaction
from django.utils.translation import gettext_lazy as _

from django_connectors.enums import BindingStatus, LandingRetention
from django_connectors.models.connection import Connection

LANDING_KEY_ATTEMPTS = 5


def generate_landing_key():
    """A short, unique, dlt-safe token identifying this Binding's landing table.

    Must start with a letter: dlt's naming convention prefixes a leading digit
    with an underscore, which would make the generated table name differ from
    the one we computed.
    """
    return f"b{uuid.uuid4().hex[:10]}"


class Binding(models.Model):
    """One configured, independently scheduled slice of a Connection.

    A Binding owns its own dlt pipeline, its own landing tables and its own
    incremental cursor state, which is why so much identity hangs off it.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    connection = models.ForeignKey(
        Connection,
        related_name="bindings",
        on_delete=models.CASCADE,
    )

    source = models.CharField(max_length=150, help_text=_("Registered source key."))
    # dlt resource names to select from the source. Empty means "all".
    resources = models.JSONField(default=list, blank=True)
    config = models.JSONField(default=dict, blank=True)

    # Short unique token that makes every landing table name distinct. Each
    # Binding gets its own physical table: sharing one table between Bindings
    # was measured losing 20-27% of loads with errors=0 and no exception, because
    # dlt keys its staging table on (dataset, table_name) alone and each load
    # issues an unconditional auto-committed DELETE against it. Per-Binding
    # tables also bound merge cost and make purging a DROP TABLE.
    landing_key = models.CharField(max_length=12, unique=True, editable=False)

    enabled = models.BooleanField(default=True)
    poll_interval = models.DurationField(null=True, blank=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    # Floor between runs, so a webhook burst cannot drive continuous syncing.
    min_run_interval = models.DurationField(null=True, blank=True)
    # A lease older than this is considered abandoned and is reaped.
    run_timeout = models.DurationField(null=True, blank=True)

    webhook_enabled = models.BooleanField(default=False)
    # Set when a webhook arrives while a Run is already in flight; the runner
    # schedules exactly one follow-up rather than queueing one per delivery.
    dirty = models.BooleanField(default=False)

    landing_retention = models.CharField(
        max_length=32,
        choices=LandingRetention,
        default=LandingRetention.CURRENT_STATE,
    )

    status = models.CharField(
        max_length=32,
        choices=BindingStatus,
        default=BindingStatus.PENDING,
    )

    # Snapshot of the dlt schema after the last successful Run. Projection
    # validation and the mapping UI read this instead of re-running extraction.
    landing_schema = models.JSONField(default=dict, blank=True)
    landing_schema_version_hash = models.CharField(max_length=100, blank=True)
    landing_schema_at = models.DateTimeField(null=True, blank=True)
    # Set once landing tables have been dropped. A Binding that has ever loaded
    # may not be deleted while this is null, or its rows are stranded in the
    # landing database forever with no row to attribute them to.
    landing_purged_at = models.DateTimeField(null=True, blank=True)

    last_success_at = models.DateTimeField(null=True, blank=True)
    last_failure_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("binding")
        verbose_name_plural = _("bindings")
        ordering = ("-created_at",)
        # Triggering ingestion is a privileged act distinct from editing the
        # row: it spends provider quota and can be aimed at internal hosts.
        # Django resolves `has_run_binding_permission` by name and will
        # silently offer an admin action whose permission does not exist.
        permissions = (("run_binding", _("Can trigger a binding run")),)
        indexes = (
            models.Index(fields=("connection", "enabled"), name="dc_bind_conn_idx"),
            models.Index(fields=("enabled", "next_run_at"), name="dc_bind_due_idx"),
            models.Index(fields=("status",), name="dc_bind_status_idx"),
        )

    def __str__(self):
        return f"{self.source} ({self.landing_key or 'unsaved'})"

    def save(self, *args, **kwargs):
        if self.landing_key:
            return super().save(*args, **kwargs)

        # Uniqueness is enforced by the index, not hoped for from a truncated
        # UUID: a collision would put two tenants in one landing table.
        for attempt in range(LANDING_KEY_ATTEMPTS):
            self.landing_key = generate_landing_key()
            try:
                with transaction.atomic():
                    return super().save(*args, **kwargs)
            except IntegrityError:
                if attempt == LANDING_KEY_ATTEMPTS - 1:
                    raise
                self.landing_key = ""
        return None

    @property
    def pipeline_name(self):
        """Deterministic, stable, and unique per Binding.

        A Binding is an independently configured source with its own cursor
        state, so it gets its own pipeline. Two processes must never open the
        same pipeline concurrently — dlt has no cross-process lock and was
        measured stealing load packages between them — which is what BindingLock
        prevents.
        """
        return f"django_connectors_{self.id.hex}"

    @property
    def schema_name(self):
        """Per-Binding dlt schema name.

        A cold restore resolves a schema *by name* from `_dlt_version` and takes
        the newest row, so sharing a schema name across Bindings hands one
        Binding another's columns.
        """
        return f"dc_{self.landing_key}"

    @property
    def is_runnable(self):
        return self.enabled and self.status in (
            BindingStatus.PENDING,
            BindingStatus.ACTIVE,
            BindingStatus.NEEDS_REVIEW,
        )


class BindingLock(models.Model):
    """A lease guaranteeing one in-flight Run per Binding.

    A portable lease rather than ``SELECT GET_LOCK()``: that is MySQL-only, so
    it could not be exercised by the default sqlite test tier, and a lock that
    only runs in one CI job is a lock nobody tests. Expiry makes it
    self-healing when a worker dies without releasing.
    """

    binding = models.OneToOneField(
        Binding,
        related_name="lock",
        on_delete=models.CASCADE,
        primary_key=True,
    )
    # Held by the acquirer; releasing requires presenting it, so a process that
    # lost its lease to expiry cannot release someone else's.
    token = models.UUIDField(default=uuid.uuid4, editable=False)
    acquired_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    class Meta:
        verbose_name = _("binding lock")
        verbose_name_plural = _("binding locks")
        indexes = (models.Index(fields=("expires_at",), name="dc_lock_expires_idx"),)

    def __str__(self):
        return f"lock on {self.binding_id} until {self.expires_at:%Y-%m-%d %H:%M:%S}"
