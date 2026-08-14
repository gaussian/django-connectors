"""Run — one ingestion attempt for one Binding."""

from django.db import models
from django.utils.translation import gettext_lazy as _

from django_connectors._uuid import uuid7
from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.models.binding import Binding


class Run(models.Model):
    """What happened during one synchronization attempt.

    A succeeded Run means the source was reconciled into the landing tables. It
    does **not** mean any Projection succeeded — that is what ProjectionRun is
    for, and keeping them apart is what lets a failed target write be retried
    without contacting the provider again.
    """

    # UUIDv7: Runs are append-heavy and almost always read newest-first, and a
    # v4 key scatters InnoDB inserts across the index with no natural ordering.
    id = models.UUIDField(primary_key=True, default=uuid7, editable=False)
    binding = models.ForeignKey(Binding, related_name="runs", on_delete=models.CASCADE)

    trigger = models.CharField(max_length=32, choices=RunTrigger)
    status = models.CharField(
        max_length=32, choices=RunStatus, default=RunStatus.QUEUED
    )

    # Queue coalescing: set to the binding id while queued and cleared on the
    # transition to running, so at most one queued Run exists per Binding. A
    # nullable unique column rather than UniqueConstraint(condition=...),
    # because Django's MySQL backend reports supports_partial_indexes = False —
    # a partial constraint emits models.W036 into every host's `manage.py
    # check` and creates no DDL at all.
    dedupe_key = models.UUIDField(null=True, blank=True, unique=True, editable=False)

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    dlt_pipeline_name = models.CharField(max_length=255, blank=True)
    dlt_dataset_name = models.CharField(max_length=255, blank=True)
    # Load ids this Run actually landed, taken from LoadInfo.loads_ids. This is
    # the incremental projection window — NOT the run id, which is stamped at
    # extract time and is wrong whenever dlt loads a previously pending package.
    dlt_load_ids = models.JSONField(default=list, blank=True)

    # Allow-listed, JSON-serializable subset of dlt's trace. dlt's own
    # asdict() is not stdlib-JSON serializable (pendulum datetimes) and embeds
    # absolute worker filesystem paths in every job entry.
    metrics = models.JSONField(default=dict, blank=True)

    error_type = models.CharField(max_length=255, blank=True)
    # Always written through errors.scrub(); this field is rendered in the admin
    # and returned by the API.
    error_message = models.TextField(blank=True)
    # Why a Run was skipped, or any other non-error note.
    note = models.CharField(max_length=255, blank=True)

    task_reference = models.CharField(max_length=500, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _("run")
        verbose_name_plural = _("runs")
        ordering = ("-created_at",)
        indexes = (
            models.Index(fields=("binding", "-created_at"), name="dc_run_binding_idx"),
            models.Index(fields=("status", "created_at"), name="dc_run_status_idx"),
        )

    def __str__(self):
        return f"{self.trigger} run {self.id} ({self.status})"

    @property
    def is_finished(self):
        return self.status in (
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
            RunStatus.SKIPPED,
        )
