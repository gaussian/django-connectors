"""Projection — a customer's mapping from one landed resource to one target."""

import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _

from django_connectors._uuid import uuid7
from django_connectors.enums import (
    ProjectionRunMode,
    ProjectionRunStatus,
    ProjectionStatus,
)
from django_connectors.models.binding import Binding
from django_connectors.models.run import Run


class Projection(models.Model):
    """Map one landed dlt resource into one host-registered target.

    Deliberately narrow: one resource in, one target out. No joins, no
    aggregation, no cross-Binding queries. Source data that needs shaping across
    tables should be shaped *before* landing — in the customer's own SQL view or
    in the dlt source — or this stops being a connector framework and becomes a
    query engine.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    binding = models.ForeignKey(
        Binding, related_name="projections", on_delete=models.CASCADE
    )

    # dlt resource name within the Binding, and the host-registered target key.
    resource = models.CharField(max_length=150)
    target = models.CharField(max_length=150)

    name = models.CharField(max_length=255)

    # Validated JSON DSL; compiled to a Python AST and evaluated in-process.
    # No SQL is ever constructed from it.
    mapping = models.JSONField(default=dict)
    filters = models.JSONField(default=list, blank=True)

    enabled = models.BooleanField(default=True)
    # Bumped on every mapping/filter change. A ProjectionRun records the version
    # it ran, and one queued against a superseded version is refused rather than
    # executed — otherwise a stale run reverts rows a newer replay corrected.
    version = models.PositiveIntegerField(default=1)

    status = models.CharField(
        max_length=32, choices=ProjectionStatus, default=ProjectionStatus.DRAFT
    )

    last_success_at = models.DateTimeField(null=True, blank=True)
    last_failure_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("projection")
        verbose_name_plural = _("projections")
        ordering = ("-created_at",)
        # Preview reads customer data and returns it to the caller, and
        # replay rewrites host records — both are privileged beyond "change".
        permissions = (
            ("preview_projection", _("Can preview a projection")),
            ("replay_projection", _("Can replay a projection")),
        )
        indexes = (
            models.Index(
                fields=("binding", "resource", "enabled"), name="dc_proj_binding_idx"
            ),
            models.Index(fields=("target",), name="dc_proj_target_idx"),
            models.Index(fields=("status",), name="dc_proj_status_idx"),
        )

    def __str__(self):
        return f"{self.name or self.resource} → {self.target}"

    @property
    def is_runnable(self):
        return self.enabled and self.status == ProjectionStatus.ACTIVE


class ProjectionRun(models.Model):
    """What happened during one execution of a Projection."""

    id = models.UUIDField(primary_key=True, default=uuid7, editable=False)
    projection = models.ForeignKey(
        Projection, related_name="runs", on_delete=models.CASCADE
    )
    # SET_NULL so pruning Run history never destroys projection history.
    source_run = models.ForeignKey(
        Run,
        related_name="projection_runs",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )

    mode = models.CharField(max_length=32, choices=ProjectionRunMode)
    projection_version = models.PositiveIntegerField()
    status = models.CharField(
        max_length=32,
        choices=ProjectionRunStatus,
        default=ProjectionRunStatus.QUEUED,
    )

    # dlt load ids covered by this run. The sweeper computes outstanding work as
    # (load ids of the Binding's succeeded Runs) minus (load ids of this
    # Projection's succeeded ProjectionRuns), which self-heals both a dropped
    # dispatch and a failed-and-never-retried run with one mechanism.
    load_ids = models.JSONField(default=list, blank=True)

    records_seen = models.PositiveBigIntegerField(default=0)
    records_written = models.PositiveBigIntegerField(default=0)
    records_deleted = models.PositiveBigIntegerField(default=0)

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    error_type = models.CharField(max_length=255, blank=True)
    error_message = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _("projection run")
        verbose_name_plural = _("projection runs")
        ordering = ("-created_at",)
        indexes = (
            models.Index(
                fields=("projection", "-created_at"), name="dc_projrun_proj_idx"
            ),
            models.Index(fields=("status", "created_at"), name="dc_projrun_status_idx"),
        )

    def __str__(self):
        return f"{self.mode} projection run {self.id} ({self.status})"
