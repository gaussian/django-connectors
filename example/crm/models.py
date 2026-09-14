"""The host application's own models.

django-connectors never sees these. It knows only that a target called
``events`` accepts records with certain fields — not that ``events`` means
``CrmEvent``, not that there is a Django model at all.
"""

from django.db import models


class Team(models.Model):
    """Owns Connections. Any model can: Connection.owner is a GenericForeignKey."""

    name = models.CharField(max_length=100, unique=True)

    class Meta:
        verbose_name = "team"
        verbose_name_plural = "teams"

    def __str__(self):
        return self.name


class CrmEvent(models.Model):
    """A canonical event, however it arrived.

    Records from a REST API, a warehouse query and a spreadsheet all land here
    through different Projections. Nothing about this model is known to the
    connector library.
    """

    team = models.ForeignKey(Team, related_name="events", on_delete=models.CASCADE)
    external_id = models.CharField(max_length=255)
    occurred_at = models.DateTimeField()
    type = models.CharField(max_length=100)
    payload = models.JSONField(default=dict, blank=True)
    # The writer marks records deleted rather than removing them. What "delete"
    # means is entirely the host's decision — the library only reports that the
    # source no longer has the record.
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "CRM event"
        verbose_name_plural = "CRM events"
        ordering = ("-occurred_at",)
        constraints = (
            # Identity is (team, external_id), matching the target's
            # identity_scope="owner": two teams may legitimately have the same
            # remote id, and they must not collide.
            models.UniqueConstraint(
                fields=("team", "external_id"), name="crm_event_identity"
            ),
        )

    def __str__(self):
        return f"{self.type} {self.external_id}"
