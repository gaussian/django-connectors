"""Where the host meets the library.

This module is imported automatically: django-connectors calls
``autodiscover_modules("connectors")`` from its own ``AppConfig.ready()``, the
same contract ``django.contrib.admin`` uses for ``admin.py``. So every installed
app's ``connectors.py`` runs before anything reads the target registry — which
is why registration cannot depend on ``INSTALLED_APPS`` ordering.

The whole integration is two things: a declared shape, and a function that
persists it.
"""

from django.utils import timezone

from django_connectors import (
    DateTimeField,
    JSONField,
    StringField,
    TargetDefinition,
    register_target,
)


def event_writer(records, context):
    """Persist a batch of projected records. Returns how many were applied.

    The contract the runner guarantees, and what it means here:

    * Batches arrive in a deterministic order, and an identity appears at most
      once per batch, with a delete beating an upsert. So no in-batch conflict
      resolution is needed.
    * A raised exception means the batch was not applied and the whole
      ProjectionRun is retried from the start — there is no mid-run checkpoint.
      **So this function must be idempotent per identity**, which is why it
      uses update_or_create rather than create.
    * ``context.owner_object_id`` scopes the batch to a tenant. Ignoring it
      would discard the multi-tenant guarantee the landing layer maintains, at
      the very last step.
    """
    from crm.models import CrmEvent, Team

    team = Team.objects.get(pk=context.owner_object_id)
    applied = 0

    for record in records:
        if record.operation == "delete":
            # What "deleted" means is the host's decision. Here it is a soft
            # delete; archiving or a hard delete would be equally valid.
            applied += CrmEvent.objects.filter(
                team=team, external_id=record.identity["external_id"]
            ).update(deleted_at=timezone.now())
            continue

        CrmEvent.objects.update_or_create(
            team=team,
            external_id=record.identity["external_id"],
            defaults={
                "occurred_at": record.values["occurred_at"],
                "type": record.values["type"],
                "payload": record.values.get("payload") or {},
                "deleted_at": None,
            },
        )
        applied += 1

    return applied


register_target(
    TargetDefinition(
        key="events",
        label="Events",
        description="Canonical events, from any source.",
        fields={
            "external_id": StringField(required=True, max_length=255),
            "occurred_at": DateTimeField(required=True),
            "type": StringField(required=True, max_length=100),
            "payload": JSONField(),
        },
        identity_fields=("external_id",),
        # Required, with no default: it decides whether two owners may share an
        # identity value, and guessing wrong is a cross-tenant collision.
        # "owner" matches the (team, external_id) constraint on CrmEvent.
        identity_scope="owner",
        # The writer can replace an owner's records wholesale, so a full replay
        # is safe. Targets that cannot must leave this False — the library
        # cannot tell what a previous mapping wrote.
        supports_scope_replace=True,
        writer=event_writer,
    )
)
