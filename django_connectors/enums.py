"""Choice sets shared by models, services and the API.

Kept out of the model modules so that services, serializers and host code can
import a status value without pulling in the app registry.

These strings are persisted, so the *values* are part of the stored data
contract: labels can change freely, values cannot.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _


class ConnectionStatus(models.TextChoices):
    PENDING = "pending", _("Pending setup")
    ACTIVE = "active", _("Active")
    # Set when a backend raises CredentialsRevoked during a Run. Dependent
    # Bindings are blocked rather than retried: retrying a revoked credential
    # burns provider quota and invites rate limiting.
    REVOKED = "revoked", _("Revoked")
    ERROR = "error", _("Error")


class BindingStatus(models.TextChoices):
    PENDING = "pending", _("Pending first run")
    ACTIVE = "active", _("Active")
    # The Binding cannot run correctly but the configuration is not invalid —
    # e.g. a merge resource declared no primary key, or index provisioning
    # failed. Requires a human decision, not a retry.
    NEEDS_REVIEW = "needs_review", _("Needs review")
    # Cannot run at all: revoked credentials, or a pipeline wedged by a load
    # package that can never succeed.
    BLOCKED = "blocked", _("Blocked")
    DISABLED = "disabled", _("Disabled")
    # Landing data is being dropped; the row may not be deleted until it is.
    PURGING = "purging", _("Purging")


class LandingRetention(models.TextChoices):
    """Deliberately only two values.

    Time-based pruning (the original plan's `7_days`/`30_days`) is unrecoverable
    on a merge resource: the incremental cursor has already advanced past the
    pruned rows, so they are never re-emitted and a full replay cannot
    reconstruct the target. Both values below are no-ops under merge
    disposition; `reset_binding_state()` is the escape hatch.
    """

    CURRENT_STATE = "current_state", _("Current state only")
    PERMANENT = "permanent", _("Permanent")


class RunTrigger(models.TextChoices):
    INITIAL = "initial", _("Initial")
    SCHEDULED = "scheduled", _("Scheduled")
    WEBHOOK = "webhook", _("Webhook")
    MANUAL = "manual", _("Manual")
    BACKFILL = "backfill", _("Backfill")
    # Drains a load package left pending by an earlier interrupted Run. dlt
    # loads a pending package *instead of* extracting new data, so this must be
    # its own Run rather than being silently attributed to the next one.
    RECOVERY = "recovery", _("Recovery")


class RunStatus(models.TextChoices):
    QUEUED = "queued", _("Queued")
    RUNNING = "running", _("Running")
    SUCCEEDED = "succeeded", _("Succeeded")
    FAILED = "failed", _("Failed")
    # Another worker held the Binding's lease. Recorded rather than dropped, so
    # that "nothing happened" is distinguishable from "nothing was attempted".
    SKIPPED = "skipped", _("Skipped")


class ProjectionStatus(models.TextChoices):
    DRAFT = "draft", _("Draft")
    ACTIVE = "active", _("Active")
    # A mapped column gained a dlt variant sibling (`col__v_text`), so the
    # mapping still resolves but may now silently read NULLs.
    NEEDS_REVIEW = "needs_review", _("Needs review")
    # A mapped column vanished; the mapping cannot execute.
    INVALID = "invalid", _("Invalid")


class ProjectionRunMode(models.TextChoices):
    INCREMENTAL = "incremental", _("Incremental")
    FULL = "full", _("Full replay")
    PREVIEW = "preview", _("Preview")


class ProjectionRunStatus(models.TextChoices):
    QUEUED = "queued", _("Queued")
    RUNNING = "running", _("Running")
    SUCCEEDED = "succeeded", _("Succeeded")
    FAILED = "failed", _("Failed")
    # Queued against a mapping version that has since been superseded. Running
    # it would revert rows a newer replay already corrected.
    SUPERSEDED = "superseded", _("Superseded")


class WebhookStatus(models.TextChoices):
    PENDING = "pending", _("Pending")
    ACTIVE = "active", _("Active")
    EXPIRED = "expired", _("Expired")
    FAILED = "failed", _("Failed")
    DELETED = "deleted", _("Deleted")


class SecretEncryption(models.TextChoices):
    """How a stored credential is protected at rest.

    `none` is django-allauth's trust model — SocialToken.token is a plaintext
    column — and is a legitimate choice when the database is the trust boundary.
    It must be selected explicitly so that nobody stores plaintext by accident.
    """

    NONE = "none", _("Plaintext")
    FERNET = "fernet", _("Fernet")


class IdentityScope(models.TextChoices):
    """Whether a target's identity values are unique per owner or globally."""

    OWNER = "owner", _("Unique per owner")
    GLOBAL = "global", _("Globally unique")
