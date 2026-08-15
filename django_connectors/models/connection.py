"""Connection — how an owner can reach an external system."""

import re
import uuid

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _

from django_connectors.enums import ConnectionStatus, SecretEncryption

# Keys that must never appear in `auth_metadata`. That field is rendered in the
# admin and returned by the API, so a credential stored there is a credential
# published. The SecretStore exists for exactly this.
SECRET_KEY_PATTERN = re.compile(
    r"(?i)(token|secret|password|passwd|pwd|key|credential|assertion|private|cert)"
)


class Connection(models.Model):
    """This owner has access to this external account, system or tenant.

    The owner is a GenericForeignKey rather than a configurable FK on purpose.
    A plain ``FK(settings.SOMETHING)`` at a non-swappable target hardcodes the
    resolved model into the shipped migration, so a host that points the setting
    elsewhere gets a phantom migration generated *inside site-packages*, which
    they cannot fix. The GFK costs an index and buys hosts the freedom to own
    Connections with a Team, a User, an Organisation, or anything else.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # PROTECT, not CASCADE: with CASCADE a routine `remove_stale_contenttypes`
    # run would silently delete Connections, and Bindings, Runs and Projections
    # behind them. related_name="+" avoids fields.E304 in any host that also has
    # a `Connection` model with an unnamed ContentType FK.
    owner_content_type = models.ForeignKey(
        ContentType,
        on_delete=models.PROTECT,
        related_name="+",
        verbose_name=_("owner type"),
    )
    owner_object_id = models.CharField(max_length=255, verbose_name=_("owner id"))
    owner = GenericForeignKey("owner_content_type", "owner_object_id")

    # Who authorised this connection, when that differs from who owns it — a
    # delegated OAuth backend is User-scoped while the owner is usually a Team.
    # Without this the two are joined only by an untyped reference string.
    authorized_by_content_type = models.ForeignKey(
        ContentType,
        on_delete=models.PROTECT,
        related_name="+",
        null=True,
        blank=True,
        verbose_name=_("authorised by type"),
    )
    authorized_by_object_id = models.CharField(max_length=255, blank=True)
    authorized_by = GenericForeignKey(
        "authorized_by_content_type", "authorized_by_object_id"
    )

    provider = models.CharField(max_length=100)
    auth_backend = models.CharField(max_length=100)

    # An opaque handle resolved by the configured SecretStore — never the
    # credential itself.
    auth_reference = models.CharField(max_length=500, blank=True)
    # Non-secret facts about the authorisation: granted scopes, account email,
    # consent timestamps. Validated against SECRET_KEY_PATTERN on clean().
    auth_metadata = models.JSONField(default=dict, blank=True)

    external_account_id = models.CharField(max_length=255, blank=True)
    external_tenant_id = models.CharField(max_length=255, blank=True)

    status = models.CharField(
        max_length=32,
        choices=ConnectionStatus,
        default=ConnectionStatus.PENDING,
    )
    # Single-use nonce correlating an out-of-band auth handshake back to this
    # row, so a callback cannot be pointed at someone else's Connection.
    setup_token = models.UUIDField(null=True, blank=True, editable=False)

    metadata = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("connection")
        verbose_name_plural = _("connections")
        ordering = ("-created_at",)
        # Testing a connection makes an outbound call using stored
        # credentials, which "change" permission should not imply.
        permissions = (("test_connection", _("Can test a connection")),)
        indexes = (
            models.Index(
                fields=("owner_content_type", "owner_object_id"),
                name="dc_conn_owner_idx",
            ),
            models.Index(fields=("provider", "status"), name="dc_conn_provider_idx"),
            models.Index(fields=("setup_token",), name="dc_conn_setup_token_idx"),
        )

    def __str__(self):
        label = self.external_account_id or self.external_tenant_id or str(self.id)
        return f"{self.provider}: {label}"

    def clean(self):
        super().clean()
        offending = sorted(
            key for key in (self.auth_metadata or {}) if SECRET_KEY_PATTERN.search(key)
        )
        if offending:
            raise ValidationError(
                {
                    "auth_metadata": _(
                        "auth_metadata must not hold credentials (offending keys: "
                        "%(keys)s). It is rendered in the admin and returned by the "
                        "API. Store credentials through the configured SecretStore "
                        "and keep only the handle in auth_reference."
                    )
                    % {"keys": ", ".join(offending)}
                }
            )

    @property
    def is_usable(self):
        """Whether a Run may use this Connection at all."""
        return self.status == ConnectionStatus.ACTIVE


class ConnectionSecret(models.Model):
    """A credential belonging to a Connection.

    A dedicated model rather than a JSONField, mirroring how django-allauth
    keeps tokens on ``SocialToken`` — it can be excluded from the admin, kept
    out of every serializer, and permissioned separately, none of which is
    possible for a key inside a general-purpose blob.

    The model ships in core because models cannot live behind an optional
    extra; only the Fernet backend that writes to it is gated by ``[secrets]``.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    connection = models.ForeignKey(
        Connection,
        related_name="secrets",
        on_delete=models.CASCADE,
    )
    key = models.CharField(max_length=100)
    # Ciphertext for `fernet`; the raw value for `none`. Never rendered.
    value = models.TextField()
    encryption = models.CharField(max_length=16, choices=SecretEncryption)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = _("connection secret")
        verbose_name_plural = _("connection secrets")
        constraints = (
            models.UniqueConstraint(
                fields=("connection", "key"), name="dc_secret_unique_key"
            ),
        )

    def __str__(self):
        # Never interpolate `value`, not even truncated.
        return f"{self.key} ({self.encryption})"
