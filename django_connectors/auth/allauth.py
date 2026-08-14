"""Credentials owned by django-allauth.

Most Django applications that talk to Google, Microsoft or Slack already ran
the OAuth dance through allauth to log the user in, and the resulting
``SocialToken`` is right there. Re-implementing the handshake would mean a
second consent screen, a second client registration, and two rows that drift
apart the moment the user re-authorises.

So this backend owns nothing. It reads. allauth remains the system of record
for the token, its refresh, and its deletion — which makes the *absence* of a
``SocialToken`` this backend's most important signal: allauth deletes the row
when the user disconnects the account, so "no row" means revoked, and the
runner must stop the Bindings rather than retry them.

Nothing here is refreshed. allauth refreshes tokens inside its own
provider-configured flow, and a refresh from here would need the client secret,
network access, and a provider it does not know about — while racing allauth's
own refresh for the same row. An expired token is reported as expired.
"""

from typing import ClassVar

from django.core.exceptions import ImproperlyConfigured
from django.utils import timezone

from django_connectors.auth.base import AuthBackend, Credentials
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
)


class AllauthBackend(AuthBackend):
    """Resolve a live ``SocialToken`` for a Connection's ``authorized_by`` user.

    The Connection's ``provider`` must equal the allauth provider id
    (``"google"``, ``"microsoft"``, or a subprovider id for OpenID Connect and
    SAML), and ``external_account_id``, when set, must be the
    ``SocialAccount.uid``. Matching on the uid is what keeps a user who
    connected two Google accounts from silently getting the other one's token.
    """

    key = "allauth"
    required_extras: ClassVar[dict[str, str]] = {"allauth": "allauth"}

    def begin_setup(self, connection, request=None):
        """No handshake here: allauth's own login flow is the handshake.

        Returns ``None`` rather than an authorization URL, because producing one
        would mean a second OAuth client and a second consent screen for an
        account the user has already connected.
        """
        return None

    def get_credentials(self, connection):
        token_model = self._token_model()
        user = self._user(connection)
        provider = self.provider_for(connection)

        filters = {"account__user": user, "account__provider": provider}
        if connection.external_account_id:
            filters["account__uid"] = connection.external_account_id

        # Newest row wins. SocialToken is unique per (app, account), so more
        # than one row means the same account is connected through more than
        # one SocialApp — in which case the most recently written token is the
        # one the user's last authorisation produced.
        token = (
            token_model.objects.filter(**filters)
            .select_related("account")
            .order_by("-id")
            .first()
        )
        if token is None or not token.token:
            raise CredentialsRevoked(
                f"no allauth SocialToken for connection {connection.id} "
                f"(provider={provider!r}). The user disconnected the account, "
                f"an administrator removed the token, or it was never granted."
            )

        expires_at = token.expires_at
        if expires_at is not None and expires_at <= timezone.now():
            raise CredentialsExpired(
                f"the allauth SocialToken for connection {connection.id} "
                f"(provider={provider!r}) expired at {expires_at.isoformat()}. "
                f"allauth owns the refresh; this backend never performs one."
            )

        return Credentials(
            provider=provider,
            access_token=token.token,
            refresh_token=token.token_secret or None,
            expires_at=expires_at,
            account_uid=token.account.uid,
        )

    def revoke(self, connection):
        """Mark the Connection revoked; leave allauth's rows alone.

        The ``SocialToken`` is very likely the user's *login* credential for
        this site. Deleting it because one Binding lost access would log them
        out of the application, which is not a consequence anything in this
        package is entitled to cause.
        """
        return super().revoke(connection)

    def test(self, connection):
        credentials = self.get_credentials(connection)
        expires_at = credentials["expires_at"]
        return (
            f"token found (expires {expires_at.isoformat() if expires_at else 'never'})"
        )

    def health(self, connection):
        """Non-secret facts only: never the token, never a raw provider message."""
        try:
            credentials = self.get_credentials(connection)
        except AuthError as exc:
            return {"usable": False, "reason": type(exc).__name__}
        return {
            "usable": True,
            "expires_at": credentials["expires_at"],
            "refreshable": credentials["refresh_token"] is not None,
        }

    def provider_for(self, connection):
        """The allauth provider id to match on. Override for odd mappings."""
        if not connection.provider:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no provider; there is nothing to match a "
                f"SocialAccount against."
            )
        return connection.provider

    def _user(self, connection):
        """The user whose grant this is — ``authorized_by``, not ``owner``.

        These differ in the common case: a Connection is owned by a Team while
        the OAuth grant belongs to whichever member authorised it. Reading the
        owner instead would find no token at all, or worse, someone else's.
        """
        if not connection.authorized_by_content_type_id:
            raise ConfigurationError(
                f"connection {connection.id} uses the {self.key!r} auth backend "
                f"but has no authorized_by. An allauth token belongs to the user "
                f"who granted it, which is not necessarily the Connection's owner."
            )

        user = connection.authorized_by
        if user is None:
            # The content type is set but the row is gone: the account was
            # deleted, and allauth cascaded its SocialTokens with it.
            raise CredentialsRevoked(
                f"the user who authorised connection {connection.id} no longer "
                f"exists, so their allauth tokens are gone too."
            )

        from django.contrib.auth import get_user_model

        user_model = get_user_model()
        if not isinstance(user, user_model):
            raise ConfigurationError(
                f"connection {connection.id} has authorized_by pointing at "
                f"{type(user).__name__}, but allauth SocialAccounts belong to "
                f"{user_model.__name__}."
            )
        return user

    def _token_model(self):
        """Import allauth's model, or explain precisely which fix is needed."""
        try:
            from allauth.socialaccount.models import SocialToken
        except ImportError as exc:
            raise ConfigurationError(
                f"the {self.key!r} auth backend needs django-allauth: "
                f"pip install 'django-connectors[allauth]'"
            ) from exc
        except (ImproperlyConfigured, RuntimeError) as exc:
            # Installed but not in INSTALLED_APPS is a different fix from not
            # installed, so it gets a different message. Two exception types
            # because which one you get depends on how far the import chain
            # gets first: allauth raises ImproperlyConfigured deliberately,
            # while Django raises RuntimeError from the first allauth model it
            # cannot assign an app_label to.
            raise ConfigurationError(
                f"the {self.key!r} auth backend needs 'allauth.account' and "
                f"'allauth.socialaccount' in INSTALLED_APPS: {exc}"
            ) from exc
        return SocialToken
