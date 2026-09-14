"""Auth backend behaviour, and what a Run does with what they raise.

The distinction the runner acts on — ``CredentialsRevoked``/``CredentialsExpired``
versus everything else — is the reason most of these tests exist. Getting it
wrong in either direction is expensive: a transient error reported as revocation
takes a healthy Connection out of service, and a revocation reported as a
transient error gets the Binding retried on a schedule forever against a 401.
"""

import json
import logging
import sys
from typing import ClassVar

import pytest
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import connection as db_connection
from django.utils import timezone

from django_connectors.auth import (
    AllauthBackend,
    AuthBackend,
    Credentials,
    SecretReferenceBackend,
    StaticCredentialsBackend,
)
from django_connectors.enums import BindingStatus, ConnectionStatus, RunStatus
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsExpired,
    CredentialsRevoked,
)
from django_connectors.registry import auth_backends
from django_connectors.secrets import SecretStore
from django_connectors.services import runs as run_services
from django_connectors.sources.memory import MemorySource
from tests.conftest import memory_config

pytestmark = pytest.mark.django_db

# Long, mixed alphanumeric, and embedded in a provider-shaped message: this has
# to be something errors.scrub() must genuinely recognise.
LEAKED_TOKEN = "ya29a0AfB7bxK9QwErTyUiOp1234567890abcdefGHIJKLmn"

ALLAUTH_APPS = ("allauth", "allauth.account", "allauth.socialaccount")


# --- test doubles, reachable by dotted path --------------------------------


class ExternalStore(SecretStore):
    """Stands in for Vault / Secrets Manager: read-only, values set by a test."""

    values: ClassVar[dict[str, object]] = {}

    def get(self, connection, key):
        return self.values.get(key)

    def set(self, connection, key, value):
        raise ImproperlyConfigured("ExternalStore is read-only")

    def delete(self, connection, key):
        raise ImproperlyConfigured("ExternalStore is read-only")


class RevokingBackend(AuthBackend):
    """A provider that has revoked the grant, and says so with a token in hand."""

    key = "revoking"

    def get_credentials(self, connection):
        raise CredentialsRevoked(
            f"provider revoked the grant: refresh_token={LEAKED_TOKEN}"
        )


class LeakyBackend(AuthBackend):
    """A provider error that carries the credential, as real ones do."""

    key = "leaky"

    def get_credentials(self, connection):
        raise AuthError(
            f"401 from https://api.example.com/v1/me?access_token={LEAKED_TOKEN} "
            f"(access_token={LEAKED_TOKEN})"
        )


@pytest.fixture(autouse=True)
def _clear_external_store():
    ExternalStore.values.clear()
    yield
    ExternalStore.values.clear()


@pytest.fixture
def configure(settings):
    """Assign ``DJANGO_CONNECTORS`` so the setting_changed signal fires."""

    def _configure(**values):
        settings.DJANGO_CONNECTORS = values
        return values

    return _configure


@pytest.fixture
def model_store(configure):
    """The shipped database-backed store, explicitly in plaintext mode."""

    def _configure(**extra):
        return configure(
            SECRET_STORE="django_connectors.secrets.ModelSecretStore",
            SECRET_ENCRYPTION="none",
            **extra,
        )

    return _configure


# --- registry --------------------------------------------------------------


def test_backends_resolve_lazily_from_settings(configure):
    configure(
        AUTH_BACKENDS={
            "static": "django_connectors.auth.StaticCredentialsBackend",
            "secret_reference": "django_connectors.auth.SecretReferenceBackend",
            "allauth": "django_connectors.auth.AllauthBackend",
        }
    )
    assert isinstance(auth_backends.get("static"), StaticCredentialsBackend)
    assert isinstance(auth_backends.get("secret_reference"), SecretReferenceBackend)
    # Resolving the allauth backend must not require allauth to be usable: the
    # cost of the extra is paid on first credential lookup, not at startup.
    assert isinstance(auth_backends.get("allauth"), AllauthBackend)


def test_unknown_backend_names_the_setting(configure):
    configure(AUTH_BACKENDS={})
    with pytest.raises(ImproperlyConfigured, match="AUTH_BACKENDS"):
        auth_backends.get("nope")


def test_shipped_backends_declare_their_extras():
    """W005 reports a backend whose extra is missing; it can only do that from
    a declared mapping."""
    assert AllauthBackend.required_extras == {"allauth": "allauth"}
    assert StaticCredentialsBackend.required_extras == {}


# --- static credentials ----------------------------------------------------


def test_static_backend_returns_the_stored_value(model_store, make_connection):
    model_store()
    backend = StaticCredentialsBackend()
    connection = make_connection(auth_backend="static", auth_reference="acme_token")
    backend.store_credentials(connection, "api-key-value")

    assert backend.get_credentials(connection) == "api-key-value"


def test_static_backend_without_a_reference_is_a_configuration_error(
    model_store, make_connection
):
    """Not CredentialsRevoked: nothing was ever stored, so nothing was revoked,
    and blocking the Binding would hide a setup mistake behind an auth failure."""
    model_store()
    connection = make_connection(auth_backend="static")
    with pytest.raises(ConfigurationError, match="auth_reference"):
        StaticCredentialsBackend().get_credentials(connection)


def test_static_backend_reports_a_deleted_credential_as_revoked(
    model_store, make_connection
):
    model_store()
    connection = make_connection(auth_backend="static", auth_reference="acme_token")
    with pytest.raises(CredentialsRevoked):
        StaticCredentialsBackend().get_credentials(connection)


def test_static_backend_revoke_deletes_the_credential(model_store, make_connection):
    """A Connection marked revoked whose credential is still readable is worse
    than useless: it looks handled and is not."""
    from django_connectors.models import ConnectionSecret

    model_store()
    backend = StaticCredentialsBackend()
    connection = make_connection(auth_backend="static", auth_reference="acme_token")
    backend.store_credentials(connection, "api-key-value")

    backend.revoke(connection)

    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED
    assert not ConnectionSecret.objects.filter(connection=connection).exists()
    with pytest.raises(CredentialsRevoked):
        backend.get_credentials(connection)


def test_store_credentials_does_not_activate_the_connection(
    model_store, make_connection
):
    model_store()
    backend = StaticCredentialsBackend()
    connection = make_connection(
        auth_backend="static", status=ConnectionStatus.PENDING, auth_reference=""
    )

    reference = backend.store_credentials(connection, "api-key-value")
    connection.refresh_from_db()
    assert connection.auth_reference == reference
    assert connection.status == ConnectionStatus.PENDING

    backend.complete_setup(connection)
    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.ACTIVE


def test_complete_setup_refuses_a_connection_with_no_credential(
    model_store, make_connection
):
    """Activating first and validating later produces a Connection that looks
    healthy, schedules Bindings, and fails every one of them."""
    model_store()
    connection = make_connection(
        auth_backend="static",
        auth_reference="acme_token",
        status=ConnectionStatus.PENDING,
    )
    with pytest.raises(CredentialsRevoked):
        StaticCredentialsBackend().complete_setup(connection)

    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.PENDING


def test_static_health_reports_presence_not_the_credential(
    model_store, make_connection
):
    model_store()
    backend = StaticCredentialsBackend()
    connection = make_connection(auth_backend="static", auth_reference="acme_token")
    backend.store_credentials(connection, LEAKED_TOKEN)

    health = backend.health(connection)
    assert health["credential_present"] is True
    assert LEAKED_TOKEN not in json.dumps(health, default=str)


# --- external secret references --------------------------------------------


def test_secret_reference_backend_returns_whatever_the_store_yields(
    configure, make_connection
):
    """The store fronts someone else's system of record; reshaping its answer
    here would mean the host cannot express what their manager actually holds."""
    configure(SECRET_STORE="tests.test_auth.ExternalStore")
    payload = {"client_email": "svc@example.com", "private_key": "-----BEGIN..."}
    ExternalStore.values["projects/1/secrets/acme"] = payload

    connection = make_connection(
        auth_backend="secret_reference", auth_reference="projects/1/secrets/acme"
    )
    assert SecretReferenceBackend().get_credentials(connection) is payload


def test_secret_reference_backend_reports_a_missing_reference_as_revoked(
    configure, make_connection
):
    configure(SECRET_STORE="tests.test_auth.ExternalStore")
    connection = make_connection(
        auth_backend="secret_reference", auth_reference="projects/1/secrets/gone"
    )
    with pytest.raises(CredentialsRevoked, match="stale"):
        SecretReferenceBackend().get_credentials(connection)


def test_secret_reference_backend_never_writes_to_the_store(configure, make_connection):
    """Revoking one Connection must not delete a secret other systems may share
    — ExternalStore raises on any write, so this passing proves none happened."""
    configure(SECRET_STORE="tests.test_auth.ExternalStore")
    backend = SecretReferenceBackend()
    connection = make_connection(
        auth_backend="secret_reference", auth_reference="projects/1/secrets/acme"
    )
    ExternalStore.values["projects/1/secrets/acme"] = "value"

    backend.revoke(connection)

    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED
    assert ExternalStore.values["projects/1/secrets/acme"] == "value"
    assert not hasattr(backend, "store_credentials")


def test_secret_reference_backend_rereads_the_store_every_time(
    configure, make_connection
):
    """External managers rotate in place behind a stable reference; a cached
    value would keep serving the superseded credential until a restart."""
    configure(SECRET_STORE="tests.test_auth.ExternalStore")
    backend = SecretReferenceBackend()
    connection = make_connection(auth_backend="secret_reference", auth_reference="ref")

    ExternalStore.values["ref"] = "first"
    assert backend.get_credentials(connection) == "first"
    ExternalStore.values["ref"] = "second"
    assert backend.get_credentials(connection) == "second"


# --- allauth ---------------------------------------------------------------


@pytest.fixture
def allauth_models(transactional_db, settings):
    """Install allauth and create its tables for one test.

    tests/settings.py deliberately does not ship allauth: the `minimal` test
    tier proves this package works with no extras installed at all. Since a
    model whose app is absent cannot even be imported, the app is installed for
    the duration of the test and its three tables are created directly —
    running allauth's own migrations would cost seconds per test and prove
    nothing this backend depends on.
    """
    pytest.importorskip("allauth")
    settings.MIDDLEWARE = [
        *settings.MIDDLEWARE,
        "allauth.account.middleware.AccountMiddleware",
    ]
    settings.INSTALLED_APPS = [*settings.INSTALLED_APPS, *ALLAUTH_APPS]

    from allauth.socialaccount.models import SocialAccount, SocialApp, SocialToken

    with db_connection.schema_editor() as editor:
        for model in (SocialApp, SocialAccount, SocialToken):
            editor.create_model(model)
    try:
        yield SocialAccount, SocialToken
    finally:
        with db_connection.schema_editor() as editor:
            for model in (SocialToken, SocialAccount, SocialApp):
                editor.delete_model(model)


def social_connection(make_connection, user, **kwargs):
    """A Connection whose *authorising* identity is `user`."""
    return make_connection(
        provider=kwargs.pop("provider", "google"),
        auth_backend="allauth",
        authorized_by_content_type=ContentType.objects.get_for_model(user),
        authorized_by_object_id=str(user.pk),
        **kwargs,
    )


def test_allauth_backend_resolves_the_users_token(allauth_models, make_connection):
    SocialAccount, SocialToken = allauth_models
    user = get_user_model().objects.create(username="grantor")
    account = SocialAccount.objects.create(user=user, provider="google", uid="uid-1")
    SocialToken.objects.create(account=account, token=LEAKED_TOKEN, token_secret="rt")

    credentials = AllauthBackend().get_credentials(
        social_connection(make_connection, user)
    )

    assert credentials["access_token"] == LEAKED_TOKEN
    assert credentials["refresh_token"] == "rt"
    assert credentials["account_uid"] == "uid-1"


def test_allauth_credentials_do_not_leak_through_repr(allauth_models, make_connection):
    """Tokens escape through repr, not through deliberate printing: a traceback
    renders every local, and logger.exception writes the lot to disk."""
    SocialAccount, SocialToken = allauth_models
    user = get_user_model().objects.create(username="grantor")
    account = SocialAccount.objects.create(user=user, provider="google", uid="uid-1")
    SocialToken.objects.create(account=account, token=LEAKED_TOKEN)

    credentials = AllauthBackend().get_credentials(
        social_connection(make_connection, user)
    )

    assert LEAKED_TOKEN not in repr(credentials)
    assert LEAKED_TOKEN not in str(credentials)
    assert LEAKED_TOKEN not in f"{credentials}"
    assert "access_token" in repr(credentials)


def test_allauth_backend_reports_a_disconnected_account_as_revoked(
    allauth_models, make_connection
):
    """allauth deletes the SocialToken when the user disconnects, so "no row"
    is the revocation signal — the runner must block, not retry."""
    SocialAccount, _ = allauth_models
    user = get_user_model().objects.create(username="grantor")
    SocialAccount.objects.create(user=user, provider="google", uid="uid-1")

    with pytest.raises(CredentialsRevoked, match="SocialToken"):
        AllauthBackend().get_credentials(social_connection(make_connection, user))


def test_allauth_backend_reports_an_expired_token_as_expired(
    allauth_models, make_connection
):
    """Distinct from revoked: allauth may still refresh it, so the message must
    not claim the grant is gone."""
    SocialAccount, SocialToken = allauth_models
    user = get_user_model().objects.create(username="grantor")
    account = SocialAccount.objects.create(user=user, provider="google", uid="uid-1")
    SocialToken.objects.create(
        account=account,
        token=LEAKED_TOKEN,
        expires_at=timezone.now() - timezone.timedelta(minutes=1),
    )

    with pytest.raises(CredentialsExpired) as excinfo:
        AllauthBackend().get_credentials(social_connection(make_connection, user))
    assert LEAKED_TOKEN not in str(excinfo.value)


def test_allauth_backend_matches_the_right_account_of_two(
    allauth_models, make_connection
):
    """A user with two Google accounts must not silently get the other one's
    token — which is what happens when only (user, provider) is matched."""
    SocialAccount, SocialToken = allauth_models
    user = get_user_model().objects.create(username="grantor")
    for uid, token in (("uid-work", "work-token"), ("uid-home", "home-token")):
        account = SocialAccount.objects.create(user=user, provider="google", uid=uid)
        SocialToken.objects.create(account=account, token=token)

    connection = social_connection(
        make_connection, user, external_account_id="uid-work"
    )
    assert AllauthBackend().get_credentials(connection)["access_token"] == "work-token"


def test_allauth_backend_ignores_another_users_token(allauth_models, make_connection):
    SocialAccount, SocialToken = allauth_models
    user_model = get_user_model()
    grantor = user_model.objects.create(username="grantor")
    stranger = user_model.objects.create(username="stranger")
    account = SocialAccount.objects.create(
        user=stranger, provider="google", uid="uid-1"
    )
    SocialToken.objects.create(account=account, token="not-yours")

    with pytest.raises(CredentialsRevoked):
        AllauthBackend().get_credentials(social_connection(make_connection, grantor))


def test_allauth_backend_treats_a_deleted_grantor_as_revoked(
    allauth_models, make_connection
):
    """The GFK dangles rather than erroring, and allauth cascaded the tokens."""
    user = get_user_model().objects.create(username="grantor")
    connection = social_connection(make_connection, user)
    user.delete()

    with pytest.raises(CredentialsRevoked, match="no longer exists"):
        AllauthBackend().get_credentials(connection)


def test_allauth_backend_requires_authorized_by(allauth_models, make_connection):
    """A Connection is owned by a Team while the grant belongs to a member;
    reading the owner would find no token, or worse, someone else's."""
    connection = make_connection(auth_backend="allauth", provider="google")
    with pytest.raises(ConfigurationError, match="authorized_by"):
        AllauthBackend().get_credentials(connection)


def test_allauth_backend_health_never_carries_the_token(
    allauth_models, make_connection
):
    SocialAccount, SocialToken = allauth_models
    user = get_user_model().objects.create(username="grantor")
    account = SocialAccount.objects.create(user=user, provider="google", uid="uid-1")
    SocialToken.objects.create(account=account, token=LEAKED_TOKEN)

    health = AllauthBackend().health(social_connection(make_connection, user))
    assert health["usable"] is True
    assert LEAKED_TOKEN not in json.dumps(health, default=str)


@pytest.fixture
def allauth_uncached():
    """Force allauth's models to be imported afresh, with its apps absent.

    The purge has to happen on the way *out* as well as in. A half-executed
    import leaves the parent package without the submodule attributes its own
    module body sets, and every later import of allauth then fails with an
    AttributeError that has nothing to do with what the test was checking.
    """
    pytest.importorskip("allauth")

    def purge():
        for name in [key for key in sys.modules if key.startswith("allauth")]:
            del sys.modules[name]

    purge()
    yield
    purge()


def test_missing_allauth_names_the_extra(make_connection, monkeypatch):
    """Never a bare ImportError from inside a customer's Run."""
    monkeypatch.setitem(sys.modules, "allauth.socialaccount.models", None)
    connection = make_connection(auth_backend="allauth", provider="google")

    with pytest.raises(ConfigurationError, match=r"django-connectors\[allauth\]"):
        AllauthBackend().get_credentials(connection)


def test_allauth_installed_but_not_in_installed_apps_says_so(
    allauth_uncached, make_connection
):
    """A different fix from a missing package, so a different message."""
    connection = make_connection(auth_backend="allauth", provider="google")

    with pytest.raises(ConfigurationError, match="INSTALLED_APPS"):
        AllauthBackend().get_credentials(connection)


# --- the base contract -----------------------------------------------------


def test_credentials_mapping_still_reads_normally():
    credentials = Credentials(provider="google", access_token="t", expires_at=None)
    assert credentials["access_token"] == "t"
    assert dict(credentials) == {"access_token": "t", "expires_at": None}
    assert sorted(credentials) == ["access_token", "expires_at"]
    assert credentials.provider == "google"


def test_base_revoke_marks_the_connection(make_connection):
    connection = make_connection(auth_backend="static")
    AuthBackend().revoke(connection)
    connection.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED


def test_auth_metadata_may_not_hold_credentials(make_connection):
    """That field is rendered in the admin and returned by the API, so a
    credential stored there is a credential published."""
    connection = make_connection(
        auth_backend="static", auth_metadata={"scopes": ["read"], "token": "x"}
    )
    with pytest.raises(ValidationError) as excinfo:
        connection.full_clean()
    assert "auth_metadata" in excinfo.value.message_dict

    connection.auth_metadata = {"scopes": ["read"], "account_email": "a@example.com"}
    connection.full_clean()


# --- what a Run does with all of this --------------------------------------


def test_revocation_blocks_the_binding_and_stops_the_next_run(
    connectors_settings, make_binding, make_connection, settings, monkeypatch
):
    """End to end: the runner must stop retrying a revoked credential, because
    retrying burns provider quota and invites rate limiting."""
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "AUTH_BACKENDS": {"revoking": "tests.test_auth.RevokingBackend"},
    }
    connection = make_connection(auth_backend="revoking")
    binding = make_binding(
        connection=connection, config=memory_config(batches=[[{"id": "1"}]])
    )

    first = run_services.run_binding(binding)
    assert first.status == RunStatus.FAILED
    assert first.error_type == "CredentialsRevoked"

    connection.refresh_from_db()
    binding.refresh_from_db()
    assert connection.status == ConnectionStatus.REVOKED
    assert binding.status == BindingStatus.BLOCKED
    assert not binding.is_runnable

    # Nothing may reach the source now — extraction is what costs provider
    # quota, and a blocked Binding must not spend any.
    def refuse(*args, **kwargs):
        raise AssertionError("a blocked binding must not build its source")

    monkeypatch.setattr(MemorySource, "build_source", refuse)

    second = run_services.run_binding(binding)
    assert second.status == RunStatus.FAILED
    assert second.error_type == "ConnectorError"
    assert second.dlt_load_ids == []
    assert "blocked" in second.error_message


def test_a_token_in_a_provider_error_never_reaches_persistence_or_logs(
    connectors_settings, make_binding, make_connection, settings, caplog
):
    """Providers put the credential in the error text; the runner stores that
    text on the Run and the admin renders it."""
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "AUTH_BACKENDS": {"leaky": "tests.test_auth.LeakyBackend"},
    }
    binding = make_binding(
        connection=make_connection(auth_backend="leaky"),
        config=memory_config(batches=[[{"id": "1"}]]),
    )

    with caplog.at_level(logging.DEBUG):
        run = run_services.run_binding(binding)

    assert run.status == RunStatus.FAILED
    assert LEAKED_TOKEN not in run.error_message
    assert "***" in run.error_message

    binding.refresh_from_db()
    assert LEAKED_TOKEN not in binding.last_error

    # Serialized in full, as an API response would render it.
    assert LEAKED_TOKEN not in serializers.serialize("json", [run])
    assert LEAKED_TOKEN not in json.dumps(
        {"error": run.error_message, "metrics": run.metrics}
    )

    for record in caplog.records:
        assert LEAKED_TOKEN not in record.getMessage()
        assert LEAKED_TOKEN not in str(record.args)


def test_a_revocation_message_is_scrubbed_on_the_binding_too(
    connectors_settings, make_binding, make_connection, settings
):
    """`_block_for_credentials` writes its own copy of the message."""
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "AUTH_BACKENDS": {"revoking": "tests.test_auth.RevokingBackend"},
    }
    binding = make_binding(
        connection=make_connection(auth_backend="revoking"),
        config=memory_config(batches=[[{"id": "1"}]]),
    )

    run_services.run_binding(binding)

    binding.refresh_from_db()
    assert LEAKED_TOKEN not in binding.last_error
    assert LEAKED_TOKEN not in serializers.serialize("json", [binding])
