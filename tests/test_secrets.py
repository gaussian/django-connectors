"""SecretStore behaviour, and the leak paths a stored credential could take.

The interesting assertions here are the negative ones. A store that saves and
loads a value is easy; what actually matters is that the default refuses to
save at all, that nothing is written when the encryption scheme was never
chosen, and that a stored token cannot be read back out of the admin, a
serialized row, or a log line.
"""

import sys

import pytest
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured
from django.urls import path
from django.utils.module_loading import import_string

from django_connectors.exceptions import ConfigurationError
from django_connectors.secrets import (
    ModelSecretStore,
    NullSecretStore,
    SecretStore,
    SettingsSecretStore,
    get_secret_store,
)

# A ROOT_URLCONF the admin tests can point at. tests/settings.py has none, and
# rendering a changeform reverses admin URLs.
urlpatterns = [path("admin/", admin.site.urls)]

pytestmark = pytest.mark.django_db

# Deliberately shaped like a real credential: long, mixed alphanumeric, so that
# errors.scrub() has to actually recognise it rather than pattern-match a
# placeholder like "SECRET".
STORED_TOKEN = "sk-live-4f9c2b7a1d8e0356aa11bb22cc33dd44"

# Valid Fernet keys (32 url-safe base64 bytes), hardcoded so these tests neither
# import cryptography at module scope nor depend on it being installed.
FERNET_KEY = "8Xy5tJ0kqR2vN7wZpL4sB1cD6eF3gH9iJkMnOpQrStU="
OTHER_FERNET_KEY = "aB1cD2eF3gH4iJ5kL6mN7oP8qR9sT0uV1wX2yZ3a4b8="


@pytest.fixture
def configure(settings):
    """Set ``DJANGO_CONNECTORS`` wholesale, firing the setting_changed signal.

    Mutating the dict in place would not: conf and the store resolver both
    cache, and both are only invalidated by the signal.
    """

    def _configure(**values):
        settings.DJANGO_CONNECTORS = values
        return values

    return _configure


# --- resolution ------------------------------------------------------------


def test_the_documented_default_path_imports():
    """conf's default is a string; nothing checks it until someone uses it."""
    assert import_string("django_connectors.secrets.NullSecretStore") is (
        NullSecretStore
    )


def test_default_store_is_the_null_store(configure):
    configure()
    assert isinstance(get_secret_store(), NullSecretStore)


def test_store_is_resolved_once_and_reset_when_settings_change(configure):
    """A store fronting an external manager holds a client; rebuilding it per
    lookup would mean one auth handshake per credential."""
    configure()
    first = get_secret_store()
    assert get_secret_store() is first

    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    second = get_secret_store()
    assert isinstance(second, SettingsSecretStore)
    assert second is not first


def test_unimportable_store_path_names_the_setting(configure):
    configure(SECRET_STORE="django_connectors.secrets.NoSuchStore")
    with pytest.raises(ImproperlyConfigured, match="SECRET_STORE"):
        get_secret_store()


def test_a_store_that_is_not_a_secretstore_is_refused(configure):
    """Duck typing here would mean a typo resolves to some unrelated class and
    fails much later, inside a Run, with an AttributeError."""
    configure(SECRET_STORE="django_connectors.sources.memory.MemorySource")
    with pytest.raises(ImproperlyConfigured, match="not a SecretStore"):
        get_secret_store()


# --- the null store --------------------------------------------------------


def test_null_store_refuses_to_write_and_names_the_setting(configure, make_connection):
    """The one behaviour that must never be a silent no-op."""
    configure()
    connection = make_connection()
    with pytest.raises(ImproperlyConfigured) as excinfo:
        get_secret_store().set(connection, "token", STORED_TOKEN)

    message = str(excinfo.value)
    assert "SECRET_STORE" in message
    assert STORED_TOKEN not in message


def test_null_store_reads_nothing_and_deletes_nothing(configure, make_connection):
    configure()
    store = get_secret_store()
    connection = make_connection()
    assert store.get(connection, "token") is None
    assert store.delete(connection, "token") is False


# --- the settings store ----------------------------------------------------


def test_settings_store_reads_the_environment(configure, make_connection, monkeypatch):
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    monkeypatch.setenv("DJANGO_CONNECTORS_SECRET_ACME_TOKEN", STORED_TOKEN)
    store = get_secret_store()
    assert store.get(make_connection(), "acme_token") == STORED_TOKEN


def test_settings_store_falls_back_to_the_settings_module(
    configure, make_connection, settings
):
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    settings.DJANGO_CONNECTORS_SECRET_ACME_TOKEN = STORED_TOKEN
    assert get_secret_store().get(make_connection(), "acme_token") == STORED_TOKEN


def test_settings_store_treats_an_empty_variable_as_absent(
    configure, make_connection, monkeypatch
):
    """Container tooling materialises an unset variable as "", and a blank
    credential would reach the provider as an unexplained 401."""
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    monkeypatch.setenv("DJANGO_CONNECTORS_SECRET_ACME_TOKEN", "")
    assert get_secret_store().get(make_connection(), "acme_token") is None


def test_settings_store_cannot_be_pointed_at_an_arbitrary_setting(
    configure, make_connection
):
    """auth_reference is host data, editable in the admin. Without the prefix,
    setting it to "SECRET_KEY" would hand settings.SECRET_KEY to a source."""
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    from django.conf import settings as django_settings

    resolved = get_secret_store().get(make_connection(), "SECRET_KEY")
    assert resolved is None
    assert resolved != django_settings.SECRET_KEY


def test_settings_store_rejects_a_reference_that_cannot_name_a_variable(
    configure, make_connection
):
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    with pytest.raises(ImproperlyConfigured, match="identifier"):
        get_secret_store().get(make_connection(), "vault/prod/acme")


def test_settings_store_is_read_only(configure, make_connection):
    configure(SECRET_STORE="django_connectors.secrets.SettingsSecretStore")
    store, connection = get_secret_store(), make_connection()
    with pytest.raises(ImproperlyConfigured, match="read-only"):
        store.set(connection, "acme_token", STORED_TOKEN)
    with pytest.raises(ImproperlyConfigured, match="read-only"):
        store.delete(connection, "acme_token")


# --- the model store -------------------------------------------------------


def model_store_settings(**overrides):
    return {
        "SECRET_STORE": "django_connectors.secrets.ModelSecretStore",
        **overrides,
    }


def test_model_store_refuses_to_write_until_encryption_is_chosen(
    configure, make_connection
):
    """The invariant behind the whole package: no accidental plaintext."""
    from django_connectors.models import ConnectionSecret

    configure(**model_store_settings())
    connection = make_connection()

    with pytest.raises(ImproperlyConfigured) as excinfo:
        get_secret_store().set(connection, "token", STORED_TOKEN)

    assert "SECRET_ENCRYPTION" in str(excinfo.value)
    assert STORED_TOKEN not in str(excinfo.value)
    # And nothing was written on the way to raising.
    assert not ConnectionSecret.objects.exists()


def test_model_store_rejects_an_unknown_encryption_scheme(configure, make_connection):
    configure(**model_store_settings(SECRET_ENCRYPTION="rot13"))
    with pytest.raises(ImproperlyConfigured, match="SECRET_ENCRYPTION"):
        get_secret_store().set(make_connection(), "token", STORED_TOKEN)


def test_model_store_plaintext_round_trip_is_explicit(configure, make_connection):
    """ "none" is allauth's trust model and a legitimate deliberate choice."""
    from django_connectors.models import ConnectionSecret

    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store, connection = get_secret_store(), make_connection()

    store.set(connection, "token", STORED_TOKEN)
    assert store.get(connection, "token") == STORED_TOKEN

    row = ConnectionSecret.objects.get()
    assert row.encryption == "none"
    assert row.value == STORED_TOKEN


def test_model_store_writes_ciphertext_under_fernet(configure, make_connection):
    pytest.importorskip("cryptography")
    from django_connectors.models import ConnectionSecret

    configure(**model_store_settings(SECRET_ENCRYPTION="fernet", SECRET_KEY=FERNET_KEY))
    store, connection = get_secret_store(), make_connection()

    store.set(connection, "token", STORED_TOKEN)
    row = ConnectionSecret.objects.get()
    assert row.encryption == "fernet"
    assert STORED_TOKEN not in row.value
    assert store.get(connection, "token") == STORED_TOKEN


def test_fernet_does_not_fall_back_to_django_secret_key(configure, make_connection):
    """settings.SECRET_KEY is documented as rotatable; keying credentials off it
    would turn a routine rotation into total credential loss."""
    pytest.importorskip("cryptography")
    configure(**model_store_settings(SECRET_ENCRYPTION="fernet"))

    with pytest.raises(ImproperlyConfigured) as excinfo:
        get_secret_store().set(make_connection(), "token", STORED_TOKEN)
    assert "SECRET_KEY" in str(excinfo.value)


def test_fernet_rejects_a_key_that_is_not_a_fernet_key(configure, make_connection):
    pytest.importorskip("cryptography")
    configure(
        **model_store_settings(
            SECRET_ENCRYPTION="fernet", SECRET_KEY="not-a-fernet-key"
        )
    )
    with pytest.raises(ImproperlyConfigured, match="valid Fernet key"):
        get_secret_store().set(make_connection(), "token", STORED_TOKEN)


def test_rotating_the_fernet_key_reports_the_cause_without_leaking(
    configure, make_connection
):
    pytest.importorskip("cryptography")
    configure(**model_store_settings(SECRET_ENCRYPTION="fernet", SECRET_KEY=FERNET_KEY))
    connection = make_connection()
    get_secret_store().set(connection, "token", STORED_TOKEN)

    configure(
        **model_store_settings(SECRET_ENCRYPTION="fernet", SECRET_KEY=OTHER_FERNET_KEY)
    )
    with pytest.raises(ImproperlyConfigured) as excinfo:
        get_secret_store().get(connection, "token")

    message = str(excinfo.value)
    assert "SECRET_KEY" in message
    # cryptography's InvalidToken repr carries the ciphertext; ours must not.
    assert STORED_TOKEN not in message


def test_rows_are_decrypted_by_their_own_scheme_not_the_current_setting(
    configure, make_connection
):
    """This is what makes a none -> fernet migration possible at all."""
    pytest.importorskip("cryptography")
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    connection = make_connection()
    get_secret_store().set(connection, "legacy", STORED_TOKEN)

    configure(**model_store_settings(SECRET_ENCRYPTION="fernet", SECRET_KEY=FERNET_KEY))
    store = get_secret_store()
    assert store.get(connection, "legacy") == STORED_TOKEN

    store.set(connection, "fresh", STORED_TOKEN)
    assert store.get(connection, "fresh") == STORED_TOKEN


def test_missing_cryptography_names_the_extra_instead_of_raising_importerror(
    configure, make_connection, monkeypatch
):
    """Rule for every optional dependency: a clear ConfigurationError naming the
    extra, never a bare ImportError inside a customer's Run."""
    configure(**model_store_settings(SECRET_ENCRYPTION="fernet", SECRET_KEY=FERNET_KEY))
    monkeypatch.setitem(sys.modules, "cryptography.fernet", None)

    with pytest.raises(ConfigurationError, match=r"django-connectors\[secrets\]"):
        get_secret_store().set(make_connection(), "token", STORED_TOKEN)


def test_setting_the_same_key_twice_replaces_rather_than_conflicts(
    configure, make_connection
):
    """(connection, key) is unique; a naive create() would raise on rotation."""
    from django_connectors.models import ConnectionSecret

    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store, connection = get_secret_store(), make_connection()

    store.set(connection, "token", STORED_TOKEN)
    store.set(connection, "token", "second-value")

    assert ConnectionSecret.objects.count() == 1
    assert store.get(connection, "token") == "second-value"


def test_model_store_refuses_a_key_the_column_cannot_hold(configure, make_connection):
    """auth_reference holds 500 characters and ConnectionSecret.key holds 100.

    MySQL raises "Data too long for column" while sqlite stores it anyway, so
    the defect is invisible in development and certain in production.
    """
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store, connection = get_secret_store(), make_connection()

    with pytest.raises(ImproperlyConfigured, match="100"):
        store.set(connection, "k" * 101, STORED_TOKEN)


def test_model_store_refuses_to_stringify_a_structured_credential(
    configure, make_connection
):
    """str({"token": ...}) writes a Python repr that reads back as an
    unparseable string, and the caller only finds out inside a Run."""
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store, connection = get_secret_store(), make_connection()

    with pytest.raises(TypeError, match=r"json\.dumps"):
        store.set(connection, "token", {"access_token": STORED_TOKEN})


def test_secrets_are_scoped_to_their_connection(configure, make_connection):
    """The interface takes (connection, key) so that cross-tenant reads are not
    expressible, not merely discouraged."""
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store = get_secret_store()
    first, second = make_connection(owner_id="1"), make_connection(owner_id="2")

    store.set(first, "token", STORED_TOKEN)
    assert store.get(second, "token") is None


def test_delete_reports_whether_anything_was_removed(configure, make_connection):
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    store, connection = get_secret_store(), make_connection()

    store.set(connection, "token", STORED_TOKEN)
    assert store.delete(connection, "token") is True
    assert store.delete(connection, "token") is False
    assert store.get(connection, "token") is None


def test_every_shipped_store_implements_the_whole_interface():
    """A store missing a method fails at the worst possible moment: mid-Run."""
    for store_class in (NullSecretStore, SettingsSecretStore, ModelSecretStore):
        for method in ("get", "set", "delete"):
            assert getattr(store_class, method) is not getattr(SecretStore, method), (
                f"{store_class.__name__}.{method} is not implemented"
            )


# --- leak paths ------------------------------------------------------------


def test_a_stored_token_appears_in_no_admin_page(
    configure, make_connection, client, settings
):
    """The admin is the most likely place for a credential to be read back out
    by someone who should not have it, so assert on the rendered HTML."""
    from django_connectors.models import ConnectionSecret

    settings.ROOT_URLCONF = __name__
    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    connection = make_connection(auth_reference="token")
    get_secret_store().set(connection, "token", STORED_TOKEN)
    secret = ConnectionSecret.objects.get()

    client.force_login(
        get_user_model().objects.create_superuser(
            username="root", email="root@example.com", password="pw"
        )
    )
    for url in (
        f"/admin/django_connectors/connection/{connection.id}/change/",
        "/admin/django_connectors/connectionsecret/",
        f"/admin/django_connectors/connectionsecret/{secret.id}/change/",
    ):
        response = client.get(url)
        assert response.status_code == 200, f"{url} -> {response.status_code}"
        assert STORED_TOKEN not in response.content.decode(), url


def test_the_secret_row_never_renders_its_value(configure, make_connection):
    """__str__ runs in log lines, admin breadcrumbs and DoesNotExist messages."""
    from django_connectors.models import ConnectionSecret

    configure(**model_store_settings(SECRET_ENCRYPTION="none"))
    connection = make_connection()
    get_secret_store().set(connection, "token", STORED_TOKEN)

    row = ConnectionSecret.objects.get()
    assert STORED_TOKEN not in str(row)
    assert STORED_TOKEN not in repr(row)
