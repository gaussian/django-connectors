"""Model-layer invariants that are silent when broken."""

import uuid

import pytest
from django.apps import apps
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import IntegrityError, models, transaction

from django_connectors.enums import RunStatus, RunTrigger
from django_connectors.models import (
    Binding,
    BindingLock,
    Connection,
    ConnectionSecret,
    Projection,
    ProjectionRun,
    Run,
    WebhookSubscription,
)

EXPECTED_MODELS = {
    "Binding",
    "BindingLock",
    "Connection",
    "ConnectionSecret",
    "Projection",
    "ProjectionRun",
    "Run",
    "WebhookSubscription",
}


def test_registered_models_match_exactly():
    """A model whose module is not imported is invisible — silently.

    `makemigrations` omits it with no error, so this is the only place the
    mistake surfaces.
    """
    registered = {
        model.__name__
        for model in apps.get_app_config("django_connectors").get_models()
    }
    assert registered == EXPECTED_MODELS


def test_every_model_defines_str():
    """DJ008 catches a missing __str__, but not one that raises."""
    for model in apps.get_app_config("django_connectors").get_models():
        assert "__str__" in vars(model), f"{model.__name__} has no __str__"


def test_every_foreign_key_to_an_external_model_sets_related_name():
    """An unnamed FK to ContentType is fields.E304 in a host with its own.

    That would hard-fail `manage.py check` in the host's project, fixable only
    from this side.
    """
    for model in apps.get_app_config("django_connectors").get_models():
        for field in model._meta.get_fields():
            if not isinstance(field, models.ForeignKey):
                continue
            if field.related_model._meta.app_label == "django_connectors":
                continue
            assert field.remote_field.related_name, (
                f"{model.__name__}.{field.name} points at "
                f"{field.related_model.__name__} without a related_name"
            )


def test_contenttype_foreign_keys_are_protected():
    """CASCADE here means `remove_stale_contenttypes` deletes tenant data."""
    for field_name in ("owner_content_type", "authorized_by_content_type"):
        field = Connection._meta.get_field(field_name)
        assert field.remote_field.on_delete is models.PROTECT


@pytest.mark.django_db
def test_landing_key_is_generated_and_dlt_safe():
    binding = _make_binding()
    assert binding.landing_key
    assert len(binding.landing_key) <= 12
    # Must start with a letter: dlt prefixes a leading digit with an underscore,
    # which would make the real table name differ from the computed one.
    assert binding.landing_key[0].isalpha()
    assert binding.landing_key.islower() or binding.landing_key.isalnum()


@pytest.mark.django_db
def test_landing_keys_are_unique_across_bindings():
    keys = {_make_binding().landing_key for _ in range(25)}
    assert len(keys) == 25


@pytest.mark.django_db
def test_landing_key_survives_a_collision():
    """The unique index is the guarantee; generation just has to cope."""
    from django_connectors.models import binding as binding_module

    existing = _make_binding()
    collisions = iter([existing.landing_key, existing.landing_key, "bfeedfacecaf"])

    original = binding_module.generate_landing_key
    binding_module.generate_landing_key = lambda: next(collisions, original())
    try:
        fresh = _make_binding()
    finally:
        binding_module.generate_landing_key = original

    assert fresh.landing_key == "bfeedfacecaf"


@pytest.mark.django_db
def test_pipeline_and_schema_names_are_per_binding():
    """Sharing either across Bindings was measured handing one another's data."""
    first, second = _make_binding(), _make_binding()
    assert first.pipeline_name != second.pipeline_name
    assert first.schema_name != second.schema_name
    # MySQL's identifier limit is 64.
    assert len(first.pipeline_name) <= 64
    assert len(first.schema_name) <= 64


@pytest.mark.django_db
def test_run_dedupe_key_allows_many_nulls_but_one_value():
    """Queue coalescing relies on nullable-unique, not a partial constraint.

    Django's MySQL backend reports supports_partial_indexes = False, so
    UniqueConstraint(condition=...) emits models.W036 and creates no DDL.
    """
    binding = _make_binding()
    for _ in range(5):
        Run.objects.create(
            binding=binding, trigger=RunTrigger.MANUAL, status=RunStatus.SUCCEEDED
        )
    assert Run.objects.filter(dedupe_key__isnull=True).count() == 5

    key = uuid.uuid4()
    Run.objects.create(
        binding=binding,
        trigger=RunTrigger.SCHEDULED,
        status=RunStatus.QUEUED,
        dedupe_key=key,
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        Run.objects.create(
            binding=binding,
            trigger=RunTrigger.SCHEDULED,
            status=RunStatus.QUEUED,
            dedupe_key=key,
        )


@pytest.mark.django_db
def test_run_ids_sort_in_creation_order():
    binding = _make_binding()
    runs = [
        Run.objects.create(binding=binding, trigger=RunTrigger.SCHEDULED)
        for _ in range(50)
    ]
    assert [run.id for run in runs] == sorted(run.id for run in runs)


@pytest.mark.django_db
def test_connection_rejects_credentials_in_auth_metadata():
    """That field is rendered by the API; a credential there is published."""
    connection = _make_connection()
    for bad_key in ("refresh_token", "clientSecret", "API_KEY", "private_key"):
        connection.auth_metadata = {bad_key: "value"}
        with pytest.raises(ValidationError) as excinfo:
            connection.full_clean()
        assert "auth_metadata" in excinfo.value.message_dict


@pytest.mark.django_db
def test_connection_allows_non_secret_auth_metadata():
    connection = _make_connection()
    connection.auth_metadata = {"scopes": ["mail.read"], "account_email": "a@b.com"}
    connection.full_clean()


@pytest.mark.django_db
def test_connection_secret_str_never_contains_its_value():
    connection = _make_connection()
    secret = ConnectionSecret.objects.create(
        connection=connection, key="client_secret", value="TOPSECRET", encryption="none"
    )
    assert "TOPSECRET" not in str(secret)


@pytest.mark.django_db
def test_connection_secret_keys_are_unique_per_connection():
    connection = _make_connection()
    ConnectionSecret.objects.create(
        connection=connection, key="k", value="a", encryption="none"
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        ConnectionSecret.objects.create(
            connection=connection, key="k", value="b", encryption="none"
        )


@pytest.mark.django_db
def test_binding_lock_is_one_per_binding():
    from django.utils import timezone

    binding = _make_binding()
    expires = timezone.now() + timezone.timedelta(hours=1)
    BindingLock.objects.create(binding=binding, expires_at=expires)
    with pytest.raises(IntegrityError), transaction.atomic():
        BindingLock.objects.create(binding=binding, expires_at=expires)


@pytest.mark.django_db
def test_projection_run_records_the_version_it_ran():
    binding = _make_binding()
    projection = Projection.objects.create(
        binding=binding, resource="events", target="events", name="p", mapping={}
    )
    run = ProjectionRun.objects.create(
        projection=projection, mode="incremental", projection_version=projection.version
    )
    assert run.projection_version == 1


@pytest.mark.django_db
def test_webhook_public_id_is_random_and_distinct_from_pk():
    binding = _make_binding()
    subscription = WebhookSubscription.objects.create(binding=binding)
    assert subscription.public_id != subscription.id


# --- helpers ---------------------------------------------------------------


def _make_connection():
    return Connection.objects.create(
        owner_content_type=ContentType.objects.get_for_model(ContentType),
        owner_object_id="1",
        provider="demo",
        auth_backend="static",
    )


def _make_binding():
    return Binding.objects.create(connection=_make_connection(), source="memory")
