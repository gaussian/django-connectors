"""The shipped migration must match the models and apply cleanly.

A library that ships models but drifts from its migration forces every host to
generate one inside site-packages, which they cannot commit or fix.
"""

import io

import pytest
from django.core.management import call_command
from django.db import connection


@pytest.mark.django_db
def test_no_missing_migrations():
    """`makemigrations --check` must stay clean after `ruff format`.

    Generated migrations are reformatted (Django emits imports in an order
    ruff's isort rejects), so this also asserts that reformatting is
    semantically inert.
    """
    out = io.StringIO()
    call_command("makemigrations", "--check", "--dry-run", stdout=out, verbosity=1)
    assert "No changes detected" in out.getvalue()


@pytest.mark.django_db
def test_only_one_migration_ships():
    """The whole persistent schema is frozen in 0001.

    Nothing after the schema-freeze phase may add a field, so a second
    migration appearing is a design regression, not a routine change.
    """
    from django.db.migrations.loader import MigrationLoader

    loader = MigrationLoader(None, ignore_no_migrations=True)
    names = sorted(
        name
        for app_label, name in loader.disk_migrations
        if app_label == "django_connectors"
    )
    assert names == ["0001_initial"]


@pytest.mark.django_db
def test_tables_exist_after_migrate():
    table_names = set(connection.introspection.table_names())
    expected = {
        "django_connectors_connection",
        "django_connectors_connectionsecret",
        "django_connectors_binding",
        "django_connectors_bindinglock",
        "django_connectors_run",
        "django_connectors_projection",
        "django_connectors_projectionrun",
        "django_connectors_webhooksubscription",
    }
    assert expected <= table_names


@pytest.mark.django_db
def test_generated_identifiers_fit_mysql():
    """MySQL's identifier limit is 64 and the longest name is already 63.

    Auto-generated FK constraint names are the longest identifiers here, so a
    longer app label or field name would break `migrate` on MySQL only.
    """
    from django.apps import apps

    for model in apps.get_app_config("django_connectors").get_models():
        assert len(model._meta.db_table) <= 64, model._meta.db_table
        for index in model._meta.indexes:
            assert len(index.name) <= 64, index.name
        for constraint in model._meta.constraints:
            assert len(constraint.name) <= 64, constraint.name


@pytest.mark.django_db
def test_custom_permissions_are_created():
    """Admin actions resolve these by name; a missing one silently opens up."""
    from django.contrib.auth.models import Permission

    codenames = set(
        Permission.objects.filter(
            content_type__app_label="django_connectors"
        ).values_list("codename", flat=True)
    )
    assert {
        "run_binding",
        "preview_projection",
        "replay_projection",
        "test_connection",
        "renew_webhooksubscription",
    } <= codenames
