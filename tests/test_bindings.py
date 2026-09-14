"""Binding validation runs where a human is editing, not only where it runs.

``services.bindings.validate_binding`` existed and was exercised, but nothing in
the package called it: a malformed config, an unregistered source or a landing
table name over 63 characters all surfaced as a failed Run in front of a
customer rather than as a form error in front of whoever typed it.

``full_clean()`` — which the admin and every ModelForm call — is the hook. The
DRF serializer needs its own, because DRF does not call ``full_clean()``; those
tests live in ``tests/test_api.py``, behind the same ``drf`` extra skip.
"""

import pytest
from django.core.exceptions import ValidationError

from django_connectors.models import Binding
from django_connectors.models.binding import generate_landing_key
from django_connectors.services import bindings as binding_services

pytestmark = pytest.mark.django_db

VALID = {"resources": {"events": {"primary_key": "id", "batches": []}}}


# --- full_clean, i.e. the admin and every ModelForm -------------------------


def test_full_clean_accepts_a_usable_binding(connectors_settings, make_connection):
    binding = Binding(connection=make_connection(), source="memory", config=VALID)
    binding.full_clean(exclude=["landing_key"])


def test_full_clean_rejects_an_unregistered_source(
    connectors_settings, make_connection
):
    binding = Binding(connection=make_connection(), source="nope", config=VALID)
    with pytest.raises(ValidationError) as excinfo:
        binding.full_clean(exclude=["landing_key"])
    assert "source" in excinfo.value.message_dict
    assert "not registered" in str(excinfo.value)


def test_full_clean_rejects_a_malformed_config_on_the_config_field(
    connectors_settings, make_connection
):
    """The source's own ``validate_config``, run where the config was typed."""
    binding = Binding(connection=make_connection(), source="memory", config={})
    with pytest.raises(ValidationError) as excinfo:
        binding.full_clean(exclude=["landing_key"])
    assert "config" in excinfo.value.message_dict
    assert "resources" in str(excinfo.value)


def test_full_clean_rejects_a_landing_table_name_that_would_not_fit(
    connectors_settings, make_connection
):
    """PostgreSQL truncates at 63 and dlt injects a hash rather than failing.

    The physical table name then becomes unpredictable, and two Bindings whose
    names differ only past the cut collide onto one table. Only a rename fixes
    it, which is exactly why it belongs in the form and not in a Run.
    """
    binding = Binding(
        connection=make_connection(),
        source="memory",
        config=VALID,
        resources=["r" * 60],
    )
    with pytest.raises(ValidationError) as excinfo:
        binding.full_clean(exclude=["landing_key"])
    assert "resources" in excinfo.value.message_dict
    assert "63" in str(excinfo.value)


def test_the_length_check_works_before_the_row_has_a_landing_key(
    connectors_settings, make_connection
):
    """An unsaved Binding has no landing_key, and it is 12 characters of the name.

    Checking the name without one would understate the length and let through a
    name that the first save then makes illegal.
    """
    assert len(binding_services.UNSAVED_LANDING_KEY) == len(generate_landing_key())

    unsaved = Binding(connection=make_connection(), source="memory", config=VALID)
    assert unsaved.landing_key == ""
    names = binding_services.validate_landing_names(unsaved)
    assert names and binding_services.UNSAVED_LANDING_KEY in names[0]


def test_every_problem_is_reported_in_one_pass(connectors_settings, make_connection):
    """One save, every error. Otherwise an operator fixes them one save apart."""
    binding = Binding(
        connection=make_connection(), source="memory", config={}, resources=["r" * 60]
    )
    errors = binding_services.binding_field_errors(binding)
    assert set(errors) == {"config", "resources"}


class BrokenSource:
    """A source whose ``validate_config`` is buggy rather than strict."""

    key = "broken"

    def validate_config(self, config):
        raise TypeError("this is a bug, not a configuration problem")


def test_an_unexpected_error_from_a_source_is_not_disguised_as_a_form_error(
    connectors_settings, settings, make_connection
):
    """A source raising TypeError is a bug in the source, not bad user input."""
    settings.DJANGO_CONNECTORS = {
        **settings.DJANGO_CONNECTORS,
        "SOURCES": {
            **settings.DJANGO_CONNECTORS["SOURCES"],
            "broken": "tests.test_bindings.BrokenSource",
        },
    }
    binding = Binding(connection=make_connection(), source="broken", config={})
    with pytest.raises(TypeError):
        binding_services.binding_field_errors(binding)


def test_validation_resolves_no_registry_at_import_time():
    """The registry is lazy on purpose, and Binding is imported during setup.

    Importing the service must therefore not touch ``DJANGO_CONNECTORS`` — a
    registry read during app loading depends on ``INSTALLED_APPS`` ordering,
    which nobody treats as semantic.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import os\n"
            "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
            "import django, sys\n"
            "django.setup()\n"
            "import django_connectors.services.bindings\n"
            "from django_connectors.registry import sources\n"
            "assert not sources._resolved, sources._resolved\n"
            "assert 'dlt' not in sys.modules, 'dlt was imported'\n",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


# --- save() is deliberately still unvalidated -------------------------------


def test_save_does_not_validate_so_a_broken_binding_stays_disableable(
    connectors_settings, make_connection, settings
):
    """The operational reason validation lives in clean() and not in save().

    A source key that stops being registered — a renamed source, an uninstalled
    extra — must not make its Binding unsavable, or nobody can disable the row
    that is failing and automated recovery cannot write ``enabled=False``.
    """
    binding = Binding.objects.create(
        connection=make_connection(), source="memory", config=VALID
    )
    settings.DJANGO_CONNECTORS = {**settings.DJANGO_CONNECTORS, "SOURCES": {}}

    binding.enabled = False
    binding.save(update_fields=["enabled"])

    binding.refresh_from_db()
    assert binding.enabled is False
