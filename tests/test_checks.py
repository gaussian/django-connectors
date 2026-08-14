"""Each system check must fire on its own trigger and stay silent otherwise."""

from typing import ClassVar

from django.test import override_settings

from django_connectors.checks import (
    check_contenttypes_installed,
    check_landing_url,
    check_registry_extras_available,
    check_registry_paths_import,
)

GOOD_LANDING = "mysql+pymysql://user:pw@localhost:3306/connectors_landing"


def _ids(messages):
    return [message.id for message in messages]


def test_contenttypes_present_produces_no_error():
    assert check_contenttypes_installed(None) == []


@override_settings(
    INSTALLED_APPS=["django.contrib.auth", "django_connectors"],
)
def test_e001_when_contenttypes_missing():
    assert _ids(check_contenttypes_installed(None)) == ["django_connectors.E001"]


def test_w002_when_landing_url_unset():
    """A fresh install must warn, not error — the host may not be ready yet."""
    assert _ids(check_landing_url(None)) == ["django_connectors.W002"]


@override_settings(DJANGO_CONNECTORS={"LANDING_URL": GOOD_LANDING})
def test_no_message_for_a_valid_landing_url():
    assert check_landing_url(None) == []


@override_settings(DJANGO_CONNECTORS={"LANDING_URL": 12345})
def test_e002_when_landing_url_is_not_a_string():
    assert _ids(check_landing_url(None)) == ["django_connectors.E002"]


@override_settings(DJANGO_CONNECTORS={"LANDING_URL": "not-a-dsn"})
def test_e002_when_landing_url_has_no_scheme_or_database():
    assert _ids(check_landing_url(None)) == ["django_connectors.E002"]


@override_settings(DJANGO_CONNECTORS={"LANDING_URL": "mysql+pymysql://host:3306/"})
def test_e002_when_landing_url_omits_the_database_name():
    assert _ids(check_landing_url(None)) == ["django_connectors.E002"]


def test_no_registry_errors_when_nothing_is_registered():
    assert check_registry_paths_import(None) == []


@override_settings(
    DJANGO_CONNECTORS={"SOURCES": {"ghost": "django_connectors.nope.Missing"}}
)
def test_e003_when_a_configured_dotted_path_cannot_be_imported():
    messages = check_registry_paths_import(None)
    assert _ids(messages) == ["django_connectors.E003"]
    assert "ghost" in messages[0].msg


@override_settings(DJANGO_CONNECTORS={"SOURCES": "not-a-dict"})
def test_e003_when_a_registry_setting_is_the_wrong_shape():
    assert _ids(check_registry_paths_import(None)) == ["django_connectors.E003"]


@override_settings(
    DJANGO_CONNECTORS={"SOURCES": {"needy": "tests.test_checks.SourceNeedingExtra"}}
)
def test_w005_when_a_backend_declares_an_uninstalled_extra():
    messages = check_registry_extras_available(None)
    assert _ids(messages) == ["django_connectors.W005"]
    assert "django-connectors[imaginary]" in messages[0].hint


@override_settings(
    DJANGO_CONNECTORS={"SOURCES": {"fine": "tests.test_checks.SourceWithPresentExtra"}}
)
def test_no_w005_when_the_declared_extra_is_installed():
    assert check_registry_extras_available(None) == []


class SourceNeedingExtra:
    """Stands in for a source whose extra the host has not installed."""

    required_extras: ClassVar[dict[str, str]] = {
        "a_module_that_does_not_exist": "imaginary"
    }


class SourceWithPresentExtra:
    required_extras: ClassVar[dict[str, str]] = {"json": "stdlib"}
