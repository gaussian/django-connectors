"""Settings access must be lazy, cached, validated, and override-friendly."""

import datetime as dt

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

from django_connectors.conf import DEFAULTS, conf


def test_every_key_has_a_default():
    """A host that sets nothing must still get a usable object."""
    for key in DEFAULTS:
        getattr(conf, key)


def test_defaults_are_returned_when_unset():
    assert conf.LANDING_DATASET == "connectors_landing"
    assert conf.PROJECTION_BATCH_SIZE == 1000
    assert dt.timedelta(days=7) == conf.PROJECTION_SWEEP_LOOKBACK


def test_user_settings_override_defaults():
    with override_settings(DJANGO_CONNECTORS={"PROJECTION_BATCH_SIZE": 25}):
        assert conf.PROJECTION_BATCH_SIZE == 25
    # The override_settings signal must clear the cache, or the next read here
    # would return 25 forever.
    assert conf.PROJECTION_BATCH_SIZE == 1000


def test_unknown_setting_is_rejected_by_name():
    with (
        override_settings(DJANGO_CONNECTORS={"LANDNIG_URL": "sqlite://"}),
        pytest.raises(ImproperlyConfigured) as excinfo,
    ):
        _ = conf.LANDING_URL
    assert "LANDNIG_URL" in str(excinfo.value)


def test_non_dict_setting_is_rejected():
    with (
        override_settings(DJANGO_CONNECTORS=["not", "a", "dict"]),
        pytest.raises(ImproperlyConfigured, match="must be a dict"),
    ):
        _ = conf.LANDING_URL


def test_unknown_attribute_is_an_attribute_error_listing_valid_names():
    with pytest.raises(AttributeError) as excinfo:
        _ = conf.NOT_A_REAL_SETTING
    assert "NOT_A_REAL_SETTING" in str(excinfo.value)
    assert "LANDING_URL" in str(excinfo.value)


def test_require_names_the_missing_key():
    with pytest.raises(ImproperlyConfigured) as excinfo:
        conf.require("LANDING_URL")
    message = str(excinfo.value)
    assert "LANDING_URL" in message
    assert "DJANGO_CONNECTORS" in message


def test_require_returns_a_configured_value():
    with override_settings(DJANGO_CONNECTORS={"LANDING_URL": "sqlite:///x.db"}):
        assert conf.require("LANDING_URL") == "sqlite:///x.db"


def test_is_set_distinguishes_explicit_from_default():
    assert conf.is_set("LANDING_DATASET") is False
    with override_settings(DJANGO_CONNECTORS={"LANDING_DATASET": "connectors_landing"}):
        assert conf.is_set("LANDING_DATASET") is True
