import re

import django_connectors


def test_version_is_semver():
    assert re.fullmatch(r"\d+\.\d+\.\d+", django_connectors.__version__)
