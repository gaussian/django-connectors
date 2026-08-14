"""Minimal Django settings used to run the test suite."""

SECRET_KEY = "django-connectors-test-key-not-secret"

DEBUG = True

USE_TZ = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_connectors",
]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
