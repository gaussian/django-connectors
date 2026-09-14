"""Minimal Django settings used to run the test suite.

django.contrib.admin is installed deliberately, not incidentally: it is what
causes ``django_connectors/admin.py`` to be eagerly autodiscovered, which is the
condition the import-contract tests assert against. Without it, an accidental
heavyweight import in admin.py would go unnoticed until a host hit it.
"""

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
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.admin",
    "django_connectors",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ]
        },
    }
]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Local-memory cache: webhook delivery dedupe and debounce are cache-backed, so
# a dummy cache would make those tests silently vacuous.
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    }
}

USE_I18N = False
