"""Settings for the django-connectors example project.

Small on purpose: everything here is the minimum a host actually needs.
"""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "example-only-not-a-real-secret"
DEBUG = True
ALLOWED_HOSTS = ["*"]
USE_TZ = True
ROOT_URLCONF = "example_project.urls"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.admin",
    # django.contrib.contenttypes is a hard requirement: Connection.owner is a
    # GenericForeignKey so the host can own Connections with any model at all.
    "django_connectors",
    "crm",
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

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "example.sqlite3",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}

DJANGO_CONNECTORS = {
    # The landing database. Deliberately NOT a Django DATABASES alias — it is
    # reached only through dlt, which makes routing an ORM model there
    # structurally impossible. In production this is your MySQL DSN, e.g.
    # "mysql+pymysql://user:pw@host:3306/connectors_landing".
    "LANDING_URL": f"sqlite:///{BASE_DIR / 'landing.db'}",
    "LANDING_DATASET": "connectors_landing",
    "PIPELINES_DIR": str(BASE_DIR / ".dlt-pipelines"),
    # Sources are registered by dotted path and resolved lazily, so a source
    # module (and its dependencies) is imported only when a Run needs it.
    "SOURCES": {
        "memory": "django_connectors.sources.memory.MemorySource",
        "rest": "django_connectors.sources.rest.RestSource",
    },
    "AUTH_BACKENDS": {},
}
