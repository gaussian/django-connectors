"""System checks.

Misconfiguration is this library's dominant failure mode — the integration
surface is host-supplied dotted paths, a DSN, and registry keys — so every way
of getting it wrong should surface at ``manage.py check``, by name, rather than
inside a customer's Run at 3am.

These checks open **no database connection**. Checks that need the database are
tagged ``Tags.database`` and no-op when Django passes no databases, so
``manage.py check`` stays connection-free.
"""

from urllib.parse import urlsplit

from django.apps import apps
from django.core.checks import Error, Tags, Warning, register

from django_connectors.conf import SETTING_NAME, conf


@register(Tags.compatibility)
def check_contenttypes_installed(app_configs, **kwargs):
    """django.contrib.contenttypes is a hard requirement.

    Connection.owner is a GenericForeignKey. Without contenttypes Django dies
    during setup with a RuntimeError that never names this library, so say it
    plainly here for the case where the check does get to run.
    """
    if apps.is_installed("django.contrib.contenttypes"):
        return []
    return [
        Error(
            "django.contrib.contenttypes must be in INSTALLED_APPS.",
            hint=(
                "django_connectors.Connection uses a GenericForeignKey so that "
                "the host can own Connections with any model."
            ),
            id="django_connectors.E001",
        )
    ]


@register(Tags.compatibility)
def check_landing_url(app_configs, **kwargs):
    """LANDING_URL must be absent-but-warned, or present and parseable."""
    url = conf.LANDING_URL
    if not url:
        return [
            Warning(
                f"{SETTING_NAME}['LANDING_URL'] is not set; no Binding can run.",
                hint=(
                    "Set it to a SQLAlchemy DSN for the landing database, e.g. "
                    "'mysql+pymysql://user:pw@host:3306/connectors_landing'. "
                    "It is not a Django DATABASES alias."
                ),
                id="django_connectors.W002",
            )
        ]
    if not isinstance(url, str):
        return [
            Error(
                f"{SETTING_NAME}['LANDING_URL'] must be a string, "
                f"got {type(url).__name__}.",
                id="django_connectors.E002",
            )
        ]
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        return [
            Error(
                f"{SETTING_NAME}['LANDING_URL'] could not be parsed: {exc}",
                id="django_connectors.E002",
            )
        ]
    if not parts.scheme or not parts.path.strip("/"):
        return [
            Error(
                f"{SETTING_NAME}['LANDING_URL'] must be a SQLAlchemy DSN "
                f"including a database name, e.g. "
                f"'mysql+pymysql://user:pw@host:3306/connectors_landing'.",
                id="django_connectors.E002",
            )
        ]
    return []


@register(Tags.compatibility)
def check_registry_paths_import(app_configs, **kwargs):
    """Every configured dotted path must import.

    Resolving the whole registry here is safe and deliberate: it is the one
    moment where paying the import cost buys something, and a path that only
    fails at Run time is a path that fails in front of a customer.
    """
    from django_connectors.registry import auth_backends, sources

    errors = []
    for registry in (sources, auth_backends):
        try:
            keys = registry.keys()
        except Exception as exc:
            errors.append(
                Error(
                    f"{SETTING_NAME}['{registry.setting_key}'] is invalid: {exc}",
                    id="django_connectors.E003",
                )
            )
            continue
        for key in keys:
            try:
                registry.get(key)
            except Exception as exc:
                errors.append(
                    Error(
                        f"{registry.label} {key!r} could not be loaded: {exc}",
                        hint=(
                            "Check the dotted path, and that any extra it needs "
                            "is installed (e.g. `pip install "
                            "'django-connectors[microsoft]'`)."
                        ),
                        id="django_connectors.E003",
                    )
                )
    return errors


@register(Tags.compatibility)
def check_registry_extras_available(app_configs, **kwargs):
    """Warn when a loaded backend declares an extra that is not installed.

    Distinct from E003 because a backend can import fine and still be unusable:
    dlt's filesystem readers, for one, import lazily, so a missing pandas
    surfaces as a failure inside a customer's Run rather than at startup.
    """
    from django_connectors.registry import auth_backends, sources

    warnings = []
    candidates = []

    for registry in (sources, auth_backends):
        try:
            resolved = registry.all()
        except Exception:
            continue
        for key, obj in resolved.items():
            candidates.append((registry.label, key, obj))
            # A source's webhook adapter carries its own dependencies and is
            # reachable through no registry of its own.
            adapter = getattr(obj, "webhook", None)
            if adapter is not None:
                candidates.append((f"{registry.label} webhook adapter", key, adapter))

    # The secret store is configured by a single dotted path rather than a
    # registry, so it would otherwise never be inspected — and it is the one
    # component whose missing extra silently blocks every credential write.
    try:
        from django_connectors.secrets import get_secret_store

        candidates.append(("secret store", conf.SECRET_STORE, get_secret_store()))
    except Exception:
        pass

    for label, key, obj in candidates:
        for module_name, extra in getattr(obj, "required_extras", {}).items():
            if not _module_available(module_name):
                warnings.append(
                    Warning(
                        f"{label} {key!r} needs {module_name!r}, "
                        f"which is not installed.",
                        hint=f"pip install 'django-connectors[{extra}]'",
                        id="django_connectors.W005",
                    )
                )
    return warnings


def _module_available(module_name: str) -> bool:
    from importlib.util import find_spec

    try:
        return find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False
