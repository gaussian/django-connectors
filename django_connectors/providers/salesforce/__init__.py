"""Salesforce: OAuth2 JWT-bearer credentials plus a config-driven SOQL source.

Register the two halves independently — a host that already holds Salesforce
sessions from somewhere else needs only the source::

    DJANGO_CONNECTORS = {
        "AUTH_BACKENDS": {
            "salesforce": (
                "django_connectors.providers.salesforce.SalesforceBackend"
            ),
        },
        "SOURCES": {
            "salesforce": "django_connectors.providers.salesforce.SalesforceSource",
        },
    }

Importing this module imports both submodules but nothing heavier: no ``dlt``,
no ``cryptography``, no ``requests``. Each is imported inside the function that
needs it, so a host that never runs a Salesforce Binding never pays for any of
them, and a missing one is reported as a ConfigurationError naming the extra to
install rather than as an ImportError inside a customer's Run.

Both modules carry an explicit "Unverified against a live provider" section:
this connector was written and tested entirely against in-process HTTP servers
replaying Salesforce's documented payload shapes, with no org, sandbox or
credential available.
"""

from django_connectors.providers.salesforce.auth import SalesforceBackend
from django_connectors.providers.salesforce.source import SalesforceSource

__all__ = ["SalesforceBackend", "SalesforceSource"]
