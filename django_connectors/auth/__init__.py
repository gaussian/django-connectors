"""Auth backends: how a Connection turns into a usable credential.

Backends are named in ``DJANGO_CONNECTORS["AUTH_BACKENDS"]`` and resolved
lazily, so importing this package costs nothing but Django and the standard
library — the provider SDK behind a backend is imported when a Run first needs
that backend, not when Django starts.

The three shipped backends differ in *who owns the credential*, which is the
only distinction that changes behaviour:

:class:`StaticCredentialsBackend`
    we own it. Stored through the SecretStore, deleted on revoke.
:class:`SecretReferenceBackend`
    an external secret manager owns it. Read-only; revoke is local.
:class:`AllauthBackend`
    django-allauth owns it. Read-only; a missing ``SocialToken`` *is* the
    revocation signal.
"""

from django_connectors.auth.allauth import AllauthBackend
from django_connectors.auth.base import AuthBackend, Credentials
from django_connectors.auth.secret import SecretReferenceBackend
from django_connectors.auth.static import StaticCredentialsBackend

__all__ = [
    "AllauthBackend",
    "AuthBackend",
    "Credentials",
    "SecretReferenceBackend",
    "StaticCredentialsBackend",
]
