"""Microsoft 365 connectors: Entra app-only auth, drive files, Excel workbooks.

Nothing here is active until a host names it in ``DJANGO_CONNECTORS``::

    DJANGO_CONNECTORS = {
        "AUTH_BACKENDS": {
            "entra": "django_connectors.providers.microsoft.auth.EntraBackend",
        },
        "SOURCES": {
            "entra_files": (
                "django_connectors.providers.microsoft.files.EntraFilesSource"
            ),
            "entra_excel": (
                "django_connectors.providers.microsoft.excel.EntraExcelSource"
            ),
        },
    }

Both registries resolve their dotted paths lazily, so importing this package
costs nothing until a Connection or a Binding actually names one of these.

The re-exports below are for hosts subclassing these (to lift a guard, or to
supply azure-identity options); the registry itself always takes the full dotted
path, so that ``DJANGO_CONNECTORS`` says exactly which class is in use.

Everything in this package needs ``pip install 'django-connectors[microsoft]'``
(azure-identity for token acquisition, openpyxl for workbook parsing), and each
module reports a missing one as a ConfigurationError naming the extra rather
than as an ImportError inside a customer's Run.

**Unverified against a live provider.** These connectors were written against
Microsoft's published API behaviour and tested only against in-process fakes:
there was no tenant, no app registration and no network available when they were
built. Each module's docstring lists what specifically has not been exercised.
Treat the first run against a real tenant as the real test.
"""

from django_connectors.providers.microsoft.auth import EntraBackend
from django_connectors.providers.microsoft.excel import EntraExcelSource
from django_connectors.providers.microsoft.files import EntraFilesSource

__all__ = ["EntraBackend", "EntraExcelSource", "EntraFilesSource"]
