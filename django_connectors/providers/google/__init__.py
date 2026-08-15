"""Google Workspace connectors: Gmail, Google Sheets, and the auth backend.

Register what you need, by dotted path::

    DJANGO_CONNECTORS = {
        "AUTH_BACKENDS": {
            "google_workspace":
                "django_connectors.providers.google.auth.GoogleWorkspaceBackend",
        },
        "SOURCES": {
            "gmail": "django_connectors.providers.google.gmail.GmailSource",
            "google_sheets":
                "django_connectors.providers.google.sheets.GoogleSheetsSource",
        },
    }

Deliberately empty of imports. Naming a source in ``SOURCES`` imports this
package, so anything re-exported here would be imported by every host that
registers *any* Google connector — including the ones they did not register.

**Division of labour.** ``google-auth`` (the ``google`` extra) is used for
credential acquisition only: it signs the service-account JWT and performs the
token exchange, which is the one part of this integration that must not be
hand-rolled. Every data request goes through ``dlt.sources.helpers.rest_client``
instead of ``google-api-python-client``, because dlt resources are synchronous
generators and the client libraries are heavyweight, cache discovery documents,
and are awkward to intercept in a test.

**Unverified against a live provider.** These connectors were built and tested
entirely against mocked HTTP. Not exercised anywhere: real OAuth/domain-wide
delegation consent, a real Gmail ``historyId`` expiring after ~a week, real
quota throttling, and Sheets/Drive responses for spreadsheets large enough to
paginate. The request shapes, error mapping and state handling follow Google's
published API contracts; treat behaviour against a real tenant as unproven.
"""
