"""Schema and sample access for the mapping UI.

A customer cannot author a mapping without seeing what landed: column names,
types, and a few real rows. That belongs here rather than in every host app,
which would otherwise each reverse-engineer the landing tables.

Everything reads through :mod:`django_connectors.landing.access`, so the tenant
scope is applied in one place. These endpoints return customer data, so callers
must be owner-scoped by the host, and the row caps below are server-side.
"""

import logging

from django_connectors.conf import conf
from django_connectors.enums import ProjectionStatus
from django_connectors.exceptions import LandingSchemaError
from django_connectors.landing import access, schema
from django_connectors.models import Projection
from django_connectors.registry import sources

logger = logging.getLogger(__name__)


def get_landing_schema(binding):
    """What landed for this Binding. Never raises for a Binding that has not run."""
    return schema.landing_schema(binding)


def list_resources(binding):
    return sorted(get_landing_schema(binding)["resources"])


def sample_resource(binding, resource, *, limit=None):
    """A bounded sample of landed rows, with internal columns removed.

    Internal columns are stripped because they are ours, not the customer's:
    exposing ``_connector_binding_id`` invites a mapping that reads it, and the
    tenant column is not a source field.
    """
    limit = min(limit or conf.SAMPLE_MAX_ROWS, conf.SAMPLE_MAX_ROWS)
    rows = access.sample_rows(binding, resource, limit=limit)
    from django_connectors.landing.naming import is_internal_column

    return [
        {key: value for key, value in row.items() if not is_internal_column(key)}
        for row in rows
    ]


def discover_remote(connection, *, query=None, source_key=None, credentials=None):
    """Ask the provider what is available to synchronize.

    Distinct from :func:`get_landing_schema`, which describes what has already
    landed. This one talks to the provider and is the expensive call.
    """
    key = source_key or connection.provider
    if key not in sources:
        raise LandingSchemaError(
            f"no registered source {key!r} to discover with; registered: "
            f"{sources.keys()}"
        )
    definition = sources.get(key)
    if credentials is None and connection.auth_backend:
        from django_connectors.registry import auth_backends

        credentials = auth_backends.get(connection.auth_backend).get_credentials(
            connection
        )
    return definition.discover(
        connection=connection, credentials=credentials, query=query
    )


def refresh_projection_drift(binding):
    """Re-grade every Projection on `binding` against the current landed schema.

    Called after a successful Run. A mapping that has silently started reading
    NULLs must not keep reporting itself active.
    """
    updated = []
    for projection in Projection.objects.filter(binding=binding):
        columns = schema.columns_for(binding, projection.resource)
        if not columns:
            continue
        status, messages = schema.detect_drift(projection, columns=columns)
        if status == projection.status and not messages:
            continue
        # Never silently promote a projection the customer disabled or that a
        # validator marked invalid for a reason drift cannot see.
        if projection.status == ProjectionStatus.DRAFT:
            continue
        projection.status = status
        projection.last_error = "; ".join(messages)
        projection.save(update_fields=["status", "last_error"])
        updated.append(projection)
    return updated
