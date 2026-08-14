"""Exception hierarchy.

Every failure a Run or ProjectionRun records is one of these. The class name is
persisted verbatim in ``Run.error_type`` / ``ProjectionRun.error_type``, so the
hierarchy is part of the public API: renaming a class changes stored data.

Anything raised out of third-party code (dlt, provider SDKs, DB drivers) is
wrapped in the closest of these before it reaches persistence, and its message
is put through :func:`django_connectors.errors.scrub` first.
"""


class ConnectorError(Exception):
    """Root of every error this package raises deliberately."""


class ConfigurationError(ConnectorError):
    """The host's configuration is wrong — settings, dotted paths, extras."""


class AuthError(ConnectorError):
    """Credentials could not be obtained or are not usable."""


class CredentialsRevoked(AuthError):
    """The provider has revoked access.

    The runner treats this as terminal: it sets ``Connection.status="revoked"``
    and blocks dependent Bindings rather than retrying, because retrying a
    revoked credential burns provider quota and can trigger rate limiting.
    """


class CredentialsExpired(AuthError):
    """Credentials expired and could not be refreshed automatically."""


class SourceError(ConnectorError):
    """A SourceDefinition could not be built or failed while extracting."""


class LandingError(ConnectorError):
    """Something went wrong between the source and the landing tables."""


class LandingSchemaError(LandingError):
    """The landing schema is missing, ambiguous, or not yet created."""


class LandingWedgedError(LandingError):
    """The pipeline holds a pending load package it can never load.

    dlt retries a failing load job forever; a single un-loadable row (a >64KB
    string against MySQL's ``TEXT``, say) leaves ``has_pending_data`` true and
    makes every later run raise. This needs operator action, not a retry.
    """


class ProjectionError(ConnectorError):
    """A Projection could not be validated or executed."""


class MappingValidationError(ProjectionError):
    """The customer's mapping is not valid against the source or the target."""


class CastError(ProjectionError):
    """A value could not be cast to the type the target field declares."""


class SchemaDriftError(ProjectionError):
    """The landing schema changed in a way the mapping can no longer satisfy."""


class TargetWriteError(ConnectorError):
    """The host's target writer raised.

    The batch is treated as not applied, and the whole ProjectionRun is retried
    from the start — there is no mid-run checkpoint — so host writers must be
    idempotent per identity.
    """


class LockNotAcquired(ConnectorError):
    """Another worker holds this Binding's lease.

    Not an error condition in itself: the caller records a skipped Run rather
    than failing, because concurrent execution of one Binding is what the lock
    exists to prevent.
    """
