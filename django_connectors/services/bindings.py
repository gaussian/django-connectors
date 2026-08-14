"""Binding configuration and validation.

Validation happens when a Binding is saved, not when it runs. A source whose
config is malformed, whose extra is missing, or whose landing table name would
not survive dlt's naming convention should be rejected in front of the person
editing it — not surface as a PipelineStepFailed inside a customer's Run.
"""

from django_connectors.exceptions import ConfigurationError
from django_connectors.landing.naming import landing_table_name
from django_connectors.registry import sources


def validate_binding(binding):
    """Raise ``ConfigurationError`` if this Binding could not run.

    Returns the resolved SourceDefinition so callers do not resolve it twice.
    """
    if binding.source not in sources:
        raise ConfigurationError(
            f"binding names source {binding.source!r}, which is not registered "
            f"in DJANGO_CONNECTORS['SOURCES']. Registered: {sources.keys()}"
        )
    source_definition = sources.get(binding.source)
    source_definition.validate_config(binding.config or {})
    validate_landing_names(binding, source_definition)
    return source_definition


def validate_landing_names(binding, source_definition=None):
    """Check every landing table name this Binding would produce.

    Names are checked here rather than at run time because the failure is
    structural: a resource name that pushes the identifier past 64 characters
    can only be fixed by renaming, never by retrying.
    """
    resources = list(binding.resources or [])
    if not resources:
        # Nothing selected means "all resources"; validate what we can now and
        # the rest on first run, when the source's resource names are known.
        resources = ["resource"]
    return [
        landing_table_name(binding.source, resource, binding.landing_key)
        for resource in resources
    ]
