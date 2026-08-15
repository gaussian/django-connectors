"""Binding configuration and validation.

Validation happens when a Binding is *edited*, not when it runs. A source whose
config is malformed, whose extra is missing, or whose landing table name would
not survive dlt's naming convention should be rejected in front of the person
editing it — not surface as a PipelineStepFailed inside a customer's Run.

Two entry points, because they are wanted in different shapes:

:func:`validate_binding`
    raises, and returns the resolved SourceDefinition. For code that is about
    to use the Binding.
:func:`binding_field_errors`
    returns ``{field: message}`` and raises nothing. For a form or a serializer,
    which wants every problem at once and wants each next to the input that
    caused it.

Both are wired into ``Binding.clean()`` and ``BindingSerializer.validate()``,
and deliberately **not** into ``Binding.save()``. A Binding whose source key
stops being registered — a deploy that renames one, an extra that gets
uninstalled — must still be savable, or an operator cannot even disable the
row that is failing. Validation belongs where a human is editing; ``save()``
is also how automated recovery writes ``enabled=False``.

Nothing here resolves the registry at import time. The registry is lazy by
design (see :mod:`django_connectors.registry`), and ``Binding`` is imported
while Django is still building the app registry.
"""

from django_connectors.exceptions import ConfigurationError, ConnectorError
from django_connectors.landing.naming import landing_table_name
from django_connectors.registry import sources

#: Stand-in used while checking the landing table names of a Binding that has
#: not been saved yet, so that the 63-character limit is enforced in the form
#: rather than deferred to the first Run. Every real key is exactly this wide —
#: ``Binding.generate_landing_key`` returns ``"b"`` plus 10 hex characters — and
#: a test asserts the two stay the same length.
UNSAVED_LANDING_KEY = "b" + "0" * 10


def validate_binding(binding):
    """Raise ``ConfigurationError`` if this Binding could not run.

    Returns the resolved SourceDefinition so callers do not resolve it twice.
    """
    _require_registered_source(binding)
    source_definition = sources.get(binding.source)
    source_definition.validate_config(binding.config or {})
    validate_landing_names(binding, source_definition)
    return source_definition


def binding_field_errors(binding):
    """Everything wrong with `binding`, as ``{field name: message}``.

    Empty when the Binding could run. Never raises for a configuration
    problem — that is the whole point — but does let an unexpected exception
    out of a third-party ``validate_config``: a source raising ``TypeError`` is
    a bug in the source, and turning it into a polite form error would hide it.

    Each stage after the first is still attempted when an earlier one fails, so
    an operator sees every problem in one pass rather than one per save.
    """
    errors = {}

    try:
        _require_registered_source(binding)
    except ConnectorError as exc:
        # Nothing downstream can be checked without a source definition.
        return {"source": str(exc)}

    try:
        source_definition = sources.get(binding.source)
    except Exception as exc:
        # ImproperlyConfigured: the key is registered but its dotted path does
        # not import or does not instantiate. Still the source field's fault.
        return {"source": str(exc)}

    try:
        source_definition.validate_config(binding.config or {})
    except ConnectorError as exc:
        errors["config"] = str(exc)

    try:
        validate_landing_names(binding, source_definition)
    except ConnectorError as exc:
        errors["resources"] = str(exc)

    return errors


def validate_landing_names(binding, source_definition=None):
    """Check every landing table name this Binding would produce.

    Names are checked here rather than at run time because the failure is
    structural: a resource name that pushes the identifier past 63 characters
    can only be fixed by renaming, never by retrying.
    """
    resources = list(binding.resources or [])
    if not resources:
        # Nothing selected means "all resources"; validate what we can now and
        # the rest on first run, when the source's resource names are known.
        resources = ["resource"]
    landing_key = binding.landing_key or UNSAVED_LANDING_KEY
    return [
        landing_table_name(binding.source, resource, landing_key)
        for resource in resources
    ]


def _require_registered_source(binding):
    if binding.source not in sources:
        raise ConfigurationError(
            f"binding names source {binding.source!r}, which is not registered "
            f"in DJANGO_CONNECTORS['SOURCES']. Registered: {sources.keys()}"
        )
