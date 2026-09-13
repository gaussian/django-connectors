"""The SourceDefinition contract, as checks instead of prose.

Every source this package ships — and every source a host registers — must
satisfy the same contract, because the landing layer relies on it: an explicit
write disposition, a cursor declared as kwargs rather than constructed, a
configuration validator that fails with one exception type. Each rule here
names the failure it prevents.

The checks are functions rather than a pytest module so a host can run them
against its own sources::

    from django_connectors.sources.conformance import assert_source_conforms
    assert_source_conforms(MySource())

Nothing here imports dlt at module scope.
"""

from types import SimpleNamespace

from django_connectors.exceptions import ConfigurationError
from django_connectors.sources.base import SourceDefinition

#: Values a host could store in ``Binding.config`` (a JSON field) that a source
#: must refuse with ``ConfigurationError`` and nothing else. A bare
#: ``AttributeError`` from ``"x".get`` is a 500 in the Binding editor.
GARBAGE_CONFIGS = (
    None,
    {},
    [],
    "not an object",
    7,
    {"resources": "not a mapping"},
    {"resources": {"x": "not a mapping"}},
    {"resources": {"x": {}}},
    {"resources": {}},
)

WEBHOOK_ADAPTER_METHODS = ("create", "renew", "delete", "verify", "parse")


def static_problems(definition):
    """Every contract violation in `definition` that needs no credentials.

    Returns a list of messages; empty means conformant. Collects rather than
    raising on the first, so a new source sees everything at once.
    """
    problems = []
    if not isinstance(definition, SourceDefinition):
        return [
            f"{type(definition).__name__} is not a SourceDefinition subclass "
            f"instance; the registry instantiates classes and expects instances"
        ]

    problems += _identity_problems(definition)
    problems += _flag_problems(definition)
    problems += _validate_config_problems(definition)
    problems += _incremental_problems(definition)
    problems += _webhook_problems(definition)
    problems += _discover_problems(definition)
    return problems


def assert_source_conforms(definition):
    """Raise ``AssertionError`` listing every violation, or return None."""
    problems = static_problems(definition)
    if problems:
        name = getattr(definition, "key", "") or type(definition).__name__
        raise AssertionError(
            f"source {name!r} violates the SourceDefinition contract:\n  - "
            + "\n  - ".join(problems)
        )


# --- individual rules --------------------------------------------------------


def _identity_problems(definition):
    problems = []
    key = definition.key
    if not isinstance(key, str) or not key:
        problems.append("`key` must be a non-empty string; it is the registry key")
    else:
        # The key is part of every landing table name for the source, so it must
        # survive dlt's naming convention unchanged — otherwise the table the
        # library computes is not the table dlt creates.
        from django_connectors.landing.naming import normalize_identifier

        try:
            normalized = normalize_identifier(key)
        except ValueError:
            normalized = ""
        if normalized != key:
            problems.append(
                f"`key` {key!r} is rewritten by dlt's naming convention to "
                f"{normalized!r}; use lowercase letters, digits and underscores"
            )
    if not isinstance(definition.provider, str) or not definition.provider:
        problems.append("`provider` must be a non-empty string")
    return problems


def _flag_problems(definition):
    problems = []
    # `is` checks, not truthiness: `emits_tombstones = 1` would pass the API's
    # honesty check and mislead every customer reading it.
    if (
        definition.emits_tombstones is not True
        and definition.emits_tombstones is not False
    ):
        problems.append(
            f"`emits_tombstones` must be True or False, got "
            f"{definition.emits_tombstones!r}; the API reports it to customers"
        )
    extras = definition.required_extras
    if not isinstance(extras, dict) or not all(
        isinstance(module, str) and isinstance(extra, str)
        for module, extra in extras.items()
    ):
        problems.append(
            "`required_extras` must be a dict of {module_name: extra_name} "
            "strings; the W005 system check reads it"
        )
    backends = definition.supported_auth_backends
    if not isinstance(backends, list | tuple) or not all(
        isinstance(item, str) for item in backends
    ):
        problems.append(
            "`supported_auth_backends` must be a tuple or list of backend keys"
        )
    return problems


def _validate_config_problems(definition):
    problems = []
    for garbage in GARBAGE_CONFIGS:
        try:
            result = definition.validate_config(garbage)
        except ConfigurationError:
            continue
        except Exception as exc:
            problems.append(
                f"validate_config({garbage!r}) raised "
                f"{type(exc).__name__}: {exc}; it may raise only "
                f"ConfigurationError, or the Binding editor shows a 500 "
                f"instead of a form error"
            )
            continue
        if result is not None:
            problems.append(
                f"validate_config({garbage!r}) returned {result!r}; it must "
                f"return None on success"
            )
    return problems


def _incremental_problems(definition):
    """The library builds every Incremental. A source that constructs one has
    already lost the two settings that stop records being dropped."""
    binding = SimpleNamespace(config={}, source=definition.key, resources=[])
    try:
        result = definition.incremental_for("resource", binding)
    except Exception as exc:
        return [
            f"incremental_for() raised {type(exc).__name__} on an empty "
            f"config: {exc}; it must return None when no cursor is declared"
        ]
    if result is None:
        return []
    if isinstance(result, dict):
        if "cursor_path" not in result:
            return ["incremental_for() returned kwargs without 'cursor_path'"]
        return []
    if type(result).__module__.startswith("dlt"):
        return [
            f"incremental_for() returned a {type(result).__name__}; sources "
            f"declare cursor kwargs and never construct the Incremental, or "
            f"primary_key=() and on_cursor_value_missing='include' are lost"
        ]
    return [
        f"incremental_for() must return None or a dict, got {type(result).__name__}"
    ]


def _webhook_problems(definition):
    adapter = definition.webhook
    if adapter is None:
        return []
    missing = [
        name
        for name in WEBHOOK_ADAPTER_METHODS
        if not callable(getattr(adapter, name, None))
    ]
    if missing:
        return [f"`webhook` adapter lacks {missing}; see webhooks.base.WebhookAdapter"]
    return []


DISCOVER_KWARGS = ("connection", "credentials", "query", "path", "cursor", "limit")


def _discover_problems(definition):
    """``discover`` takes the browsing keywords, or a UI cannot page or descend."""
    import inspect

    try:
        parameters = inspect.signature(definition.discover).parameters
    except (TypeError, ValueError):
        return ["`discover` is not introspectable"]
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
        return []
    missing = [name for name in DISCOVER_KWARGS if name not in parameters]
    if missing:
        return [
            f"`discover` does not accept {missing}; every source takes "
            f"{list(DISCOVER_KWARGS)} as keyword arguments so one UI can browse "
            f"them all"
        ]
    return []
