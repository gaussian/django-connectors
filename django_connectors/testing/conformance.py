"""The connector contract, as a suite instead of as prose.

Every SourceDefinition must pass this — the ones this library ships and the ones
a host registers in ``DJANGO_CONNECTORS["SOURCES"]``. It needs no credentials
and reaches no network beyond whatever the caller's own fixture serves, so it is
cheap enough to run on every pull request.

Each check exists because the mistake it catches is **silent**. That is the bar
for adding one: a rule whose violation produces an error is not worth a test
here, because the error is the test.

======================================  =====================================
Check                                   What is silent without it
======================================  =====================================
``build_source`` returns a ``DltSource``  a source returning ``None`` or a bare
                                        resource fails deep inside dlt with a
                                        message that names dlt, not the source
every resource states a write            ``dlt.resource()`` defaults the hint
disposition that survives                to ``"append"``, so an omission
instrumentation                          appends a fresh copy of every
                                        re-fetched record on every run, with
                                        the merge key correctly configured and
                                        no error raised
``incremental_for`` returns kwargs       dlt strips the incremental from a
and never an ``Incremental``            *bound* resource's signature, so a
                                        source that builds its own is beyond
                                        the library's reach — and the two
                                        settings the library forces (empty
                                        dedup key, include-missing-cursor)
                                        each drop records with no error
no module-scope ``import dlt``          0.6s on every ``manage.py``, worker
                                        fork and test collection, paid by
                                        hosts that never run a pipeline
``validate_config`` rejects garbage      a bad config becomes a failed Run in
with ``ConfigurationError``             front of a customer instead of a form
                                        error in front of its author
``required_extras`` is declared          the W005 system check has nothing to
                                        report, and a missing optional
                                        dependency surfaces mid-Run
``emits_tombstones`` is honest           a tombstone carrying more than the
                                        merge key nulls those columns on the
                                        delete path (``delete-insert`` merge
                                        replaces the whole row), so any target
                                        identity sourced from them is None
======================================  =====================================

Usage — the three phases are separate because they need different things::

    from django_connectors.testing import (
        check_definition, check_built_source, check_landing_invariants,
    )

    # No binding, no credentials, no network.
    assert check_definition(MySource(), invalid_configs=[{}]) == []

    # Needs a Binding and whatever credentials the source expects.
    assert check_built_source(MySource(), binding=binding, credentials=token) == []

    # After a successful Run.
    assert check_landing_invariants(binding) == []

Every function **returns a list of strings** rather than raising or asserting.
A conformance run should report every violation at once — a suite that stops at
the first one takes as many iterations to fix as there are problems — and
returning data keeps the module free of a pytest dependency, which matters
because it ships in the wheel.
"""

import ast
import inspect
import pathlib

from django_connectors.exceptions import ConfigurationError
from django_connectors.landing.naming import (
    BINDING_ID_COLUMN,
    DELETED_COLUMN,
    RUN_ID_COLUMN,
    landing_table_name,
)

#: Dispositions dlt understands. Anything else is a typo that dlt turns into an
#: unhelpful KeyError deep in the normalizer.
WRITE_DISPOSITIONS = frozenset({"merge", "append", "replace", "skip"})


def check_source_conformance(
    definition, *, binding=None, credentials=None, run=None, invalid_configs=()
):
    """Run every applicable phase and return the combined failures.

    ``binding`` is optional: without one, only :func:`check_definition` runs,
    because there is nothing to build a source against.
    """
    failures = check_definition(definition, invalid_configs=invalid_configs)
    if binding is not None:
        failures += check_built_source(
            definition, binding=binding, credentials=credentials, run=run
        )
    return failures


# --- phase 1: the class alone ----------------------------------------------


def check_definition(definition, *, invalid_configs=()):
    """Checks that need nothing but the SourceDefinition itself."""
    label = _label(definition)
    failures = []

    key = getattr(definition, "key", "")
    if not isinstance(key, str) or not key:
        failures.append(f"{label}: `key` must be a non-empty string, got {key!r}")

    extras = getattr(definition, "required_extras", None)
    if not isinstance(extras, dict):
        failures.append(
            f"{label}: `required_extras` must be a dict of "
            f"{{module_name: extra_name}} — an empty one is a declaration too, "
            f"and the W005 system check reads it. Got {type(extras).__name__}."
        )
    else:
        for module_name, extra in extras.items():
            if not isinstance(module_name, str) or not isinstance(extra, str):
                failures.append(
                    f"{label}: `required_extras` entry {module_name!r}: {extra!r} "
                    f"is not {{str: str}}; W005 renders both into its hint."
                )

    if not isinstance(getattr(definition, "emits_tombstones", None), bool):
        failures.append(
            f"{label}: `emits_tombstones` must be a bool. The API reports it "
            f"verbatim, and it is how a host learns that deletions are "
            f"genuinely unsupported rather than merely unimplemented."
        )

    backends = getattr(definition, "supported_auth_backends", None)
    if not isinstance(backends, tuple | list) or not all(
        isinstance(item, str) for item in backends
    ):
        failures.append(
            f"{label}: `supported_auth_backends` must be a sequence of registry "
            f"keys (empty means any), got {backends!r}"
        )

    failures += _check_module_imports(definition)
    failures += _check_resource_calls_state_a_disposition(definition)
    failures += _check_rejects_garbage(definition, invalid_configs)
    return failures


def _check_module_imports(definition):
    """No module-scope ``import dlt`` in the module that defines the source.

    Sources are resolved lazily precisely so this cost is not paid by hosts
    that never run a pipeline; a module-scope import undoes that for everyone
    the moment the registry resolves anything.
    """
    label = _label(definition)
    failures = []
    for path, tree in _module_asts(definition):
        for node in tree.body:
            imports_dlt = (
                isinstance(node, ast.Import)
                and any(alias.name.split(".")[0] == "dlt" for alias in node.names)
            ) or (
                isinstance(node, ast.ImportFrom)
                and node.module
                and node.module.split(".")[0] == "dlt"
            )
            if imports_dlt:
                failures.append(
                    f"{label}: module-scope dlt import in {path}:{node.lineno}"
                )
    return failures


def _check_resource_calls_state_a_disposition(definition):
    """Every ``dlt.resource(...)`` in the module must pass write_disposition.

    This is a source-code check rather than a behavioural one because the
    behaviour is *unobservable*: ``dlt.resource()`` defaults the hint to
    ``"append"``, not to ``None``, so a resource that omits it is byte-identical
    to one that states ``"append"`` deliberately. The only place the difference
    still exists is the call site.
    """
    label = _label(definition)
    failures = []
    for path, tree in _module_asts(definition):
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            if not (
                isinstance(function, ast.Attribute) and function.attr == "resource"
            ):
                continue
            if not (
                isinstance(function.value, ast.Name) and function.value.id == "dlt"
            ):
                continue
            if not any(keyword.arg == "write_disposition" for keyword in node.keywords):
                failures.append(
                    f"{label}: dlt.resource() at {path}:{node.lineno} does not "
                    f"state write_disposition. dlt defaults it to 'append', so "
                    f"an omission appends a fresh copy of every re-fetched "
                    f"record on every run — silently, and with the merge key "
                    f"correctly configured."
                )
    return failures


def _check_rejects_garbage(definition, invalid_configs):
    """``validate_config`` must refuse each supplied bad config, by raising.

    The configs are supplied by the caller rather than invented here: a source
    with no required keys legitimately accepts ``{}``, so a generic "reject an
    empty dict" would be wrong for it and vacuous for everyone else. Supplying
    at least one is the point — a source that accepts everything has no
    save-time validation, and its errors all land in a customer's Run.
    """
    label = _label(definition)
    if not invalid_configs:
        return [
            f"{label}: no invalid configs were supplied, so validate_config was "
            f"never proven to reject anything. Pass at least one config the "
            f"source must refuse."
        ]

    failures = []
    for config in invalid_configs:
        try:
            definition.validate_config(config)
        except ConfigurationError:
            continue
        except Exception as exc:
            failures.append(
                f"{label}: validate_config({config!r}) raised "
                f"{type(exc).__name__}, not ConfigurationError. The save path "
                f"catches ConnectorError and lets anything else through as a "
                f"500."
            )
        else:
            failures.append(
                f"{label}: validate_config({config!r}) accepted a config it was "
                f"given as invalid"
            )
    return failures


# --- phase 2: the built source ---------------------------------------------


def check_built_source(definition, *, binding, credentials=None, run=None):
    """Build the source and check what instrumentation will be handed.

    Nothing is extracted: dlt resources are generators, so building one issues
    no request. That is what makes this affordable on every pull request.
    """
    from dlt.extract.incremental import Incremental
    from dlt.extract.source import DltSource

    label = _label(definition)
    failures = []

    source = definition.build_source(binding=binding, credentials=credentials, run=run)
    if not isinstance(source, DltSource):
        return [
            f"{label}: build_source returned {type(source).__name__}, not a "
            f"DltSource. Call `dlt.source(...)` as a function — the decorator "
            f"takes the source name from `__name__` and cannot produce a "
            f"runtime-chosen one."
        ]

    resources = list(source.resources.values())
    if not resources:
        return [f"{label}: build_source produced a source with no resources"]

    for resource in resources:
        failures += _check_resource_hints(label, resource)
        failures += _check_incremental_kwargs(
            label, definition, resource.name, binding, Incremental
        )

    failures += _check_instrumentation_accepts(label, definition, source, binding, run)
    return failures


def _check_resource_hints(label, resource):
    declared = resource._hints.get("write_disposition")
    disposition = declared["disposition"] if isinstance(declared, dict) else declared
    failures = []
    if disposition not in WRITE_DISPOSITIONS:
        failures.append(
            f"{label}.{resource.name}: write_disposition {disposition!r} is not "
            f"one of {sorted(WRITE_DISPOSITIONS)}"
        )
    primary_key = resource._hints.get("primary_key")
    if disposition == "merge" and not primary_key:
        failures.append(
            f"{label}.{resource.name}: merge disposition with no primary_key. "
            f"Merge cannot identify the rows to replace."
        )
    if disposition == "append" and primary_key:
        failures.append(
            f"{label}.{resource.name}: primary_key with append disposition, "
            f"which dlt ignores — every run appends a second copy of every "
            f"re-fetched record. dlt defaults the hint to 'append', so this is "
            f"most likely an omitted write_disposition."
        )
    for column, hint in (resource._hints.get("columns") or {}).items():
        if isinstance(hint, dict) and hint.get("hard_delete"):
            failures.append(
                f"{label}.{resource.name}: sets dlt's hard_delete hint on "
                f"{column!r}, which physically removes the row on merge — "
                f"Projection could never observe the deletion. Emit "
                f"{DELETED_COLUMN}=True instead."
            )
    return failures


def _check_incremental_kwargs(label, definition, resource_name, binding, incremental):
    kwargs = definition.incremental_for(resource_name, binding)
    if kwargs is None:
        return []
    if isinstance(kwargs, incremental):
        return [
            f"{label}.{resource_name}: incremental_for returned an Incremental. "
            f"Return kwargs instead — the library builds it, so that the empty "
            f"dedup key and include-missing-cursor settings cannot be "
            f"forgotten. Each silently drops records."
        ]
    if not isinstance(kwargs, dict):
        return [
            f"{label}.{resource_name}: incremental_for returned "
            f"{type(kwargs).__name__}; return a dict of Incremental kwargs, or "
            f"None for a full load."
        ]
    failures = []
    if not kwargs.get("cursor_path"):
        failures.append(
            f"{label}.{resource_name}: incremental kwargs declare no "
            f"cursor_path, so there is nothing to be incremental on"
        )
    if "primary_key" in kwargs:
        failures.append(
            f"{label}.{resource_name}: incremental kwargs set primary_key. The "
            f"library forces it empty and would discard this silently — dlt's "
            f"default drops a record updated at exactly the stored cursor "
            f"value."
        )
    return failures


def _check_instrumentation_accepts(label, definition, source, binding, run):
    """The source must survive the only path by which it can reach a pipeline.

    A source that builds cleanly and is then rejected by ``instrument_source``
    is one whose every Run fails, so this is the difference between finding out
    now and finding out in production.
    """
    from django_connectors.landing.instrument import instrument_source

    try:
        instrument_source(
            source, binding=binding, run=run, source_definition=definition
        )
    except Exception as exc:
        return [
            f"{label}: instrument_source rejected the built source: "
            f"{type(exc).__name__}: {exc}"
        ]
    return []


# --- phase 3: what actually landed ------------------------------------------


def check_landing_invariants(binding, *, expected_resources=None):
    """Check the landed schema of a Binding after a successful Run.

    Three invariants, all of them things a connector can break without any
    error being raised:

    * **tenant columns** — every root record carries the binding id, the run id
      and the deleted flag. ``add_map`` decides how to call the injector by
      counting its parameters, and the wrong count writes NULL binding ids on
      every row rather than failing.
    * **no child tables** — nesting is off with no opt-out, because a child
      table carries no tenant scope and no run filter and is structurally
      unprojectable. dlt would create them silently.
    * **merge identity** — the binding id leads the merge key. Keyed on the
      remote id alone, one Binding's merge was measured deleting 10 of another
      Binding's 50 rows.
    """
    from django_connectors.landing.destination import build_pipeline

    failures = []
    pipeline = build_pipeline(binding)
    try:
        schema = pipeline.schemas.get(binding.schema_name)
    except Exception:
        schema = None
    if schema is None:
        return [f"binding {binding.id} has no landing schema; nothing landed"]

    landed = [name for name in schema.tables if not name.startswith("_dlt")]
    if not landed:
        return [f"binding {binding.id} landed no tables"]

    allowed = None
    if expected_resources is not None:
        allowed = {
            landing_table_name(binding.source, resource, binding.landing_key)
            for resource in expected_resources
        }

    for table_name in landed:
        table = schema.tables[table_name]
        columns = table.get("columns") or {}

        if allowed is not None and table_name not in allowed:
            failures.append(
                f"{table_name}: not a landing table for any selected resource "
                f"({sorted(allowed)}). A nested child table carries no tenant "
                f"scope and no run filter, and is unprojectable."
            )

        for required in (BINDING_ID_COLUMN, RUN_ID_COLUMN, DELETED_COLUMN):
            if required not in columns:
                failures.append(f"{table_name}: missing {required}")

        if table.get("parent"):
            failures.append(
                f"{table_name}: is a nested child of {table['parent']!r}; "
                f"max_table_nesting is 0 with no opt-out"
            )

        disposition = table.get("write_disposition")
        if disposition == "merge":
            key_columns = [
                name for name, column in columns.items() if column.get("primary_key")
            ]
            if not key_columns or key_columns[0] != BINDING_ID_COLUMN:
                failures.append(
                    f"{table_name}: merge key is {key_columns}, which does not "
                    f"lead with {BINDING_ID_COLUMN}. One Binding's merge would "
                    f"delete another's rows."
                )

    return failures


def check_tombstone_shape(record, *, merge_key_columns):
    """A tombstone must carry the merge key, the deleted flag, and nothing else.

    ``delete-insert`` merge replaces the whole row, so every column a tombstone
    omits lands as NULL. A tombstone carrying *extra* columns is not the
    problem — a tombstone whose identity is *incomplete* is, and so is one
    that implies a target identity field can be sourced from outside the merge
    key, because that field is None on the delete path.
    """
    failures = []
    if not record.get(DELETED_COLUMN):
        failures.append(
            f"tombstone {record!r} does not set {DELETED_COLUMN}; without it the "
            f"deletion is indistinguishable from an update"
        )
    missing = [column for column in merge_key_columns if column not in record]
    if missing:
        failures.append(
            f"tombstone {record!r} omits merge key column(s) {missing}, so the "
            f"merge cannot identify the row it deletes"
        )
    extra = sorted(
        set(record) - set(merge_key_columns) - {DELETED_COLUMN} - {BINDING_ID_COLUMN}
    )
    if extra:
        failures.append(
            f"tombstone {record!r} carries non-key column(s) {extra}. They are "
            f"the only columns that survive the delete-insert replacement, "
            f"which makes the landed row a partial record rather than an "
            f"unambiguous deletion."
        )
    return failures


# --- helpers ----------------------------------------------------------------


def _label(definition):
    return getattr(definition, "key", None) or type(definition).__name__


def _module_asts(definition):
    """``[(path, tree)]`` for every module in the definition's ancestry.

    The whole MRO, not just the defining module: a host pointing a shipped
    connector at a sandbox does it by subclassing (that is the documented way to
    override a base URL), and checking only the subclass's own module would then
    check an empty file and report conformance.
    """
    parsed = {}
    for klass in type(definition).__mro__:
        if klass is object:
            continue
        try:
            path = inspect.getsourcefile(klass)
        except TypeError:
            continue
        if not path or path in parsed:
            continue
        try:
            parsed[path] = ast.parse(pathlib.Path(path).read_text())
        except (OSError, SyntaxError):
            continue
    return list(parsed.items())
