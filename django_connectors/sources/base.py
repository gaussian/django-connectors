"""The SourceDefinition contract.

A SourceDefinition turns a Binding's stored configuration into a dlt source. It
is the library's documented extension point: hosts register their own via
``DJANGO_CONNECTORS["SOURCES"]``.

Implementations must build resources by *calling* ``dlt.resource(...)`` rather
than decorating, for two reasons: the ``@dlt.source`` decorator takes the
source name from the function's ``__name__`` and so cannot produce a
runtime-chosen name, and module-scope ``import dlt`` is forbidden across this
package because it costs ~0.6s per interpreter start.
"""

from typing import ClassVar

from django_connectors.exceptions import SourceError


class SourceDefinition:
    """Base class for every source."""

    #: Registry key, matching ``Binding.source``.
    key = ""
    #: Human-facing provider grouping, e.g. "google", "microsoft".
    provider = ""
    #: Auth backend keys this source can work with. Empty means "any".
    supported_auth_backends = ()
    #: ``{module_name: extra_name}`` — surfaced by the W005 system check.
    required_extras: ClassVar[dict[str, str]] = {}

    #: A WebhookAdapter instance, or None. Adapters hang off the source rather
    #: than living in a registry of their own: an adapter is meaningless without
    #: the source whose data it announces.
    webhook = None

    #: Whether this source can detect remote deletions and emit tombstones.
    #: False for every cursor-based source (REST, warehouse): a remote deletion
    #: emits nothing at all, so deletion propagation is genuinely unsupported
    #: rather than merely unimplemented, and the API reports that honestly.
    emits_tombstones = False

    def build_source(self, *, binding, credentials, run):
        """Return a ``dlt.DltSource`` for this Binding.

        Never call ``pipeline.run()`` on the result directly — it must go
        through ``instrument_source()``, which applies the tenant metadata,
        merge identity and nesting invariants.
        """
        raise NotImplementedError

    def incremental_for(self, resource_name, binding):
        """Return ``Incremental`` kwargs for a resource, or None for a full load.

        Sources declare *what* the cursor is — ``{"cursor_path": "updated_at"}``,
        optionally ``initial_value``/``last_value_func`` — and never construct
        the ``Incremental`` themselves. The library builds it, so that the two
        settings which silently lose records cannot be forgotten:

        ``primary_key=()``
            dlt defaults the incremental's deduplication key to the resource's
            primary key, and a record updated at *exactly* the stored cursor
            value is then silently dropped. Verified: run 2 emitted an updated
            row and the table still held the old one.

        ``on_cursor_value_missing="include"``
            otherwise a record with no cursor value — a tombstone carrying only
            identity columns, typically — raises
            ``IncrementalCursorPathMissing`` and fails the entire Run.

        These cannot be repaired after the fact: dlt strips the incremental
        from a bound resource's signature, so a source that declares its own
        via a parameter default is beyond the library's reach.
        """
        return None

    def validate_config(self, config):
        """Raise ``ConfigurationError`` if `config` is unusable.

        Called when a Binding is saved, so that a bad configuration is rejected
        in front of the person editing it rather than inside a Run at 3am. This
        is also where a missing optional dependency must be caught: dlt's
        filesystem readers import lazily, so a missing pandas otherwise
        surfaces as a PipelineStepFailed in a customer's Run.
        """
        return None

    def check_connection(self, *, connection, credentials):
        """Cheaply verify the credentials work. Return a short status string."""
        raise SourceError(f"source {self.key!r} does not support connection tests")

    def discover(self, *, connection, credentials, query=None):
        """List what could be synchronized — tables, folders, endpoints."""
        raise SourceError(f"source {self.key!r} does not support discovery")

    def __str__(self):
        return self.key or type(self).__name__
