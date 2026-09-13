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

from django_connectors.exceptions import ConfigurationError, SourceError


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

    def reshape_for(self, resource_name, binding):
        """An ``unpivot`` spec for a resource, or None to land rows as they are.

        Declared the way a cursor is: the source says *what* to reshape and
        the landing layer applies it — before the tenant metadata is stamped,
        and with the merge key extended by the new name column so the rows one
        wide row becomes do not merge back into one. See ``sources.reshape``.
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

    def discover(
        self, *, connection, credentials, query=None, path=None, cursor=None, limit=None
    ):
        """Browse what could be synchronized, one page at a time.

        Every source answers in one shape, because the person configuring a
        Binding is looking at one screen whatever the provider is::

            {"items": [{"id": ..., "name": ..., "kind": ..., "path": ...}, ...],
             "next_cursor": None | str}

        ``path`` is where to look — a folder id, a schema name, a bucket
        prefix — and is whatever an earlier item's ``path`` said, so a UI
        passes it back without knowing what it means. ``None`` is the top.
        ``cursor`` is opaque and comes from the previous page. ``query``
        narrows by name. ``kind`` tells the UI what an item is: ``folder``,
        ``file``, ``schema``, ``table``, ``spreadsheet``, ``sheet``, ``site``,
        ``drive``, ``label``, ``object``, ``field``, ``resource``.
        """
        raise SourceError(f"source {self.key!r} does not support discovery")

    def __str__(self):
        return self.key or type(self).__name__


def as_config(config):
    """Return `config` as a dict, or explain that it is not one.

    ``Binding.config`` is a JSON field, so a string or a number is a value the
    database will happily store. Every source reads the config with ``.get``,
    and a bare ``AttributeError`` from inside ``validate_config`` is not a form
    error the Binding editor can show — it is a 500. Sources call this first so
    the wrong shape is refused with the same exception as every other
    configuration problem.
    """
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise ConfigurationError(
            f"source config must be a JSON object, got {type(config).__name__}"
        )
    return config


def page_limit(limit):
    """`limit` clamped to ``1..DISCOVERY_PAGE_SIZE``; None or non-positive is the default.

    Bounded below as well as above: a negative slice returned the same page
    forever with a cursor that never advanced, and Drive and Graph reject a
    negative page size outright.
    """
    from django_connectors.conf import conf

    ceiling = int(conf.DISCOVERY_PAGE_SIZE)
    try:
        size = int(limit) if limit is not None else ceiling
    except (TypeError, ValueError):
        raise ConfigurationError(f"discovery limit {limit!r} is not a number") from None
    return ceiling if size <= 0 else min(size, ceiling)


def discovery_page(items, *, cursor=None, limit=None, query=None, name_key="name"):
    """One page of an in-memory list, in the discovery envelope.

    For sources that must fetch the whole list anyway (a database's tables,
    an org's objects, a mailbox's labels). The cursor is the offset as text,
    which is opaque enough: a client that fabricates one gets a page, not an
    exception. Providers with a native page token do not use this.
    """

    if query:
        needle = str(query).strip().lower()
        items = [
            item for item in items if needle in str(item.get(name_key, "")).lower()
        ]
    size = page_limit(limit)
    try:
        start = max(int(cursor or 0), 0)
    except (TypeError, ValueError):
        raise ConfigurationError(
            f"discovery cursor {cursor!r} is not one this source issued"
        ) from None
    page = items[start : start + size]
    next_cursor = str(start + size) if start + size < len(items) else None
    return {"items": page, "next_cursor": next_cursor}


def sign_cursor(value, *, scope):
    """An opaque cursor for a continuation link a provider returned.

    A raw URL as a cursor is fetched with the Connection's credentials, so a
    caller who could hand one in could read any same-origin collection with the
    tenant's token. Signing it proves it came from the provider's own response
    to *this* scope, and turns it back into what the provider gave us.
    """
    from django.core import signing

    return signing.dumps(value, salt=f"django_connectors.discover:{scope}")


def unsign_cursor(cursor, *, scope):
    from django.core import signing

    try:
        return signing.loads(cursor, salt=f"django_connectors.discover:{scope}")
    except signing.BadSignature:
        raise ConfigurationError(
            "discovery cursor was not issued by this source for this connection"
        ) from None
