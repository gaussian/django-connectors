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

from django.core import signing

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

    #: Whether :meth:`fetch_content` can return the bytes behind a landed
    #: record. False for row-shaped sources (a query result, a spreadsheet
    #: range): there is nothing behind a row but the row.
    provides_content = False

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

    @staticmethod
    def resource_spec(binding, resource_name):
        """One entry of ``Binding.config["resources"]``, or ``{}``.

        For sources whose config is ``{"resources": {name: spec}}`` — memory,
        rest, filesystem — which is where their cursor and unpivot live.
        """
        return ((binding.config or {}).get("resources") or {}).get(resource_name) or {}

    def reshape_for(self, resource_name, binding):
        """An ``unpivot`` spec for a resource, or None to land rows as they are.

        Declared the way a cursor is: the source says *what* to reshape and
        the landing layer applies it — before the tenant metadata is stamped,
        and with the merge key extended by the new name column so the rows one
        wide row becomes do not merge back into one. See ``sources.reshape``.
        The default reads ``resources[name].unpivot``, the shape memory, rest
        and filesystem share; a source with another config shape overrides.
        """
        return self.resource_spec(binding, resource_name).get("unpivot")

    def validate_reshape(self, spec, *, where="unpivot", primary_key=(), cursor=None):
        """Every rule an ``unpivot`` must satisfy on *this* source, in one place.

        Called by a source's ``validate_config`` with the key and cursor it
        already knows, and again by the landing layer before the reshape is
        applied — so a Binding written past ``validate_binding`` meets the
        same rules, and the three of them (tombstones, key survival, cursor
        survival) cannot drift between save time and run time.
        """
        from django_connectors.sources.reshape import survives, validate_unpivot

        spec = validate_unpivot(spec, where=where, primary_key=primary_key)
        if self.emits_tombstones:
            raise ConfigurationError(
                f"{where}: source {self.key!r} emits tombstones, which cannot be "
                f"unpivoted — a tombstone carries the case identity only, and one "
                f"case-level deletion cannot become the per-stage deletes the landed "
                f"rows would need. Deletions would be lost silently."
            )
        if cursor and not survives(spec, cursor):
            raise ConfigurationError(
                f"{where}: the incremental cursor {cursor!r} does not survive the "
                f"unpivot (it is consumed, or not in 'keep'), so it would be NULL on "
                f"every landed row. Keep it, or choose a cursor the unpivot leaves alone."
            )
        return spec

    def validate_config(self, config):
        """Raise ``ConfigurationError`` if `config` is unusable.

        Called when a Binding is saved, so that a bad configuration is rejected
        in front of the person editing it rather than inside a Run at 3am. This
        is also where a missing optional dependency must be caught: dlt's
        filesystem readers import lazily, so a missing pandas otherwise
        surfaces as a PipelineStepFailed in a customer's Run.
        """
        return None

    def fetch_content(self, *, binding, credentials, resource, reference, max_bytes):
        """Return ``(bytes, content_type)`` for the record `reference` names.

        File bytes are never landed — a landing column is the wrong place for
        a 40MB document — but a host that lands file *metadata* needs the file
        itself to do anything with it. This is the supported way to get it.
        `reference` is a mapping of the landed columns the source needs to
        address the content (documented per source: a driveItem id and its
        drive, say); the host has those because its own mapping put them in
        the record. `max_bytes` is not advisory.
        """
        raise SourceError(f"source {self.key!r} does not expose record content")

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
    return signing.dumps(value, salt=f"django_connectors.discover:{scope}")


def unsign_cursor(cursor, *, scope):
    try:
        return signing.loads(cursor, salt=f"django_connectors.discover:{scope}")
    except signing.BadSignature:
        raise ConfigurationError(
            "discovery cursor was not issued by this source for this connection"
        ) from None


def read_capped(response, *, max_bytes, what, limit_hint=""):
    """``(bytes, content_type)`` from a streamed response, under `max_bytes`.

    The size is checked twice: from ``Content-Length`` when present (cheap,
    refuses before the transfer) and while streaming (authoritative — the
    header is optional, and absent after a redirect). Every source that
    fetches content goes through this, so the ceiling rule lives once.
    """
    declared = response.headers.get("Content-Length")
    if declared and declared.isdigit() and int(declared) > max_bytes:
        raise SourceError(
            f"{what} is {declared} bytes, over the {max_bytes}-byte limit this "
            f"call was given. {limit_hint}".rstrip()
        )
    chunks, total = [], 0
    for chunk in response.iter_content(1 << 16):
        total += len(chunk)
        if total > max_bytes:
            raise SourceError(
                f"{what} exceeded the {max_bytes}-byte limit while downloading; "
                f"the transfer was abandoned."
            )
        chunks.append(chunk)
    return b"".join(chunks), response.headers.get("Content-Type")


def name_matches(name, glob):
    """Case-insensitive glob on a file name, because the providers are.

    ``fnmatch.fnmatch`` follows the *host* filesystem's rules, so on Linux
    ``*.xlsx`` silently misses ``Budget.XLSX`` while on macOS it matches — a
    Binding that works on a laptop and drops half the documents in
    production. SharePoint, OneDrive and Drive all compare names
    case-insensitively, so the fold is explicit rather than inherited from
    wherever the worker runs.
    """
    import fnmatch

    return fnmatch.fnmatchcase((name or "").lower(), glob.lower())
