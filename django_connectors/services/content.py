"""Fetching the bytes behind a landed record.

The landing layer lands metadata, never file bytes, for reasons
``providers/microsoft/files.py`` spells out. A host that lands file metadata
still needs the file — to store it, extract text from it, hand it to a
pipeline — and reaching into a provider module with a hand-built session is
the boundary leaking in exactly the direction this library exists to prevent.
This is the supported route: the source that landed the record fetches its
content, with the Connection's credentials resolved the same way a Run
resolves them, under a size ceiling the host cannot lift past the setting.
"""

from dataclasses import dataclass

from django_connectors.conf import conf
from django_connectors.exceptions import SourceError
from django_connectors.landing import access, schema
from django_connectors.landing.naming import is_internal_column
from django_connectors.registry import sources


@dataclass(frozen=True)
class FetchedContent:
    data: bytes
    content_type: str | None = None

    @property
    def size(self):
        return len(self.data)


def fetch_record_content(binding, resource, reference, *, max_bytes=None):
    """The content behind one landed record of `binding`'s `resource`.

    `reference` is what the source needs to address it — the landed columns
    its docstring names — and it must name a row this Binding actually
    landed: the reference is checked against the landed table on the
    resource's merge key before any credential is used. A host will sooner
    or later build a reference from request input, and without that check a
    caller scoped to one folder could fetch any item the tenant-wide token
    can read. Raises ``SourceError`` for a source that has no content to
    give, a reference that matches nothing landed, and a payload over the
    ceiling.
    """
    from django_connectors.services.runs import credentials_for

    definition = sources.get(binding.source)
    if not getattr(definition, "provides_content", False):
        raise SourceError(
            f"source {binding.source!r} lands rows with nothing behind them; "
            f"it does not provide record content"
        )
    reference = dict(reference or {})
    _assert_landed(binding, resource, reference)
    ceiling = int(conf.CONTENT_MAX_BYTES)
    limit = (
        ceiling
        if not max_bytes or int(max_bytes) <= 0
        else min(int(max_bytes), ceiling)
    )
    data, content_type = definition.fetch_content(
        binding=binding,
        credentials=credentials_for(binding.connection),
        resource=resource,
        reference=reference,
        max_bytes=limit,
    )
    return FetchedContent(data=bytes(data), content_type=content_type)


def _assert_landed(binding, resource, reference):
    """The reference must equal a landed row on the resource's merge key.

    The merge key is read from the landed dlt schema, as the projection
    validator reads it, and the rows are streamed rather than filtered in SQL:
    a content fetch costs far more than a scan of one Binding's table, and a
    Python comparison is the same on every dialect.
    """
    key = _merge_key(binding, resource)
    missing = sorted(column for column in key if reference.get(column) in (None, ""))
    if missing:
        raise SourceError(
            f"reference must carry the landed merge key column(s) {sorted(key)}; "
            f"missing {missing}"
        )
    wanted = {column: str(reference[column]) for column in key}
    # Pushed into SQL: the business-key columns carry no precision hint, so
    # the cast dlt renders for `where` is plain, on every dialect.
    relation = access.binding_relation(binding, resource)
    for column, value in wanted.items():
        relation = relation.where(column, "eq", value)
    if any(True for _ in access.iter_rows(relation.limit(1), binding=binding)):
        return
    raise SourceError(
        f"no landed {resource!r} row of this binding matches {wanted}; content "
        f"is fetched only for records this binding landed"
    )


def _merge_key(binding, resource):
    try:
        columns = schema.merge_key_for(binding, resource)
    except KeyError:
        raise SourceError(
            f"resource {resource!r} has not landed for this binding"
        ) from None
    business = {name for name in columns or () if not is_internal_column(name)}
    if not business:
        raise SourceError(
            f"resource {resource!r} has not landed with a merge key, so a "
            f"reference cannot be matched to a landed row"
        )
    return business
