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
    its docstring names. Raises ``SourceError`` for a source that has no
    content to give, and for a payload over the ceiling.
    """
    from django_connectors.services.runs import _credentials_for

    definition = sources.get(binding.source)
    if not getattr(definition, "provides_content", False):
        raise SourceError(
            f"source {binding.source!r} lands rows with nothing behind them; "
            f"it does not provide record content"
        )
    ceiling = int(conf.CONTENT_MAX_BYTES)
    limit = (
        ceiling
        if not max_bytes or int(max_bytes) <= 0
        else min(int(max_bytes), ceiling)
    )
    data, content_type = definition.fetch_content(
        binding=binding,
        credentials=_credentials_for(binding.connection),
        resource=resource,
        reference=dict(reference or {}),
        max_bytes=limit,
    )
    return FetchedContent(data=bytes(data), content_type=content_type)
