"""Source definitions.

Sources are resolved lazily from ``DJANGO_CONNECTORS["SOURCES"]`` so that a
module is imported only when a Run actually needs it.
"""

from django_connectors.sources.base import SourceDefinition

__all__ = ["SourceDefinition"]
