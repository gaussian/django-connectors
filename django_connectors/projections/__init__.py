"""Projection: mapping landed source records into host-declared targets.

Deliberately narrow — one landed resource to one target, with field mapping,
constants, object construction, a small set of safe functions, casts and simple
filters. No joins, no aggregation, no arbitrary SQL, no customer Python. Source
data needing more shaping than that should be shaped before landing, in the
customer's own view or in the dlt source, or this stops being a connector
framework and becomes a query engine.
"""

from django_connectors.projections.targets import (
    ProjectedRecord,
    TargetDefinition,
    WriterContext,
    register_target,
)

__all__ = [
    "ProjectedRecord",
    "TargetDefinition",
    "WriterContext",
    "register_target",
]
