"""Test helpers this library ships for its hosts.

Importing this package pulls in nothing but the standard library and Django.
It is not imported by anything at runtime, and nothing in it is required for a
production install — but it lives in the distribution rather than in ``tests/``
on purpose: the connector contract applies to *host-defined* SourceDefinitions
too, and a suite that only exists in this repository can only be run against the
connectors this repository happens to ship.
"""

from django_connectors.testing.conformance import (
    check_built_source,
    check_definition,
    check_landing_invariants,
    check_source_conformance,
    check_tombstone_shape,
)

__all__ = [
    "check_built_source",
    "check_definition",
    "check_landing_invariants",
    "check_source_conformance",
    "check_tombstone_shape",
]
