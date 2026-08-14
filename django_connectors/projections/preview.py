"""Projection preview.

First-class because a customer cannot be asked to author a mapping blind. It
runs the real compiled mapping over real landed rows and reports, per row, what
came out — including the rows a filter excluded and the fields that failed to
cast, which is information a database CAST cannot produce (MySQL yields NULL
plus a session warning rather than naming the row).

**The writer is never invoked.** Preview must be safe to run against a
production target from a configuration screen.

Preview returns customer data to its caller, so it is a data-exfiltration
primitive if permissions are wrong: callers must be owner-scoped, and the row
and byte caps below are server-side, not hints.
"""

import json

from django_connectors.conf import conf
from django_connectors.exceptions import CastError, ProjectionError
from django_connectors.landing import access
from django_connectors.landing.naming import DELETED_COLUMN, is_internal_column
from django_connectors.projections.compiler import compile_mapping
from django_connectors.projections.runner import ORDER_COLUMNS, _project
from django_connectors.projections.targets import get_target


def preview_projection(projection, *, limit=None):
    """Return sample rows, their projected records, and per-row diagnostics."""
    limit = min(limit or conf.PREVIEW_MAX_ROWS, conf.PREVIEW_MAX_ROWS)
    target = get_target(projection.target)
    compiled = compile_mapping(projection.mapping, projection.filters)

    relation = access.binding_relation(projection.binding, projection.resource)
    rows = list(
        access.iter_rows(
            relation.limit(limit),
            order_by=ORDER_COLUMNS,
            binding=projection.binding,
        )
    )

    results = []
    budget = conf.PREVIEW_MAX_BYTES
    for row in rows:
        entry = {
            "source": {
                key: value for key, value in row.items() if not is_internal_column(key)
            },
            "deleted": bool(row.get(DELETED_COLUMN)),
        }
        if not compiled.matches(row):
            entry["status"] = "filtered"
        else:
            try:
                record = _project(row, compiled, target)
            except (CastError, ProjectionError) as exc:
                entry["status"] = "error"
                entry["error"] = str(exc)
            else:
                entry["status"] = "ok"
                entry["record"] = {
                    "operation": record.operation,
                    "identity": record.identity,
                    "values": record.values,
                }
        results.append(entry)

        budget -= len(json.dumps(entry, default=str))
        if budget <= 0:
            entry["truncated"] = True
            break

    return {
        "target": target.key,
        "resource": projection.resource,
        "row_count": len(results),
        "ok_count": sum(1 for item in results if item["status"] == "ok"),
        "filtered_count": sum(1 for item in results if item["status"] == "filtered"),
        "error_count": sum(1 for item in results if item["status"] == "error"),
        "rows": results,
    }
