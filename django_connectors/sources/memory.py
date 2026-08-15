"""A source driven by literal records held in ``Binding.config``.

Public and documented, not a test fixture — though the test suite is its main
consumer. It is the only way to drive the landing layer through behaviours that
real providers produce rarely and unreproducibly: a record updated at exactly
the incremental cursor boundary, a tombstone carrying only identity columns, a
value too large for the destination column, a source that raises mid-run.

Configuration::

    {
      "resources": {
        "events": {
          "primary_key": "id",
          "write_disposition": "merge",
          "cursor": "updated_at",
          "batches": [
            [{"id": "1", "updated_at": "2024-01-01T00:00:00Z"}],
            [{"id": "1", "updated_at": "2024-01-01T00:00:00Z", "v": "updated"}]
          ]
        }
      },
      "fail_on_batch": 2
    }

Each run consumes the next batch, tracked in dlt's own resource state so that
it survives across runs exactly as a real cursor would. A run past the last
batch yields nothing, which is what an unchanged remote source looks like.
"""

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.landing.naming import DELETED_COLUMN
from django_connectors.sources.base import SourceDefinition

BATCH_INDEX_STATE_KEY = "memory_batch_index"


class MemorySource(SourceDefinition):
    key = "memory"
    provider = "memory"
    # Batches are explicit, so a batch may mark a record deleted and the
    # deletion genuinely is detectable — unlike a cursor-based source.
    emits_tombstones = True

    def validate_config(self, config):
        resources = (config or {}).get("resources")
        if not isinstance(resources, dict) or not resources:
            raise ConfigurationError(
                "memory source config needs a non-empty 'resources' mapping"
            )
        for name, spec in resources.items():
            if not isinstance(spec, dict):
                raise ConfigurationError(f"resource {name!r} config must be an object")
            batches = spec.get("batches", [])
            if not isinstance(batches, list) or any(
                not isinstance(batch, list) for batch in batches
            ):
                raise ConfigurationError(
                    f"resource {name!r}: 'batches' must be a list of lists of records"
                )
            disposition = spec.get("write_disposition", "merge")
            if disposition == "merge" and not spec.get("primary_key"):
                raise ConfigurationError(
                    f"resource {name!r} uses merge disposition and must declare "
                    f"'primary_key'"
                )
        return None

    def incremental_for(self, resource_name, binding):
        spec = (binding.config or {}).get("resources", {}).get(resource_name) or {}
        cursor = spec.get("cursor")
        return {"cursor_path": cursor} if cursor else None

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        fail_on_batch = config.get("fail_on_batch")

        resources = [
            self._build_resource(dlt, name, spec, fail_on_batch)
            for name, spec in config["resources"].items()
        ]

        # dlt.source() called as a function, not used as a decorator: the
        # decorator takes the source name from the function's __name__ and
        # cannot produce a runtime-chosen one.
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _build_resource(self, dlt, name, spec, fail_on_batch):
        batches = spec.get("batches", [])

        # No `incremental=dlt.sources.incremental(...)` parameter default here:
        # the library applies the incremental via apply_hints so that the
        # deduplication and missing-cursor settings cannot be forgotten. See
        # SourceDefinition.incremental_for.
        def emit():
            state = dlt.current.resource_state()
            index = state.get(BATCH_INDEX_STATE_KEY, 0)
            state[BATCH_INDEX_STATE_KEY] = index + 1

            if fail_on_batch is not None and index + 1 == fail_on_batch:
                raise SourceError(
                    f"memory source configured to fail on batch {fail_on_batch}"
                )
            if index >= len(batches):
                return
            for record in batches[index]:
                yield dict(record)

        return dlt.resource(
            emit,
            name=name,
            primary_key=spec.get("primary_key"),
            write_disposition=spec.get("write_disposition", "merge"),
        )()

    def check_connection(self, *, connection, credentials):
        return "ok"

    def discover(self, *, connection, credentials, query=None):
        return {"resources": []}


def tombstone(primary_key_values):
    """Build a record marking a remote row deleted.

    Deliberately carries identity columns only, which is exactly what makes
    deletions lossy: ``delete-insert`` merge replaces the whole row, so every
    unsupplied column lands as NULL. Any target identity field mapped from
    outside the merge key is therefore None on the delete path — which is why
    Projection validation refuses that configuration up front.
    """
    return {**primary_key_values, DELETED_COLUMN: True}
