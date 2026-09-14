"""One-time preparation of the landing dataset.

The first *concurrent* loads into a fresh dataset fail loudly — ``Can't create
database 'X_staging'; database exists``, ``Table '_dlt_version' already
exists`` — regardless of how well the landing tables themselves are isolated,
because every pipeline races to create the same shared ``_dlt_version``,
``_dlt_loads`` and staging objects. Warm steady-state concurrency is clean; only
the cold start is not.

So this runs once, serially, at deploy or migrate time. Call it from a release
step, or let the first Run pay for it — but never let N workers discover an
empty dataset simultaneously.
"""

import logging

from django_connectors.conf import conf
from django_connectors.landing.destination import (
    landing_dataset_name,
    landing_destination,
)

logger = logging.getLogger(__name__)

WARMUP_PIPELINE_NAME = "django_connectors_warmup"
WARMUP_TABLE = "connector_warmup"


def warm_landing_dataset(*, url=None, pipelines_dir=None):
    """Create the dataset and dlt's shared bookkeeping tables. Idempotent."""
    import dlt

    pipeline = dlt.pipeline(
        pipeline_name=WARMUP_PIPELINE_NAME,
        destination=landing_destination(url),
        dataset_name=landing_dataset_name(),
        pipelines_dir=pipelines_dir or conf.PIPELINES_DIR,
        progress=None,
    )
    load_info = pipeline.run(
        [{"id": 1, "note": "landing dataset warm-up"}],
        table_name=WARMUP_TABLE,
        write_disposition="replace",
    )
    logger.info("warmed landing dataset %s", landing_dataset_name())
    return {
        "dataset": landing_dataset_name(),
        "load_ids": [str(load_id) for load_id in (load_info.loads_ids or [])],
    }
