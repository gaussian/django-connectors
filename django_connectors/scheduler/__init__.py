"""Periodic work: due Bindings, stale Runs, webhook renewals, pending projections.

:mod:`django_connectors.scheduler.base` holds plain synchronous functions with
no queue dependency at all — they run from a management command, a cron job, a
test, or a Celery beat task. :mod:`django_connectors.scheduler.celery` wraps
them as tasks and is **never** imported from here: it is the only module in the
package that touches Celery, and the ``test-minimal`` CI tier installs no extras.
"""

from django_connectors.scheduler.base import (
    dispatch_pending_projections,
    reap_stale_runs,
    renew_due_webhooks,
    run_due_bindings,
    tick,
)

__all__ = [
    "dispatch_pending_projections",
    "reap_stale_runs",
    "renew_due_webhooks",
    "run_due_bindings",
    "tick",
]
