"""Synchronous scheduling primitives.

No queue, no broker, no Celery. Everything here is an ordinary function that
does the work in the calling process, which is what makes the whole scheduler
testable on sqlite with no services running — and what lets a small deployment
drive the library from ``cron`` alone. :mod:`django_connectors.scheduler.celery`
is a thin wrapper over these, not an alternative implementation.

The one subtle rule is how a Binding is *claimed*. Advancing ``next_run_at``
happens in a conditional UPDATE against the value we selected, before any work
starts, so two schedulers running concurrently cannot both claim the same
Binding — and ``dirty`` is cleared in that same UPDATE, so a webhook delivery
arriving while the Run is extracting re-sets the flag and earns a follow-up Run
instead of being swallowed by the one already in flight.
"""

import logging
from functools import partial

from django.db.models import F, Q
from django.utils import timezone

from django_connectors.conf import conf
from django_connectors.enums import BindingStatus, ConnectionStatus, RunTrigger
from django_connectors.models import Binding
from django_connectors.services import runs as run_services

logger = logging.getLogger(__name__)

# Mirrors Binding.is_runnable. Declared here as well so the database does the
# filtering instead of loading every Binding and discarding most of them.
RUNNABLE_STATUSES = (
    BindingStatus.PENDING,
    BindingStatus.ACTIVE,
    BindingStatus.NEEDS_REVIEW,
)


def run_due_bindings(*, now=None, limit=None, dispatch=None):
    """Run every Binding that is due, and return the Runs.

    Due means: enabled, in a runnable status, on an active Connection, and
    either scheduled for now-or-earlier or flagged ``dirty`` by a webhook
    delivery. ``min_run_interval`` overrides all of that — it is the floor that
    stops a webhook burst driving continuous syncing — and a Binding held back
    by it has its ``next_run_at`` pushed to the earliest permitted moment so the
    next sweep does not reconsider it every tick.

    `dispatch` is the seam for asynchronous execution: given
    ``dispatch(binding, run)`` the Run is handed off instead of executed here.
    Left None, the Run executes inline.
    """
    now = now or timezone.now()

    due = (
        Binding.objects.select_related("connection")
        .filter(
            enabled=True,
            status__in=RUNNABLE_STATUSES,
            connection__status=ConnectionStatus.ACTIVE,
        )
        .filter(Q(dirty=True) | Q(next_run_at__isnull=True) | Q(next_run_at__lte=now))
        .order_by(F("next_run_at").asc(nulls_first=True))
    )
    if limit:
        due = due[:limit]

    runs = []
    for binding in due:
        allowed_at = _min_interval_floor(binding)
        if allowed_at is not None and allowed_at > now:
            _defer(binding, allowed_at)
            continue

        claim = _claim(binding, now)
        if claim is None:
            # Another scheduler took this Binding between the SELECT and here.
            continue

        run = run_services.enqueue_run(binding, trigger=claim)
        if dispatch is None:
            run = run_services.run_binding(binding, trigger=claim, run=run)
        else:
            dispatch(binding, run)
        runs.append(run)
    return runs


def reap_stale_runs():
    """Fail Runs whose lease expired and clear the leases.

    Re-exported rather than reimplemented — the logic belongs with the Run
    lifecycle, and having two versions of "what counts as abandoned" is how they
    drift apart.
    """
    return run_services.reap_stale_runs()


def renew_due_webhooks(*, now=None, limit=None):
    """Renew provider subscriptions before they expire.

    Imported lazily so that a deployment that never uses webhooks does not pay
    for the webhook services on every scheduler import.
    """
    from django_connectors.webhooks import services as webhook_services

    return webhook_services.renew_due_webhooks(now=now, limit=limit)


def expire_subscriptions(*, now=None):
    """Retire subscriptions the provider has already stopped delivering to."""
    from django_connectors.webhooks import services as webhook_services

    return webhook_services.expire_subscriptions(now=now)


def dispatch_pending_projections(*, lookback=None):
    """Sweep active Projections for landed loads nobody projected."""
    from django_connectors.services import projections as projection_services

    return projection_services.dispatch_pending_projections(lookback=lookback)


def tick(*, now=None, limit=None, dispatch=None):
    """One full scheduler pass, ordered so each step helps the next.

    Reaping first releases leases held by dead workers, so a Binding stuck
    behind one becomes runnable in this same pass rather than the next.
    Expiring before renewing keeps the renewal sweep from calling a provider
    about a subscription that is already gone. Projections come last, after the
    Runs that produce the loads they consume.

    Every step is isolated: one provider outage during renewal must not stop
    Bindings from running.
    """
    now = now or timezone.now()
    results = {}
    steps = (
        ("reaped", reap_stale_runs),
        ("expired_webhooks", partial(expire_subscriptions, now=now)),
        ("renewed_webhooks", partial(renew_due_webhooks, now=now)),
        ("runs", partial(run_due_bindings, now=now, limit=limit, dispatch=dispatch)),
        ("projection_runs", dispatch_pending_projections),
    )
    for name, step in steps:
        try:
            results[name] = step()
        except Exception:
            logger.exception("scheduler step %r failed", name)
            results[name] = None
    return results


# --- internals -------------------------------------------------------------


def _min_interval_floor(binding):
    """Earliest moment this Binding may run again, or None if unconstrained."""
    if not binding.min_run_interval:
        return None
    attempts = [
        moment
        for moment in (binding.last_success_at, binding.last_failure_at)
        if moment is not None
    ]
    if not attempts:
        return None
    return max(attempts) + binding.min_run_interval


def _defer(binding, until):
    """Push `next_run_at` out to `until` without running anything.

    Leaving ``next_run_at`` in the past instead would make this Binding come
    back on every single tick for as long as the floor holds — and ``dirty``
    Bindings would come back forever.
    """
    Binding.objects.filter(pk=binding.pk).update(next_run_at=until)


def _claim(binding, now):
    """Take ownership of this Binding's next Run, or return None.

    The UPDATE is conditional on ``next_run_at`` still holding the value we
    selected, which makes it a compare-and-swap: exactly one of two concurrent
    schedulers gets a non-zero rowcount. ``dirty`` is cleared in the same
    statement and *before* extraction starts, so a delivery arriving mid-Run
    sets it again and is not lost.

    Returns the RunTrigger to record: a Binding claimed while dirty ran because
    of a webhook, and attributing that to the poll timer would erase the only
    evidence that push delivery is working.
    """
    interval = binding.poll_interval or conf.DEFAULT_POLL_INTERVAL
    claimed = Binding.objects.filter(pk=binding.pk, next_run_at=binding.next_run_at)
    if not claimed.update(next_run_at=now + interval, dirty=False):
        return None
    if binding.status == BindingStatus.PENDING:
        return RunTrigger.INITIAL
    return RunTrigger.WEBHOOK if binding.dirty else RunTrigger.SCHEDULED
