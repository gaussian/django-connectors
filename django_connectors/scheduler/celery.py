"""Celery task wrappers, behind the ``celery`` extra.

Two constraints shape this module, and both are enforced by tests:

**Celery is never imported at module scope.** ``celery`` is an optional extra,
and the ``test-minimal`` CI tier installs none — so an import here would make
``import django_connectors.scheduler`` explode for every deployment that drives
the library from cron. It is imported inside :func:`register_tasks`, and a
missing install surfaces as a ``ConfigurationError`` naming the extra rather
than as a bare ``ImportError`` somewhere downstream.

**Nothing in the package imports this module.** The task objects are built on
demand, not at import time, which is why registration is an explicit call. Wire
it up next to your Celery app::

    from django_connectors.scheduler.celery import register_tasks

    register_tasks()

The tasks are thin: every one of them is a call into
:mod:`django_connectors.scheduler.base`, which is where the behaviour lives and
where it is tested. Nothing here may contain logic that cron-driven deployments
would not get.
"""

import logging

from django_connectors.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

#: Mirrors the SourceDefinition/W005 convention: ``{module: extra}``.
REQUIRED_EXTRAS = {"celery": "celery"}

# Task names are pinned explicitly rather than derived from the module path.
# Celery's default name is "<module>.<function>", so moving or renaming this
# module would silently orphan every queued message and every beat entry
# pointing at the old name.
TASK_NAME_PREFIX = "django_connectors"

TASK_ATTRIBUTES = (
    "run_due_bindings_task",
    "run_binding_task",
    "reap_stale_runs_task",
    "renew_due_webhooks_task",
    "dispatch_pending_projections_task",
    "tick_task",
)

_tasks: dict = {}


def register_tasks():
    """Build and register the Celery tasks. Returns ``{attribute: task}``.

    Idempotent: Celery raises on a duplicate task name, and this is called from
    host code that may well run more than once (an app module imported twice
    under different names, a test that reconfigures).
    """
    if _tasks:
        return dict(_tasks)

    shared_task = _shared_task()

    @shared_task(name=f"{TASK_NAME_PREFIX}.run_due_bindings")
    def run_due_bindings_task(limit=None, inline=False):
        """Sweep for due Bindings.

        By default each due Binding is handed to :func:`run_binding_task` rather
        than executed here: a sweep that ran every Binding inline would hold one
        worker for the sum of every provider's latency, and a single slow source
        would starve the rest. ``inline=True`` is for small deployments with one
        worker and no wish to fan out.
        """
        from django_connectors.scheduler import base

        dispatch = None if inline else _dispatch_to_worker()
        runs = base.run_due_bindings(limit=limit, dispatch=dispatch)
        return [str(run.id) for run in runs if run is not None]

    @shared_task(name=f"{TASK_NAME_PREFIX}.run_binding")
    def run_binding_task(binding_id, run_id=None):
        """Execute one Binding's ingestion.

        Safe to deliver twice: ``run_binding`` takes the Binding's lease and
        records a skipped Run rather than executing concurrently, which is what
        makes at-least-once delivery acceptable here.
        """
        from django_connectors.enums import RunTrigger
        from django_connectors.models import Binding, Run
        from django_connectors.services import runs as run_services

        binding = Binding.objects.select_related("connection").get(pk=binding_id)
        run = Run.objects.filter(pk=run_id).first() if run_id else None
        trigger = run.trigger if run is not None else RunTrigger.SCHEDULED
        return str(run_services.run_binding(binding, trigger=trigger, run=run).id)

    @shared_task(name=f"{TASK_NAME_PREFIX}.reap_stale_runs")
    def reap_stale_runs_task():
        from django_connectors.scheduler import base

        return base.reap_stale_runs()

    @shared_task(name=f"{TASK_NAME_PREFIX}.renew_due_webhooks")
    def renew_due_webhooks_task():
        from django_connectors.scheduler import base

        expired = base.expire_subscriptions()
        result = base.renew_due_webhooks()
        # Ids, not model instances: the default result serializer is JSON.
        return {
            "expired": expired,
            "renewed": [str(item.id) for item in result["renewed"]],
            "failed": [str(item.id) for item in result["failed"]],
        }

    @shared_task(name=f"{TASK_NAME_PREFIX}.dispatch_pending_projections")
    def dispatch_pending_projections_task():
        from django_connectors.scheduler import base

        return len(base.dispatch_pending_projections())

    @shared_task(name=f"{TASK_NAME_PREFIX}.tick")
    def tick_task(limit=None):
        """One full scheduler pass, for deployments that want a single beat entry."""
        from django_connectors.scheduler import base

        results = base.tick(limit=limit, dispatch=_dispatch_to_worker())
        return {key: _summarize(value) for key, value in results.items()}

    _tasks.update(
        run_due_bindings_task=run_due_bindings_task,
        run_binding_task=run_binding_task,
        reap_stale_runs_task=reap_stale_runs_task,
        renew_due_webhooks_task=renew_due_webhooks_task,
        dispatch_pending_projections_task=dispatch_pending_projections_task,
        tick_task=tick_task,
    )
    return dict(_tasks)


def beat_schedule(*, poll=60, reap=300, webhooks=300, projections=300):
    """A ready-made ``CELERY_BEAT_SCHEDULE`` fragment, in seconds.

    Offered because the failure mode of *not* scheduling these is invisible:
    nothing errors, Runs simply stop happening, webhook subscriptions quietly
    expire, and failed projections are never healed.
    """
    return {
        "django-connectors-run-due-bindings": {
            "task": f"{TASK_NAME_PREFIX}.run_due_bindings",
            "schedule": poll,
        },
        "django-connectors-reap-stale-runs": {
            "task": f"{TASK_NAME_PREFIX}.reap_stale_runs",
            "schedule": reap,
        },
        "django-connectors-renew-due-webhooks": {
            "task": f"{TASK_NAME_PREFIX}.renew_due_webhooks",
            "schedule": webhooks,
        },
        "django-connectors-dispatch-pending-projections": {
            "task": f"{TASK_NAME_PREFIX}.dispatch_pending_projections",
            "schedule": projections,
        },
    }


def _shared_task():
    """Celery's ``shared_task``, or a ConfigurationError naming the extra."""
    try:
        from celery import shared_task
    except ImportError as exc:
        raise ConfigurationError(
            "Celery is not installed, so django_connectors.scheduler.celery "
            "cannot register its tasks. Install it with "
            "`pip install 'django-connectors[celery]'`, or drive "
            "django_connectors.scheduler.base from cron or a management "
            "command instead."
        ) from exc
    return shared_task


def _dispatch_to_worker():
    """A ``dispatch(binding, run)`` that hands each Run to a worker."""
    task = register_tasks()["run_binding_task"]

    def dispatch(binding, run):
        task.delay(str(binding.id), str(run.id) if run is not None else None)

    return dispatch


def _summarize(value):
    """Reduce a step result to something the JSON serializer accepts."""
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        return {key: _summarize(item) for key, item in value.items()}
    if isinstance(value, int | str) or value is None:
        return value
    return str(value)


def __getattr__(name):
    """Expose the tasks as module attributes without building them at import.

    ``from django_connectors.scheduler.celery import tick_task`` therefore works
    and registers on first touch — but merely importing this module still costs
    nothing and still does not require Celery to be installed.
    """
    if name in TASK_ATTRIBUTES:
        return register_tasks()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
