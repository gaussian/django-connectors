"""Scheduling: which Bindings are due, and how Celery stays optional.

Most tests drive ``run_due_bindings`` through its ``dispatch`` seam rather than
executing dlt, because what is under test is *selection and claiming* — which
Binding runs, when it runs again, and what the Run is attributed to. One test
runs inline against the memory source so the seam is not the only path proven.

The Celery assertions are the ``test-minimal`` CI tier in miniature: that job
installs zero extras, so anything importing Celery at module scope, or any
package module importing the wrapper eagerly, breaks it.
"""

import datetime as dt
import subprocess
import sys

import pytest
from django.utils import timezone

from django_connectors.enums import (
    BindingStatus,
    ConnectionStatus,
    RunStatus,
    RunTrigger,
)
from django_connectors.exceptions import ConfigurationError
from django_connectors.models import Binding, Run
from django_connectors.scheduler import base
from tests.conftest import memory_config
from tests.test_import_contract import REPO_ROOT

pytestmark = pytest.mark.django_db

MINUTE = dt.timedelta(minutes=1)
HOUR = dt.timedelta(hours=1)


@pytest.fixture
def recorder():
    """A dispatch seam that records instead of executing."""
    dispatched = []

    def dispatch(binding, run):
        dispatched.append((binding.id, run.trigger))

    dispatch.dispatched = dispatched
    return dispatch


# --- selection -------------------------------------------------------------


def test_only_due_runnable_bindings_on_active_connections_are_swept(
    make_binding, make_connection, recorder
):
    now = timezone.now()
    due = make_binding(next_run_at=now - MINUTE, status=BindingStatus.ACTIVE)
    never_run = make_binding(next_run_at=None, status=BindingStatus.ACTIVE)
    make_binding(next_run_at=now + HOUR, status=BindingStatus.ACTIVE)
    make_binding(next_run_at=None, enabled=False)
    make_binding(next_run_at=None, status=BindingStatus.BLOCKED)
    make_binding(next_run_at=None, status=BindingStatus.PURGING)
    make_binding(
        next_run_at=None,
        connection=make_connection(status=ConnectionStatus.REVOKED),
    )

    base.run_due_bindings(now=now, dispatch=recorder)

    assert {binding_id for binding_id, _ in recorder.dispatched} == {
        due.id,
        never_run.id,
    }


def test_next_run_at_is_advanced_by_the_bindings_poll_interval(make_binding, recorder):
    now = timezone.now()
    binding = make_binding(next_run_at=None, poll_interval=dt.timedelta(minutes=5))

    base.run_due_bindings(now=now, dispatch=recorder)

    binding.refresh_from_db()
    assert binding.next_run_at == now + dt.timedelta(minutes=5)


def test_a_binding_without_a_poll_interval_falls_back_to_the_default(
    make_binding, recorder, settings
):
    settings.DJANGO_CONNECTORS = {"DEFAULT_POLL_INTERVAL": dt.timedelta(minutes=7)}
    now = timezone.now()
    binding = make_binding(next_run_at=None)

    base.run_due_bindings(now=now, dispatch=recorder)

    binding.refresh_from_db()
    assert binding.next_run_at == now + dt.timedelta(minutes=7)


def test_a_pending_binding_is_recorded_as_an_initial_run(make_binding, recorder):
    binding = make_binding(next_run_at=None, status=BindingStatus.PENDING)

    base.run_due_bindings(dispatch=recorder)

    assert recorder.dispatched == [(binding.id, RunTrigger.INITIAL)]


def test_a_dirty_binding_runs_early_and_is_attributed_to_the_webhook(
    make_binding, recorder
):
    """Otherwise the only evidence that push delivery works would be erased."""
    now = timezone.now()
    binding = make_binding(
        next_run_at=now + HOUR, dirty=True, status=BindingStatus.ACTIVE
    )

    base.run_due_bindings(now=now, dispatch=recorder)

    assert recorder.dispatched == [(binding.id, RunTrigger.WEBHOOK)]
    binding.refresh_from_db()
    # Cleared before extraction starts, so a delivery arriving mid-Run sets it
    # again and earns exactly one follow-up.
    assert binding.dirty is False
    assert binding.next_run_at > now


def test_min_run_interval_holds_a_binding_back_and_moves_it_out_of_the_way(
    make_binding, recorder
):
    now = timezone.now()
    last = now - dt.timedelta(minutes=10)
    binding = make_binding(
        next_run_at=None,
        status=BindingStatus.ACTIVE,
        min_run_interval=HOUR,
        last_success_at=last,
    )

    base.run_due_bindings(now=now, dispatch=recorder)

    assert recorder.dispatched == []
    binding.refresh_from_db()
    # Pushed to the earliest permitted moment; leaving it in the past would
    # make this Binding reappear on every single tick for the next hour.
    assert binding.next_run_at == last + HOUR


def test_min_run_interval_also_holds_back_a_dirty_binding(make_binding, recorder):
    """The floor exists so a webhook burst cannot drive continuous syncing."""
    now = timezone.now()
    binding = make_binding(
        next_run_at=None,
        status=BindingStatus.ACTIVE,
        min_run_interval=HOUR,
        last_failure_at=now - MINUTE,
        dirty=True,
    )

    base.run_due_bindings(now=now, dispatch=recorder)

    assert recorder.dispatched == []
    binding.refresh_from_db()
    assert binding.dirty is True


def test_a_binding_runs_once_the_floor_has_passed(make_binding, recorder):
    now = timezone.now()
    binding = make_binding(
        next_run_at=None,
        status=BindingStatus.ACTIVE,
        min_run_interval=MINUTE,
        last_success_at=now - HOUR,
    )

    base.run_due_bindings(now=now, dispatch=recorder)

    assert [binding_id for binding_id, _ in recorder.dispatched] == [binding.id]


def test_limit_bounds_the_sweep(make_binding, recorder):
    for _ in range(3):
        make_binding(next_run_at=None)

    base.run_due_bindings(dispatch=recorder, limit=2)

    assert len(recorder.dispatched) == 2


def test_two_schedulers_cannot_claim_the_same_binding(make_binding):
    """The claim is a compare-and-swap on next_run_at, not a read-then-write."""
    now = timezone.now()
    binding = make_binding(next_run_at=None)
    stale = Binding.objects.get(pk=binding.pk)

    assert base._claim(binding, now) is not None
    assert base._claim(stale, now) is None


def test_a_sweep_queues_at_most_one_run_per_binding(make_binding, recorder):
    """Two sweeps before the first Run starts must not pile up the queue."""
    binding = make_binding(next_run_at=None, status=BindingStatus.ACTIVE)

    base.run_due_bindings(dispatch=recorder)
    binding.refresh_from_db()
    binding.next_run_at = None
    binding.save(update_fields=["next_run_at"])
    base.run_due_bindings(dispatch=recorder)

    assert Run.objects.filter(status=RunStatus.QUEUED).count() == 1


def test_run_due_bindings_executes_inline_when_no_dispatch_is_given(
    connectors_settings, make_binding
):
    binding = make_binding(
        config=memory_config(batches=[[{"id": "1", "v": "a"}]]),
        next_run_at=None,
    )

    runs = base.run_due_bindings()

    assert [run.status for run in runs] == [RunStatus.SUCCEEDED], runs[0].error_message
    binding.refresh_from_db()
    assert binding.last_success_at is not None
    assert binding.next_run_at is not None


# --- the other sweeps ------------------------------------------------------


def test_reap_stale_runs_fails_a_run_whose_lease_vanished(make_binding):
    binding = make_binding()
    run = Run.objects.create(
        binding=binding, trigger=RunTrigger.SCHEDULED, status=RunStatus.RUNNING
    )

    result = base.reap_stale_runs()

    run.refresh_from_db()
    assert run.status == RunStatus.FAILED
    assert run.error_type == "LeaseExpired"
    assert result["runs_failed"] == 1


def test_renew_due_webhooks_is_the_webhook_service(monkeypatch):
    from django_connectors.webhooks import services as webhook_services

    calls = []
    monkeypatch.setattr(
        webhook_services,
        "renew_due_webhooks",
        lambda **kwargs: calls.append(kwargs) or "renewed",
    )

    now = timezone.now()
    assert base.renew_due_webhooks(now=now, limit=3) == "renewed"
    assert calls == [{"now": now, "limit": 3}]


def test_dispatch_pending_projections_is_the_projection_service(monkeypatch):
    from django_connectors.services import projections as projection_services

    calls = []
    monkeypatch.setattr(
        projection_services,
        "dispatch_pending_projections",
        lambda **kwargs: calls.append(kwargs) or [],
    )

    assert base.dispatch_pending_projections(lookback=HOUR) == []
    assert calls == [{"lookback": HOUR}]


def test_tick_runs_every_step_and_isolates_a_failing_one(
    make_binding, recorder, monkeypatch
):
    """A provider outage during renewal must not stop Bindings from running."""

    def explode(**kwargs):
        raise RuntimeError("provider unreachable")

    monkeypatch.setattr(base, "renew_due_webhooks", explode)
    binding = make_binding(next_run_at=None, status=BindingStatus.ACTIVE)

    results = base.tick(dispatch=recorder)

    assert results["renewed_webhooks"] is None
    assert [binding_id for binding_id, _ in recorder.dispatched] == [binding.id]
    assert results["expired_webhooks"] == 0
    assert results["projection_runs"] == []


# --- Celery stays optional -------------------------------------------------


def _subprocess(code):
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )


BLOCK_CELERY = """
import importlib.abc, sys

class Blocked(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "celery" or fullname.startswith("celery."):
            raise ImportError("No module named 'celery'")
        return None

sys.meta_path.insert(0, Blocked())
"""


def test_the_package_works_with_celery_uninstalled():
    """This is the `test-minimal` CI tier: zero extras installed."""
    result = _subprocess(
        BLOCK_CELERY + "import os\n"
        "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
        "import django\n"
        "django.setup()\n"
        "import django_connectors\n"
        "import django_connectors.scheduler\n"
        "import django_connectors.scheduler.celery as wrapper\n"
        "import django_connectors.webhooks, django_connectors.webhooks.urls\n"
        "assert 'celery' not in sys.modules\n"
        "from django_connectors.exceptions import ConfigurationError\n"
        "try:\n"
        "    wrapper.register_tasks()\n"
        "except ConfigurationError as exc:\n"
        '    assert "django-connectors[celery]" in str(exc), str(exc)\n'
        "else:\n"
        "    raise AssertionError('register_tasks() did not raise')\n"
        "print('ok')\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_nothing_in_the_package_imports_the_celery_wrapper_eagerly():
    """Even with Celery installed, importing it must be the host's choice."""
    result = _subprocess(
        "import os, sys\n"
        "os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'\n"
        "import django\n"
        "django.setup()\n"
        "import django_connectors.scheduler\n"
        "import django_connectors.webhooks\n"
        "assert 'celery' not in sys.modules, 'celery was imported'\n"
        "assert 'django_connectors.scheduler.celery' not in sys.modules\n"
        "print('ok')\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_register_tasks_names_the_extra_when_celery_is_missing(monkeypatch):
    from django_connectors.scheduler import celery as wrapper

    monkeypatch.setattr(wrapper, "_tasks", {})
    monkeypatch.setitem(sys.modules, "celery", None)

    with pytest.raises(ConfigurationError, match=r"django-connectors\[celery\]"):
        wrapper.register_tasks()


def test_register_tasks_is_idempotent_and_pins_every_task_name(monkeypatch):
    pytest.importorskip("celery")
    from django_connectors.scheduler import celery as wrapper

    monkeypatch.setattr(wrapper, "_tasks", {})
    tasks = wrapper.register_tasks()

    assert set(tasks) == set(wrapper.TASK_ATTRIBUTES)
    # Pinned, not derived from the module path: moving this module must not
    # orphan queued messages or beat entries.
    assert tasks["tick_task"].name == "django_connectors.tick"
    assert tasks["run_binding_task"].name == "django_connectors.run_binding"
    assert wrapper.register_tasks() == tasks


def test_the_beat_schedule_only_names_registered_tasks(monkeypatch):
    pytest.importorskip("celery")
    from django_connectors.scheduler import celery as wrapper

    monkeypatch.setattr(wrapper, "_tasks", {})
    registered = {task.name for task in wrapper.register_tasks().values()}

    schedule = wrapper.beat_schedule()
    assert schedule
    assert {entry["task"] for entry in schedule.values()} <= registered
