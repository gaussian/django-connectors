"""Handing ProjectionRuns to a queue instead of running them inline.

Inline execution happens inside `run_binding`, while the Binding's lease is
held, so a target whose writer does real work per record runs against
`run_timeout`. With `PROJECTION_DISPATCH` set to a callable, a successful Run
records its ProjectionRuns as QUEUED and hands each to that callable; they
execute later, wherever the queue delivers them. Two properties keep that
safe: a run no longer QUEUED is not executed again (at-least-once delivery),
and the sweeper executes a run the queue dropped once it is older than
`PROJECTION_QUEUE_GRACE`, before counting its loads as outstanding.
"""

import datetime as dt

import pytest
from django.utils import timezone

from django_connectors.enums import ProjectionRunStatus, RunTrigger
from django_connectors.models import ProjectionRun
from django_connectors.services import projections as projection_services
from django_connectors.services import runs as run_services
from tests.conftest import make_projection, memory_config

pytestmark = pytest.mark.django_db

RECORD = {"id": "e1", "happened_at": "2024-03-01T10:00:00Z", "kind": "k"}

#: What the configured callable was handed, per test.
HANDED = []


def enqueue(projection_run):
    HANDED.append(projection_run)


@pytest.fixture(autouse=True)
def clear_handed():
    HANDED.clear()
    yield
    HANDED.clear()


@pytest.fixture
def queued_settings(connectors_settings, settings):
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "PROJECTION_DISPATCH": "tests.test_projection_dispatch.enqueue",
    }
    return settings.DJANGO_CONNECTORS


def test_inline_is_the_default(
    connectors_settings, make_binding, events_target, writer
):
    binding = make_binding(config=memory_config(batches=[[RECORD], [RECORD]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    make_projection(binding)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert len(writer.records) == 1
    assert not HANDED


def test_a_dispatch_callable_receives_a_queued_run_and_nothing_executes(
    queued_settings, make_binding, events_target, writer
):
    binding = make_binding(config=memory_config(batches=[[RECORD], [RECORD]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    make_projection(binding)

    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)

    assert len(HANDED) == 1
    assert HANDED[0].status == ProjectionRunStatus.QUEUED
    assert writer.records == []


def test_executing_a_queued_run_projects_it_once(
    queued_settings, make_binding, events_target, writer
):
    binding = make_binding(config=memory_config(batches=[[RECORD], [RECORD]]))
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    make_projection(binding)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    queued = HANDED[0]

    first = projection_services.execute_projection_run(queued.id)
    assert first.status == ProjectionRunStatus.SUCCEEDED
    assert len(writer.records) == 1

    # Delivered again: the row says it already ran, so nothing happens.
    again = projection_services.execute_projection_run(queued.id)
    assert again.status == ProjectionRunStatus.SUCCEEDED
    assert len(writer.records) == 1


def test_the_sweeper_executes_a_run_the_queue_dropped(
    queued_settings, make_binding, events_target, writer
):
    second = {**RECORD, "id": "e2"}
    binding = make_binding(config=memory_config(batches=[[RECORD], [second]]))
    make_projection(binding)  # before any run: every load goes through the queue
    run_services.run_binding(binding, trigger=RunTrigger.INITIAL)
    run_services.run_binding(binding, trigger=RunTrigger.SCHEDULED)
    assert [r.status for r in HANDED] == [ProjectionRunStatus.QUEUED] * 2

    # Within the grace period the sweeper leaves them to the queue — and does
    # not count their loads as outstanding, so it queues nothing new either.
    assert projection_services.dispatch_pending_projections() == []
    assert ProjectionRun.objects.filter(status=ProjectionRunStatus.QUEUED).count() == 2

    ProjectionRun.objects.update(created_at=timezone.now() - dt.timedelta(hours=1))
    healed = projection_services.dispatch_pending_projections()
    assert [r.id for r in healed] == [r.id for r in HANDED]
    assert {r.status for r in healed} == {ProjectionRunStatus.SUCCEEDED}
    assert len(writer.records) == 2
    assert not ProjectionRun.objects.filter(status=ProjectionRunStatus.QUEUED).exists()


def test_the_celery_enqueue_callable_hands_the_run_id_to_the_task(monkeypatch):
    pytest.importorskip("celery")
    from django_connectors.scheduler import celery as celery_module

    sent = []

    class Task:
        def delay(self, run_id):
            sent.append(run_id)

    monkeypatch.setattr(
        celery_module, "register_tasks", lambda: {"execute_projection_run_task": Task()}
    )

    class Run:
        id = "abc"

    celery_module.enqueue_projection_run(Run())
    assert sent == ["abc"]


def test_beat_schedule_can_route_every_entry_to_one_queue():
    from django_connectors.scheduler.celery import beat_schedule

    plain = beat_schedule()
    assert all("options" not in entry for entry in plain.values())
    routed = beat_schedule(queue="connectors")
    assert {entry["options"]["queue"] for entry in routed.values()} == {"connectors"}
    assert set(routed) == set(plain)
