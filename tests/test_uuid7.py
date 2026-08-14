"""UUIDv7 must be sortable by creation time — that is the only reason it exists.

Run and ProjectionRun are append-heavy and almost always read newest-first. A v4
primary key scatters InnoDB inserts across the index and carries no ordering.
"""

import uuid

import pytest

from django_connectors._uuid import uuid7


def test_version_and_variant_bits_are_rfc_9562():
    value = uuid7()
    assert isinstance(value, uuid.UUID)
    assert value.version == 7
    # RFC 9562 variant is 0b10 in the two most significant bits of octet 8.
    assert (value.int >> 62) & 0b11 == 0b10


def test_ids_sort_in_creation_order():
    """Includes bursts within a single millisecond, which is the hard part."""
    values = [uuid7() for _ in range(1000)]
    assert values == sorted(values)
    assert [v.bytes for v in values] == sorted(v.bytes for v in values)


def test_ids_are_unique():
    values = [uuid7() for _ in range(5000)]
    assert len(set(values)) == len(values)


def test_timestamp_is_current_wall_clock_ms():
    import time

    before = time.time_ns() // 1_000_000
    value = uuid7()
    after = time.time_ns() // 1_000_000

    timestamp_ms = value.int >> 80
    assert before <= timestamp_ms <= after


def test_ordering_holds_across_threads():
    """Ordering must come from the shared counter, not from luck."""
    import threading

    produced: list[uuid.UUID] = []
    lock = threading.Lock()

    def worker():
        for _ in range(200):
            # Generate and record under one lock. Splitting them would only
            # prove that two threads can interleave between the two statements,
            # which says nothing about the generator.
            with lock:
                produced.append(uuid7())

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(set(produced)) == len(produced)
    # Ids appended under the lock must already be non-decreasing.
    assert produced == sorted(produced)


# --- the fallback's own state machine ---------------------------------------
#
# These drive `_uuid7_fallback` directly rather than `uuid7`: from Python 3.14
# the stdlib implementation is preferred, and the branch under test is ours.


@pytest.fixture
def rewound_clock():
    """Leave the fallback believing the clock just stepped an hour backwards.

    Restores the module globals afterwards, or every later test in this file
    would generate ids stamped an hour in the future.
    """
    import time

    from django_connectors import _uuid

    previous = (_uuid._last_timestamp_ms, _uuid._counter)
    ahead = time.time_ns() // 1_000_000 + 3_600_000
    _uuid._last_timestamp_ms = ahead
    _uuid._counter = 0
    try:
        yield ahead
    finally:
        _uuid._last_timestamp_ms, _uuid._counter = previous


def test_a_backwards_clock_step_cannot_overflow_the_counter(rewound_clock):
    """The counter is 12 bits and sits directly under the version nibble.

    Unbounded, the 4097th id issued inside a backwards step carries into the
    version bits: ids stop being v7 and stop sorting, which is the sole reason
    this module exists. The step's whole width is spent in this branch, so
    4096 ids is not a theoretical burst — it is a busy minute.
    """
    from django_connectors._uuid import _uuid7_fallback

    values = [_uuid7_fallback() for _ in range(40_000)]

    assert {value.version for value in values} == {7}
    assert all((value.int >> 62) & 0b11 == 0b10 for value in values)
    assert values == sorted(values)
    assert len(set(values)) == len(values)


def test_a_backwards_clock_step_still_sorts_after_ids_issued_before_it(
    rewound_clock,
):
    """Ordering across the step is the point: ids issued before it must sort first."""
    from django_connectors._uuid import _uuid7_fallback

    earlier = _uuid7_fallback()
    later = [_uuid7_fallback() for _ in range(5_000)]

    assert earlier < min(later)
