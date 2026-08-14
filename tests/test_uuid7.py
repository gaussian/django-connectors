"""UUIDv7 must be sortable by creation time — that is the only reason it exists.

Run and ProjectionRun are append-heavy and almost always read newest-first. A v4
primary key scatters InnoDB inserts across the index and carries no ordering.
"""

import uuid

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
