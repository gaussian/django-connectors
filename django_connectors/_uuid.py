"""RFC 9562 UUIDv7 generation.

``Run`` and ``ProjectionRun`` are append-heavy and are almost always read in
creation order. UUIDv4 primary keys scatter InnoDB inserts across the index and
give no natural ordering, so those two models use UUIDv7: a 48-bit millisecond
timestamp followed by randomness, which sorts by creation time as a byte string.

``uuid.uuid7`` is stdlib from Python 3.14. This repo's floor is 3.12, so the
implementation below is used there and the stdlib one is preferred when present.
"""

import os
import threading
import time
import uuid

__all__ = ["uuid7"]

_lock = threading.Lock()
_last_timestamp_ms = -1
_counter = 0

# 12 bits of rand_a are used as a monotonic counter within a millisecond, so a
# burst of ids created in the same millisecond still sorts in creation order.
_COUNTER_MAX = 0xFFF


def _uuid7_fallback() -> uuid.UUID:
    global _last_timestamp_ms, _counter

    with _lock:
        timestamp_ms = time.time_ns() // 1_000_000
        if timestamp_ms == _last_timestamp_ms:
            _counter += 1
            if _counter > _COUNTER_MAX:
                # More than 4096 ids in one millisecond: wait for the clock
                # rather than let the counter wrap and break ordering.
                while timestamp_ms <= _last_timestamp_ms:
                    timestamp_ms = time.time_ns() // 1_000_000
                _counter = 0
        else:
            if timestamp_ms < _last_timestamp_ms:
                # The clock went backwards (NTP step). Keep issuing ordered ids
                # from the last observed millisecond instead of emitting values
                # that sort before ids we have already handed out.
                timestamp_ms = _last_timestamp_ms
                _counter += 1
                if _counter > _COUNTER_MAX:
                    # The counter must stay inside its 12 bits: it is ORed in at
                    # bits 64-75, so one more increment carries into the version
                    # nibble and starts emitting ids that are neither v7 nor in
                    # order. Waiting for the clock (what the same-millisecond
                    # branch does) is not available here — the step can be hours
                    # wide — so borrow the next millisecond instead. It is
                    # already ahead of the wall clock by construction.
                    timestamp_ms = _last_timestamp_ms + 1
                    _counter = 0
            else:
                _counter = 0
        _last_timestamp_ms = timestamp_ms
        counter = _counter

    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFFFFFFFFFFFFFF

    value = (timestamp_ms & 0xFFFFFFFFFFFF) << 80
    value |= 0x7 << 76
    value |= counter << 64
    value |= 0x2 << 62
    value |= rand_b
    return uuid.UUID(int=value)


uuid7 = getattr(uuid, "uuid7", _uuid7_fallback)
