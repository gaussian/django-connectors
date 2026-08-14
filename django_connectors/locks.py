"""Per-Binding execution leases.

dlt has **no cross-process lock** on a pipeline working directory. Two processes
on the same pipeline name were measured producing hard failures, one process
loading the *other's* load package under its own LoadInfo, and one case
reporting success while its own 200 rows never landed. So exclusion is this
library's responsibility, not dlt's.

A database lease rather than ``SELECT GET_LOCK()``: that is MySQL-only, and a
lock exercised by only one CI job is a lock nobody tests. Leases expire, so a
worker killed mid-run does not block its Binding forever.
"""

import uuid

from django.db import IntegrityError, transaction
from django.utils import timezone

from django_connectors.conf import conf
from django_connectors.models import BindingLock


def acquire(binding, *, timeout=None):
    """Take the lease for `binding`, or return None if another worker holds it.

    Returns an opaque token that must be presented to :func:`release`.
    """
    now = timezone.now()
    expires_at = now + (timeout or binding.run_timeout or conf.DEFAULT_RUN_TIMEOUT)
    token = uuid.uuid4()

    # Atomic conditional UPDATE: takes over only a lease that has expired. Two
    # workers racing here produce exactly one non-zero rowcount.
    taken_over = BindingLock.objects.filter(
        binding=binding, expires_at__lte=now
    ).update(token=token, acquired_at=now, expires_at=expires_at)
    if taken_over:
        return token

    try:
        with transaction.atomic():
            BindingLock.objects.create(
                binding=binding, token=token, expires_at=expires_at
            )
    except IntegrityError:
        # A live lease already exists.
        return None
    return token


def release(binding, token):
    """Release the lease iff `token` still holds it.

    Presenting the token matters: a worker whose lease expired and was taken
    over by someone else must not be able to release the new holder's lock.
    """
    deleted, _ = BindingLock.objects.filter(binding=binding, token=token).delete()
    return bool(deleted)


def renew(binding, token, *, timeout=None):
    """Extend a held lease, for runs that legitimately outlast the timeout."""
    expires_at = timezone.now() + (
        timeout or binding.run_timeout or conf.DEFAULT_RUN_TIMEOUT
    )
    return bool(
        BindingLock.objects.filter(binding=binding, token=token).update(
            expires_at=expires_at
        )
    )


def is_locked(binding):
    return BindingLock.objects.filter(
        binding=binding, expires_at__gt=timezone.now()
    ).exists()


def reap_expired():
    """Delete every expired lease. Returns how many.

    Called by the scheduler; taking over an expired lease does not require it,
    but it keeps the table clean and makes abandoned runs visible.
    """
    deleted, _ = BindingLock.objects.filter(expires_at__lte=timezone.now()).delete()
    return deleted
