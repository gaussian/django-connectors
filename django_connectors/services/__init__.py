"""Service layer — the documented, stable integration surface.

Every function takes explicit objects/ids plus an optional `actor`, never a
``request``, so the same call works from a view, a Celery task, a management
command or a test.
"""
