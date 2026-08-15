"""Optional DRF API.

Requires the ``drf`` extra and is inert until the host routes
``django_connectors.api.urls``. Nothing in the package imports this module, so a
host without djangorestframework installed is unaffected.

Provisional in v0.1: the shape may change before 1.0. The **service layer** is
the stable integration surface.
"""
