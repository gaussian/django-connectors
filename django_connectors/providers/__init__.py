"""Connectors for named third-party providers.

A namespace package and nothing else. Importing it must never import a provider
subpackage: each one is reached by the dotted path a host puts in
``DJANGO_CONNECTORS["SOURCES"]`` / ``["AUTH_BACKENDS"]``, and the registries
resolve those lazily so that a host synchronizing only Sheets never pays for
Gmail's module, let alone another provider's optional SDK.

Every source under here obeys the same three rules as the built-in sources:
no module-scope ``import dlt``, no ``dlt.sources.incremental`` constructed by
the source (see :meth:`~django_connectors.sources.base.SourceDefinition.incremental_for`),
and ``write_disposition`` stated explicitly on every resource.
"""
