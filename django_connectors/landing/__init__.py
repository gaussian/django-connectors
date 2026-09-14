"""The dlt landing layer.

Named `landing`, not `dlt`: a subpackage called `dlt` would shadow the real one
in any module that does `from django_connectors import dlt`, and renaming it
after a release would be a breaking change.
"""
