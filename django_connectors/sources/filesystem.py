"""Files on disk, landed as records.

JSONL costs nothing: ``fsspec`` and dlt's ``read_jsonl`` are both core dlt, so
this source works on a bare ``pip install django-connectors``. CSV and Parquet
do not — ``read_csv`` imports pandas and ``read_parquet`` imports pyarrow, and
both do it **lazily, inside the reader**, which means a Binding configured for
Parquet on a host without pyarrow saves cleanly, schedules cleanly, and then
dies with a bare ``ModuleNotFoundError`` inside a customer's Run at 3am. So the
format's dependency is checked in :meth:`FilesystemSource.validate_config`,
i.e. when the Binding is saved, and the error names the extra to install.

Only ``file://`` is supported in v0.1. The config shape is dlt's own
``bucket_url`` + ``file_glob``, so adding S3/GCS/Azure later is a matter of
allowing more schemes and passing credentials through — no Binding needs
rewriting.

The path is host-filesystem access driven by Binding configuration, so it is
only as safe as the people who can edit Bindings: a worker that can read
``/etc`` will read ``/etc`` if asked. Relative paths are refused outright
because they would resolve against whatever working directory the worker
happened to start in.
"""

import os
from typing import ClassVar
from urllib.parse import urlsplit

from django_connectors.exceptions import ConfigurationError
from django_connectors.sources.base import SourceDefinition

# format -> (module it needs at read time, the extra that installs it).
# None means "core dlt is enough".
FORMAT_REQUIREMENTS = {
    "jsonl": None,
    "csv": ("pandas", "csv"),
    "parquet": ("pyarrow", "parquet"),
}

GLOB_CHARACTERS = "*?["


class FilesystemSource(SourceDefinition):
    """Local files matched by a glob, parsed into records."""

    key = "filesystem"
    provider = "filesystem"
    # Deliberately empty: what this source needs depends on the *format* each
    # Binding declares, and a static declaration would make the W005 check warn
    # about pyarrow at every host that only ever reads JSONL. The per-format
    # requirement is enforced in validate_config instead, where the format is
    # actually known.
    required_extras: ClassVar[dict[str, str]] = {}

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run — including a missing extra."""
        resources = (config or {}).get("resources")
        if not isinstance(resources, dict) or not resources:
            raise ConfigurationError(
                "filesystem source config needs a non-empty 'resources' mapping "
                "of {resource_name: file config}."
            )

        for name, spec in resources.items():
            if not isinstance(spec, dict):
                raise ConfigurationError(f"resources.{name} must be an object")

            file_format = spec.get("format", "jsonl")
            if file_format not in FORMAT_REQUIREMENTS:
                raise ConfigurationError(
                    f"resources.{name}.format must be one of "
                    f"{sorted(FORMAT_REQUIREMENTS)}, got {file_format!r}."
                )
            requirement = FORMAT_REQUIREMENTS[file_format]
            if requirement is not None:
                module, extra = requirement
                if not module_available(module):
                    raise ConfigurationError(
                        f"resources.{name} reads {file_format} files, which "
                        f"needs {module!r}. dlt imports it lazily inside the "
                        f"reader, so without this check the failure would land "
                        f"inside a Run instead of here. Install it with: "
                        f"pip install 'django-connectors[{extra}]'"
                    )

            # Raises on a cloud scheme or a relative local path.
            resolve_location(name, spec)

            disposition = spec.get("write_disposition", "merge")
            if disposition == "merge" and not spec.get("primary_key"):
                raise ConfigurationError(
                    f"resources.{name} uses merge disposition and must declare "
                    f"'primary_key'. Without a stable key, re-reading a file "
                    f"appends every row again instead of updating it."
                )
        return None

    # --- extraction --------------------------------------------------------

    def incremental_for(self, resource_name, binding):
        """Cursor kwargs only. See ``SourceDefinition.incremental_for``."""
        spec = ((binding.config or {}).get("resources") or {}).get(resource_name) or {}
        cursor = spec.get("cursor")
        return {"cursor_path": cursor} if cursor else None

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)

        resources = [
            self._build_resource(name, spec)
            for name, spec in config["resources"].items()
        ]
        # dlt.source() as a function, not a decorator: the decorator takes the
        # source name from __name__ and cannot produce a runtime-chosen one.
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _build_resource(self, name, spec):
        from dlt.sources.filesystem import filesystem

        bucket_url, file_glob = resolve_location(name, spec)
        files = filesystem(bucket_url=bucket_url, file_glob=file_glob)
        # `with_name` renames the piped resource; without it every Binding's
        # resource would be called "filesystem" and the landing table name
        # would carry no hint of what is in it.
        resource = (files | _reader(spec)).with_name(name)

        hints = {"write_disposition": spec.get("write_disposition", "merge")}
        if spec.get("primary_key"):
            hints["primary_key"] = spec["primary_key"]
        resource.apply_hints(**hints)
        return resource


def _reader(spec):
    """The dlt transformer that turns matched files into records.

    Every one of these yields dicts (``read_parquet`` only yields Arrow batches
    with ``use_pyarrow=True``, which is not offered), because a batch cannot be
    annotated with the tenant metadata the landing layer stamps on each record.
    """
    from dlt.sources.filesystem import read_csv, read_jsonl, read_parquet

    readers = {"jsonl": read_jsonl, "csv": read_csv, "parquet": read_parquet}
    return readers[spec.get("format", "jsonl")]()


def resolve_location(name, spec):
    """Return ``(bucket_url, file_glob)`` for a resource, or explain what is wrong.

    Accepts either dlt's own two-part shape (``bucket_url`` + ``file_glob``) or
    a single ``path`` that may end in a glob — ``/data/events/*.jsonl`` splits
    into a directory and a pattern.
    """
    bucket_url = spec.get("bucket_url")
    file_glob = spec.get("file_glob")

    if not bucket_url:
        path = spec.get("path")
        if not path or not isinstance(path, str):
            raise ConfigurationError(
                f"resources.{name} needs a 'path' (a directory or a glob) or a "
                f"'bucket_url'."
            )
        head, _, tail = path.rpartition("/")
        if any(character in tail for character in GLOB_CHARACTERS):
            bucket_url, file_glob = head or "/", file_glob or tail
        else:
            bucket_url = path

    scheme = urlsplit(bucket_url).scheme
    if scheme and scheme != "file":
        raise ConfigurationError(
            f"resources.{name}: bucket scheme {scheme!r} is not supported in "
            f"v0.1; only local files ('file://' or an absolute path) are. The "
            f"config shape is dlt's own, so cloud buckets need no Binding "
            f"changes when they land."
        )
    if not scheme:
        if not os.path.isabs(bucket_url):
            raise ConfigurationError(
                f"resources.{name}: {bucket_url!r} must be an absolute path. A "
                f"relative one resolves against whatever working directory the "
                f"worker started in, which is not the same on every machine."
            )
        bucket_url = f"file://{bucket_url}"

    return bucket_url, file_glob or "*"


def module_available(module_name):
    """Whether `module_name` could be imported, without importing it.

    Separate function so it can be substituted in tests: the "extra is missing"
    path is the one the whole optional-dependency design rests on, and it is
    invisible in a test environment that installs every extra.
    """
    from importlib.util import find_spec

    try:
        return find_spec(module_name) is not None
    except (ImportError, ValueError):
        return False
