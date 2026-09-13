"""Files on disk or in a bucket, landed as records.

JSONL costs nothing: ``fsspec`` and dlt's ``read_jsonl`` are both core dlt, so
this source works on a bare ``pip install django-connectors``. CSV and Parquet
do not — ``read_csv`` imports pandas and ``read_parquet`` imports pyarrow, and
both do it **lazily, inside the reader**, which means a Binding configured for
Parquet on a host without pyarrow saves cleanly, schedules cleanly, and then
dies with a bare ``ModuleNotFoundError`` inside a customer's Run at 3am. The
same is true of the bucket drivers: ``s3fs``, ``gcsfs`` and ``adlfs`` are all
imported by fsspec on first use. So the format's dependency *and* the scheme's
are checked in :meth:`FilesystemSource.validate_config`, i.e. when the Binding
is saved, and the error names the extra to install.

Three ways to address what to read, all through one ``path``::

    "/data/exports"                one directory, every file in it
    "/data/exports/**/*.csv"       a glob, recursive with ``**``
    "/data/exports/orders.csv"     one file — a name with an extension and
                                   no wildcard is taken as a file, not a
                                   directory, because a directory glob over a
                                   file lands nothing and reports success

dlt's own two-part shape, ``bucket_url`` + ``file_glob``, is accepted as well
and is the way to be explicit when a directory name happens to contain a dot.

Bucket credentials come from the Connection's auth backend, never from
``Binding.config``: a DSN or a key pair in config is rendered in the admin and
returned by the API. For ``s3://`` the backend returns the boto names
(``aws_access_key_id``, ``aws_secret_access_key``, optionally
``aws_session_token``, ``region_name``, ``endpoint_url`` for S3-compatible
stores); returning nothing means "use the worker's ambient credentials" — an
IAM role, an instance profile. For any scheme the backend may instead return a
ready ``fsspec`` filesystem, which is also how the test suite drives this
source through ``memory://`` with no network.

Local paths are host-filesystem access driven by Binding configuration, so
they are only as safe as the people who can edit Bindings: a worker that can
read ``/etc`` will read ``/etc`` if asked. Relative paths are refused outright
because they would resolve against whatever working directory the worker
happened to start in.
"""

import os
from typing import ClassVar
from urllib.parse import urlsplit

from django_connectors.exceptions import ConfigurationError
from django_connectors.sources.base import SourceDefinition, as_config

# format -> (module it needs at read time, the extra that installs it).
# None means "core dlt is enough".
FORMAT_REQUIREMENTS = {
    "jsonl": None,
    "csv": ("pandas", "csv"),
    "parquet": ("pyarrow", "parquet"),
}

GLOB_CHARACTERS = "*?["

#: Bucket schemes, mapped to (fsspec driver module, extra that installs it).
#: ``file`` needs nothing. Anything absent here is refused at save time.
SCHEME_REQUIREMENTS = {
    "file": None,
    "s3": ("s3fs", "s3"),
    "gs": ("gcsfs", "gs"),
    "gcs": ("gcsfs", "gs"),
    "az": ("adlfs", "az"),
    "abfss": ("adlfs", "az"),
}

#: Credential keys the S3 spec takes, as the boto names a host already knows.
AWS_CREDENTIAL_KEYS = (
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "region_name",
    "endpoint_url",
)


class FilesystemSource(SourceDefinition):
    """Local files matched by a glob, parsed into records."""

    key = "filesystem"
    provider = "filesystem"
    #: Schemes a Binding may use. A subclass widens this — the test suite adds
    #: ``memory`` — so that an in-process filesystem never becomes something a
    #: customer can point a production Binding at.
    allowed_schemes = frozenset(SCHEME_REQUIREMENTS)
    # Deliberately empty: what this source needs depends on the *format* each
    # Binding declares, and a static declaration would make the W005 check warn
    # about pyarrow at every host that only ever reads JSONL. The per-format
    # requirement is enforced in validate_config instead, where the format is
    # actually known.
    required_extras: ClassVar[dict[str, str]] = {}

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run — including a missing extra."""
        resources = as_config(config).get("resources")
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

            # Raises on an unknown scheme or a relative local path.
            bucket_url, _ = resolve_location(name, spec, allowed=self.allowed_schemes)
            requirement = SCHEME_REQUIREMENTS.get(urlsplit(bucket_url).scheme)
            if requirement is not None:
                module, extra = requirement
                if not module_available(module):
                    raise ConfigurationError(
                        f"resources.{name} reads from {bucket_url!r}, which "
                        f"needs {module!r}. fsspec imports it lazily on first "
                        f"use, so without this check the failure would land "
                        f"inside a Run instead of here. Install it with: "
                        f"pip install 'django-connectors[{extra}]'"
                    )

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
            self._build_resource(name, spec, credentials)
            for name, spec in config["resources"].items()
        ]
        # dlt.source() as a function, not a decorator: the decorator takes the
        # source name from __name__ and cannot produce a runtime-chosen one.
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _build_resource(self, name, spec, credentials):
        from dlt.sources.filesystem import filesystem

        bucket_url, file_glob = resolve_location(
            name, spec, allowed=self.allowed_schemes
        )
        kwargs = {"bucket_url": bucket_url, "file_glob": file_glob}
        # Local files take no credentials; passing None would make dlt look for
        # its own configured secret and fail the Run on a missing setting.
        if urlsplit(bucket_url).scheme != "file":
            kwargs["credentials"] = bucket_credentials(bucket_url, credentials)
        files = filesystem(**kwargs)
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


def resolve_location(name, spec, *, allowed=frozenset(SCHEME_REQUIREMENTS)):
    """Return ``(bucket_url, file_glob)`` for a resource, or explain what is wrong.

    Accepts either dlt's own two-part shape (``bucket_url`` + ``file_glob``) or
    a single ``path``. The path's last segment decides how it is read: a glob
    (``*.jsonl``) is split off; a name with an extension (``orders.csv``) is
    taken as one file and becomes an exact glob; anything else is a directory.
    A directory glob over a file matches nothing and reports success, which is
    why a file name is recognised rather than left to chance.
    """
    bucket_url = spec.get("bucket_url")
    file_glob = spec.get("file_glob")

    if not bucket_url:
        path = spec.get("path")
        if not path or not isinstance(path, str):
            raise ConfigurationError(
                f"resources.{name} needs a 'path' (a directory, a glob or one "
                f"file) or a 'bucket_url'."
            )
        head, _, tail = path.rpartition("/")
        if any(character in tail for character in GLOB_CHARACTERS) or _looks_like_file(
            tail
        ):
            bucket_url, file_glob = head or "/", file_glob or tail
        else:
            bucket_url = path

    scheme = urlsplit(bucket_url).scheme
    if scheme and scheme not in allowed:
        raise ConfigurationError(
            f"resources.{name}: bucket scheme {scheme!r} is not supported; "
            f"supported: {sorted(allowed)}."
        )
    if not scheme:
        if not os.path.isabs(bucket_url):
            raise ConfigurationError(
                f"resources.{name}: {bucket_url!r} must be an absolute path. A "
                f"relative one resolves against whatever working directory the "
                f"worker started in, which is not the same on every machine."
            )
        bucket_url = f"file://{bucket_url}"
    elif scheme != "file" and not urlsplit(bucket_url).netloc:
        raise ConfigurationError(f"resources.{name}: {bucket_url!r} names no bucket.")

    return bucket_url, file_glob or "*"


def _looks_like_file(segment):
    """``orders.csv`` yes; ``exports``, ``.hidden`` and ``v1.2`` no."""
    stem, dot, extension = segment.rpartition(".")
    return bool(dot and stem and extension.isalnum() and not extension.isdigit())


def bucket_credentials(bucket_url, credentials):
    """What dlt's filesystem source needs to open `bucket_url`.

    A ready ``fsspec`` filesystem is passed through for any scheme. For
    ``s3://`` a mapping of boto names becomes an ``AwsCredentials``; an empty
    one means the worker's ambient credentials. Other schemes need the backend
    to return a dlt credential spec or a filesystem, because their key shapes
    (a service-account JSON, a service-principal triple) do not reduce to a
    flat mapping without guessing.
    """
    from fsspec.spec import AbstractFileSystem

    if isinstance(credentials, AbstractFileSystem):
        return credentials
    scheme = urlsplit(bucket_url).scheme
    if scheme == "s3":
        from dlt.common.configuration.specs import AwsCredentials

        if isinstance(credentials, AwsCredentials):
            return credentials
        values = {}
        if credentials is not None:
            for key in AWS_CREDENTIAL_KEYS:
                value = _read_credential(credentials, key)
                if value:
                    values[key] = value
        return AwsCredentials(**values)
    if credentials is None or isinstance(credentials, str | dict):
        raise ConfigurationError(
            f"{scheme}:// needs the Connection's auth backend to return a dlt "
            f"credential spec or an fsspec filesystem; a bare mapping is not "
            f"enough to build one without guessing its shape."
        )
    return credentials


def _read_credential(credentials, key):
    if hasattr(credentials, "get"):
        return credentials.get(key)
    return getattr(credentials, key, None)


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
