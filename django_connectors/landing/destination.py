"""Construction of the dlt landing destination.

The landing database is reached only through dlt, and deliberately is not a
Django ``DATABASES`` alias: a router's ``allow_migrate=False`` does not stop
``.using("landing")``, ``migrate --database=landing`` would still create
``django_migrations`` there, and the test runner would create a test database
for it. Keeping it out of ``DATABASES`` makes routing an ORM model into the
landing database structurally impossible rather than merely discouraged.
"""

from django_connectors.conf import conf


def landing_credentials(url=None):
    """Wrap the configured DSN in the credentials type dlt actually requires.

    A bare DSN string is not sufficient and fails unpredictably — observed as
    ``AttributeError: 'str' object has no attribute 'get_dialect_class'`` in one
    context and ``ConfigFieldMissingException`` in another, depending on what
    dlt has already resolved in the process.
    """
    from dlt.destinations.impl.sqlalchemy.configuration import SqlalchemyCredentials

    return SqlalchemyCredentials(url or conf.require("LANDING_URL"))


def landing_destination(url=None):
    """The dlt destination factory for the landing database.

    Never log, ``repr()`` or interpolate the return value: dlt renders the
    plaintext password in ``repr(factory)`` and in
    ``credentials.to_native_representation()``. Only ``str(credentials)`` masks.
    """
    from dlt.destinations import sqlalchemy

    return sqlalchemy(
        credentials=landing_credentials(url),
        # dlt creates no index of any kind on MySQL, and a declared primary key
        # would be worse than none: `create_primary_keys=True` hard-fails unless
        # every key column carries a `precision` hint, emits columns in an order
        # different from the declared one, and — being a real uniqueness
        # constraint — turns a source that emits a duplicate key within one
        # batch from a soft dedupe into a permanently poisoned load package.
        # Indexes are provisioned separately after the first successful load.
        create_primary_keys=False,
        create_unique_indexes=False,
    )


def landing_dataset_name():
    return conf.LANDING_DATASET


def build_pipeline(binding, *, url=None, pipelines_dir=None):
    """A pipeline dedicated to one Binding.

    Two processes must never open the same pipeline concurrently: dlt has no
    cross-process lock on a pipeline working directory, and concurrent access
    was measured producing hard failures, one process loading another's load
    package under its own LoadInfo, and a silent 200-row loss reported as
    success. `BindingLock` is what prevents that; this function assumes it is
    already held.
    """
    import dlt

    return dlt.pipeline(
        pipeline_name=binding.pipeline_name,
        destination=landing_destination(url),
        dataset_name=landing_dataset_name(),
        pipelines_dir=pipelines_dir or conf.PIPELINES_DIR,
        progress=None,
    )
