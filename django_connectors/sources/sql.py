"""Landing rows out of a customer's own database or warehouse.

Snowflake, Postgres, MySQL, anything SQLAlchemy speaks. The Binding declares
either a raw query or a table, and it lands as a single resource.

This is the one module in the package that composes SQL text, and it is worth
being explicit about why that is not a contradiction of
``landing/access.py``'s "no SQL text anywhere" rule: that rule is about the
*landing* database, whose tenancy filter must never be re-implemented. Here the
statement runs against the **customer's own** database using the customer's own
credentials, and the query is customer-authored by design. The only identifiers
this module ever interpolates — table, schema, cursor column — are validated
against :data:`IDENTIFIER_RE` first; the cursor *value* is a bound parameter.

Two deliberate refusals:

``backend="sqlalchemy"``, always
    dlt's arrow and pandas backends yield batches rather than dicts, and a
    batch cannot be annotated with the tenant metadata every landing row must
    carry — dlt raises "object does not support item assignment". Rows would
    have to land unscoped, so the configuration is rejected at build time
    instead.

no ``sql_database`` reflection
    ``sql_database``'s ``query_adapter_callback`` at the default reflection
    level lands the *union* of the reflected table's columns and the query's
    projection, with NULLs in every column the query does not produce. A plain
    ``dlt.resource`` over the statement lands exactly what was asked for.
"""

import re
from typing import ClassVar

from django_connectors.errors import scrub
from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.sources.base import SourceDefinition

# Bare identifiers only. Anything interpolated into a statement must match this;
# everything else travels as a bound parameter.
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

DEFAULT_RESOURCE_NAME = "rows"
DEFAULT_CHUNK_SIZE = 1000

# Incremental keys a Binding may declare; they become ``Incremental(**kwargs)``.
INCREMENTAL_KEYS = frozenset({"cursor_path", "initial_value", "end_value"})

# DBAPI modules this package can install for you. Anything else is the host's
# to provide — there is no extra for every warehouse driver in existence.
DRIVER_EXTRAS = {"pymysql": "mysql"}

# The alias the pushdown wrapper uses. MySQL and Postgres both reject a derived
# table without one.
SUBQUERY_ALIAS = "dc_source"
CURSOR_PARAM = "dc_cursor_value"


class SqlSource(SourceDefinition):
    """One query (or one table) from a customer database, landed as one resource."""

    key = "sql"
    provider = "sql"
    # Empty on purpose. SQLAlchemy is a core dependency (dlt[sqlalchemy] is the
    # landing destination), and the DBAPI driver depends on the DSN this
    # particular Binding uses — declaring every driver statically would make
    # the W005 check warn about Snowflake at hosts that only read MySQL. The
    # driver is checked against the actual DSN in :func:`create_engine`.
    required_extras: ClassVar[dict[str, str]] = {}

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run, or would land wrong data."""
        config = config or {}

        backend = config.get("backend", "sqlalchemy")
        if backend != "sqlalchemy":
            raise ConfigurationError(
                f"sql source backend must be 'sqlalchemy', got {backend!r}. The "
                f"arrow and pandas backends yield batches, and a batch cannot "
                f"be annotated with the tenant metadata every landing row "
                f'carries (dlt raises "object does not support item '
                f'assignment"), so those rows would land with no binding id.'
            )

        query = config.get("query")
        table = config.get("table")
        if bool(query) == bool(table):
            raise ConfigurationError(
                "sql source config needs exactly one of 'query' (raw SQL) or "
                "'table' (a table name)."
            )
        if query is not None and not isinstance(query, str):
            raise ConfigurationError("'query' must be a string of SQL")
        if table is not None:
            _validate_identifier(table, "table")
        if config.get("db_schema"):
            _validate_identifier(config["db_schema"], "db_schema")

        disposition = config.get("write_disposition", "merge")
        if disposition == "merge" and not config.get("primary_key"):
            raise ConfigurationError(
                "sql source uses merge disposition and must declare "
                "'primary_key' — merge without a key cannot identify the rows "
                "it replaces."
            )

        incremental = config.get("incremental")
        if incremental is not None:
            if not isinstance(incremental, dict):
                raise ConfigurationError("'incremental' must be an object")
            cursor_path = incremental.get("cursor_path")
            if not cursor_path:
                raise ConfigurationError(
                    "'incremental' must declare 'cursor_path' — the column the "
                    "rows are ordered by."
                )
            # It is interpolated into the pushdown predicate, so it has to be a
            # plain column name and not an expression.
            _validate_identifier(cursor_path, "incremental.cursor_path")
            unknown = set(incremental) - INCREMENTAL_KEYS
            if unknown:
                raise ConfigurationError(
                    f"'incremental' has unknown key(s) {sorted(unknown)}; "
                    f"supported: {sorted(INCREMENTAL_KEYS)}."
                )

        chunk_size = config.get("chunk_size", DEFAULT_CHUNK_SIZE)
        if not isinstance(chunk_size, int) or chunk_size < 1:
            raise ConfigurationError("'chunk_size' must be a positive integer")

        _validate_identifier(_resource_name(config), "resource")
        # Cheap and worth doing at save time whenever the DSN is known here.
        if config.get("url"):
            create_engine(config["url"]).dispose()
        return None

    # --- extraction --------------------------------------------------------

    def incremental_for(self, resource_name, binding):
        """Cursor kwargs only. See ``SourceDefinition.incremental_for``."""
        config = binding.config or {}
        if resource_name != _resource_name(config):
            return None
        incremental = config.get("incremental")
        if not incremental:
            return None
        return {key: incremental[key] for key in INCREMENTAL_KEYS if key in incremental}

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)

        engine = create_engine(connection_url(config, credentials))
        name = _resource_name(config)
        chunk_size = config.get("chunk_size", DEFAULT_CHUNK_SIZE)

        def emit():
            from sqlalchemy import text
            from sqlalchemy.exc import SQLAlchemyError

            statement, params = build_query(config, _stored_cursor_value(dlt, config))
            try:
                with engine.connect() as connection:
                    # stream_results keeps a large table off the worker's heap;
                    # without it SQLAlchemy buffers the whole result set first.
                    result = connection.execution_options(
                        stream_results=True, max_row_buffer=chunk_size
                    ).execute(text(statement), params)
                    for partition in result.mappings().partitions(chunk_size):
                        # Dicts, never the driver's row objects: the landing
                        # layer stamps tenant columns onto each record in place.
                        yield [dict(row) for row in partition]
            except SQLAlchemyError as exc:
                raise SourceError(
                    f"the query against the source database failed: {scrub(exc)}"
                ) from exc
            finally:
                engine.dispose()

        resource = dlt.resource(
            emit,
            name=name,
            primary_key=config.get("primary_key"),
            write_disposition=config.get("write_disposition", "merge"),
        )()
        return dlt.source(lambda: [resource], name=self.key, section=self.key)()

    def check_connection(self, *, connection, credentials, binding=None):
        """Open one connection and run ``SELECT 1``."""
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        engine = create_engine(connection_url(config, credentials))
        try:
            with engine.connect() as opened:
                opened.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise SourceError(
                f"could not reach the source database: {scrub(exc)}"
            ) from exc
        finally:
            engine.dispose()
        return "ok"


# --- helpers ---------------------------------------------------------------


def _resource_name(config):
    """The dlt resource name, which becomes part of the landing table name."""
    return config.get("resource") or config.get("table") or DEFAULT_RESOURCE_NAME


def build_query(config, cursor_value=None):
    """Return ``(statement, params)`` for one extraction.

    The incremental predicate is pushed down rather than left to dlt's
    client-side filter, because the alternative is shipping the whole table
    across the network every run and discarding almost all of it.

    ``>=``, never ``>``: dlt's ``Incremental`` uses a closed lower bound, and a
    record written at *exactly* the stored cursor value is a real record — the
    library's whole incremental design exists to stop that one being dropped.
    A strict inequality here would reintroduce the loss below dlt's notice,
    where no test of dlt's behaviour could see it.
    """
    base = config.get("query")
    if not base:
        prefix = f"{config['db_schema']}." if config.get("db_schema") else ""
        base = f"SELECT * FROM {prefix}{config['table']}"

    if cursor_value is None:
        return base, {}

    column = (config.get("incremental") or {})["cursor_path"]
    # Wrapped rather than appended: the customer's query may already carry its
    # own WHERE, GROUP BY or ORDER BY, and `SELECT *` over the derived table
    # preserves exactly the projection they asked for.
    statement = (
        f"SELECT * FROM ({base}) AS {SUBQUERY_ALIAS} WHERE {column} >= :{CURSOR_PARAM}"
    )
    return statement, {CURSOR_PARAM: cursor_value}


def _stored_cursor_value(dlt, config):
    """The incremental's last value, read out of dlt's own resource state.

    Read rather than received: the library builds the ``Incremental`` (see
    ``landing/instrument.py``), so the resource function never gets handed one.
    Falling back to ``initial_value`` matters on the first run — otherwise the
    first extraction scans everything the customer has ever had.
    """
    incremental = config.get("incremental") or {}
    cursor_path = incremental.get("cursor_path")
    if not cursor_path:
        return None
    try:
        stored = (dlt.current.resource_state().get("incremental") or {}).get(
            cursor_path
        ) or {}
    except Exception:
        # No pipeline state yet, or dlt changed where it keeps it. Falling back
        # to a full scan is slow but never wrong; dlt still filters the records.
        stored = {}
    return stored.get("last_value") or incremental.get("initial_value")


def connection_url(config, credentials):
    """The SQLAlchemy DSN for the customer's database."""
    url = _credentials_url(credentials) or config.get("url")
    if not url:
        raise ConfigurationError(
            "sql source needs a SQLAlchemy DSN. Prefer returning one from the "
            "Connection's auth backend; a DSN placed in Binding.config is "
            "stored in plaintext and rendered in the admin."
        )
    return url


def _credentials_url(credentials):
    if credentials is None:
        return None
    if isinstance(credentials, str):
        return credentials
    for key in ("url", "dsn", "connection_string"):
        value = (
            credentials.get(key)
            if isinstance(credentials, dict)
            else getattr(credentials, key, None)
        )
        if value:
            return value
    return None


def create_engine(url):
    """A SQLAlchemy engine, with a missing driver reported as configuration.

    SQLAlchemy imports the DBAPI when the engine is constructed, so doing this
    up front turns "ModuleNotFoundError: No module named 'pymysql'" raised from
    inside dlt's extract step into a message naming the extra to install.
    """
    from sqlalchemy import create_engine as sqlalchemy_create_engine
    from sqlalchemy.exc import ArgumentError, NoSuchModuleError

    try:
        return sqlalchemy_create_engine(url)
    except (NoSuchModuleError, ImportError) as exc:
        module = (getattr(exc, "name", "") or "").split(".")[0]
        extra = DRIVER_EXTRAS.get(module)
        hint = (
            f"pip install 'django-connectors[{extra}]'"
            if extra
            else "install the DBAPI driver this dialect needs"
        )
        raise ConfigurationError(
            f"the database driver for this DSN is not installed: {scrub(exc)}. {hint}"
        ) from exc
    except ArgumentError as exc:
        raise ConfigurationError(
            f"the sql source DSN is not a SQLAlchemy URL: {scrub(exc)}"
        ) from exc


def _validate_identifier(value, label):
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise ConfigurationError(
            f"{label} must be a plain identifier (letters, digits, "
            f"underscores), got {value!r}. It is interpolated into SQL, so an "
            f"expression is refused rather than escaped."
        )
    return value
