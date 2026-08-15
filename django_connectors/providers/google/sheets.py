"""Google Sheets ranges, landed as tables.

A spreadsheet is not a database and pretending otherwise is where connectors go
wrong. Four decisions carry this module.

**There is no incremental cursor, so the default is a keyed snapshot.** The
Sheets API exposes no "changed since" filter and no per-row revision, so every
run reads the whole range. What makes that safe rather than duplicative is
``key_column``: declare it and rows merge on that key, so re-reading a sheet
updates rows instead of stacking copies of them. A sheet with **no** stable key
is landed with ``write_disposition="replace"`` — a full replace, stated
explicitly. It is emphatically *not* merged on row index: a single row inserted
at the top of a sheet shifts every index below it, and an index merge would
then rewrite the identity of every row in the table while reporting success.

**"Skip when unchanged" is available, opt-in, and only for keyed ranges.**
Drive's ``modifiedTime`` on the spreadsheet file tells us whether anything at
all changed, which turns an unchanged sheet into one cheap request. It is off
by default because it needs a second OAuth scope (``drive.metadata.readonly``)
that a Sheets-only Connection will not have — and a 403 from Drive would
otherwise break a sync that was working. It is refused outright for keyless
ranges, because of a behaviour verified against dlt 1.30: **a ``replace``
resource that yields no rows truncates its table.** "Nothing changed" would
therefore empty the landing table, and the next Projection run would propagate
that emptiness downstream. Under ``merge`` an empty yield is a no-op, which is
what makes the optimisation safe there and only there.

**Real sheets are ragged, and none of it may raise.** Trailing empty cells are
omitted from the API response entirely, so rows arrive at different widths;
blank rows appear in the middle; header cells are duplicated, blank, or differ
only by case and spacing. Widths are padded to the widest row observed, blank
rows are dropped, blank headers become ``column_N``, and duplicates are
suffixed. Header names are put through dlt's own naming convention first, so
that ``Order ID`` and ``order_id`` are recognised as the collision they are
*here* — rather than in the destination, where one column would silently
overwrite the other.

**A row that cannot be identified is refused, not guessed.** With
``key_column`` declared, a row whose key is blank has no identity; landing it
would merge every such row into a single one. The default is to fail the Run
naming the sheet rows, because losing rows silently is the failure this whole
package is built to prevent. ``missing_key: "skip"`` is available for sheets
where half-typed rows are normal.

**Unverified against a live provider.** Built and tested against mocked HTTP
only. Not exercised: real OAuth consent, Drive's actual ``modifiedTime``
granularity and clock behaviour, real quota throttling, spreadsheets large
enough for the API to truncate a range, and how Sheets renders every cell type
in practice (dates, durations, errors, formulas).
"""

import logging
from typing import ClassVar

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.landing.naming import normalize_identifier
from django_connectors.providers.google.auth import (
    bearer_token,
    google_client,
    google_json,
)
from django_connectors.sources.base import SourceDefinition

logger = logging.getLogger(__name__)

SHEETS_API_BASE_URL = "https://sheets.googleapis.com/v4/"
DRIVE_API_BASE_URL = "https://www.googleapis.com/drive/v3/"

MODIFIED_TIME_STATE_KEY = "sheets_drive_modified_time"

# UNFORMATTED_VALUE keeps numbers numeric instead of handing back the locale's
# display string, which is what makes a currency column land as a number.
# FORMULA is offered for auditing a sheet, never for ingesting one.
VALUE_RENDER_OPTIONS = frozenset({"FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"})
DEFAULT_VALUE_RENDER_OPTION = "UNFORMATTED_VALUE"

# Paired with UNFORMATTED_VALUE on purpose: without it, dates come back as
# Lotus-1-2-3 serial numbers and land as meaningless floats.
DATE_TIME_RENDER_OPTION = "FORMATTED_STRING"

MISSING_KEY_POLICIES = frozenset({"error", "skip"})

#: Injected onto every row: its 1-based position **within the configured
#: range**, not within the sheet — a range starting at ``A5`` numbers its first
#: row 1. It is a debugging aid and never an identity: inserting a row shifts
#: every number below it. Reserved, so a header of the same name is suffixed
#: rather than allowed to overwrite it.
ROW_NUMBER_COLUMN = "sheet_row_number"

# How many offending row numbers to name before giving up on the list. A sheet
# with 40,000 keyless rows should still produce a readable error.
MAX_REPORTED_ROWS = 20


class GoogleSheetsSource(SourceDefinition):
    """One spreadsheet, one dlt resource per declared range.

    Configuration::

        {
          "spreadsheet_id": "1AbC…",
          "ranges": {
            "orders": {"range": "Orders!A:F", "key_column": "order_id"},
            "lookup": "Config!A:B"
          },
          "header_row": true,
          "skip_unchanged": false,
          "value_render_option": "UNFORMATTED_VALUE",
          "missing_key": "error"
        }

    A range may be a bare A1 string when the defaults suffice. Every top-level
    key except ``spreadsheet_id`` and ``ranges`` is a default that a range spec
    may override.
    """

    key = "google_sheets"
    provider = "google"
    supported_auth_backends = ("google_workspace", "allauth", "static")
    # A sheet gives no way to learn that a row that is no longer present was
    # *deleted* rather than never read, so deletion propagation is genuinely
    # unsupported. The keyless path replaces the whole table, which reconciles
    # removals without ever emitting a tombstone.
    emits_tombstones = False
    required_extras: ClassVar[dict[str, str]] = {}

    #: OAuth scopes. The Drive scope is needed only for ``skip_unchanged``.
    scopes = (
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    )

    @property
    def api_base_url(self):
        """The Sheets API host. A class attribute, never Binding config.

        A customer-supplied base URL would forward the Connection's OAuth
        bearer token to a host of their choosing. Redirecting this at a test
        server is a deliberate subclass registration, as with ``RestSource``.
        """
        return SHEETS_API_BASE_URL

    @property
    def drive_api_base_url(self):
        """The Drive API host, used only by ``skip_unchanged``."""
        return DRIVE_API_BASE_URL

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run, or would land wrong data."""
        config = config or {}

        spreadsheet_id = config.get("spreadsheet_id")
        if not isinstance(spreadsheet_id, str) or not spreadsheet_id:
            raise ConfigurationError(
                "google_sheets source config needs a 'spreadsheet_id' string — "
                "the long id in the sheet's URL."
            )
        if "/" in spreadsheet_id or "?" in spreadsheet_id:
            # It is interpolated into the request path.
            raise ConfigurationError(
                f"'spreadsheet_id' must be the bare id, not a URL "
                f"(got {spreadsheet_id!r})."
            )

        ranges = config.get("ranges")
        if not isinstance(ranges, dict) or not ranges:
            raise ConfigurationError(
                "google_sheets source config needs a non-empty 'ranges' mapping "
                "of {resource_name: A1 range}."
            )

        render = config.get("value_render_option", DEFAULT_VALUE_RENDER_OPTION)
        if render not in VALUE_RENDER_OPTIONS:
            raise ConfigurationError(
                f"'value_render_option' must be one of "
                f"{sorted(VALUE_RENDER_OPTIONS)}, got {render!r}."
            )

        policy = config.get("missing_key", "error")
        if policy not in MISSING_KEY_POLICIES:
            raise ConfigurationError(
                f"'missing_key' must be one of {sorted(MISSING_KEY_POLICIES)}, "
                f"got {policy!r}."
            )

        for name, raw_spec in ranges.items():
            spec = range_spec(name, raw_spec, config)
            if not spec["range"]:
                raise ConfigurationError(
                    f"ranges.{name} needs a non-empty A1 'range', e.g. 'Sheet1!A:F'."
                )
            _validate_resource_name(name)
            if spec["key_column"] is not None and not isinstance(
                spec["key_column"], str
            ):
                raise ConfigurationError(f"ranges.{name}.key_column must be a string")
            # Without a header row the columns are named column_1..N, which a
            # key_column could legitimately name — but a customer who wrote a
            # business name here has made a mistake worth catching at save time.
            if (
                not spec["header_row"]
                and spec["key_column"]
                and not spec["key_column"].startswith("column_")
            ):
                raise ConfigurationError(
                    f"ranges.{name} sets header_row=false, so columns are named "
                    f"column_1..column_N; key_column {spec['key_column']!r} can "
                    f"never exist."
                )

        if config.get("skip_unchanged"):
            keyless = sorted(
                name
                for name, raw_spec in ranges.items()
                if not range_spec(name, raw_spec, config)["key_column"]
            )
            if keyless:
                # Verified against dlt 1.30: a `replace` resource that yields
                # nothing truncates its table. "Nothing changed" would empty the
                # landing table and Projection would propagate the emptiness.
                raise ConfigurationError(
                    f"'skip_unchanged' cannot be used with ranges that declare "
                    f"no key_column ({keyless}). Those land with replace "
                    f"disposition, and a replace resource that yields no rows "
                    f"truncates its landing table — so skipping an unchanged "
                    f"sheet would delete every row it had already landed. "
                    f"Declare a key_column, or leave skip_unchanged off."
                )
        return None

    def incremental_for(self, resource_name, binding):
        """Always ``None``. Sheets exposes no per-row cursor of any kind.

        There is no "modified since" filter and no row revision, so a range is
        re-read in full every run and reconciled by ``key_column``. See the
        module docstring for why an index-based cursor is not an alternative.
        """
        return None

    # --- extraction --------------------------------------------------------

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        # Resolved here and discarded: dlt wraps anything a resource generator
        # raises in `PipelineStepFailed`, and `services.runs` classifies on the
        # exception class — so a CredentialsRevoked raised during extraction is
        # recorded as a generic failure and the Connection keeps being retried.
        # Raised from build time it reaches the runner intact.
        bearer_token(credentials)

        client = google_client(base_url=self.api_base_url, credentials=credentials)
        specs = {
            name: range_spec(name, raw_spec, config)
            for name, raw_spec in config["ranges"].items()
        }
        # One batchGet for every range, fetched on first use and shared. dlt
        # extracts resources sequentially, so a lazy cache in the closure turns
        # N resources into one request instead of N.
        fetch = _batch_fetcher(client, config, specs)
        modified_time = _drive_modified_time_reader(
            self, config, credentials, enabled=bool(config.get("skip_unchanged"))
        )

        resources = [
            self._range_resource(dlt, name, spec, fetch, modified_time)
            for name, spec in specs.items()
        ]
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _range_resource(self, dlt, name, spec, fetch, modified_time):
        key_column = spec["key_column"]

        def emit():
            state = dlt.current.resource_state()
            if key_column and modified_time is not None:
                current = modified_time()
                if current and state.get(MODIFIED_TIME_STATE_KEY) == current:
                    logger.info(
                        "spreadsheet unchanged since %s; skipping range %s",
                        current,
                        name,
                    )
                    return
                # Written after the rows, never before: a run that fails
                # mid-extract must not record the sheet as already synced.
                yield from rows_from_values(name, fetch(name), spec)
                if current:
                    state[MODIFIED_TIME_STATE_KEY] = current
                return
            yield from rows_from_values(name, fetch(name), spec)

        return dlt.resource(
            emit,
            name=name,
            primary_key=key_column or None,
            # Stated, never omitted. dlt defaults this hint to "append", so
            # leaving it out would stack a fresh copy of the whole sheet on
            # every run — silently, with the merge key correctly configured.
            write_disposition="merge" if key_column else "replace",
        )()

    # --- operations --------------------------------------------------------

    def check_connection(self, *, connection, credentials, binding=None):
        """Read the spreadsheet's title. One request, no cell data."""
        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        spreadsheet_id = config.get("spreadsheet_id")
        if not spreadsheet_id:
            raise ConfigurationError(
                "no 'spreadsheet_id' to test: put one in the Binding's config or "
                "in Connection.metadata."
            )
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        payload = google_json(
            client,
            f"spreadsheets/{spreadsheet_id}",
            params={"fields": "properties.title"},
            what="testing the Google Sheets connection",
        )
        title = (payload.get("properties") or {}).get("title", "untitled")
        return f"ok ({title})"

    def discover(self, *, connection, credentials, query=None):
        """Every tab in the spreadsheet, as a candidate range."""
        config = connection.metadata or {}
        spreadsheet_id = config.get("spreadsheet_id")
        if not spreadsheet_id:
            raise ConfigurationError(
                "discovery needs Connection.metadata['spreadsheet_id']."
            )
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        payload = google_json(
            client,
            f"spreadsheets/{spreadsheet_id}",
            params={"fields": "sheets.properties"},
            what="listing the spreadsheet's tabs",
        )
        resources = []
        for sheet in payload.get("sheets") or ():
            properties = (sheet or {}).get("properties") or {}
            title = properties.get("title")
            if not title:
                continue
            if query and query.lower() not in title.lower():
                continue
            resources.append(
                {
                    "name": normalize_identifier(title),
                    "range": f"{title}",
                    "rows": (properties.get("gridProperties") or {}).get("rowCount"),
                    "columns": (properties.get("gridProperties") or {}).get(
                        "columnCount"
                    ),
                }
            )
        return {"resources": resources}


# --- configuration helpers -------------------------------------------------


def _validate_resource_name(name):
    """A range's key becomes part of its landing table name, so check it here.

    ``validate_landing_names`` only checks the resources a Binding *selected*,
    so a range named ``Orders 2024`` in a Binding that selects nothing would
    otherwise sail through save and fail inside a Run — where renaming it is
    the only fix and the customer is not present.
    """
    if not isinstance(name, str) or not name:
        raise ConfigurationError("every key of 'ranges' must be a non-empty string")
    if _safe_identifier(name) != name:
        raise ConfigurationError(
            f"range name {name!r} is not usable as a landing table name. Use "
            f"lowercase letters, digits and single underscores — it would "
            f"otherwise be rewritten by dlt's naming convention and the "
            f"physical table would not be the one this Binding computes."
        )


def range_spec(name, raw_spec, config):
    """Normalize one entry of ``ranges`` into a full spec.

    A bare string is the common case (``"orders": "Orders!A:F"``) and stays
    supported; anything a range does not state falls back to the top-level
    default so that a ten-tab spreadsheet is not ten copies of the same
    options.
    """
    if isinstance(raw_spec, str):
        raw_spec = {"range": raw_spec}
    if not isinstance(raw_spec, dict):
        raise ConfigurationError(
            f"ranges.{name} must be an A1 range string or an object, got "
            f"{type(raw_spec).__name__}."
        )
    return {
        "range": raw_spec.get("range") or "",
        "key_column": raw_spec.get("key_column", config.get("key_column")),
        "header_row": bool(raw_spec.get("header_row", config.get("header_row", True))),
        "missing_key": raw_spec.get("missing_key", config.get("missing_key", "error")),
    }


def _batch_fetcher(client, config, specs):
    """A callable returning one range's raw ``values``, fetched once for all.

    ``values.batchGet`` answers in request order, so the response is zipped
    back onto the resource names by position — the ``range`` string Google
    echoes is normalized (``Orders!A:F`` comes back as ``Orders!A1:F1000``) and
    is not a reliable key.
    """
    order = list(specs)
    cache = {}

    def fetch(name):
        if not cache:
            payload = google_json(
                client,
                f"spreadsheets/{config['spreadsheet_id']}/values:batchGet",
                params={
                    "ranges": [specs[key]["range"] for key in order],
                    "majorDimension": "ROWS",
                    "valueRenderOption": config.get(
                        "value_render_option", DEFAULT_VALUE_RENDER_OPTION
                    ),
                    "dateTimeRenderOption": DATE_TIME_RENDER_OPTION,
                },
                what="reading the spreadsheet's values",
            )
            value_ranges = payload.get("valueRanges") or []
            if len(value_ranges) != len(order):
                raise SourceError(
                    f"asked Google Sheets for {len(order)} ranges and got "
                    f"{len(value_ranges)} back; the response cannot be matched "
                    f"to the configured resources."
                )
            for key, value_range in zip(order, value_ranges, strict=True):
                cache[key] = (value_range or {}).get("values") or []
        return cache[name]

    return fetch


def _drive_modified_time_reader(source, config, credentials, *, enabled):
    """A callable returning the spreadsheet's Drive ``modifiedTime``, or None.

    Cached for the whole source: every range in one spreadsheet shares one
    modification time, and asking Drive once per resource would spend a request
    to learn the same thing.
    """
    if not enabled:
        return None

    cache = {}

    def modified_time():
        if "value" not in cache:
            client = google_client(
                base_url=source.drive_api_base_url, credentials=credentials
            )
            payload = google_json(
                client,
                f"files/{config['spreadsheet_id']}",
                params={"fields": "modifiedTime", "supportsAllDrives": "true"},
                what="reading the spreadsheet's modification time",
            )
            cache["value"] = payload.get("modifiedTime")
        return cache["value"]

    return modified_time


# --- values to records -----------------------------------------------------


def rows_from_values(resource_name, values, spec):
    """Turn one range's raw ``values`` grid into landing records.

    `values` is exactly what the API returned: a list of rows, each a list of
    cells, with trailing empty cells omitted and no guarantee that any two rows
    are the same length.
    """
    values = list(values or [])
    if not values:
        return

    width = max((len(row) for row in values), default=0)
    if spec["header_row"]:
        header_cells = values[0]
        data_rows = values[1:]
        first_data_row_number = 2
    else:
        header_cells = []
        data_rows = values
        first_data_row_number = 1

    columns = header_columns(header_cells, width)
    key_column = spec["key_column"]
    if key_column and key_column not in columns:
        raise SourceError(
            f"range {resource_name!r} declares key_column {key_column!r}, which "
            f"is not one of its columns: {columns}. The header may have been "
            f"renamed in the sheet, or the range may start on the wrong row."
        )

    missing = []
    for offset, row in enumerate(data_rows):
        row_number = first_data_row_number + offset
        record = row_record(row, columns, row_number)
        if record is None:
            # A wholly blank row. Sheets is full of them and they carry nothing.
            continue
        if key_column and record.get(key_column) in (None, ""):
            if spec["missing_key"] == "skip":
                missing.append(row_number)
                continue
            raise SourceError(
                f"range {resource_name!r} row {row_number} has no value in "
                f"key_column {key_column!r}. Every such row would merge into a "
                f"single landing row, so the run is refused rather than "
                f"collapsing them. Fix the sheet, or set missing_key='skip' to "
                f"drop keyless rows deliberately."
            )
        yield record

    if missing:
        logger.warning(
            "range %s: skipped %s row(s) with no %s (rows %s)",
            resource_name,
            len(missing),
            key_column,
            missing[:MAX_REPORTED_ROWS],
        )


def header_columns(header_cells, width):
    """Column names for a range, from its header row.

    Three real-world shapes are handled and none of them may raise: a blank
    header cell, two headers that differ only in case or punctuation, and a
    header row narrower than the widest data row.

    Names go through dlt's naming convention before being deduplicated, because
    that is the name the column will actually have in the landing table —
    ``Order ID`` and ``order_id`` both become ``order_id``, and catching that
    collision here is the difference between a suffixed second column and one
    silently overwriting the other.
    """
    used = {ROW_NUMBER_COLUMN}
    columns = []
    for index in range(width):
        raw = header_cells[index] if index < len(header_cells) else None
        text = "" if raw is None else str(raw).strip()
        candidate = _safe_identifier(text)
        if not candidate:
            candidate = f"column_{index + 1}"

        base = candidate
        suffix = 1
        while candidate in used:
            suffix += 1
            candidate = f"{base}_{suffix}"
        used.add(candidate)
        columns.append(candidate)
    return columns


def _safe_identifier(text):
    """dlt's normalized form of `text`, or ``""`` if it has none.

    ``normalize_identifier`` raises on an empty string, and a header cell
    holding a stray space, an emoji or a line break is ordinary. A header that
    cannot be normalized becomes a positional name rather than failing the Run.
    """
    if not text:
        return ""
    try:
        return normalize_identifier(text)
    except ValueError:
        return ""


def row_record(row, columns, row_number):
    """One data row as a record, or ``None`` when the row is entirely blank."""
    record = {ROW_NUMBER_COLUMN: row_number}
    populated = False
    for index, column in enumerate(columns):
        value = cell_value(row[index] if index < len(row) else None)
        if value is not None:
            populated = True
        record[column] = value
    return record if populated else None


def cell_value(value):
    """Normalize one cell. Blank becomes ``None``, never ``""``.

    Sheets returns ``""`` for a blank cell inside a row and omits it entirely
    at the end of one, so the same empty cell arrives two different ways.
    Collapsing both to ``None`` is what stops a column's type flipping between
    text and null depending on where the blank happened to sit.
    """
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value
