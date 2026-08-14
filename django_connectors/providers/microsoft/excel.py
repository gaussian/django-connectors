"""Worksheet rows out of ``.xlsx`` workbooks living in SharePoint or OneDrive.

Built on :mod:`django_connectors.providers.microsoft.files`: the same drive
addressing, the same delta enumeration, the same throttling contract. What
changes is the payload — instead of landing one row per file, this downloads
each changed workbook and lands one row per worksheet row.

**The Excel REST API is deliberately not used.** Graph exposes a real workbook
API (``/workbook/worksheets/{id}/usedRange``) which would avoid parsing files at
all, and it is the wrong tool here for one disqualifying reason: it is only
available to *delegated* tokens. Driving it means going back to a single named
user's permissions — which is precisely what the app-only design in
:mod:`~django_connectors.providers.microsoft.auth` exists to avoid, and it would
silently reintroduce every failure mode described there (the sync narrows to what
that person can see, and stops when they leave). It also holds a server-side
session per workbook and is throttled far harder than ``/content``. So the
workbook is downloaded and parsed locally, with app-only credentials, and the
sync keeps working when nobody is logged in.

**Two merge modes, and the difference is not cosmetic.**

``key_columns`` declared
    the sheet has a stable business identity — an order number, an employee id —
    and rows merge on ``(_file_id, _sheet_name, *key_columns)``. Editing a cell
    updates that row; inserting a row above it does not disturb anything. This
    is delta-driven: only workbooks Graph reports as changed are re-downloaded.

no ``key_columns``
    there is no identity to merge on, so the resource is ``replace``: every
    matching workbook is re-read on every run and the whole table is rebuilt.
    The tempting alternative — merging on the row's *position* in the sheet — is
    refused rather than defaulted to, because inserting one row at the top of a
    spreadsheet then rewrites the identity of every row beneath it, and every
    downstream target record silently becomes somebody else's data. Full replace
    is slower and completely correct.

Note what replace mode cannot do: if a run matches *no* workbook at all, dlt has
no load package for the table and the previous contents stay. Deleting the last
file in a folder therefore leaves its rows until something else lands.

**Deletions.** ``emits_tombstones`` is False here, unlike the file source. In
keyed mode a deleted workbook is reported by delta, but nothing in this package
knows which row keys it contained — the keys are in the file, and the file is
gone — so no tombstone can be emitted and its rows stay until the Binding is
re-run in replace mode or purged. In replace mode deletion is handled by the
rebuild instead. Claiming tombstone support would make the API tell customers
that deletions propagate, which for this source is not true.

**Unverified against a live provider.** Parsing is exercised against real
``.xlsx`` files built by openpyxl in the test suite; everything Microsoft-side is
a fake. Not exercised: workbooks written by Excel itself (openpyxl writes no
cached formula results, so the cached-value path below has never seen a real
one), files stored with sensitivity labels or IRM, checked-out documents,
``.xlsb``/``.xls``, workbooks over a few megabytes, and Graph's ``/content``
redirect to a pre-authenticated SharePoint download host.
"""

import io
import zipfile
from typing import ClassVar

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.landing.naming import DELETED_COLUMN, normalize_identifier
from django_connectors.providers.microsoft.files import (
    DEFAULT_TIMEOUT_SECONDS,
    EntraFilesSource,
    access_token,
    assert_graph_id,
    download_item_content,
    drive_address,
    graph_request,
    graph_session,
    item_record,
    raise_for_graph_error,
)

DEFAULT_RESOURCE = "worksheet_rows"

#: 32 MiB of compressed ``.xlsx``. An xlsx is a zip of XML and expands roughly
#: ten-to-one in openpyxl's object graph even in read-only mode, so this is
#: already a ~300MB worker at the ceiling. Configurable, but not unbounded: the
#: alternative is that one oversized upload OOMs the worker for every tenant
#: sharing it.
DEFAULT_MAX_FILE_BYTES = 32 * 1024 * 1024

#: Cells one worksheet may hold before the parse is refused. ``max_file_bytes``
#: bounds the download and not the memory, and the two are only loosely related:
#: the grid costs ``rows x width``, and *width* is whatever the workbook's own
#: ``<dimension>`` element claims — a number nothing validates against the cells
#: that follow it. This is the second half of the same promise: no single
#: workbook, however it is shaped, takes the worker (and every co-tenant Run on
#: it) down with an OOM kill.
DEFAULT_MAX_CELLS = 2_000_000

#: openpyxl reads these. ``.xls`` (BIFF) and ``.xlsb`` (binary) it cannot open at
#: all, and a folder full of mixed documents is normal, so non-matching files are
#: skipped rather than failing the Run — except when the Binding names one
#: explicitly with ``item_id``, where skipping would sync nothing and say nothing.
SUPPORTED_EXTENSIONS = (".xlsx", ".xlsm")

#: Excel's own lock files, which appear in a shared library whenever somebody has
#: a workbook open. They are valid zip files and parse into garbage.
LOCK_FILE_PREFIX = "~$"

#: Provenance columns stamped on every row. Written *after* the sheet's own
#: columns, so a spreadsheet whose header is literally ``_file_id`` loses that
#: header rather than corrupting the merge key. Leading underscores keep them out
#: of the way of real headers while staying visible to Projection mappings —
#: unlike ``_connector_*``/``_dlt_*``, which the library hides.
FILE_ID_COLUMN = "_file_id"
FILE_NAME_COLUMN = "_file_name"
FILE_PATH_COLUMN = "_file_path"
FILE_MODIFIED_COLUMN = "_file_modified_at"
SHEET_NAME_COLUMN = "_sheet_name"
ROW_NUMBER_COLUMN = "_row_number"

#: Per-item content tags from the last successful run, so keyed mode can skip a
#: workbook Graph re-reported without a content change.
CTAG_STATE_KEY = "workbook_ctags"

ON_MISSING_KEY_CHOICES = ("error", "skip")


class EntraExcelSource(EntraFilesSource):
    """Land worksheet rows from the ``.xlsx`` files in a drive or folder.

    Configuration::

        {
          "site_id": "contoso.sharepoint.com,<guid>,<guid>",
          "folder_path": "/Finance/Budgets",
          "name_glob": "*.xlsx",
          "sheets": ["Budget"],
          "header_row": 1,
          "key_columns": ["cost_centre", "month"],
          "resource": "budget_rows"
        }

    Or one specific workbook with ``"item_id": "01ABC..."`` instead of a folder.
    """

    key = "entra_excel"
    provider = "microsoft"
    supported_auth_backends = ("entra", "static")
    required_extras: ClassVar[dict[str, str]] = {"openpyxl": "microsoft"}

    #: See the module docstring: a deleted workbook cannot be turned into row
    #: tombstones, because the keys that identified its rows went with it.
    emits_tombstones = False

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        config = config or {}
        self.validate_location(config)

        item_id = config.get("item_id")
        if item_id:
            assert_single_workbook(config)

        resource = config.get("resource", DEFAULT_RESOURCE)
        if not isinstance(resource, str) or not resource:
            raise ConfigurationError(
                f"'resource' must be a non-empty string, got {resource!r}."
            )

        header_row = config.get("header_row", 1)
        if not isinstance(header_row, int) or isinstance(header_row, bool):
            raise ConfigurationError(
                "'header_row' must be an integer: the 1-based row holding the "
                "column names, or 0 for a sheet with no header (columns are "
                "then named column_1, column_2, ...)."
            )
        if header_row < 0:
            raise ConfigurationError("'header_row' may not be negative.")

        skip_rows = config.get("skip_rows", 0)
        if (
            not isinstance(skip_rows, int)
            or isinstance(skip_rows, bool)
            or skip_rows < 0
        ):
            raise ConfigurationError(
                "'skip_rows' must be a non-negative integer: how many rows "
                "between the header and the first data row to discard."
            )

        sheets = normalized_sheets(config)
        if sheets is not None and not sheets:
            raise ConfigurationError(
                "'sheets' must name at least one worksheet, or be omitted to "
                "read every visible sheet."
            )

        key_columns = config.get("key_columns")
        if key_columns is not None:
            if not isinstance(key_columns, list) or any(
                not isinstance(column, str) or not column.strip()
                for column in key_columns
            ):
                raise ConfigurationError(
                    "'key_columns' must be a list of non-empty header names."
                )
            duplicates = _duplicates([normalize_identifier(c) for c in key_columns])
            if duplicates:
                raise ConfigurationError(
                    f"'key_columns' names {sorted(duplicates)} more than once "
                    f"(header names are normalized before comparison, so 'ID' "
                    f"and 'id' are the same column)."
                )

        on_missing = config.get("on_missing_key", "error")
        if on_missing not in ON_MISSING_KEY_CHOICES:
            raise ConfigurationError(
                f"'on_missing_key' must be one of {list(ON_MISSING_KEY_CHOICES)}, "
                f"got {on_missing!r}."
            )

        max_bytes = config.get("max_file_bytes", DEFAULT_MAX_FILE_BYTES)
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes <= 0
        ):
            raise ConfigurationError("'max_file_bytes' must be a positive integer.")

        max_cells = config.get("max_cells", DEFAULT_MAX_CELLS)
        if (
            not isinstance(max_cells, int)
            or isinstance(max_cells, bool)
            or max_cells <= 0
        ):
            raise ConfigurationError(
                "'max_cells' must be a positive integer: the most cells one "
                "worksheet may hold before the parse is refused."
            )

        glob = config.get("name_glob")
        if glob is not None and not isinstance(glob, str):
            raise ConfigurationError("'name_glob' must be a string like '*.xlsx'.")
        return None

    # --- extraction --------------------------------------------------------

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        token = access_token(credentials)
        resource_name = config.get("resource", DEFAULT_RESOURCE)
        key_columns = self.key_columns(config)

        def emit():
            yield from self.iter_rows(config, token)

        # write_disposition is stated in both branches, never inherited: dlt
        # defaults the hint to "append", which here would mean every run adding
        # a second copy of every row of every workbook it re-read, silently.
        if key_columns:
            resource = dlt.resource(
                emit,
                name=resource_name,
                # The file and sheet are part of the identity because the same
                # business key legitimately appears in two workbooks and in two
                # tabs of one workbook; merging those together would have one
                # sheet's rows quietly overwrite the other's.
                primary_key=(FILE_ID_COLUMN, SHEET_NAME_COLUMN, *key_columns),
                write_disposition="merge",
            )()
        else:
            resource = dlt.resource(
                emit, name=resource_name, write_disposition="replace"
            )()
        return dlt.source(lambda: [resource], name=self.key, section=self.key)()

    def key_columns(self, config):
        """Configured key columns, normalized to the names that actually land."""
        return tuple(
            normalize_identifier(str(column))
            for column in (config or {}).get("key_columns") or ()
        )

    def iter_rows(self, config, token):
        """Yield one record per worksheet row of every workbook in scope."""
        keyed = bool(self.key_columns(config))
        max_bytes = int(config.get("max_file_bytes", DEFAULT_MAX_FILE_BYTES))
        base = self.base_url(config)
        session = graph_session(token, timeout=self.timeout(config))
        try:
            for item in self.workbook_items(
                config, token, session=session, keyed=keyed
            ):
                drive_id = item.get("drive_id")
                if not drive_id:
                    raise SourceError(
                        f"Graph returned driveItem {item.get('id')!r} without a "
                        f"parentReference.driveId, so its content cannot be "
                        f"addressed. This is a Graph response shape this source "
                        f"does not understand rather than a configuration error."
                    )
                data = download_item_content(
                    session,
                    base_url=base,
                    drive_id=drive_id,
                    item_id=item["id"],
                    max_bytes=max_bytes,
                )
                yield from self.rows_for_workbook(data, item, config)
        finally:
            session.close()

    def workbook_items(self, config, token, *, session, keyed):
        """The driveItems to parse this run.

        Folder mode reuses the file source's delta enumeration — with state in
        keyed mode (only changed workbooks) and without it in replace mode (the
        full current snapshot, because the table is being rebuilt). Single-item
        mode fetches the one item's metadata and, in keyed mode, compares its
        ``cTag`` against the last run's so that an unchanged workbook is not
        downloaded and re-parsed for nothing.
        """
        if config.get("item_id"):
            item = self.single_item(config, session=session)
            # Replace mode always re-reads: the table is being rebuilt, so
            # skipping an unchanged workbook would empty it instead.
            if keyed and not self.content_changed(item):
                return
            yield item
            return

        for record in self.iter_items(config, token, use_state=keyed, session=session):
            if record.get(DELETED_COLUMN):
                # A deleted workbook: nothing to download, and no row tombstones
                # are possible (see the module docstring).
                continue
            if record.get("is_folder"):
                continue
            if is_parsable_workbook(record.get("name")):
                yield record

    def single_item(self, config, *, session):
        """Metadata for the one workbook a Binding names with ``item_id``."""
        base = self.base_url(config)
        url = f"{base}/{drive_address(config)}/items/{config['item_id']}"
        response = graph_request(session, "GET", url)
        raise_for_graph_error(response, what=f"workbook {config['item_id']!r}")
        item = item_record(response.json(), drive_id=config.get("drive_id") or "")
        name = item.get("name") or ""
        if not is_parsable_workbook(name):
            # Explicitly named, so silence would mean "synced nothing, reported
            # success" — the worst possible answer.
            raise SourceError(
                f"driveItem {config['item_id']!r} is named {name!r}, which "
                f"openpyxl cannot read. Supported: "
                f"{', '.join(SUPPORTED_EXTENSIONS)}; legacy .xls and binary "
                f".xlsb are not, and would have to be re-saved."
            )
        return item

    def content_changed(self, item):
        """Whether this workbook's content differs from the last run's.

        Keyed on ``cTag``, which Graph changes on a *content* edit only —
        ``eTag`` also moves when somebody renames the file or edits a column in
        the library view, which would re-download a 20MB workbook for nothing.
        """
        state = self.delta_state()
        tags = state.setdefault(CTAG_STATE_KEY, {})
        item_id = item.get("id")
        current = item.get("ctag") or item.get("etag") or ""
        if item_id and current and tags.get(item_id) == current:
            return False
        if item_id:
            tags[item_id] = current
        return True

    def rows_for_workbook(self, data, item, config):
        """Parse one downloaded workbook into landing records."""
        key_columns = self.key_columns(config)
        on_missing = config.get("on_missing_key", "error")
        name = item.get("name") or item.get("id")

        for sheet_name, row_number, values in worksheet_records(
            data,
            source=name,
            sheets=normalized_sheets(config),
            header_row=int(config.get("header_row", 1)),
            skip_rows=int(config.get("skip_rows", 0)),
            include_hidden=bool(config.get("include_hidden_sheets")),
            unmerge=bool(config.get("unmerge")),
            max_cells=int(config.get("max_cells", DEFAULT_MAX_CELLS)),
        ):
            if key_columns:
                missing = [column for column in key_columns if column not in values]
                if missing:
                    # Merging on a key the sheet does not have would collapse
                    # every row onto one identity. Loud, at the first row.
                    raise SourceError(
                        f"sheet {sheet_name!r} of {name!r} has no column(s) "
                        f"{missing} to merge on. Available: "
                        f"{sorted(values)}. Header names are normalized "
                        f"(lower-cased, punctuation to underscores) before "
                        f"matching, so configure 'key_columns' in that form or "
                        f"in the sheet's own wording — both resolve the same."
                    )
                blank = [column for column in key_columns if is_blank(values[column])]
                if blank:
                    if on_missing == "skip":
                        continue
                    raise SourceError(
                        f"row {row_number} of sheet {sheet_name!r} in {name!r} "
                        f"has no value for merge key column(s) {blank}. A NULL "
                        f"merge key never matches itself, so the row would be "
                        f"re-inserted on every run. Fix the sheet, or set "
                        f"'on_missing_key': 'skip' to drop such rows."
                    )

            yield {
                **values,
                FILE_ID_COLUMN: item.get("id"),
                FILE_NAME_COLUMN: item.get("name"),
                FILE_PATH_COLUMN: item.get("path"),
                FILE_MODIFIED_COLUMN: item.get("last_modified_at"),
                SHEET_NAME_COLUMN: sheet_name,
                ROW_NUMBER_COLUMN: row_number,
            }

    # --- operations --------------------------------------------------------

    # check_connection and discover are inherited unchanged: they probe the
    # drive, which is exactly the access this source needs.

    def timeout(self, config):
        # Workbook downloads are much slower than metadata calls, so the default
        # read timeout is generous rather than the file source's.
        return float(
            (config or {}).get("request_timeout") or DEFAULT_TIMEOUT_SECONDS * 5
        )


# --- workbook parsing ------------------------------------------------------


def worksheet_records(
    data,
    *,
    source="workbook",
    sheets=None,
    header_row=1,
    skip_rows=0,
    include_hidden=False,
    unmerge=False,
    max_cells=DEFAULT_MAX_CELLS,
):
    """Yield ``(sheet_name, row_number, {column: value})`` for one workbook.

    ``data_only=True`` — cell *values*, not formulas, because a landing column
    holding ``"=SUM(B2:B40)"`` is useless to every consumer. **The trap:** an
    xlsx stores a cached result alongside each formula, and only the application
    that last saved the file writes one. A workbook produced by a script (or by
    this project's own tests, which use openpyxl) has no cached results at all,
    so every formula cell reads as ``None`` here — not an error, not a warning,
    just silent NULLs in a column the customer expects numbers in. There is no
    way to compute the value locally without a full spreadsheet engine. If a
    column matters and lands empty, the workbook needs opening and saving in
    Excel once.

    ``read_only=True`` unless `unmerge` is set: it streams the sheet instead of
    materialising every cell object, which is the difference between parsing a
    large workbook and exhausting the worker. openpyxl's read-only worksheets do
    not expose merged-cell ranges at all, so filling merged regions requires the
    slow path, and it is opt-in for that reason.
    """
    from openpyxl import load_workbook
    from openpyxl.utils.exceptions import InvalidFileException

    try:
        workbook = load_workbook(
            io.BytesIO(data), data_only=True, read_only=not unmerge
        )
    except (
        InvalidFileException,
        zipfile.BadZipFile,
        OSError,
        ValueError,
        KeyError,
    ) as exc:
        # An xlsx is a zip, and the failures worth naming happen below openpyxl:
        # BadZipFile (truncated download, or an encrypted/IRM-protected file,
        # which is an OLE container rather than a zip) subclasses Exception and
        # not OSError, so listing it is not redundant.
        raise SourceError(
            f"{source!r} could not be opened as an xlsx workbook "
            f"({type(exc).__name__}: {exc}). It may be encrypted, protected by "
            f"an information-rights label, truncated in transit, or not really "
            f"a spreadsheet."
        ) from exc

    try:
        for worksheet in select_sheets(workbook, sheets, include_hidden, source=source):
            grid = sheet_grid(
                worksheet, unmerge=unmerge, max_cells=max_cells, source=source
            )
            yield from sheet_records(
                worksheet.title,
                grid,
                header_row=header_row,
                skip_rows=skip_rows,
                source=source,
            )
    finally:
        workbook.close()


def select_sheets(workbook, sheets, include_hidden, *, source="workbook"):
    """The worksheets to read, in workbook order.

    A named sheet that is absent is an error rather than a skip: a Binding that
    names ``"Budget"`` and finds no such tab has not synced an empty budget, it
    has synced the wrong thing, and silence would land zero rows and report
    success.
    """
    if sheets:
        missing = [name for name in sheets if name not in workbook.sheetnames]
        if missing:
            raise SourceError(
                f"{source!r} has no worksheet named {missing}; it has "
                f"{workbook.sheetnames}."
            )
        return [workbook[name] for name in sheets]

    return [
        workbook[name]
        for name in workbook.sheetnames
        # Hidden tabs are usually lookup tables, scratch calculations or an old
        # copy somebody kept "just in case" — landing them alongside the real
        # data silently doubles the row count.
        if include_hidden
        or getattr(workbook[name], "sheet_state", "visible") == "visible"
    ]


def sheet_grid(
    worksheet, *, unmerge=False, max_cells=DEFAULT_MAX_CELLS, source="workbook"
):
    """The sheet as a rectangular list of rows, trailing emptiness removed.

    Excel's ``max_row``/``max_column`` count every cell that has ever been
    *formatted*, so a sheet where somebody once selected a column and set a
    border reports thousands of empty rows. Trimming here is what keeps those
    from landing as thousands of all-NULL records.

    Three details here are defences rather than tidying:

    ``reset_dimensions`` on a read-only sheet
        openpyxl believes the workbook's own ``<dimension>`` element, and
        nothing checks it against the cells that follow. A file declaring
        ``A1:XFD1048576`` makes openpyxl pad every streamed row out to 16,384
        columns, so a parse costs ``real_rows x declared_width`` and
        ``max_file_bytes`` bounds none of it. Measured on a 46 KB workbook of
        3,000 honest rows: 44 MB and 3s with a truthful dimension, 394 MB and
        27s with a lying one. Resetting derives the extents from the rows
        actually parsed.

    each row is trimmed as it streams, not after the grid exists
        running the trailing trim on a materialised grid runs it after the
        allocation it exists to prevent.

    the merged fill happens *after* the trailing blank rows are dropped
        the fill writes a value into every cell of a merged region, so a region
        extending past the last row of real data (``A2:A200`` over two rows of
        data) makes those rows non-blank and the trim then finds nothing to
        remove — 197 all-NULL records, each one a row a keyed merge either
        refuses or collapses onto one identity. Trimming first lets the
        ``min(..., len(grid))`` clamps below clip the region to the real data.
    """
    # Only read-only worksheets carry (and trust) the declared dimension.
    reset_dimensions = getattr(worksheet, "reset_dimensions", None)
    if reset_dimensions is not None:
        reset_dimensions()

    grid = []
    cells = 0
    for row in worksheet.iter_rows(values_only=True):
        # The unmerge path cannot trim: a merged region's cells read blank
        # until the fill below has run, and a trimmed row has nowhere to fill.
        values = list(row) if unmerge else _without_trailing_blanks(row)
        cells += len(values)
        if cells > max_cells:
            raise SourceError(
                f"sheet {worksheet.title!r} of {source!r} holds more than "
                f"{max_cells} cells, which will not be parsed: openpyxl builds "
                f"the whole sheet in memory and the worker is shared. Raise "
                f"'max_cells' in the Binding config if the sheet really is this "
                f"large."
            )
        grid.append(values)

    while grid and all(is_blank(value) for value in grid[-1]):
        grid.pop()

    if unmerge:
        _fill_merged_ranges(worksheet, grid)

    width = 0
    for row in grid:
        for index in range(len(row) - 1, -1, -1):
            if not is_blank(row[index]):
                width = max(width, index + 1)
                break

    return [_fit(row, width) for row in grid]


def _without_trailing_blanks(row):
    """`row` as a list, with its trailing blank cells dropped."""
    for index in range(len(row) - 1, -1, -1):
        if not is_blank(row[index]):
            return list(row[: index + 1])
    return []


def _fill_merged_ranges(worksheet, grid):
    """Repeat each merged region's value across the region, in place.

    A merged region stores its value in the top-left cell only; every other
    cell of the region reads None. Repeating the value is what makes a merged
    header ("Q1" spanning three columns) produce three usable column names
    instead of one plus two blanks.
    """
    merged = getattr(worksheet, "merged_cells", None)
    for cell_range in getattr(merged, "ranges", ()):
        top, left = cell_range.min_row - 1, cell_range.min_col - 1
        if top >= len(grid) or left >= len(grid[top]):
            continue
        value = grid[top][left]
        for row_index in range(top, min(cell_range.max_row, len(grid))):
            row = grid[row_index]
            for column_index in range(left, min(cell_range.max_col, len(row))):
                row[column_index] = value


def sheet_records(sheet_name, grid, *, header_row=1, skip_rows=0, source="workbook"):
    """Yield ``(sheet_name, excel_row_number, values)`` for one trimmed grid."""
    if not grid:
        return

    if header_row > 0:
        if header_row > len(grid):
            raise SourceError(
                f"sheet {sheet_name!r} of {source!r} has {len(grid)} non-empty "
                f"row(s) but 'header_row' is {header_row}; there is no header "
                f"row to read column names from."
            )
        header_values = grid[header_row - 1]
        first_data_index = header_row + skip_rows
    else:
        header_values = []
        first_data_index = skip_rows

    columns = column_keys(header_values, len(grid[0]))

    for offset, values in enumerate(grid[first_data_index:]):
        if all(is_blank(value) for value in values):
            # A blank line inside the used range is a spacer, not a record.
            continue
        yield (
            sheet_name,
            first_data_index + offset + 1,
            {column: values[index] for index, column in enumerate(columns)},
        )


def column_keys(header_values, width):
    """Turn a header row into stable, unique, landing-safe column names.

    Names are normalized with dlt's own convention *here* rather than being left
    to the normalizer downstream, because two different headers routinely
    collapse to one identifier — ``"ID"`` and ``"id"``, or ``"Total"`` twice
    after somebody copied a column — and dlt resolves that collision by keeping
    whichever value it wrote last. Deduplicating up front turns silent data loss
    into a second column named ``id_2``.

    Blank headers become ``column_<n>``, one-based, matching what a person
    reading the spreadsheet counts.
    """
    keys = []
    seen = set()
    for index in range(width):
        raw = header_values[index] if index < len(header_values) else None
        text = "" if raw is None else str(raw).strip()
        base = normalize_identifier(text) if text else ""
        if not base:
            base = f"column_{index + 1}"
        key = base
        suffix = 1
        while key in seen:
            suffix += 1
            key = f"{base}_{suffix}"
        seen.add(key)
        keys.append(key)
    return keys


def is_blank(value):
    """Whether a cell holds nothing worth landing."""
    if value is None:
        return True
    return isinstance(value, str) and not value.strip()


def is_parsable_workbook(name):
    """Whether openpyxl can be pointed at a file with this name."""
    text = (name or "").strip()
    if not text or text.startswith(LOCK_FILE_PREFIX):
        return False
    lowered = text.lower()
    return any(lowered.endswith(extension) for extension in SUPPORTED_EXTENSIONS)


def normalized_sheets(config):
    """``sheets``/``sheet`` as a list of worksheet names, or None for "all"."""
    sheets = (config or {}).get("sheets")
    if sheets is None:
        single = (config or {}).get("sheet")
        if single is None:
            return None
        sheets = [single]
    if not isinstance(sheets, list) or any(
        not isinstance(name, str) for name in sheets
    ):
        raise ConfigurationError(
            "'sheets' must be a list of worksheet names (or use 'sheet' for one)."
        )
    return sheets


def assert_single_workbook(config):
    """``item_id`` names one workbook, so folder narrowing is meaningless."""
    assert_graph_id("item_id", config["item_id"])
    conflicting = sorted(
        key for key in ("folder_path", "folder_item_id", "name_glob") if config.get(key)
    )
    if conflicting:
        raise ConfigurationError(
            f"'item_id' names one workbook, so {conflicting} cannot also apply. "
            f"Drop 'item_id' to sync a folder, or drop {conflicting} to sync "
            f"that one file."
        )


def _fit(row, width):
    """Pad or truncate a row to `width`, so every record has every column."""
    if len(row) >= width:
        return row[:width]
    return row + [None] * (width - len(row))


def _duplicates(values):
    seen = set()
    repeated = set()
    for value in values:
        if value in seen:
            repeated.add(value)
        seen.add(value)
    return repeated
