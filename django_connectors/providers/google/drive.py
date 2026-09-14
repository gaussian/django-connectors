"""Google Drive files, landed as metadata and synchronized through the changes feed.

**Why the changes feed and not a modified-time query.** Drive's ``files.list``
can filter on ``modifiedTime``, and for files that is wrong in the same three
ways it is wrong on SharePoint: a move does not touch the items underneath, a
file restored from the bin keeps its old timestamp and is never re-read, and a
deletion emits nothing at all — the row stays forever and a projection keeps
writing a target record for a document that no longer exists. ``changes.list``
is Drive's answer: a feed keyed by an opaque page token, reporting removals
and trashing explicitly. That is why :attr:`GoogleDriveSource.emits_tombstones`
is True here and False for every cursor-based source.

**Scope is applied by the source, not by Drive.** The changes feed is
drive-wide. A Binding scoped to a folder therefore keeps the set of folder
ids under its root in resource state — filled by the cold-start walk, grown as
folders appear in the feed — and keeps a change whose file has a parent in
that set. A removed file carries no parents, so its tombstone is emitted
whatever the scope: a spurious tombstone for a file never landed inserts one
dead row, while a dropped one leaves a deleted document live forever. A folder
*moved* into scope brings children the feed will not replay; they arrive on
the next full re-enumeration (``reset_binding_state``), and that is said here
rather than hidden.

**The cold start asks for the page token first**, then walks. A change that
happens during the walk is then replayed by the first warm run rather than
lost between the two.

**File bytes are never landed.** Only metadata becomes rows; content is
fetched on request through :meth:`GoogleDriveSource.fetch_content`, which
exports native Google formats (a Doc as text, a Sheet as CSV) and downloads
everything else, under the caller's byte ceiling.

**Unverified against a live provider.** Built against Drive's published v3
behaviour and an in-process fake. Not exercised: real page-token expiry (Drive
invalidates a token after a long gap; the source re-enumerates), shared-drive
membership changes, files with several parents, ``changes.list`` paging over a
very large drive, export size limits on big documents, and real throttling.
"""

import logging
import re
from typing import ClassVar

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.providers.google.auth import (
    bearer_token,
    error_payload,
    google_client,
    google_json,
    google_request,
    paginate,
    raise_for_google_error,
)
from django_connectors.sources.base import (
    SourceDefinition,
    as_config,
    name_matches,
    read_capped,
)
from django_connectors.sources.memory import tombstone

logger = logging.getLogger(__name__)

DRIVE_API_BASE_URL = "https://www.googleapis.com/drive/v3/"

FOLDER_MIME = "application/vnd.google-apps.folder"
SPREADSHEET_MIME = "application/vnd.google-apps.spreadsheet"

#: Drive ids are URL-safe base64-ish. Anything else would change which
#: resource a request path or a query literal addresses.
DRIVE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")

DEFAULT_RESOURCE = "drive_files"
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 1000
#: Pending folders asked for in one `files.list` query on the cold walk.
FOLDERS_PER_QUERY = 20

#: Everything this source keeps in dlt's resource state lives under one key,
#: with the Binding's *scope* recorded beside it: a Binding repointed from one
#: folder to another must re-enumerate, not replay the old folder's feed. The
#: entry holds the feed position (``token``), the folder ids under the root
#: and the files landed, each mapped to its parent (``folders``, ``files``) —
#: so a folder leaving the scope, or being trashed, can name the rows that
#: leave with it — and, for a single file, whether it was ever ``seen``.
STATE_KEY = "google_drive"

FILE_FIELDS = (
    "id,name,mimeType,size,md5Checksum,parents,trashed,version,"
    "createdTime,modifiedTime,webViewLink,driveId"
)
LIST_FIELDS = f"nextPageToken,files({FILE_FIELDS})"
CHANGES_FIELDS = (
    f"nextPageToken,newStartPageToken,changes(fileId,removed,file({FILE_FIELDS}))"
)

NATIVE_PREFIX = "application/vnd.google-apps."

#: What a native Google format exports as when the caller does not say.
#: Text for the things people read, CSV for a spreadsheet, PNG for a drawing.
DEFAULT_EXPORTS = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.drawing": "image/png",
}


class _TokenExpired(Exception):
    """Drive rejected the changes token: a long gap, or a migration."""


def _token_rejected(response):
    """Drive answers a dead page token with 404, or with 400 ``invalid`` on
    ``pageToken``; both mean "start again", and neither is a credential problem."""
    if response.status_code == 404:
        return True
    if response.status_code != 400:
        return False
    errors = error_payload(response).get("errors") or []
    return any(
        (error or {}).get("reason") == "invalid"
        and "pagetoken" in str((error or {}).get("location") or "").lower()
        for error in errors
    )


def _scope_key(config):
    return "|".join(
        f"{key}={config.get(key) or ''}" for key in ("file_id", "folder_id", "drive_id")
    )


def _state_for(state, config):
    """This scope's entry, replacing another scope's if the Binding was repointed."""
    entry = state.get(STATE_KEY)
    scope = _scope_key(config)
    if not isinstance(entry, dict) or entry.get("scope") != scope:
        entry = {
            "scope": scope,
            "token": None,
            "folders": {},
            "files": {},
            "seen": False,
        }
        state[STATE_KEY] = entry
    return entry


class GoogleDriveSource(SourceDefinition):
    """Land file metadata for one file, one folder (recursively), or a drive.

    Configuration::

        {
          "folder_id": "1AbC…",          # or "file_id": "…", or neither
          "drive_id": "0AbC…",           # optional: a shared drive
          "name_glob": "*.pdf",
          "resource": "drive_files",
          "page_size": 200
        }
    """

    key = "google_drive"
    provider = "google"
    supported_auth_backends = ("google_workspace", "allauth", "static")
    required_extras: ClassVar[dict[str, str]] = {}
    #: The changes feed reports removals and trashing.
    emits_tombstones = True
    #: `reference` needs the landed ``id``; ``mime_type`` saves a round trip.
    provides_content = True

    scopes = ("https://www.googleapis.com/auth/drive.readonly",)

    @property
    def api_base_url(self):
        """A class attribute, never Binding config — see the Sheets source."""
        return DRIVE_API_BASE_URL

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        config = as_config(config)
        file_id, folder_id = config.get("file_id"), config.get("folder_id")
        if file_id and folder_id:
            raise ConfigurationError(
                "give 'file_id' or 'folder_id', not both — one file or one "
                "folder's contents, never a mix."
            )
        for key in ("file_id", "folder_id", "drive_id"):
            if config.get(key) is not None:
                assert_drive_id(config[key])
        resource = config.get("resource", DEFAULT_RESOURCE)
        if not isinstance(resource, str) or not resource:
            raise ConfigurationError("'resource' must be a non-empty string.")
        glob = config.get("name_glob")
        if glob is not None and not isinstance(glob, str):
            raise ConfigurationError("'name_glob' must be a string like '*.pdf'.")
        page_size = config.get("page_size", DEFAULT_PAGE_SIZE)
        if (
            not isinstance(page_size, int)
            or isinstance(page_size, bool)
            or not 1 <= page_size <= MAX_PAGE_SIZE
        ):
            raise ConfigurationError(
                f"'page_size' must be an integer between 1 and {MAX_PAGE_SIZE}."
            )
        return None

    def incremental_for(self, resource_name, binding):
        """Always ``None``: the changes feed is the increment."""
        return None

    # --- extraction --------------------------------------------------------

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        bearer_token(credentials)  # a revoked credential fails here, classified
        resource_name = config.get("resource", DEFAULT_RESOURCE)

        def emit():
            yield from self.iter_files(config, credentials)

        resource = dlt.resource(
            emit, name=resource_name, primary_key="id", write_disposition="merge"
        )()
        return dlt.source(lambda: [resource], name=self.key, section=self.key)()

    def iter_files(self, config, credentials):
        import dlt

        client = google_client(base_url=self.api_base_url, credentials=credentials)
        entry = _state_for(dlt.current.resource_state(), config)
        if config.get("file_id"):
            yield from self._single_file(client, config, entry)
            return

        if entry["token"]:
            try:
                yield from self._changes(client, config, entry)
                return
            except _TokenExpired:
                # Resource state persists only on a successful load, so
                # "fail now and re-enumerate next run" would replay the same
                # dead token forever. Re-enumerate now, once — the only
                # correct response, and lossy for deletions made while the
                # token was invalid, which is logged because it cannot be
                # detected afterwards.
                logger.warning(
                    "Drive no longer accepts the changes token for %s; "
                    "re-enumerating. Deletions made meanwhile are not reported.",
                    config.get("folder_id") or config.get("drive_id") or "the drive",
                )
        # Token first, walk second: a change during the walk is replayed by
        # the first warm run instead of falling between the two.
        token = self._start_page_token(client, config)
        yield from self._cold_walk(client, config, entry)
        entry["token"] = token

    def _single_file(self, client, config, entry):
        response = google_request(
            client,
            f"files/{config['file_id']}",
            params={"fields": FILE_FIELDS, "supportsAllDrives": "true"},
        )
        if response.status_code == 404 and entry["seen"]:
            yield tombstone({"id": config["file_id"]})
            return
        raise_for_google_error(response, what=f"reading file {config['file_id']!r}")
        payload = response.json()
        if payload.get("trashed"):
            if entry["seen"]:
                yield tombstone({"id": config["file_id"]})
            return
        if payload.get("mimeType") == FOLDER_MIME:
            raise SourceError(
                f"file {config['file_id']!r} is a folder; use 'folder_id' to "
                f"sync its contents."
            )
        entry["seen"] = True
        yield file_record(payload)

    def _start_page_token(self, client, config):
        params = {"supportsAllDrives": "true"}
        if config.get("drive_id"):
            params["driveId"] = config["drive_id"]
        payload = google_json(
            client,
            "changes/startPageToken",
            params=params,
            what="reading the Drive changes token",
        )
        token = payload.get("startPageToken")
        if not token:
            raise SourceError("Drive returned no startPageToken")
        return token

    def _cold_walk(self, client, config, entry):
        """Every in-scope file now, by listing; folders and files recorded.

        Drive-wide: one listing. Folder-scoped: a breadth-first walk, asking
        for several pending folders per request (``'a' in parents or 'b' in
        parents``), since a deep, sparse tree is the common shape.
        """
        folder_id = config.get("folder_id")
        glob = config.get("name_glob")
        tracking = bool(folder_id or glob)
        entry["folders"], entry["files"] = {}, {}
        if folder_id:
            assert_drive_id(folder_id)
            entry["folders"][folder_id] = None
        pending = [folder_id] if folder_id else [None]
        while pending:
            batch, pending = pending[:FOLDERS_PER_QUERY], pending[FOLDERS_PER_QUERY:]
            if batch == [None]:
                query = "trashed = false"
            else:
                parents = " or ".join(f"'{parent}' in parents" for parent in batch)
                query = f"({parents}) and trashed = false"
            for record in self._list(client, config, query=query):
                if record["is_folder"]:
                    if folder_id and record["id"] not in entry["folders"]:
                        entry["folders"][record["id"]] = record["parent_id"]
                        pending.append(record["id"])
                    continue
                if glob and not name_matches(record["name"], glob):
                    continue
                if tracking:
                    entry["files"][record["id"]] = record["parent_id"]
                yield record

    def _list(self, client, config, *, query):
        """``files.list`` over `query`, every page, as records; folders included."""
        params = {
            "q": query,
            "fields": LIST_FIELDS,
            "pageSize": int(config.get("page_size", DEFAULT_PAGE_SIZE)),
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if config.get("drive_id"):
            params.update({"corpora": "drive", "driveId": config["drive_id"]})
        for payload in paginate(
            client, "files", params=params, what="listing Drive files"
        ):
            for item in payload.get("files") or []:
                yield file_record(item)

    def _changes(self, client, config, entry):
        """Replay the feed from the stored token, scoped, and store the new one.

        Scope is a set of folders. A change is in scope when its parent is one
        of them (or the Binding is drive-wide). A file that leaves the scope —
        moved out, renamed out of the glob, trashed, or under a folder that
        was — is a deletion of a row this Binding landed, and is tombstoned
        because the entry remembers landing it. A folder that leaves takes its
        subtree with it. Children of a folder *moved in* are not replayed by
        the feed; ``reset_binding_state`` re-enumerates.
        """
        folder_scoped = bool(config.get("folder_id"))
        glob = config.get("name_glob")
        tracking = folder_scoped or bool(glob)
        folders, files = entry["folders"], entry["files"]
        params = {
            "pageToken": entry["token"],
            "fields": CHANGES_FIELDS,
            "pageSize": int(config.get("page_size", DEFAULT_PAGE_SIZE)),
            "includeRemoved": "true",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        }
        if config.get("drive_id"):
            params["driveId"] = config["drive_id"]

        def drop_subtree(folder_id):
            """Tombstone every landed file under `folder_id`, and forget the tree."""
            children = {}
            for child, parent in folders.items():
                children.setdefault(parent, []).append(child)
            gone, frontier = {folder_id}, [folder_id]
            while frontier:
                current = frontier.pop()
                for child in children.get(current, ()):
                    if child not in gone:
                        gone.add(child)
                        frontier.append(child)
            for child in gone:
                folders.pop(child, None)
            for file_id, parent in list(files.items()):
                if parent in gone:
                    del files[file_id]
                    yield tombstone({"id": file_id})

        while True:
            response = google_request(client, "changes", params=params)
            if _token_rejected(response):
                raise _TokenExpired
            raise_for_google_error(response, what="reading the Drive changes feed")
            payload = response.json()
            for change in payload.get("changes") or []:
                file_id = change.get("fileId")
                if not file_id:
                    continue
                item = change.get("file") or {}
                gone = change.get("removed") or item.get("trashed")
                if file_id in folders:
                    parent = (item.get("parents") or [None])[0]
                    is_root = folders[file_id] is None
                    if gone:
                        yield from drop_subtree(file_id)
                    elif is_root or parent is None:
                        # The root's own parent is never in scope; a rename or
                        # a starring of the root must not read as a move out.
                        pass
                    elif parent not in folders:
                        yield from drop_subtree(file_id)
                    else:
                        # Moved within scope: record the new parent, or a later
                        # drop of the old one would take this subtree with it.
                        folders[file_id] = parent
                    continue
                if gone:
                    if tracking:
                        if files.pop(file_id, None) is not None:
                            yield tombstone({"id": file_id})
                    elif item.get("mimeType") != FOLDER_MIME:
                        # Drive-wide and unfiltered: nothing is tracked, and a
                        # spurious tombstone for a file never landed inserts
                        # one dead row, while a dropped one leaves a deleted
                        # document live forever.
                        yield tombstone({"id": file_id})
                    continue
                record = file_record(item)
                if record["is_folder"]:
                    if folder_scoped and record["parent_id"] in folders:
                        folders[file_id] = record["parent_id"]
                    continue
                in_scope = (not folder_scoped or record["parent_id"] in folders) and (
                    not glob or name_matches(record["name"], glob)
                )
                if in_scope:
                    if tracking:
                        files[file_id] = record["parent_id"]
                    yield record
                elif tracking and files.pop(file_id, None) is not None:
                    yield tombstone({"id": file_id})

            new_token = payload.get("newStartPageToken")
            if new_token:
                entry["token"] = new_token
                return
            params["pageToken"] = payload.get("nextPageToken") or ""
            if not params["pageToken"]:
                raise SourceError(
                    "Drive changes page carried neither a next nor a new token"
                )

    # --- content -----------------------------------------------------------

    def fetch_content(self, *, binding, credentials, resource, reference, max_bytes):
        """Download a file, or export a native Google format.

        `reference` carries the landed ``id``; ``mime_type`` saves a metadata
        round trip; ``export_mime_type`` overrides the default export.
        """
        file_id = reference.get("id")
        if not file_id:
            raise SourceError("fetch_content needs the landed 'id' in `reference`")
        try:
            assert_drive_id(str(file_id))
        except ConfigurationError as exc:
            raise SourceError(str(exc)) from exc
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        mime = reference.get("mime_type")
        if not mime:
            meta = google_json(
                client,
                f"files/{file_id}",
                params={"fields": "mimeType", "supportsAllDrives": "true"},
                what=f"reading file {file_id!r}",
            )
            mime = meta.get("mimeType") or ""
        if mime.startswith(NATIVE_PREFIX):
            export = reference.get("export_mime_type") or DEFAULT_EXPORTS.get(mime)
            if not export:
                raise SourceError(
                    f"{mime!r} has no default export; pass 'export_mime_type'."
                )
            path, params = f"files/{file_id}/export", {"mimeType": export}
        else:
            path, params = (
                f"files/{file_id}",
                {"alt": "media", "supportsAllDrives": "true"},
            )
        return _stream(
            client, path, params, max_bytes=max_bytes, what=f"file {file_id!r}"
        )

    # --- operations --------------------------------------------------------

    def check_connection(self, *, connection, credentials, binding=None):
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        payload = google_json(
            client,
            "about",
            params={"fields": "user(emailAddress)"},
            what="testing the Drive connection",
        )
        who = (payload.get("user") or {}).get("emailAddress", "")
        return f"ok ({who})" if who else "ok"

    def discover(
        self, *, connection, credentials, query=None, path=None, cursor=None, limit=None
    ):
        """Folders and files: the drive at the top, a folder under ``folder/<id>``."""
        _, parent = parse_drive_path(path)
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        return list_drive(
            client, parent=parent, query=query, cursor=cursor, limit=limit
        )


# --- shared with the Sheets source -----------------------------------------


def parse_drive_path(path, *, kinds=("folder",)):
    """``(kind, id)`` from a discovery path such as ``folder/<id>``, or
    ``(None, None)`` for the top. Raises for a path this source did not issue."""
    if not path:
        return None, None
    kind, _, target = path.partition("/")
    if kind in kinds and target:
        assert_drive_id(target)
        return kind, target
    expected = " or ".join(f"'{kind}/<id>'" for kind in kinds)
    raise ConfigurationError(f"discovery path must be {expected}, got {path!r}.")


def assert_drive_id(value):
    if not isinstance(value, str) or not DRIVE_ID_RE.fullmatch(value):
        raise ConfigurationError(
            f"{value!r} is not a Drive id; ids are letters, digits, '-' and '_'."
        )


def list_drive(
    client, *, parent, query, cursor, limit, mime_types=None, spreadsheet_paths=False
):
    """One page of Drive, in the discovery envelope.

    `mime_types` narrows to those types (folders are always included below a
    parent, so a UI can descend). `parent` None lists across the whole drive.
    `spreadsheet_paths` gives a spreadsheet a ``spreadsheet/<id>`` path — only
    the Sheets source can descend into one, so only it asks for that.
    """
    terms = ["trashed = false"]
    if parent:
        assert_drive_id(parent)
        terms.append(f"'{parent}' in parents")
    if mime_types:
        wanted = list(mime_types) + ([FOLDER_MIME] if parent else [])
        terms.append("(" + " or ".join(f"mimeType = '{m}'" for m in wanted) + ")")
    if query:
        needle = str(query).replace("\\", "\\\\").replace("'", "\\'")
        terms.append(f"name contains '{needle}'")
    params = {
        "q": " and ".join(terms),
        "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,webViewLink,size)",
        "pageSize": int(limit or DEFAULT_PAGE_SIZE),
        "orderBy": "folder,modifiedTime desc",
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }
    if cursor:
        params["pageToken"] = cursor
    payload = google_json(client, "files", params=params, what="listing Drive files")
    items = []
    for entry in payload.get("files") or []:
        mime = entry.get("mimeType") or ""
        kind = _kind_of(mime)
        file_id = entry.get("id")
        items.append(
            {
                "id": file_id,
                "name": entry.get("name"),
                "kind": kind,
                "path": (
                    f"folder/{file_id}"
                    if kind == "folder"
                    else f"spreadsheet/{file_id}"
                    if spreadsheet_paths and kind == "spreadsheet"
                    else None
                ),
                "mime_type": mime,
                "modified_at": entry.get("modifiedTime"),
                "web_url": entry.get("webViewLink"),
                "size": entry.get("size"),
                # What a Binding needs, so the UI copies rather than derives.
                "spreadsheet_id": file_id if mime == SPREADSHEET_MIME else None,
                "file_id": None if kind == "folder" else file_id,
                "folder_id": file_id if kind == "folder" else None,
            }
        )
    return {"items": items, "next_cursor": payload.get("nextPageToken") or None}


def _kind_of(mime):
    if mime == FOLDER_MIME:
        return "folder"
    if mime == SPREADSHEET_MIME:
        return "spreadsheet"
    if mime == "application/vnd.google-apps.document":
        return "document"
    return "file"


def file_record(entry):
    """Flatten a Drive ``file`` into one landing row."""
    mime = entry.get("mimeType") or ""
    parents = entry.get("parents") or []
    size = entry.get("size")
    return {
        "id": entry.get("id"),
        "name": entry.get("name"),
        "mime_type": mime,
        "is_folder": mime == FOLDER_MIME,
        "is_native": mime.startswith(NATIVE_PREFIX),
        "size": int(size) if isinstance(size, str) and size.isdigit() else size,
        "md5": entry.get("md5Checksum"),
        "parent_id": parents[0] if parents else None,
        "drive_id": entry.get("driveId"),
        "version": entry.get("version"),
        "web_url": entry.get("webViewLink"),
        "created_at": entry.get("createdTime"),
        "modified_at": entry.get("modifiedTime"),
    }


def _stream(client, path, params, *, max_bytes, what):
    """``(bytes, content_type)`` under `max_bytes`, checked while streaming."""
    response = google_request(client, path, params=params, stream=True)
    try:
        raise_for_google_error(response, what=f"downloading {what}")
        return read_capped(response, max_bytes=max_bytes, what=what)
    finally:
        response.close()
