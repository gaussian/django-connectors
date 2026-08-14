"""SharePoint / OneDrive ``driveItem`` metadata, synchronized through Graph delta.

**Why delta and not a modified-time cursor.** Every other incremental source in
this package walks a ``lastModifiedDateTime``-style cursor, and for files that
is quietly wrong in three ways. A rename or a move does not always bump the
modified time on the items *underneath* it, so a whole subtree silently stops
being reachable at the path the customer configured. A file restored from the
recycle bin comes back with its *original* timestamp, below the stored cursor,
and is never re-fetched. And most importantly a deletion emits nothing at all —
the row stays in the landing table forever, and a Projection keeps writing a
target record for a document that no longer exists. ``/delta`` is Graph's answer
to exactly this: it returns a change feed, it reports deletions explicitly, and
its token is opaque state rather than a value that has to be ordered. That is
why :attr:`EntraFilesSource.emits_tombstones` is True here and False for the
cursor-based sources.

**The delta token is state, not a cursor.** It is stored in dlt's resource state
(so it advances only when a load actually succeeds) keyed by the scope it was
issued for, because a token is only meaningful for the drive and folder it came
from — repointing a Binding at a different library and reusing the old token
would return a change feed for the old one. Graph can also invalidate a token
outright (HTTP 410 ``resyncRequired``, typically after a long gap or a
service-side migration); that is handled by discarding it and re-enumerating,
which is the only correct response and is *lossy for deletions* — anything
removed while the token was invalid is never reported, so those rows stay in the
landing table until the next full reconciliation. Said plainly here because it
cannot be detected after the fact.

**File bytes are never landed.** Only metadata becomes rows. A landing column is
the wrong place for a 40MB spreadsheet — it makes every merge rewrite the blob,
it exceeds MySQL's ``TEXT`` limits and wedges the load package, and it puts
customer document content in a table whose whole purpose is to be sampled and
previewed in a UI. Content download is available as a separate, explicit call:
:func:`download_item_content`, which is what
:mod:`django_connectors.providers.microsoft.excel` is built on.

**Unverified against a live provider.** Written against Microsoft's published
Graph v1.0 behaviour and exercised only against an in-process fake. Not
exercised: real delta tokens and their expiry/``resyncRequired`` behaviour; real
Graph throttling (the ``Retry-After`` values Microsoft actually sends, and the
per-app vs per-tenant limits behind them); delta scoped to a *path-addressed*
sub-folder (``/root:/Folder:/delta``), which is the least-documented of the
addressing forms used here; SharePoint document libraries with retention labels
or checked-out files; drives large enough for Graph to page a delta feed into
hundreds of pages; and OneDrive Personal, where delta is only supported on the
drive root.
"""

import datetime as dt
import fnmatch
import re
import time
from email.utils import parsedate_to_datetime
from typing import ClassVar
from urllib.parse import quote, unquote, urlsplit

from django_connectors.errors import scrub
from django_connectors.exceptions import (
    AuthError,
    ConfigurationError,
    CredentialsRevoked,
    SourceError,
)
from django_connectors.sources.base import SourceDefinition
from django_connectors.sources.memory import tombstone

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

#: Graph endpoints for the global and sovereign clouds. ``graph_base_url`` is
#: Binding configuration, and every request carries an *organisation-wide*
#: app-only bearer token — so an unconstrained base URL is not an SSRF bug, it
#: is credential exfiltration with a tenant's entire document estate attached.
#: Lifting it takes a registered subclass, exactly as the REST source's
#: private-address guard does.
ALLOWED_GRAPH_HOSTS = frozenset(
    {
        "graph.microsoft.com",
        "graph.microsoft.us",
        "dod-graph.microsoft.us",
        "microsoftgraph.chinacloudapi.cn",
        "graph.microsoft.de",
    }
)

DEFAULT_RESOURCE = "drive_items"
DEFAULT_PAGE_SIZE = 200
DEFAULT_TIMEOUT_SECONDS = 60

#: Where the delta link lives in dlt's resource state.
DELTA_STATE_KEY = "graph_delta"

#: Statuses worth retrying in place. 429 is Graph's normal operating mode under
#: load; 503/504 are its normal operating mode during a service update.
RETRYABLE_STATUS = frozenset({429, 503, 504})
DEFAULT_MAX_ATTEMPTS = 5

#: A hostile or buggy ``Retry-After`` must not be able to park a worker for a
#: day while it holds the Binding's lease — the lease would expire, the Run
#: would be reaped as stale, and the delta token would never advance. Failing
#: fast and letting the scheduler try again later is strictly better.
MAX_RETRY_AFTER_SECONDS = 120
MAX_RETRY_BUDGET_SECONDS = 300

#: Location keys, mapped to the Graph collection that addresses them. Exactly
#: one must be present: two would silently mean whichever this dict yields first.
DRIVE_ADDRESSES = {
    "drive_id": "drives/{value}",
    "site_id": "sites/{value}/drive",
    "user_id": "users/{value}/drive",
    "group_id": "groups/{value}/drive",
}

#: Characters a Graph id may contain. Site ids carry commas and dots, drive ids
#: carry ``!``, ``-`` and ``_``. Everything that could change which resource the
#: URL addresses — ``/``, ``\``, ``?``, ``#``, ``%``, whitespace — is refused
#: rather than escaped, because an escaped id that Graph then rejects is a much
#: worse diagnostic than a refusal at save time.
GRAPH_ID_RE = re.compile(r"^[A-Za-z0-9!$&'()*+,.:;=@_~-]{1,512}$")


class EntraFilesSource(SourceDefinition):
    """Land ``driveItem`` metadata for one drive (or one folder of it).

    Configuration::

        {
          "site_id": "contoso.sharepoint.com,<guid>,<guid>",
          "folder_path": "/Finance/Reports",
          "name_glob": "*.xlsx",
          "resource": "drive_items"
        }

    Exactly one of ``drive_id`` / ``site_id`` / ``user_id`` / ``group_id``
    addresses the drive. ``folder_path`` or ``folder_item_id`` narrows the delta
    feed to a subtree; omitting both syncs the whole drive.
    """

    key = "entra_files"
    provider = "microsoft"
    supported_auth_backends = ("entra", "static")
    # Nothing optional: this talks to Graph over dlt's own requests client.
    # azure-identity is the *auth backend's* dependency, not this source's, so
    # declaring it here would make the W005 check nag hosts that inject a token
    # from somewhere else entirely.
    required_extras: ClassVar[dict[str, str]] = {}

    #: Delta reports deletions, which is what makes tombstones honest here.
    emits_tombstones = True

    #: Opt-in escape hatch for a non-Microsoft Graph endpoint — a recorded-proxy
    #: test harness, or a cloud Microsoft has not shipped yet. A class attribute
    #: so that only a host registering a subclass can lift it.
    allow_custom_graph_base_url = False

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run, at Binding save time."""
        config = config or {}
        self.validate_location(config)

        resource = config.get("resource", DEFAULT_RESOURCE)
        if not isinstance(resource, str) or not resource:
            raise ConfigurationError(
                f"'resource' must be a non-empty string naming the landing "
                f"resource, got {resource!r}."
            )

        glob = config.get("name_glob")
        if glob is not None and not isinstance(glob, str):
            raise ConfigurationError("'name_glob' must be a string like '*.xlsx'.")

        page_size = config.get("page_size", DEFAULT_PAGE_SIZE)
        if not isinstance(page_size, int) or isinstance(page_size, bool):
            raise ConfigurationError("'page_size' must be an integer.")
        if not 1 <= page_size <= 999:
            raise ConfigurationError(
                f"'page_size' must be between 1 and 999 (Graph's $top ceiling), "
                f"got {page_size}."
            )

        select = config.get("select")
        if select is not None and (
            not isinstance(select, list)
            or any(not isinstance(field, str) for field in select)
        ):
            raise ConfigurationError(
                "'select' must be a list of driveItem field names."
            )
        return None

    def validate_location(self, config):
        """Validate the drive address and the Graph base URL.

        Split out from :meth:`validate_config` because
        :class:`~django_connectors.providers.microsoft.excel.EntraExcelSource`
        addresses drives identically and validates everything else differently.
        """
        present = sorted(key for key in DRIVE_ADDRESSES if config.get(key))
        if len(present) != 1:
            raise ConfigurationError(
                f"exactly one of {sorted(DRIVE_ADDRESSES)} must identify the "
                f"drive; got {present or 'none'}. 'site_id' syncs a SharePoint "
                f"site's default document library, 'user_id' a person's "
                f"OneDrive, 'drive_id' a specific library."
            )
        assert_graph_id(present[0], config[present[0]])

        folder_path = config.get("folder_path")
        folder_item_id = config.get("folder_item_id")
        if folder_path and folder_item_id:
            raise ConfigurationError(
                "give either 'folder_path' or 'folder_item_id', not both — they "
                "address the same thing and would silently disagree."
            )
        if folder_path is not None:
            assert_folder_path(folder_path)
        if folder_item_id:
            assert_graph_id("folder_item_id", folder_item_id)

        assert_graph_url(
            self.base_url(config), allow_custom=self.allow_custom_graph_base_url
        )
        return None

    def base_url(self, config):
        return str((config or {}).get("graph_base_url") or GRAPH_BASE_URL).rstrip("/")

    # --- extraction --------------------------------------------------------

    def incremental_for(self, resource_name, binding):
        """Always ``None``: delta replaces the incremental entirely.

        Declaring a cursor here would be actively harmful. dlt's ``Incremental``
        filters on a *comparable value*, and the delta token is not one; worse,
        a tombstone carries no ``lastModifiedDateTime`` at all, so the cursor
        would have to be told to let it through and would then have no filtering
        left to do. The change feed is already the increment.
        """
        return None

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        token = access_token(credentials)
        resource_name = config.get("resource", DEFAULT_RESOURCE)

        def emit():
            yield from self.iter_items(config, token, use_state=True)

        # write_disposition is stated, never inherited. dlt defaults the hint to
        # "append", not to nothing, and an append here would add a fresh copy of
        # every re-fetched driveItem on every delta page — and would make the
        # tombstones meaningless, since the deleted row and the live row would
        # both sit in the table with no way to tell which came last.
        resource = dlt.resource(
            emit,
            name=resource_name,
            primary_key="id",
            write_disposition="merge",
        )()
        # dlt.source() called as a function: the decorator takes the source name
        # from the wrapped function's __name__ and cannot produce a chosen one.
        return dlt.source(lambda: [resource], name=self.key, section=self.key)()

    def iter_items(self, config, token, *, use_state=False, session=None):
        """Yield one record per changed ``driveItem``, following the delta feed.

        `use_state` False re-enumerates from scratch and stores nothing, which
        is how a full-replace consumer gets a complete current snapshot: Graph
        returns the entire hierarchy for a delta call made without a token.

        `session` lets a caller that is about to make further Graph calls — the
        Excel source downloads every changed workbook — reuse one connection
        pool and one token rather than opening a second session per item.
        """
        state = self.delta_state() if use_state else None
        owned = session is None
        session = session or graph_session(token, timeout=self.timeout(config))
        base = self.base_url(config)
        scope = delta_scope(config)
        first_url = self.initial_delta_url(config, base=base, scope=scope)

        stored = (state or {}).get(DELTA_STATE_KEY) or {}
        # A token issued for a different drive or folder describes a different
        # hierarchy; reusing it would return changes for the old scope forever.
        url = stored.get("delta_link") if stored.get("scope") == scope else None
        url = url or first_url
        resynced = False

        try:
            while url:
                response = graph_request(session, "GET", url)
                if is_resync_required(response):
                    if resynced:
                        raise SourceError(
                            f"Graph asked for a delta resync twice in one run "
                            f"for {scope!r}; refusing to loop."
                        )
                    resynced = True
                    if state is not None:
                        state.pop(DELTA_STATE_KEY, None)
                    url = first_url
                    continue

                raise_for_graph_error(response, what=f"delta feed for {scope!r}")
                payload = response.json()

                for item in payload.get("value") or []:
                    record = self.record_for(item, config)
                    if record is not None:
                        yield record

                delta_link = payload.get("@odata.deltaLink")
                if delta_link:
                    assert_same_origin(delta_link, base)
                    if state is not None:
                        state[DELTA_STATE_KEY] = {
                            "scope": scope,
                            "delta_link": delta_link,
                        }
                    url = None
                    continue

                url = payload.get("@odata.nextLink")
                if url:
                    # A nextLink is server-supplied and is followed carrying the
                    # bearer token, so it is held to the same origin as the base.
                    assert_same_origin(url, base)
        finally:
            if owned:
                session.close()

    def record_for(self, item, config):
        """One landing record for a delta entry, or None to skip it."""
        if not item.get("id"):
            return None
        if "root" in item:
            # The scope's own root folder is echoed in every delta response and
            # is not a thing anyone wants a row for.
            return None

        deleted = item.get("deleted") is not None
        is_folder = "folder" in item
        include_folders = bool(config.get("include_folders"))
        glob = config.get("name_glob")
        name = item.get("name") or ""

        if is_folder and not include_folders:
            return None
        if glob and not is_folder and name and not name_matches(name, glob):
            return None

        if deleted:
            # Identity only. Graph's deleted entries reliably carry an id and
            # little else, and delete-insert merge nulls every column the
            # tombstone does not supply anyway — see sources/memory.tombstone.
            # A tombstone with no name cannot be glob-filtered, so it is emitted:
            # a spurious tombstone for a file we never landed inserts one dead
            # row, while a dropped one leaves a deleted document live forever.
            return tombstone({"id": item["id"]})

        return item_record(item, drive_id=config.get("drive_id") or "")

    def delta_state(self):
        """dlt's per-resource state dict, which only persists on a good load."""
        import dlt

        return dlt.current.resource_state()

    def initial_delta_url(self, config, *, base, scope):
        """The delta URL for a cold start — no token, full current hierarchy."""
        params = [f"$top={int(config.get('page_size', DEFAULT_PAGE_SIZE))}"]
        select = config.get("select")
        if select:
            # Opt-in: Graph's delta feed does support $select, but a $select
            # that omits a facet this module reads (``file``, ``folder``,
            # ``deleted``) turns every item into a half-populated row with no
            # error, so the default asks for the whole driveItem.
            params.append("$select=" + quote(",".join(select), safe=","))
        return f"{base}/{scope}/delta?{'&'.join(params)}"

    def timeout(self, config):
        return float((config or {}).get("request_timeout") or DEFAULT_TIMEOUT_SECONDS)

    # --- operations --------------------------------------------------------

    def check_connection(self, *, connection, credentials, binding=None):
        """One request against the configured drive.

        `binding` is optional so a Connection can be tested before any Binding
        exists; ``Connection.metadata`` then supplies the drive address.
        """
        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        self.validate_location(config)
        base = self.base_url(config)
        session = graph_session(access_token(credentials), timeout=self.timeout(config))
        try:
            response = graph_request(session, "GET", f"{base}/{drive_address(config)}")
            raise_for_graph_error(response, what="drive lookup")
            drive = response.json()
        finally:
            session.close()
        return f"ok: {drive.get('driveType', 'drive')} {drive.get('name', '')}".strip()

    def discover(self, *, connection, credentials, query=None):
        """List what could be synchronized: sites, drives, or a folder's children.

        Driven by ``Connection.metadata`` because discovery happens *before* any
        Binding exists to carry a config.
        """
        config = connection.metadata or {}
        base = self.base_url(config)
        assert_graph_url(base, allow_custom=self.allow_custom_graph_base_url)
        session = graph_session(access_token(credentials), timeout=self.timeout(config))
        try:
            if config.get("drive_id") or config.get("site_id"):
                self.validate_location(config)
                url = f"{base}/{drive_address(config)}/root/children?$top=200"
                key = "items"
            else:
                url = f"{base}/sites?search={quote(str(query or ''), safe='')}"
                key = "sites"
            response = graph_request(session, "GET", url)
            raise_for_graph_error(response, what="discovery")
            payload = response.json()
        finally:
            session.close()

        return {
            key: [
                {
                    "id": entry.get("id"),
                    "name": entry.get("name") or entry.get("displayName"),
                    "web_url": entry.get("webUrl"),
                    "is_folder": "folder" in entry,
                }
                for entry in payload.get("value") or []
            ]
        }


# --- HTTP ------------------------------------------------------------------


def graph_session(token, *, timeout=DEFAULT_TIMEOUT_SECONDS):
    """A requests session carrying the app-only bearer token.

    Built on dlt's retry client so transport failures behave like every other
    request dlt makes, but with **status retries turned off**: Graph's throttling
    contract is ``Retry-After``, and honouring it precisely is the difference
    between being throttled for a minute and being throttled for an hour. That
    is done explicitly in :func:`graph_request` instead.

    The token is set as a session header rather than per request, which is safe
    for the redirect that ``/content`` issues: ``requests`` strips
    ``Authorization`` when a redirect crosses hosts, so the pre-authenticated
    SharePoint download URL never receives the tenant-wide token.
    """
    from dlt.sources.helpers.requests.retry import Client

    session = Client(
        raise_for_status=False,
        status_codes=(),
        request_max_attempts=3,
        request_timeout=timeout,
    ).session
    session.headers["Authorization"] = f"Bearer {token}"
    session.headers["Accept"] = "application/json"
    # Microsoft asks for an identifying User-Agent and uses it when diagnosing
    # throttling; an unset one is served worse.
    session.headers["User-Agent"] = "django-connectors"
    return session


def pause(seconds):
    """Sleep between retries.

    A module-level function so a test can replace it: the throttling path is the
    one behaviour here that must be exercised, and exercising it for real would
    mean a test suite that sleeps for minutes.
    """
    time.sleep(seconds)


def graph_request(
    session,
    method,
    url,
    *,
    max_attempts=DEFAULT_MAX_ATTEMPTS,
    budget=MAX_RETRY_BUDGET_SECONDS,
    **kwargs,
):
    """Perform one Graph request, honouring ``Retry-After`` on 429/503/504.

    Graph throttles aggressively and per-resource, and ignoring ``Retry-After``
    is how an integration escalates from "slow" to "blocked": Microsoft extends
    the window for clients that keep hammering. Returns the response rather than
    raising, because the delta loop has to inspect 410 before any error mapping
    happens.
    """
    attempt = 0
    spent = 0.0
    while True:
        attempt += 1
        response = session.request(method, url, **kwargs)
        if response.status_code not in RETRYABLE_STATUS or attempt >= max_attempts:
            return response

        delay = retry_delay(response, attempt)
        if spent + delay > budget:
            # Give the response back and let the caller raise a real error.
            # Sleeping past the budget would hold the Binding's lease long
            # enough for the reaper to fail the Run anyway.
            return response
        response.close()
        pause(delay)
        spent += delay


def retry_delay(response, attempt):
    """Seconds to wait: what Graph asked for, else exponential backoff."""
    header = response.headers.get("Retry-After")
    seconds = parse_retry_after(header) if header else None
    if seconds is None:
        seconds = 2.0 ** (attempt - 1)
    return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)


def parse_retry_after(header):
    """``Retry-After`` as seconds — delta-seconds or an HTTP date — else None."""
    text = str(header).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    now = dt.datetime.now(tz=when.tzinfo or dt.UTC)
    return max((when - now).total_seconds(), 0.0)


def is_resync_required(response):
    """Whether Graph has invalidated the delta token we sent.

    Any 410 counts, not only the documented ``resyncRequired`` /
    ``resyncChangesApplyDifferences`` codes. A 410 on a delta URL means the link
    is gone whatever the code says, and the two possible mistakes are not
    symmetric: treating an unrecognised 410 as a resync costs one full
    re-enumeration, while treating it as a hard error wedges the Binding on
    every subsequent run until someone clears the dlt state by hand.
    """
    return response.status_code == 410


def graph_error_fields(response):
    """``(code, message)`` from Graph's error envelope, defensively."""
    try:
        payload = response.json()
    except ValueError:
        return "", (response.text or "")[:500]
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return "", ""
    message = error.get("message")
    if not isinstance(message, str):
        message = ""
    code = error.get("code")
    return (code if isinstance(code, str) else ""), message


def raise_for_graph_error(response, *, what):
    """Map a Graph error response onto this package's exception hierarchy.

    The 401/403 split is the one that matters, because it decides whether a
    Connection is taken out of service. A 401 means the bearer token itself was
    refused — for an app-only credential minted seconds earlier that means the
    application's grant is gone, which is revocation and no retry recovers it. A
    403 means the token is *fine* and this application simply lacks a permission
    (``Files.Read.All`` never consented, or a sensitivity label blocking the
    item). Marking the Connection revoked for that would take every other
    Binding on the same tenant offline over one unreadable folder, so it is a
    plain AuthError.
    """
    status = response.status_code
    if status < 400:
        return response

    code, message = graph_error_fields(response)
    request_id = response.headers.get("request-id") or response.headers.get(
        "client-request-id"
    )
    detail = scrub(
        f"HTTP {status} {code or '(no code)'}: {message}"
        + (f" [request-id {request_id}]" if request_id else "")
    )

    if status == 401:
        raise CredentialsRevoked(
            f"Graph refused the app-only token on the {what}. For a token "
            f"minted at the start of this Run that means the application's "
            f"grant in this tenant is gone — an administrator removed the "
            f"enterprise application or revoked consent. {detail}"
        )
    if status == 403:
        raise AuthError(
            f"Graph accepted the token but denied the {what}: the application "
            f"is missing a permission (Files.Read.All / Sites.Read.All "
            f"application permission, granted with admin consent), or the item "
            f"carries a sensitivity label this application cannot read. "
            f"{detail}"
        )
    if status == 404:
        raise SourceError(
            f"Graph could not find the {what}. The drive, site or folder id is "
            f"wrong, or the item was deleted and purged. {detail}"
        )
    if status == 429:
        raise SourceError(
            f"Graph is still throttling the {what} after the configured "
            f"retries. {detail}"
        )
    raise SourceError(f"Graph failed the {what}. {detail}")


def download_item_content(
    session, *, base_url, drive_id, item_id, max_bytes, chunk_size=1 << 16
):
    """Return the bytes of one ``driveItem``. The documented content hook.

    Deliberately *not* wired into the resource: file bytes must never become a
    landing column (see the module docstring). Callers that need content — the
    Excel source, a host's own OCR step — ask for it here, one item at a time,
    and decide what to do with it themselves.

    `max_bytes` is not advisory. ``/content`` streams whatever the drive holds,
    a worker's memory is finite, and "the customer uploaded a 4GB video into the
    reports folder" is a Tuesday. The size is checked both from the declared
    ``Content-Length`` (cheap, refuses before the transfer) and while streaming
    (authoritative, because the header is optional after a redirect).
    """
    url = f"{base_url}/drives/{drive_id}/items/{item_id}/content"
    response = graph_request(session, "GET", url, stream=True)
    try:
        raise_for_graph_error(response, what=f"content of item {item_id!r}")

        declared = response.headers.get("Content-Length")
        if declared and declared.isdigit() and int(declared) > max_bytes:
            raise SourceError(
                f"driveItem {item_id!r} is {declared} bytes, over the "
                f"{max_bytes}-byte limit for this binding. Raise "
                f"'max_file_bytes' if the worker can genuinely hold it."
            )

        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size):
            total += len(chunk)
            if total > max_bytes:
                raise SourceError(
                    f"driveItem {item_id!r} exceeded the {max_bytes}-byte limit "
                    f"while downloading; the transfer was abandoned."
                )
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        response.close()


# --- records ---------------------------------------------------------------


def item_record(item, *, drive_id=""):
    """Flatten a ``driveItem`` into one landing row.

    Flattened deliberately rather than landed as nested JSON: the landing layer
    forces ``max_table_nesting = 0``, so a nested ``parentReference`` would land
    as an opaque JSON string that no Projection mapping can read without the
    customer hand-writing a ``json_extract``.
    """
    file_facet = item.get("file") or {}
    hashes = file_facet.get("hashes") or {}
    parent = item.get("parentReference") or {}
    return {
        "id": item.get("id"),
        "name": item.get("name"),
        "path": item_path(item),
        "parent_id": parent.get("id"),
        "drive_id": parent.get("driveId") or drive_id or None,
        "is_folder": "folder" in item,
        "size": item.get("size"),
        "mime_type": file_facet.get("mimeType"),
        # Whichever hash the drive computes: OneDrive for Business and
        # SharePoint use quickXorHash, OneDrive Personal uses sha256.
        "content_hash": (
            hashes.get("quickXorHash")
            or hashes.get("sha256Hash")
            or hashes.get("sha1Hash")
        ),
        "etag": item.get("eTag"),
        # cTag changes on *content* change only; eTag also changes on metadata
        # edits. A consumer deciding whether to re-download wants cTag.
        "ctag": item.get("cTag"),
        "web_url": item.get("webUrl"),
        "created_at": item.get("createdDateTime"),
        "last_modified_at": item.get("lastModifiedDateTime"),
        "created_by": identity_name(item.get("createdBy")),
        "last_modified_by": identity_name(item.get("lastModifiedBy")),
    }


def name_matches(name, glob):
    """Case-insensitive glob match, because SharePoint file names are.

    ``fnmatch.fnmatch`` follows the *host* filesystem's rules, so on Linux
    ``*.xlsx`` silently fails to match ``Budget.XLSX`` while on macOS it
    matches — a Binding that works on a developer's laptop and drops half the
    documents in production. Neither of those is the provider's rule: SharePoint
    and OneDrive treat names case-insensitively, so the comparison is folded
    explicitly rather than inherited from wherever the worker happens to run.
    """
    return fnmatch.fnmatchcase(name.lower(), glob.lower())


def identity_name(identity_set):
    """The display name out of a Graph ``identitySet``.

    Checks ``application`` and ``device`` too: a file created by a Power
    Automate flow or a sync client has no ``user`` at all, and reading only
    ``user`` would land NULL for every one of them.
    """
    for slot in ("user", "application", "device"):
        entry = (identity_set or {}).get(slot)
        if isinstance(entry, dict) and entry.get("displayName"):
            return entry["displayName"]
    return None


def item_path(item):
    """The drive-relative path of an item, percent-decoding Graph's own encoding.

    ``parentReference.path`` looks like ``/drive/root:/Finance/Q3%20Reports``.
    Landing the raw form would put ``%20`` in front of every customer mapping
    that matches on a folder name.
    """
    raw = (item.get("parentReference") or {}).get("path") or ""
    _, marker, tail = raw.partition("root:")
    folder = unquote(tail) if marker else ""
    name = item.get("name") or ""
    if not name:
        return folder or "/"
    return f"{folder.rstrip('/')}/{name}"


# --- addressing ------------------------------------------------------------


def drive_address(config):
    """The Graph path segment addressing the configured drive."""
    for key, template in DRIVE_ADDRESSES.items():
        value = config.get(key)
        if value:
            return template.format(value=value)
    raise ConfigurationError(
        f"no drive address in this configuration; one of {sorted(DRIVE_ADDRESSES)} "
        f"is required."
    )


def delta_scope(config):
    """The Graph path the delta feed hangs off, and the state key for its token.

    Doubles as the token's identity on purpose: if the customer repoints the
    Binding at another folder, the scope changes, the stored token no longer
    matches, and the next run re-enumerates instead of replaying a change feed
    for a hierarchy nobody asked about any more.
    """
    address = drive_address(config)
    item_id = config.get("folder_item_id")
    if item_id:
        return f"{address}/items/{item_id}"
    folder_path = (config.get("folder_path") or "").strip("/")
    if folder_path:
        # Path addressing: /drives/{id}/root:/Finance/Reports:/delta
        encoded = "/".join(quote(part, safe="") for part in folder_path.split("/"))
        return f"{address}/root:/{encoded}:"
    return f"{address}/root"


def assert_graph_id(field, value):
    """Refuse an id that could change which resource a URL addresses."""
    if not isinstance(value, str) or not GRAPH_ID_RE.fullmatch(value):
        raise ConfigurationError(
            f"{field!r} is not a usable Graph identifier. It must be 1-512 "
            f"characters and may not contain '/', '\\', '?', '#', '%' or "
            f"whitespace — those would change which resource the request "
            f"addresses rather than which item."
        )


def assert_folder_path(path):
    """Refuse a folder path that could escape the configured subtree."""
    if not isinstance(path, str):
        raise ConfigurationError("'folder_path' must be a string.")
    if "\\" in path:
        raise ConfigurationError(
            "'folder_path' must use '/' separators; Graph does not accept '\\'."
        )
    parts = [part for part in path.split("/") if part]
    if any(part == ".." for part in parts):
        raise ConfigurationError(
            "'folder_path' may not contain '..'. Point the Binding at the "
            "folder it should sync rather than walking up out of it."
        )
    if any(part == "." for part in parts):
        raise ConfigurationError("'folder_path' may not contain '.' segments.")


def assert_graph_url(url, *, allow_custom=False):
    """Refuse a base URL that is not a Microsoft Graph endpoint."""
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise ConfigurationError(f"{url!r} is not a usable URL: {exc}") from exc

    scheme = (parts.scheme or "").lower()
    host = (parts.hostname or "").lower()
    if allow_custom:
        if scheme not in ("http", "https") or not host:
            raise ConfigurationError(
                f"'graph_base_url' must be an http(s) URL with a host, got {url!r}."
            )
        return
    if scheme != "https" or host not in ALLOWED_GRAPH_HOSTS:
        raise ConfigurationError(
            f"refusing to send an organisation-wide Graph token to {url!r}. "
            f"'graph_base_url' must be https and one of "
            f"{sorted(ALLOWED_GRAPH_HOSTS)}. A deployment that must reach a "
            f"different endpoint should register an EntraFilesSource subclass "
            f"with allow_custom_graph_base_url = True."
        )


def assert_same_origin(url, base):
    """Hold a server-supplied continuation link to the origin we started at.

    ``@odata.nextLink`` and ``@odata.deltaLink`` come out of a response body and
    are followed with the bearer token attached. Checking the origin means a
    single malformed or tampered response cannot walk a tenant-wide token to
    another host.
    """
    link = urlsplit(url)
    origin = urlsplit(base)
    if (link.scheme, link.hostname, link.port) != (
        origin.scheme,
        origin.hostname,
        origin.port,
    ):
        raise SourceError(
            f"Graph returned a continuation link pointing at "
            f"{link.hostname!r}, which is not the endpoint this request went "
            f"to ({origin.hostname!r}). Refusing to follow it with the tenant's "
            f"bearer token attached."
        )


# --- credentials -----------------------------------------------------------

_TOKEN_KEYS = ("access_token", "token")


def access_token(credentials):
    """The bearer token out of whatever the auth backend returned.

    Accepts a bare string, a Mapping (including ``auth.Credentials``) and an
    object with an ``access_token`` attribute, so a host injecting a token from
    its own infrastructure does not have to adopt this package's credential
    type.
    """
    if isinstance(credentials, str) and credentials:
        return credentials
    if credentials is not None:
        for key in _TOKEN_KEYS:
            value = (
                credentials.get(key)
                if hasattr(credentials, "get")
                else getattr(credentials, key, None)
            )
            if value:
                return value
    raise AuthError(
        "no Microsoft Graph access token was available for this Binding. The "
        "Connection needs an auth backend that returns one — "
        "'django_connectors.providers.microsoft.auth.EntraBackend' acquires an "
        "app-only token from the tenant."
    )
