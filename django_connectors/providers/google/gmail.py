"""Gmail messages and labels, landed incrementally through the history API.

Gmail is not a cursor-shaped source and modelling it as one loses data. The
three decisions below are what this module is for.

**The incremental key is a sync token, not a record field.** Gmail's
``historyId`` is a per-mailbox sequence number, and the only way to ask "what
changed" is ``users.history.list(startHistoryId=…)``. Declaring a
``dlt.sources.incremental`` on a message field instead is the obvious mistake
and a silent one: ``internalDate`` never changes after delivery, so an
incremental keyed on it would skip every read, star, label and archive — which
is the overwhelming majority of what actually changes in a mailbox. So
:meth:`GmailSource.incremental_for` returns ``None`` and the historyId is kept
in dlt's own resource state, exactly as ``sources/sql.py`` keeps its pushdown
cursor. The library's Incremental machinery is not bypassed; there is genuinely
no per-record cursor to give it.

**History expires, and a connector that does not expect that dies permanently.**
Google retains history for roughly a week and answers ``404`` for a
``startHistoryId`` older than that — a Binding that was paused, or whose
Connection was blocked over a holiday, comes back to a 404 on every run
forever. :func:`_plan_incremental` therefore raises internally and the resource
falls back to a full ``users.messages.list`` sync. The fallback is planned
before anything is yielded, so a partial emission can never be followed by a
second, overlapping one.

**Deletion is detectable here, which is rare enough to be worth using.**
``messagesDeleted`` history records name messages that are gone, so this source
sets ``emits_tombstones = True`` and emits ``_connector_deleted`` rows. Note the
caveat from ``landing/instrument.py``: delete-insert merge replaces the whole
row, so a tombstone — which carries the id and nothing else — nulls every other
column. That is inherent to the tombstone shape, not a defect here.

The full-sync path cannot see deletions at all: it lists what exists and has no
record of what used to. So a message deleted while history was expired stays in
the landing table with ``_connector_deleted = False`` indefinitely. That is a
real, unavoidable gap in this design and is stated rather than papered over.

**Unverified against a live provider.** Built and tested against mocked HTTP
only. Not exercised: a real ``historyId`` expiring, real quota throttling, real
domain-wide-delegation consent, mailboxes large enough to need the resumable
backfill in anger, and Gmail's actual pagination behaviour under concurrent
mailbox mutation.
"""

import base64
import binascii
import datetime as dt
import logging
from typing import ClassVar

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.landing.naming import DELETED_COLUMN
from django_connectors.providers.google.auth import (
    bearer_token,
    google_client,
    google_json,
    google_request,
    paginate,
    raise_for_google_error,
)
from django_connectors.sources.base import SourceDefinition

logger = logging.getLogger(__name__)

GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1/"

MESSAGES_RESOURCE = "messages"
LABELS_RESOURCE = "labels"

# dlt resource-state keys. Namespaced because resource state is a plain dict
# shared with whatever else dlt keeps there.
HISTORY_ID_STATE_KEY = "gmail_history_id"
BACKFILL_TOKEN_STATE_KEY = "gmail_backfill_page_token"
BACKFILL_HISTORY_STATE_KEY = "gmail_backfill_history_id"

# `metadata` returns headers without bodies and is the only format that is
# cheap enough to run over a whole mailbox. `raw` is absent deliberately: it
# returns the entire RFC822 message as one base64 string, which is precisely
# the >64KB value that wedges a MySQL landing load forever (see
# LandingWedgedError).
MESSAGE_FORMATS = frozenset({"metadata", "minimal", "full"})

DEFAULT_METADATA_HEADERS = ("Subject", "From", "To", "Cc", "Date", "Message-ID")

# History record kinds worth asking for. `messageAdded` alone would miss every
# label change, which is how "read" and "archived" are represented.
HISTORY_TYPES = ("messageAdded", "messageDeleted", "labelAdded", "labelRemoved")

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500

# Circuit breaker for the history walk, matching `auth.paginate`'s. The walk
# cannot simply call `paginate` — it has to inspect the 404 that means "this
# historyId expired" before the response is raised on — so it carries its own
# copy of the two guards `paginate` has. Without them a provider or proxy that
# echoes one pageToken back forever spins inside the run, holding the Binding's
# lease and burning quota until the lease is reaped, whereupon the next worker
# enters the same loop.
MAX_HISTORY_PAGES = 10_000

# Cap on one run's initial backfill. Not a data limit: the remaining pageToken
# is stored and the next run resumes from it, and the historyId is only
# committed once the backfill finishes — so a mailbox larger than this syncs
# across several runs instead of holding the Binding's lease until it expires.
DEFAULT_MAX_MESSAGES_PER_RUN = 5000

# Bodies are opt-in and truncated. dlt maps an unbounded str to MySQL TEXT,
# which tops out at 64KB, and a single oversized row leaves a load package dlt
# retries forever. 8000 characters is generous for a body and far under it.
DEFAULT_BODY_MAX_CHARS = 8000


class _HistoryTooOld(Exception):
    """Internal: ``startHistoryId`` predates Gmail's retention window.

    Deliberately not a ``ConnectorError``. It never escapes this module — it
    exists only to unwind out of the history walk and into the full-sync
    fallback, and a subclass of the public hierarchy would invite someone to
    catch it as a real failure.
    """


class GmailSource(SourceDefinition):
    """One Gmail mailbox as ``messages`` and ``labels``.

    Which resources a Binding lands is chosen with ``Binding.resources``; both
    are built, and the runner selects before extraction, so an unselected
    resource costs no request.
    """

    key = "gmail"
    provider = "google"
    # A bearer token is a bearer token: the delegated (allauth) path and a
    # static token both work, and `bearer_token()` accepts all three shapes.
    supported_auth_backends = ("google_workspace", "allauth", "static")
    # History records name deleted messages, so deletion propagation is real
    # here rather than merely unimplemented.
    emits_tombstones = True
    # Nothing optional. Data access is plain HTTP through dlt's rest_client;
    # only GoogleWorkspaceBackend needs google-auth, and it declares that.
    required_extras: ClassVar[dict[str, str]] = {}

    #: OAuth scopes this source needs, for a host wiring up a Connection.
    scopes = ("https://www.googleapis.com/auth/gmail.readonly",)

    @property
    def api_base_url(self):
        """The Gmail API host.

        A class attribute, never a Binding config key. A customer-controlled
        base URL here would forward a Workspace OAuth token — potentially one
        with domain-wide delegation over every mailbox — to a host of their
        choosing. Pointing this at a test server is a deliberate subclass
        registration in ``DJANGO_CONNECTORS['SOURCES']``.
        """
        return GMAIL_API_BASE_URL

    # --- configuration -----------------------------------------------------

    def validate_config(self, config):
        """Reject a configuration that could not run, or would land wrong data."""
        config = config or {}

        user_id = config.get("user_id", "me")
        if not isinstance(user_id, str) or not user_id:
            raise ConfigurationError(
                "gmail source 'user_id' must be a string — 'me' for the "
                "authorized/impersonated user, or a mailbox address."
            )
        if "/" in user_id or "?" in user_id:
            # It is interpolated into the request path.
            raise ConfigurationError(
                f"gmail source 'user_id' must be a bare mailbox identifier, got "
                f"{user_id!r}."
            )

        message_format = config.get("message_format", "metadata")
        if message_format not in MESSAGE_FORMATS:
            raise ConfigurationError(
                f"gmail source 'message_format' must be one of "
                f"{sorted(MESSAGE_FORMATS)}, got {message_format!r}. 'raw' is "
                f"not offered: it lands the whole RFC822 message as one string, "
                f"which exceeds MySQL's TEXT limit and wedges the load."
            )

        if config.get("include_body") and message_format != "full":
            raise ConfigurationError(
                "gmail source 'include_body' needs message_format='full'; "
                "Gmail returns no body data for 'metadata' or 'minimal', so the "
                "body column would land empty on every row."
            )

        page_size = config.get("page_size", DEFAULT_PAGE_SIZE)
        if not isinstance(page_size, int) or not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ConfigurationError(
                f"gmail source 'page_size' must be an integer between 1 and "
                f"{MAX_PAGE_SIZE}, got {page_size!r}."
            )

        max_messages = config.get("max_messages_per_run", DEFAULT_MAX_MESSAGES_PER_RUN)
        if max_messages is not None and (
            not isinstance(max_messages, int) or max_messages < 1
        ):
            raise ConfigurationError(
                "gmail source 'max_messages_per_run' must be a positive integer, "
                "or null for no cap."
            )

        body_max = config.get("body_max_chars", DEFAULT_BODY_MAX_CHARS)
        if not isinstance(body_max, int) or body_max < 1:
            raise ConfigurationError(
                "gmail source 'body_max_chars' must be a positive integer."
            )

        for key in ("label_ids", "metadata_headers"):
            value = config.get(key)
            if value is None:
                continue
            if not isinstance(value, list | tuple) or not all(
                isinstance(item, str) and item for item in value
            ):
                raise ConfigurationError(
                    f"gmail source {key!r} must be a list of non-empty strings."
                )

        query = config.get("query")
        if query is not None and not isinstance(query, str):
            raise ConfigurationError("gmail source 'query' must be a string.")
        return None

    def incremental_for(self, resource_name, binding):
        """Always ``None``. See the module docstring.

        Gmail's cursor is a mailbox-wide sync token, not a field on a record,
        so there is nothing for the library's ``Incremental`` to filter on. The
        token lives in dlt's resource state instead.
        """
        return None

    # --- extraction --------------------------------------------------------

    def build_source(self, *, binding, credentials, run):
        import dlt

        config = binding.config or {}
        self.validate_config(config)
        # Resolve the token here, discarding it, purely so that a credential
        # which cannot be obtained at all fails *outside* the pipeline. dlt
        # wraps anything a resource generator raises in `PipelineStepFailed`,
        # and `services.runs` matches on the exception class — so a
        # CredentialsRevoked raised during extraction is recorded as a generic
        # failure and the Connection is retried on schedule forever. Raised
        # from here it reaches the runner intact and the Connection is blocked.
        bearer_token(credentials)
        client = google_client(base_url=self.api_base_url, credentials=credentials)

        resources = [
            self._messages_resource(dlt, client, config),
            self._labels_resource(dlt, client, config),
        ]
        # `dlt.source` called, not decorated: the decorator takes the source
        # name from the function's __name__ and cannot produce a chosen one.
        return dlt.source(lambda: resources, name=self.key, section=self.key)()

    def _messages_resource(self, dlt, client, config):
        user_id = config.get("user_id", "me")

        def emit():
            state = dlt.current.resource_state()
            yield from self._emit_messages(client, config, user_id, state)

        return dlt.resource(
            emit,
            name=MESSAGES_RESOURCE,
            primary_key="id",
            # Stated, never omitted. dlt defaults this hint to "append", not to
            # nothing, so leaving it out appends a fresh copy of every
            # re-fetched message on every run — with the merge key correctly
            # configured and no error raised anywhere.
            write_disposition="merge",
        )()

    def _labels_resource(self, dlt, client, config):
        user_id = config.get("user_id", "me")

        def emit():
            # labels.list is unpaginated and returns the whole (small) set, so
            # there is no incremental story here and none is invented. Merge
            # rather than replace: a label the customer stopped syncing should
            # not vanish from the landing table mid-Projection.
            payload = google_json(
                client, f"users/{user_id}/labels", what="listing Gmail labels"
            )
            for label in payload.get("labels") or ():
                yield label_record(label)

        return dlt.resource(
            emit,
            name=LABELS_RESOURCE,
            primary_key="id",
            write_disposition="merge",
        )()

    # --- the two sync paths ------------------------------------------------

    def _emit_messages(self, client, config, user_id, state):
        """Choose a sync path, then run it. State advances only on completion."""
        stored_history_id = state.get(HISTORY_ID_STATE_KEY)
        resume_token = state.get(BACKFILL_TOKEN_STATE_KEY)

        if stored_history_id and not resume_token:
            try:
                plan = _plan_incremental(client, user_id, stored_history_id, config)
            except _HistoryTooOld as exc:
                # The whole reason this connector survives a pause. Without the
                # fallback every subsequent run raises the same 404 forever and
                # the Binding never recovers on its own.
                logger.info(
                    "Gmail history %s is no longer available (%s); falling back "
                    "to a full sync",
                    stored_history_id,
                    exc,
                )
            else:
                yield from _emit_planned_changes(client, config, user_id, plan)
                state[HISTORY_ID_STATE_KEY] = plan.history_id
                return

        yield from self._full_sync(client, config, user_id, state)

    def _full_sync(self, client, config, user_id, state):
        """List the mailbox, resuming an interrupted backfill if there is one.

        The watermark is read from ``users.getProfile`` *before* the listing
        starts, and kept in state across resumptions. Reading it afterwards
        would silently skip every change that happened during the walk; taking
        a fresh one per resumption would do the same over the whole backfill.
        """
        resume_token = state.get(BACKFILL_TOKEN_STATE_KEY)
        watermark = state.get(BACKFILL_HISTORY_STATE_KEY)

        if not resume_token or not watermark:
            profile = google_json(
                client, f"users/{user_id}/profile", what="reading the Gmail profile"
            )
            watermark = profile.get("historyId")
            resume_token = None

        params = _list_params(config)
        if resume_token:
            params["pageToken"] = resume_token

        max_messages = config.get("max_messages_per_run", DEFAULT_MAX_MESSAGES_PER_RUN)
        emitted = 0

        for page in paginate(
            client,
            f"users/{user_id}/messages",
            params=params,
            what="listing Gmail messages",
        ):
            for stub in page.get("messages") or ():
                record = _fetch_message(client, user_id, stub.get("id"), config)
                if record is not None:
                    yield record
                emitted += 1

            next_token = page.get("nextPageToken")
            if max_messages is not None and emitted >= max_messages and next_token:
                # Park the backfill rather than truncating it. Committing the
                # historyId here would mark the unread remainder as synced and
                # it would never be fetched again.
                state[BACKFILL_TOKEN_STATE_KEY] = next_token
                state[BACKFILL_HISTORY_STATE_KEY] = watermark
                logger.info(
                    "Gmail backfill paused after %s messages; resuming next run",
                    emitted,
                )
                return

        state.pop(BACKFILL_TOKEN_STATE_KEY, None)
        state.pop(BACKFILL_HISTORY_STATE_KEY, None)
        if watermark:
            state[HISTORY_ID_STATE_KEY] = watermark

    # --- operations --------------------------------------------------------

    def check_connection(self, *, connection, credentials, binding=None):
        """One ``users.getProfile`` call. Cheap, and proves the scope works."""
        config = (binding.config if binding is not None else None) or (
            connection.metadata or {}
        )
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        profile = google_json(
            client,
            f"users/{config.get('user_id', 'me')}/profile",
            what="testing the Gmail connection",
        )
        return (
            f"ok ({profile.get('emailAddress', 'unknown mailbox')}, "
            f"{profile.get('messagesTotal', '?')} messages)"
        )

    def discover(self, *, connection, credentials, query=None):
        """The two resources, plus the mailbox's labels as filter candidates."""
        client = google_client(base_url=self.api_base_url, credentials=credentials)
        user_id = (connection.metadata or {}).get("user_id", "me")
        payload = google_json(
            client, f"users/{user_id}/labels", what="listing Gmail labels"
        )
        labels = [
            {"id": label.get("id"), "name": label.get("name")}
            for label in payload.get("labels") or ()
            if query is None or query.lower() in (label.get("name") or "").lower()
        ]
        return {
            "resources": [
                {"name": MESSAGES_RESOURCE, "primary_key": "id", "deletions": True},
                {"name": LABELS_RESOURCE, "primary_key": "id", "deletions": False},
            ],
            "labels": labels,
        }


# --- the incremental plan --------------------------------------------------


class _HistoryPlan:
    """What one history walk found, before any message was fetched.

    Collected in full *first*, deliberately. The expired-history 404 can only
    be answered by re-reading the whole mailbox, and discovering that halfway
    through emitting would mean the run had already yielded records the
    fallback is about to yield again — harmless under merge, but it also means
    the 404 could arrive after the point where falling back cleanly is
    possible. Planning first keeps the two paths mutually exclusive.
    """

    __slots__ = ("changed_ids", "deleted_ids", "history_id")

    def __init__(self, changed_ids, deleted_ids, history_id):
        self.changed_ids = changed_ids
        self.deleted_ids = deleted_ids
        self.history_id = history_id


def _plan_incremental(client, user_id, start_history_id, config):
    """Walk users.history.list and return a :class:`_HistoryPlan`.

    Raises :class:`_HistoryTooOld` when Gmail no longer holds this point in the
    mailbox's history.
    """
    query = {
        "startHistoryId": str(start_history_id),
        "maxResults": config.get("page_size", DEFAULT_PAGE_SIZE),
        "historyTypes": list(HISTORY_TYPES),
    }
    label_ids = config.get("label_ids") or ()
    if label_ids:
        # history.list takes a single labelId, unlike messages.list. Narrowing
        # on the first is better than ignoring the filter entirely — and note
        # that Gmail's `q` has no equivalent here at all, so a Binding using
        # `query` will see history-driven runs land messages outside it. That
        # asymmetry is Gmail's, and it is documented rather than hidden.
        query["labelId"] = label_ids[0]

    changed = {}
    deleted = {}
    history_id = str(start_history_id)
    seen_tokens = set()

    for page_number in range(MAX_HISTORY_PAGES):
        response = google_request(client, f"users/{user_id}/history", params=query)
        if response.status_code == 404:
            raise _HistoryTooOld(
                f"startHistoryId {start_history_id} is outside Gmail's retention "
                f"window (~1 week)"
            )
        raise_for_google_error(response, what="reading Gmail history")
        payload = response.json()

        history_id = str(payload.get("historyId") or history_id)
        for record in payload.get("history") or ():
            _absorb_history_record(record, changed, deleted)

        token = payload.get("nextPageToken")
        if not token:
            break
        if token in seen_tokens:
            raise SourceError(
                f"Google repeated pageToken {token[:12]}… while reading Gmail "
                f"history at page {page_number + 1}; refusing to loop."
            )
        seen_tokens.add(token)
        query["pageToken"] = token
    else:
        raise SourceError(
            f"stopped after {MAX_HISTORY_PAGES} pages of Gmail history; the "
            f"walk did not end."
        )

    # A message that was changed and then deleted is deleted. Fetching it would
    # 404 and the tombstone is the truthful answer either way.
    for message_id in deleted:
        changed.pop(message_id, None)

    return _HistoryPlan(list(changed), list(deleted), history_id)


def _absorb_history_record(record, changed, deleted):
    """Fold one history entry into the changed/deleted id sets.

    Dicts rather than sets so insertion order survives, which keeps the emitted
    order stable and the tests deterministic.
    """
    for key in ("messagesAdded", "labelsAdded", "labelsRemoved"):
        for entry in record.get(key) or ():
            message = (entry or {}).get("message") or {}
            if message.get("id"):
                changed[message["id"]] = True

    for entry in record.get("messagesDeleted") or ():
        message = (entry or {}).get("message") or {}
        if message.get("id"):
            deleted[message["id"]] = True


def _emit_planned_changes(client, config, user_id, plan):
    """Fetch what changed, then tombstone what went away."""
    for message_id in plan.changed_ids:
        record = _fetch_message(client, user_id, message_id, config)
        if record is not None:
            yield record
            continue
        # Gone between the history walk and the fetch. History said it existed,
        # so it plausibly landed on an earlier run; a tombstone is the honest
        # record and is harmless if it never did.
        yield tombstone(message_id)

    for message_id in plan.deleted_ids:
        yield tombstone(message_id)


# --- records ---------------------------------------------------------------


def tombstone(message_id):
    """A record marking a Gmail message deleted at the provider.

    Carries the identity column and nothing else, which is what makes deletions
    lossy: ``delete-insert`` merge replaces the whole row, so every column this
    does not supply lands as NULL. Projection validation refuses mappings whose
    target identity is drawn from outside the merge key for exactly this
    reason.
    """
    return {"id": message_id, DELETED_COLUMN: True}


def _fetch_message(client, user_id, message_id, config):
    """One ``users.messages.get``. ``None`` when the message no longer exists."""
    if not message_id:
        return None

    params = {"format": config.get("message_format", "metadata")}
    if params["format"] == "metadata":
        params["metadataHeaders"] = list(
            config.get("metadata_headers") or DEFAULT_METADATA_HEADERS
        )

    response = google_request(
        client, f"users/{user_id}/messages/{message_id}", params=params
    )
    if response.status_code == 404:
        return None
    raise_for_google_error(response, what=f"fetching Gmail message {message_id}")
    return message_record(response.json(), config)


def message_record(payload, config):
    """Flatten one Gmail message into a landing record.

    Flattened here rather than left to dlt's normalizer, because nesting is off
    (``instrument.py`` sets ``max_table_nesting = 0``) and the raw shape would
    otherwise land as one opaque ``payload`` JSON column that no Projection
    mapping can reach into usefully. The header list — an array of
    ``{name, value}`` pairs — is the worst of it.
    """
    headers = {}
    for header in (payload.get("payload") or {}).get("headers") or ():
        name = (header or {}).get("name")
        if name:
            # Last one wins. Duplicate headers are legal in RFC822 and lowering
            # the name is what makes lookups here case-insensitive, as the spec
            # requires them to be.
            headers[name.lower()] = header.get("value")

    record = {
        "id": payload.get("id"),
        "thread_id": payload.get("threadId"),
        "history_id": payload.get("historyId"),
        "label_ids": payload.get("labelIds") or [],
        "snippet": payload.get("snippet"),
        "size_estimate": payload.get("sizeEstimate"),
        "internal_date": _internal_date(payload.get("internalDate")),
        "subject": headers.get("subject"),
        # Not "from"/"to": those are reserved words in MySQL and several other
        # destinations. dlt quotes identifiers, but a landing column a customer
        # cannot type into an ad-hoc query is a poor default.
        "from_address": headers.get("from"),
        "to_addresses": headers.get("to"),
        "cc_addresses": headers.get("cc"),
        "sent_at_header": headers.get("date"),
        "rfc822_message_id": headers.get("message-id"),
        "headers": headers,
    }
    if config.get("include_body"):
        record["body_text"] = _body_text(
            payload.get("payload") or {},
            config.get("body_max_chars", DEFAULT_BODY_MAX_CHARS),
        )
    return record


def label_record(label):
    """Flatten one Gmail label. Counts are absent unless the label was fetched."""
    return {
        "id": label.get("id"),
        "name": label.get("name"),
        "type": label.get("type"),
        "message_list_visibility": label.get("messageListVisibility"),
        "label_list_visibility": label.get("labelListVisibility"),
        "messages_total": label.get("messagesTotal"),
        "messages_unread": label.get("messagesUnread"),
        "threads_total": label.get("threadsTotal"),
        "threads_unread": label.get("threadsUnread"),
    }


def _internal_date(value):
    """Gmail's ``internalDate`` (epoch milliseconds, as a string) as a datetime.

    Returned as an aware datetime so dlt types the column as a timestamp. Left
    as the raw string it would land as text, and every downstream comparison
    would then be lexicographic — which is right until the millisecond count
    gains a digit.
    """
    if value in (None, ""):
        return None
    try:
        milliseconds = int(value)
    except (TypeError, ValueError):
        return None
    return dt.datetime.fromtimestamp(milliseconds / 1000, tz=dt.UTC)


def _body_text(part, max_chars):
    """The first ``text/plain`` body in a message, decoded and truncated.

    Truncation is not tidiness: dlt maps an unbounded string to MySQL ``TEXT``
    (64KB), and one oversized row leaves a load package dlt retries forever —
    ``LandingWedgedError`` exists because of exactly this.
    """
    text = _find_text_part(part)
    if text is None:
        return None
    return text[:max_chars]


def _find_text_part(part):
    if not isinstance(part, dict):
        return None
    if part.get("mimeType") == "text/plain":
        decoded = _decode_body((part.get("body") or {}).get("data"))
        if decoded is not None:
            return decoded
    for child in part.get("parts") or ():
        found = _find_text_part(child)
        if found is not None:
            return found
    return None


def _decode_body(data):
    if not data:
        return None
    try:
        raw = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
    except (binascii.Error, ValueError):
        # A body that will not decode must not fail the Run: the message's
        # metadata is still worth landing.
        return None
    return raw.decode("utf-8", errors="replace")


def _list_params(config):
    """Query parameters for ``users.messages.list``."""
    params = {"maxResults": config.get("page_size", DEFAULT_PAGE_SIZE)}
    if config.get("query"):
        params["q"] = config["query"]
    if config.get("label_ids"):
        params["labelIds"] = list(config["label_ids"])
    if config.get("include_spam_trash"):
        params["includeSpamTrash"] = "true"
    return params
