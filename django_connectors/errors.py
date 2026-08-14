"""Redaction of error text before it is persisted, logged, or serialized.

Every exception message reaches ``Run.error_message`` / ``Binding.last_error``
through :func:`scrub`, and those fields are rendered in the admin and returned
by the API. Without this module the landing database password reaches an API
response by an entirely mundane route:

1. ``LANDING_URL`` is ``mysql+pymysql://user:PASSWORD@host/db``.
2. A load fails. dlt's ``credentials.to_native_representation()`` and
   ``repr(destination_factory)`` both render the password in plaintext — only
   ``str()`` masks it.
3. The runner stores ``str(exc)``.
4. The admin and ``GET /runs/`` render that field.

The rules are **default-deny**. dlt's own URL sanitizer allow-lists eleven exact
parameter names (missing ``refresh_token``, ``sig``, ``code``, ``id_token``,
``assertion``), only rewrites query values, and is bypassed entirely when
``response_actions`` are configured — while still appending the response body.
A denylist is permanently one provider behind; an allow-list is the same amount
of code and does not rot.
"""

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

MASK = "***"

# Query parameters whose values survive scrubbing. Everything else is masked.
# These are pagination/shape parameters: knowing them is most of the value of
# having the URL at all, and none of them carries a credential.
SAFE_QUERY_PARAMS = frozenset(
    {
        "alt",
        "api-version",
        "count",
        "end",
        "end_date",
        "expand",
        "fields",
        "filter",
        "format",
        "limit",
        "maxresults",
        "offset",
        "order",
        "order_by",
        "orderby",
        "page",
        "page_size",
        "pagesize",
        "per_page",
        "q",
        "query",
        "select",
        "since",
        "skip",
        "sort",
        "start",
        "start_date",
        "top",
        "until",
        "version",
    }
)

_URL_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9+.\-]*://[^\s\"'<>`\\|]+")

# A JWT leaks its payload even unsigned, and appears in Graph/Google errors.
_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*")

_AUTH_SCHEME_RE = re.compile(
    r"\b(Bearer|Basic|Token|ApiKey)\s+[A-Za-z0-9._~+/=\-]+", re.IGNORECASE
)

# key=value / "key": "value" / key: value, for credential-shaped key names.
_SECRET_KEY_NAMES = (
    r"(?:access|refresh|id|bearer|session|sas)?[_-]?token|secret|password|passwd|pwd"
    r"|api[_-]?key|apikey|client[_-]?secret|private[_-]?key|credential|assertion"
    r"|authorization|auth|signature|sig|passphrase|connection[_-]?string|dsn"
)
_SECRET_PAIR_RE = re.compile(
    rf"(?i)(['\"]?\b(?:{_SECRET_KEY_NAMES})\b['\"]?\s*[=:]\s*)(['\"]?)([^\s,;&)\]}}'\"]+)\2"
)

# A long run of token characters containing both a digit and a letter. This is
# what catches a credential embedded in a URL *path*, where there is no key name
# to match on. Kept at 32+ so that table names, dlt load ids and dlt row ids
# (14 chars) survive — an error message with everything redacted is useless.
_HIGH_ENTROPY_RE = re.compile(r"\b(?=[A-Za-z0-9_\-]*\d)[A-Za-z0-9_\-]{32,}\b")


def _scrub_url(match: re.Match[str]) -> str:
    raw = match.group(0)
    # Trailing punctuation is almost always sentence structure, not URL.
    trailing = ""
    while raw and raw[-1] in ".,;:)]}>'\"":
        trailing = raw[-1] + trailing
        raw = raw[:-1]
    try:
        parts = urlsplit(raw)
    except ValueError:
        return MASK + trailing

    netloc = parts.netloc
    if "@" in netloc:
        # Strip userinfo entirely — this is where a DSN password lives.
        netloc = netloc.rsplit("@", 1)[1]

    query = parts.query
    if query:
        pairs = parse_qsl(query, keep_blank_values=True)
        query = urlencode(
            [
                (key, value if key.lower() in SAFE_QUERY_PARAMS else MASK)
                for key, value in pairs
            ]
        )

    # Implicit OAuth flows return tokens in the fragment.
    fragment = MASK if parts.fragment else ""

    return urlunsplit((parts.scheme, netloc, parts.path, query, fragment)) + trailing


def _landing_password() -> str | None:
    """The configured landing DSN's password, if any, for literal masking."""
    try:
        from django_connectors.conf import conf

        url = conf.LANDING_URL
    except Exception:
        return None
    if not url:
        return None
    try:
        password = urlsplit(url).password
    except ValueError:
        return None
    return password or None


def scrub(text: object) -> str:
    """Return `text` with credentials removed.

    Safe to call on anything, and never raises: a redaction failure must not be
    able to mask the underlying error it was called to report.
    """
    try:
        result = str(text)
    except Exception:
        return "<unprintable error>"

    try:
        password = _landing_password()
        if password:
            result = result.replace(password, MASK)

        result = _URL_RE.sub(_scrub_url, result)
        result = _JWT_RE.sub(MASK, result)
        result = _AUTH_SCHEME_RE.sub(lambda m: f"{m.group(1)} {MASK}", result)
        result = _SECRET_PAIR_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}{MASK}{m.group(2)}", result
        )
        result = _HIGH_ENTROPY_RE.sub(MASK, result)
    except Exception:
        return "<error message redacted: scrubbing failed>"

    return _truncate(result)


def _truncate(text: str) -> str:
    try:
        from django_connectors.conf import conf

        limit = conf.ERROR_MESSAGE_MAX_LENGTH
    except Exception:
        limit = 4096
    if len(text) <= limit:
        return text
    suffix = f"… [truncated, {len(text)} chars]"
    return text[: max(0, limit - len(suffix))] + suffix


def describe(exc: BaseException) -> tuple[str, str]:
    """Return ``(error_type, scrubbed_message)`` for persistence.

    ``error_type`` is the bare class name so it stays stable and greppable;
    the module path would leak internal structure into stored data.
    """
    return type(exc).__name__, scrub(exc)
