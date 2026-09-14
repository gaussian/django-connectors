"""Redaction tests.

The threat is concrete: dlt renders the landing DSN password in plaintext via
``credentials.to_native_representation()`` and ``repr(destination_factory)``,
the runner stores exception text in ``Run.error_message``, and the admin and API
render that field. Each test below is a route by which a credential reaches
persisted, readable text.
"""

from django.test import override_settings

from django_connectors.errors import describe, scrub

LANDING = "mysql+pymysql://root:sup3rSekritPassw0rd@db.internal:3306/connectors_landing"


def test_masks_the_landing_dsn_password_literal():
    with override_settings(DJANGO_CONNECTORS={"LANDING_URL": LANDING}):
        text = scrub(f"could not connect using {LANDING}")
    assert "sup3rSekritPassw0rd" not in text
    # Two rules fire in sequence here: the literal password is masked, and then
    # the URL rewriter drops the userinfo altogether — so the username goes too.
    assert "root" not in text
    assert "@" not in text
    # The host is what makes the error diagnosable; it must survive.
    assert "db.internal:3306" in text


def test_strips_url_userinfo_even_for_an_unconfigured_dsn():
    text = scrub("failed: postgres://someuser:hunter2@example.com:5432/db")
    assert "hunter2" not in text
    assert "someuser" not in text
    assert "example.com" in text


def test_query_parameters_are_default_deny():
    text = scrub(
        "GET https://api.example.com/v1/messages"
        "?page=3&per_page=50&access_token=ya29.SECRETVALUE&weird_param=alsosecret"
    )
    assert "ya29.SECRETVALUE" not in text
    assert "alsosecret" not in text, (
        "unknown parameters must be masked, not allow-listed"
    )
    # Pagination is most of the diagnostic value of having the URL.
    assert "page=3" in text
    assert "per_page=50" in text


def test_masks_a_token_embedded_in_a_url_path():
    """There is no key name to match on, so the entropy rule has to catch it."""
    secret = "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"
    text = scrub(f"404 at https://api.example.com/v1/tokens/{secret}/refresh")
    assert secret not in text
    assert "api.example.com" in text


def test_masks_url_fragments():
    """Implicit OAuth flows return tokens in the fragment."""
    text = scrub("redirected to https://app.example.com/cb#access_token=SECRETFRAGMENT")
    assert "SECRETFRAGMENT" not in text


def test_masks_jwts():
    jwt = (
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIn0"
        ".dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
    )
    text = scrub(f"Graph rejected token {jwt}")
    assert jwt not in text
    assert "eyJ" not in text


def test_masks_authorization_schemes():
    text = scrub("headers: {'Authorization': 'Bearer ya29.a0AfH6SMBxxxxxxxxxxxx'}")
    assert "ya29.a0AfH6SMBxxxxxxxxxxxx" not in text


def test_masks_credential_shaped_key_value_pairs():
    text = scrub(
        'response body: {"refresh_token": "1//0gSECRETREFRESH", '
        '"client_secret": "abc~SECRET", "expires_in": 3599}'
    )
    assert "1//0gSECRETREFRESH" not in text
    assert "abc~SECRET" not in text
    # Non-credential fields stay readable.
    assert "expires_in" in text


def test_leaves_ordinary_diagnostics_readable():
    """Over-redaction makes error messages useless; guard against it."""
    text = scrub(
        "LoadClientJobRetry: job events.1786740517.0330918.insert_values failed "
        "for table rest_events_b0a1b2c3d4 after 5 retries"
    )
    assert "rest_events_b0a1b2c3d4" in text
    assert "1786740517.0330918" in text
    assert "LoadClientJobRetry" in text


def test_truncates_long_messages():
    with override_settings(DJANGO_CONNECTORS={"ERROR_MESSAGE_MAX_LENGTH": 200}):
        text = scrub("x" * 10_000)
    assert len(text) <= 200
    assert "truncated" in text


def test_scrub_never_raises():
    class Exploding:
        def __str__(self):
            raise RuntimeError("nope")

    assert scrub(Exploding()) == "<unprintable error>"
    assert scrub(None) == "None"
    assert scrub(b"\xff\xfe") == "b'\\xff\\xfe'"


def test_describe_returns_bare_class_name_and_scrubbed_message():
    with override_settings(DJANGO_CONNECTORS={"LANDING_URL": LANDING}):
        error_type, message = describe(ValueError(f"bad dsn {LANDING}"))
    assert error_type == "ValueError"
    assert "sup3rSekritPassw0rd" not in message
