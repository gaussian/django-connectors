"""Browsing a source before any Binding exists.

One envelope for every source — ``{"items": [...], "next_cursor": ...}`` — so
the "pick what to sync" screen is one screen whatever the provider. Items
carry a ``path`` a UI hands back to descend without knowing what it means.
The provider-backed sources are exercised in their own modules against fakes;
this module covers the offline sources, the paging helper, the service clamp
and the API surface.
"""

import pytest

from django_connectors.exceptions import ConfigurationError, SourceError
from django_connectors.services import discovery
from django_connectors.sources.base import discovery_page
from tests import test_sources
from tests.test_sources import SQLITE_ROWS, make_sqlite_database, write_jsonl

#: Registers the `sql` and `files` sources; shared with the source tests.
source_settings = test_sources.source_settings

pytestmark = pytest.mark.django_db


# --- the paging helper -----------------------------------------------------


def test_discovery_page_pages_filters_and_ends():
    items = [{"name": f"t{i}"} for i in range(5)]
    first = discovery_page(items, limit=2)
    assert [i["name"] for i in first["items"]] == ["t0", "t1"]
    assert first["next_cursor"] == "2"
    last = discovery_page(items, limit=2, cursor="4")
    assert [i["name"] for i in last["items"]] == ["t4"]
    assert last["next_cursor"] is None
    assert discovery_page(items, query="T3")["items"] == [{"name": "t3"}]


def test_a_fabricated_cursor_is_refused_not_a_traceback():
    with pytest.raises(ConfigurationError, match="cursor"):
        discovery_page([{"name": "x"}], cursor="abc")


def test_the_service_clamps_limit_to_the_configured_page_size(
    connectors_settings, make_connection, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "DISCOVERY_PAGE_SIZE": 2}
    connection = make_connection(
        provider="memory", metadata={"resources": ["a", "b", "c", "d"]}
    )
    result = discovery.discover_remote(connection, limit=1000)
    assert len(result["items"]) == 2
    assert result["next_cursor"] == "2"


# --- sql -------------------------------------------------------------------


def test_sql_lists_schemas_then_tables(source_settings, make_connection, tmp_path):
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    connection = make_connection(provider="sql", metadata={"url": url})

    schemas = discovery.discover_remote(connection)
    assert [(i["name"], i["kind"]) for i in schemas["items"]] == [("main", "schema")]

    tables = discovery.discover_remote(connection, path=schemas["items"][0]["path"])
    orders = next(i for i in tables["items"] if i["name"] == "orders")
    assert orders["kind"] == "table"
    assert orders["db_schema"] == "main"
    assert orders["table"] == "orders"


def test_sql_descends_only_into_a_schema_it_listed(
    source_settings, make_connection, tmp_path
):
    """Membership, not an identifier regex: `my-app-prod` is a legal name."""
    url = make_sqlite_database(tmp_path, SQLITE_ROWS)
    connection = make_connection(provider="sql", metadata={"url": url})
    with pytest.raises(ConfigurationError, match="not one this database lists"):
        discovery.discover_remote(connection, path="main; drop table orders")


def test_a_non_positive_limit_means_the_default_not_a_loop(
    connectors_settings, make_connection, settings
):
    settings.DJANGO_CONNECTORS = {**connectors_settings, "DISCOVERY_PAGE_SIZE": 2}
    connection = make_connection(
        provider="memory", metadata={"resources": ["a", "b", "c"]}
    )
    for limit in (-1, 0, "0"):
        page = discovery.discover_remote(connection, limit=limit)
        assert [i["name"] for i in page["items"]] == ["a", "b"]
        assert page["next_cursor"] == "2"
    with pytest.raises(ConfigurationError, match="not a number"):
        discovery.discover_remote(connection, limit="ten")


# --- filesystem ------------------------------------------------------------


def test_filesystem_lists_one_level_folders_first(
    source_settings, make_connection, tmp_path
):
    write_jsonl(tmp_path / "exports" / "b.jsonl", [{"id": "1"}])
    write_jsonl(tmp_path / "exports" / "2024" / "a.jsonl", [{"id": "2"}])
    connection = make_connection(
        provider="files", metadata={"root": str(tmp_path / "exports")}
    )

    top = discovery.discover_remote(connection)
    assert [(i["name"], i["kind"]) for i in top["items"]] == [
        ("2024", "folder"),
        ("b.jsonl", "file"),
    ]
    assert top["items"][1]["path"] is None
    assert top["items"][1]["size"] > 0

    below = discovery.discover_remote(connection, path=top["items"][0]["path"])
    assert [i["name"] for i in below["items"]] == ["a.jsonl"]


def test_filesystem_enters_a_folder_whose_name_looks_like_a_file(
    source_settings, make_connection, tmp_path
):
    """`com.example` is a folder discovery itself listed; descending must enter it."""
    root = tmp_path / "root"  # tmp_path itself holds dlt's scratch directories
    write_jsonl(root / "com.example" / "inner.jsonl", [{"id": "1"}])
    write_jsonl(root / "[2024]" / "deep.jsonl", [{"id": "2"}])
    connection = make_connection(provider="files", metadata={"root": str(root)})

    top = discovery.discover_remote(connection)
    for folder in top["items"]:
        below = discovery.discover_remote(connection, path=folder["path"])
        assert [i["name"] for i in below["items"]] in (["inner.jsonl"], ["deep.jsonl"])


def test_every_bucket_scheme_has_a_real_fsspec_accessor():
    from dlt.common.configuration.specs import (
        AwsCredentials,
        AzureCredentials,
        GcpServiceAccountCredentials,
    )

    from django_connectors.sources import filesystem as fs_source

    spec_for = {
        "s3": AwsCredentials,
        "gs": GcpServiceAccountCredentials,
        "gcs": GcpServiceAccountCredentials,
        "az": AzureCredentials,
        "abfss": AzureCredentials,
    }
    for scheme, method in fs_source.FSSPEC_KWARGS_METHOD.items():
        assert callable(getattr(spec_for[scheme], method)), (scheme, method)


def test_filesystem_needs_somewhere_to_start(source_settings, make_connection):
    connection = make_connection(provider="files", metadata={})
    with pytest.raises(ConfigurationError, match="root"):
        discovery.discover_remote(connection)


def test_filesystem_reports_a_missing_directory(
    source_settings, make_connection, tmp_path
):
    connection = make_connection(provider="files", metadata={})
    with pytest.raises(SourceError, match="does not exist"):
        discovery.discover_remote(connection, path=str(tmp_path / "nope"))


# --- API -------------------------------------------------------------------


def test_the_api_passes_every_browsing_parameter_through(
    connectors_settings, make_connection, settings
):
    pytest.importorskip("rest_framework")
    from django.urls import reverse

    settings.ROOT_URLCONF = "tests.test_api"
    settings.DJANGO_CONNECTORS = {
        **connectors_settings,
        "DISCOVERY_PAGE_SIZE": 2,
        "API_OWNER_RESOLVER": "tests.test_api.resolve_owner",
        "API_PERMISSION_CLASSES": ("rest_framework.permissions.IsAuthenticated",),
    }
    from django.contrib.auth.models import User
    from rest_framework.test import APIClient

    client = APIClient()
    client.force_authenticate(user=User.objects.create_user(username="u", password="x"))
    connection = make_connection(
        provider="memory", metadata={"resources": ["alpha", "beta", "gamma"]}
    )
    url = reverse("django_connectors:connection-discover", kwargs={"pk": connection.pk})

    page = client.get(url, {"limit": 10}).json()
    assert [i["name"] for i in page["items"]] == ["alpha", "beta"]
    assert page["next_cursor"] == "2"
    rest = client.get(url, {"cursor": page["next_cursor"]}).json()
    assert [i["name"] for i in rest["items"]] == ["gamma"]
    assert client.get(url, {"q": "amm"}).json()["items"][0]["name"] == "gamma"
    assert client.get(url, {"cursor": "junk"}).status_code == 400
