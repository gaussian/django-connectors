"""Fetching the bytes behind a landed record, through the library.

The landing layer lands metadata and never file bytes. A host that lands file
metadata still needs the file, and the only route used to be a provider-private
function plus a hand-built session — the boundary leaking the wrong way. The
service resolves credentials the way a Run does, asks the source that landed
the record, and holds the size to a ceiling the host cannot lift. The
provider-backed path is exercised in the Microsoft provider tests.
"""

import pytest

from django_connectors.exceptions import SourceError
from django_connectors.services.content import fetch_record_content

pytestmark = pytest.mark.django_db


def test_a_row_shaped_source_says_it_has_no_content(connectors_settings, make_binding):
    binding = make_binding(
        config={"resources": {"events": {"primary_key": "id", "batches": []}}}
    )
    with pytest.raises(SourceError, match="does not provide record content"):
        fetch_record_content(binding, "events", {"id": "1"})
