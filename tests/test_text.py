"""Pure-function tests for the adapter helpers.

The adapter module imports the Hermes gateway, so these run only where
the gateway is importable (on a Hermes install); everywhere else they
skip. The client and socket suites cover the transport without it.
"""

import pytest

adapter = pytest.importorskip("adapter")


def test_html_to_text_strips_paragraph_markup():
    assert adapter.html_to_text('<p dir="auto">hello</p>\n') == "hello"


def test_html_to_text_paragraph_breaks_and_entities():
    assert adapter.html_to_text("<p>a</p><p>b &amp; c</p>") == "a\n\nb & c"


def test_chunk_text_respects_cap():
    chunks = adapter.chunk_text("word " * 300)
    assert len(chunks) > 1
    assert all(len(c) <= 1000 for c in chunks)


def test_chunk_text_short_is_single():
    assert adapter.chunk_text("short") == ["short"]
