from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.podcast.transcript_import.nitwitch_dl import (
    parse_bundle_ids_from_index,
)
from services.podcast.transcript_sync.format import make_bundle_id, parse_bundle_id


def test_parse_bundle_ids_from_index_timestamped() -> None:
    html = """
    <html><body>
    <h1>Index of /dl/transcription_exports/podcast_transcripts/</h1>
    <a href="../">Parent Directory</a>
    <a href="2026-05-20T08-15-30Z/">2026-05-20T08-15-30Z/</a>
    <a href="2026-05-22T14-30-45Z/">2026-05-22T14-30-45Z/</a>
    <a href="not-a-bundle/">not-a-bundle/</a>
    </body></html>
    """
    assert parse_bundle_ids_from_index(html) == [
        "2026-05-20T08-15-30Z",
        "2026-05-22T14-30-45Z",
    ]


def test_parse_bundle_ids_legacy_date_folders() -> None:
    html = """
    <a href="2026-05-20/">2026-05-20/</a>
    <a href="2026-05-22/">2026-05-22/</a>
    """
    assert parse_bundle_ids_from_index(html) == ["2026-05-20", "2026-05-22"]


def test_parse_bundle_ids_empty() -> None:
    assert parse_bundle_ids_from_index("<html></html>") == []


def test_bundle_id_roundtrip() -> None:
    dt = datetime(2026, 5, 22, 14, 30, 45, tzinfo=timezone.utc)
    bid = make_bundle_id(dt)
    assert bid == "2026-05-22T14-30-45Z"
    assert parse_bundle_id(bid) == dt


def test_parse_legacy_bundle_id() -> None:
    assert parse_bundle_id("2026-05-19") == datetime(
        2026, 5, 19, 0, 0, 0, tzinfo=timezone.utc
    )
