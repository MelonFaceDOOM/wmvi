from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from content_sync.format import (
    SCHEMA_VERSION,
    ContentSyncManifest,
    PlatformFileInfo,
    make_bundle_id,
    parse_bundle_id,
    platform_filename,
    read_manifest,
    write_manifest,
)
from storage.nitwitch_http import parse_bundle_ids_from_index


def test_make_bundle_id_roundtrip():
    dt = datetime(2026, 5, 22, 14, 30, 45, tzinfo=timezone.utc)
    bid = make_bundle_id(dt)
    assert bid == "2026-05-22T14-30-45Z"
    assert parse_bundle_id(bid) == dt


def test_manifest_roundtrip(tmp_path: Path):
    manifest = ContentSyncManifest(
        schema_version=SCHEMA_VERSION,
        bundle_id="2026-05-22T14-30-45Z",
        since_ts=datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc),
        until_ts=datetime(2026, 5, 22, 14, 30, 45, tzinfo=timezone.utc),
        platforms={
            "youtube_video": PlatformFileInfo(
                row_count=2, file=platform_filename("youtube_video")
            ),
        },
        sidecars={"youtube_segments": "youtube_segments_2026-05-22T14-30-45Z.jsonl"},
    )
    path = tmp_path / "manifest.json"
    write_manifest(path, manifest)
    loaded = read_manifest(path)
    assert loaded.bundle_id == manifest.bundle_id
    assert loaded.platforms["youtube_video"].row_count == 2
    assert "youtube_segments" in loaded.sidecars


def test_manifest_rejects_wrong_schema(tmp_path: Path):
    path = tmp_path / "manifest.json"
    path.write_text(
        '{"schema_version": 99, "bundle_id": "x", "until_ts": "2026-01-01T00:00:00+00:00", "platforms": {}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsupported"):
        read_manifest(path)


def test_parse_nitwitch_index():
    html = """
    <html><body>
    <a href="2026-05-20T10-00-00Z/">bundle</a>
    <a href="2026-05-21/">legacy</a>
    </body></html>
    """
    ids = parse_bundle_ids_from_index(html)
    assert "2026-05-20T10-00-00Z" in ids
    assert "2026-05-21" in ids


def test_yt_proxy_args_empty(monkeypatch):
    monkeypatch.delenv("YT_PROXY_URL", raising=False)
    from storage.yt_proxy import yt_dlp_proxy_args

    assert yt_dlp_proxy_args() == []


def test_yt_proxy_args_set(monkeypatch):
    monkeypatch.setenv("YT_PROXY_URL", "http://user:pass@proxy.example:8080")
    from storage.yt_proxy import yt_dlp_proxy_args

    assert yt_dlp_proxy_args() == ["--proxy", "http://user:pass@proxy.example:8080"]


def test_normalize_proxy_url_proxidize():
    from storage.yt_proxy import normalize_proxy_url

    raw = "pg.proxi.es:20000:myuser:mypass"
    assert normalize_proxy_url(raw) == "http://myuser:mypass@pg.proxi.es:20000"


def test_normalize_proxy_url_http_unchanged():
    from storage.yt_proxy import normalize_proxy_url

    url = "http://user:pass@proxy.example:8080"
    assert normalize_proxy_url(url) == url
