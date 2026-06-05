from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from content_sync.export_runner import run_export
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


def test_run_export_dry_run_queries_counts(monkeypatch, caplog):
    class FakeHandler:
        platform = "fake_platform"

        def count_export_delta(self, cur, *, since, until):
            assert cur is mock_cur
            return 7, {"fake_sidecar": 2}

    mock_cur = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_cur
    mock_cm.__exit__.return_value = False

    monkeypatch.setattr(
        "content_sync.export_runner.get_handlers",
        lambda platforms: [FakeHandler()],
    )
    monkeypatch.setattr(
        "db.db.getcursor",
        lambda commit=False: mock_cm,
    )
    monkeypatch.setattr(
        "content_sync.export_runner.load_export_state",
        lambda: type("S", (), {"last_exported_at": None})(),
    )
    monkeypatch.setattr(
        "content_sync.export_runner.save_export_state",
        lambda state: pytest.fail("dry-run must not save state"),
    )

    since = datetime(2026, 2, 3, tzinfo=timezone.utc)
    with caplog.at_level("INFO"):
        result = run_export(since_override=since, dry_run=True)

    assert result is None
    assert "dry-run fake_platform: 7 rows" in caplog.text
    assert "dry-run sidecar fake_sidecar: 2 rows" in caplog.text
    assert "watermark unchanged" in caplog.text


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


def test_youtube_video_export_where_includes_ingest_transcript_and_comment_parents():
    from content_sync.platforms.youtube_video import _youtube_video_export_where

    until = datetime(2026, 6, 5, 14, 31, 12, tzinfo=timezone.utc)
    since = datetime(2026, 5, 28, 17, 9, 1, tzinfo=timezone.utc)

    where, params = _youtube_video_export_where(since=since, until=until)

    assert "v.date_entered <= %s" in where
    assert "v.date_entered > %s" in where
    assert "v.transcript IS NOT NULL" in where
    assert "transcript_updated_at" in where
    assert "EXISTS (" in where
    assert "youtube.comment c" in where
    assert " OR " in where
    assert params.count(until) == 3
    assert params.count(since) == 3


def test_youtube_video_export_where_without_since():
    from content_sync.platforms.youtube_video import _youtube_video_export_where

    until = datetime(2026, 6, 5, tzinfo=timezone.utc)
    where, params = _youtube_video_export_where(since=None, until=until)

    assert "v.date_entered > %s" not in where
    assert "transcript_updated_at, v.date_entered) > %s" not in where
    assert "c.date_entered > %s" not in where
    assert params == [until, until, until]


def test_run_export_advances_watermark_to_until_ts(monkeypatch, tmp_path: Path):
    class FakeHandler:
        platform = "fake_platform"

        def export_delta(self, cur, *, since, until):
            return (
                [{"created_at_ts": "2020-01-01T00:00:00+00:00"}],
                {},
            )

    saved: list = []

    monkeypatch.setattr(
        "content_sync.export_runner.get_handlers",
        lambda platforms: [FakeHandler()],
    )
    monkeypatch.setattr(
        "content_sync.export_runner.export_bundle_dir",
        lambda bundle_id: tmp_path / bundle_id,
    )
    monkeypatch.setattr(
        "content_sync.export_runner.upload_bundle",
        lambda bundle_dir: None,
    )
    monkeypatch.setattr(
        "content_sync.export_runner.load_export_state",
        lambda: type("S", (), {"last_exported_at": None})(),
    )
    monkeypatch.setattr(
        "content_sync.export_runner.save_export_state",
        lambda state: saved.append(state),
    )
    monkeypatch.setattr(
        "db.db.getcursor",
        lambda commit=False: type(
            "CM",
            (),
            {
                "__enter__": lambda self: object(),
                "__exit__": lambda *a: False,
            },
        )(),
    )

    fixed_until = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        "content_sync.export_runner._utc_now",
        lambda: fixed_until,
    )

    manifest = run_export()
    assert manifest is not None
    assert manifest.until_ts == fixed_until
    assert len(saved) == 1
    assert saved[0].last_exported_at == fixed_until
