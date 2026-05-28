"""Tests for podcast transcript export/import sync (schema v4)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.podcast import compute_episode_id
from services.podcast.transcript_sync.bundle_import import apply_bundle, dry_run_bundle
from services.podcast.transcript_sync.format import (
    SCHEMA_VERSION,
    EpisodeExportRow,
    ExportManifest,
    ShowRow,
    episodes_payload_filename,
    iter_episode_jsonl_rows,
    iter_show_jsonl_rows,
    make_bundle_id,
    parse_bundle_id,
    read_manifest,
    should_apply_import,
    shows_payload_filename,
    write_episode_jsonl_row,
    write_manifest,
    write_show_jsonl_row,
)
from services.podcast.transcript_sync.resolve import (
    has_transcript_match_key,
    target_episode_id,
)
from services.podcast.transcript_sync.rss_url import normalize_rss_url
from services.podcast.transcript_sync.state import ExportState, load_export_state, save_export_state


def test_normalize_rss_url() -> None:
    assert normalize_rss_url("HTTPS://Example.COM/feed/") == "https://example.com/feed"
    assert normalize_rss_url("  ") is None
    assert normalize_rss_url(None) is None


def test_episode_export_row_jsonl_roundtrip(tmp_path: Path) -> None:
    ts = datetime(2026, 5, 19, 3, 14, tzinfo=timezone.utc)
    row = EpisodeExportRow(
        show_rss_url="https://example.com/feed.xml",
        guid="ep-guid-1",
        download_url="https://cdn.example.com/a.mp3",
        title="Episode 1",
        transcript="hello world",
        transcript_updated_at=ts,
        source_show_id=7,
        source_episode_id="ep_7_abc",
    )
    out = tmp_path / "rows.jsonl"
    with out.open("w", encoding="utf-8") as fp:
        write_episode_jsonl_row(fp, row)
    rows = list(iter_episode_jsonl_rows(out))
    assert len(rows) == 1
    assert rows[0].show_rss_url == "https://example.com/feed.xml"
    assert rows[0].guid == "ep-guid-1"
    assert rows[0].transcript == "hello world"


def test_show_row_jsonl_roundtrip(tmp_path: Path) -> None:
    ts = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)
    row = ShowRow(
        rss_url="https://example.com/feed.xml",
        title="My Podcast",
        source_show_id=7,
        last_fetch_ts=ts,
        last_http_status=200,
    )
    out = tmp_path / "shows.jsonl"
    with out.open("w", encoding="utf-8") as fp:
        write_show_jsonl_row(fp, row)
    rows = list(iter_show_jsonl_rows(out))
    assert len(rows) == 1
    assert rows[0].rss_url == "https://example.com/feed.xml"
    assert rows[0].source_show_id == 7


def test_manifest_roundtrip(tmp_path: Path) -> None:
    since = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    until = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    bundle_id = "2026-05-19T12-00-00Z"
    manifest = ExportManifest(
        schema_version=SCHEMA_VERSION,
        bundle_id=bundle_id,
        export_date="2026-05-19",
        since_ts=since,
        until_ts=until,
        row_count=2,
        payload=episodes_payload_filename(bundle_id),
        shows_payload=shows_payload_filename(bundle_id),
        shows_count=1,
    )
    path = tmp_path / "manifest.json"
    write_manifest(path, manifest)
    loaded = read_manifest(path)
    assert loaded.bundle_id == bundle_id
    assert loaded.schema_version == 4
    assert loaded.row_count == 2


def test_manifest_rejects_schema_v3(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(
        '{"schema_version":3,"bundle_id":"x","export_date":"2026-05-19",'
        '"until_ts":"2026-05-19T12:00:00+00:00","row_count":0,"payload":"p.jsonl"}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unsupported export bundle schema_version=3"):
        read_manifest(path)


def test_target_episode_id_stable_across_podcast_id() -> None:
    ts = datetime(2026, 5, 1, tzinfo=timezone.utc)
    row = EpisodeExportRow(
        show_rss_url="https://example.com/feed",
        guid="g1",
        transcript="t",
        transcript_updated_at=ts,
    )
    id_a = target_episode_id(7, row)
    id_b = target_episode_id(42, row)
    assert id_a != id_b
    assert id_a == compute_episode_id(
        podcast_id=7, guid="g1", download_url=None, created_at_ts=None, title=None
    )


def test_has_transcript_match_key() -> None:
    ts = datetime(2026, 5, 1, tzinfo=timezone.utc)
    assert has_transcript_match_key(
        EpisodeExportRow(show_rss_url="https://x.com/f", guid="g", transcript="t", transcript_updated_at=ts)
    )
    assert has_transcript_match_key(
        EpisodeExportRow(
            show_rss_url="https://x.com/f",
            download_url="https://cdn/x.mp3",
            transcript="t",
            transcript_updated_at=ts,
        )
    )
    assert not has_transcript_match_key(
        EpisodeExportRow(
            show_rss_url="https://x.com/f",
            title="Only title",
            created_at_ts=ts,
            transcript="t",
            transcript_updated_at=ts,
        )
    )


@pytest.mark.parametrize(
    "prod_transcript,prod_updated,incoming,expected",
    [
        (None, None, datetime(2026, 1, 2, tzinfo=timezone.utc), True),
        ("old", None, datetime(2026, 1, 2, tzinfo=timezone.utc), True),
        ("old", datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc), True),
        (
            "old",
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            False,
        ),
        (
            "old",
            datetime(2026, 1, 3, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
            False,
        ),
    ],
)
def test_should_apply_import_newer_wins(
    prod_transcript: str | None,
    prod_updated: datetime | None,
    incoming: datetime,
    expected: bool,
) -> None:
    assert (
        should_apply_import(
            prod_transcript=prod_transcript,
            prod_updated_at=prod_updated,
            incoming_updated_at=incoming,
        )
        is expected
    )


def test_export_watermark_not_advanced_on_failed_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "export_state.json"
    monkeypatch.setenv("PODCAST_SYNC_STATE_FILE", str(state_path))
    save_export_state(ExportState(last_exported_at=None))

    def fail_upload(_manifest: Path, _payload: Path, _shows: Path | None = None) -> None:
        raise RuntimeError("upload failed")

    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.upload_export",
        fail_upload,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.init_pool",
        lambda prefix: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.close_pool",
        lambda: None,
    )
    _FakeCursor._fetchall_calls = 0
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.getcursor",
        _fake_export_cursor,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.export_bundle_dir",
        lambda bundle_id: tmp_path / "bundle",
    )

    from services.podcast.transcript_export.exporter import run_export

    with pytest.raises(RuntimeError, match="upload failed"):
        run_export(prod=False, dry_run=False)

    state = load_export_state(state_path)
    assert state.last_exported_at is None


def test_export_writes_referenced_shows_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PODCAST_EXPORT_STORAGE_KIND", "skip")
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.upload_export",
        lambda _m, _p, _s=None: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.init_pool",
        lambda prefix: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.close_pool",
        lambda: None,
    )
    _FakeCursor._fetchall_calls = 0
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.getcursor",
        _fake_export_cursor,
    )
    show = ShowRow(
        rss_url="https://example.com/rss",
        title="Test Show",
        source_show_id=3,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.db_shows.fetch_shows_by_ids",
        lambda cur, ids: [show] if ids == [3] else [],
    )
    bundle = tmp_path / "bundle"
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.export_bundle_dir",
        lambda bundle_id: bundle,
    )

    from services.podcast.transcript_export.exporter import run_export

    run_export(prod=False, dry_run=False)
    manifest = read_manifest(bundle / "manifest.json")
    shows_path = bundle / manifest.shows_payload
    assert shows_path.is_file()
    rows = list(iter_show_jsonl_rows(shows_path))
    assert len(rows) == 1
    assert rows[0].rss_url == "https://example.com/rss"


def test_import_dry_run_counts_skipped_transcript_keys(tmp_path: Path) -> None:
    bundle = tmp_path / "2026-05-19T12-00-00Z"
    bundle.mkdir()
    ts = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    show = ShowRow(rss_url="https://example.com/rss", title="Show")
    shows_path = bundle / shows_payload_filename("2026-05-19T12-00-00Z")
    with shows_path.open("w", encoding="utf-8") as fp:
        write_show_jsonl_row(fp, show)
    episodes_path = bundle / episodes_payload_filename("2026-05-19T12-00-00Z")
    with episodes_path.open("w", encoding="utf-8") as fp:
        write_episode_jsonl_row(
            fp,
            EpisodeExportRow(
                show_rss_url="https://example.com/rss",
                guid="g1",
                transcript="a",
                transcript_updated_at=ts,
            ),
        )
        write_episode_jsonl_row(
            fp,
            EpisodeExportRow(
                show_rss_url="https://example.com/rss",
                title="no key",
                transcript="b",
                transcript_updated_at=ts,
            ),
        )

    stats = dry_run_bundle(shows_path=shows_path, episodes_path=episodes_path)
    assert stats.episodes_seen == 2
    assert stats.skipped_no_transcript_key == 1


def test_import_calls_upsert_before_episodes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle_id = "2026-05-19T12-00-00Z"
    bundle = tmp_path / bundle_id
    bundle.mkdir()
    ts = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    shows_path = bundle / shows_payload_filename(bundle_id)
    with shows_path.open("w", encoding="utf-8") as fp:
        write_show_jsonl_row(fp, ShowRow(rss_url="https://example.com/rss", title="Show"))
    episodes_path = bundle / episodes_payload_filename(bundle_id)
    with episodes_path.open("w", encoding="utf-8") as fp:
        write_episode_jsonl_row(
            fp,
            EpisodeExportRow(
                show_rss_url="https://example.com/rss",
                guid="g1",
                transcript="text",
                transcript_updated_at=ts,
            ),
        )
    manifest = ExportManifest(
        schema_version=SCHEMA_VERSION,
        bundle_id=bundle_id,
        export_date="2026-05-19",
        since_ts=None,
        until_ts=ts,
        row_count=1,
        payload=episodes_payload_filename(bundle_id),
        shows_payload=shows_payload_filename(bundle_id),
        shows_count=1,
    )
    write_manifest(bundle / "manifest.json", manifest)

    call_order: list[str] = []

    def fake_upsert(cur, rows):
        call_order.append("shows")
        return {"https://example.com/rss": 99}

    def fake_insert_eps(cur, rows):
        call_order.append("episodes")
        return len(rows)

    def fake_transcripts(cur, rows):
        call_order.append("transcripts")
        from services.podcast.transcript_sync.db_import import BatchImportResult

        return BatchImportResult(seen=len(rows), updated=len(rows), registered=len(rows))

    monkeypatch.setattr(
        "services.podcast.transcript_sync.bundle_import.db_shows.upsert_shows",
        fake_upsert,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_sync.bundle_import.db_episodes.insert_new_episodes",
        fake_insert_eps,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_sync.bundle_import.db_import.apply_transcript_batch",
        fake_transcripts,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.init_pool",
        lambda prefix: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.close_pool",
        lambda: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.getcursor",
        _fake_import_cursor,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.export_bundle_dir",
        lambda bid: bundle,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.db_import_state.set_last_imported_at",
        lambda cur, ts: None,
    )

    from services.podcast.transcript_import.importer import run_import

    run_import(prod=True, bundle_id=bundle_id, dry_run=False, force=True)
    assert call_order == ["shows", "episodes", "transcripts"]


def test_export_watermark_advanced_after_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "export_state.json"
    monkeypatch.setenv("PODCAST_SYNC_STATE_FILE", str(state_path))
    monkeypatch.setenv("PODCAST_EXPORT_STORAGE_KIND", "skip")
    save_export_state(ExportState(last_exported_at=None))

    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.upload_export",
        lambda _m, _p, _s=None: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.db_shows.fetch_shows_by_ids",
        lambda cur, ids: [],
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.init_pool",
        lambda prefix: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.close_pool",
        lambda: None,
    )
    _FakeCursor._fetchall_calls = 0
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.getcursor",
        _fake_export_cursor,
    )
    bundle = tmp_path / "bundle"
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.export_bundle_dir",
        lambda bundle_id: bundle,
    )

    from services.podcast.transcript_export.exporter import run_export

    run_export(prod=False, dry_run=False)
    state = load_export_state(state_path)
    assert state.last_exported_at is not None
    assert state.last_exported_at == datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)


def test_parse_bundle_id() -> None:
    assert parse_bundle_id("2026-05-19T12-00-00Z") == datetime(
        2026, 5, 19, 12, 0, 0, tzinfo=timezone.utc
    )


class _FakeCursor:
    _fetchall_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        pass

    def fetchone(self):
        return (1,)

    def fetchall(self):
        _FakeCursor._fetchall_calls += 1
        if _FakeCursor._fetchall_calls > 1:
            return []
        ts = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
        return [
            (
                "ep_3_abc",
                "guid-1",
                "https://cdn.example.com/a.mp3",
                ts,
                "Title",
                "Desc",
                "transcript text",
                ts,
                3,
                "https://example.com/rss",
            )
        ]


def _fake_export_cursor(*args, **kwargs):
    return _FakeCursor()


class _FakeImportCursor:
    def __enter__(self):
        return object()

    def __exit__(self, *args):
        return False


def _fake_import_cursor(*args, **kwargs):
    return _FakeImportCursor()
