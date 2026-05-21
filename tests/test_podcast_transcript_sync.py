"""Tests for podcast transcript export/import sync."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.podcast.transcript_sync.format import (
    ExportManifest,
    ShowRow,
    TranscriptRow,
    iter_jsonl_rows,
    iter_show_jsonl_rows,
    payload_filename,
    read_manifest,
    should_apply_import,
    shows_payload_filename,
    write_jsonl_row,
    write_manifest,
    write_show_jsonl_row,
)
from services.podcast.transcript_sync.state import ExportState, load_export_state, save_export_state


def test_transcript_row_jsonl_roundtrip(tmp_path: Path) -> None:
    ts = datetime(2026, 5, 19, 3, 14, tzinfo=timezone.utc)
    row = TranscriptRow(id="ep1", transcript="hello world", transcript_updated_at=ts)
    out = tmp_path / "rows.jsonl"
    with out.open("w", encoding="utf-8") as fp:
        write_jsonl_row(fp, row)
    rows = list(iter_jsonl_rows(out))
    assert len(rows) == 1
    assert rows[0].id == "ep1"
    assert rows[0].transcript == "hello world"
    assert rows[0].transcript_updated_at == ts


def test_show_row_jsonl_roundtrip(tmp_path: Path) -> None:
    ts = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)
    row = ShowRow(
        id=7,
        title="My Podcast",
        rss_url="https://example.com/feed.xml",
        last_fetch_ts=ts,
        last_http_status=200,
    )
    out = tmp_path / "shows.jsonl"
    with out.open("w", encoding="utf-8") as fp:
        write_show_jsonl_row(fp, row)
    rows = list(iter_show_jsonl_rows(out))
    assert len(rows) == 1
    assert rows[0].id == 7
    assert rows[0].title == "My Podcast"
    assert rows[0].rss_url == "https://example.com/feed.xml"
    assert rows[0].last_fetch_ts == ts


def test_manifest_roundtrip(tmp_path: Path) -> None:
    since = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    until = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    manifest = ExportManifest(
        schema_version=2,
        export_date="2026-05-19",
        since_ts=since,
        until_ts=until,
        row_count=2,
        payload=payload_filename("2026-05-19"),
        shows_payload=shows_payload_filename("2026-05-19"),
        shows_count=5,
    )
    path = tmp_path / "manifest.json"
    write_manifest(path, manifest)
    loaded = read_manifest(path)
    assert loaded.export_date == "2026-05-19"
    assert loaded.row_count == 2
    assert loaded.shows_count == 5
    assert loaded.since_ts == since
    assert loaded.until_ts == until


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
        "services.podcast.transcript_export.exporter.db_shows.fetch_all_shows",
        lambda cur: [],
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
        lambda export_date: tmp_path / "bundle",
    )

    from services.podcast.transcript_export.exporter import run_export

    with pytest.raises(RuntimeError, match="upload failed"):
        run_export(prod=False, dry_run=False)

    state = load_export_state(state_path)
    assert state.last_exported_at is None


def test_export_writes_shows_jsonl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PODCAST_SYNC_STORAGE_KIND", "skip")
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
    show = ShowRow(id=3, title="Test Show", rss_url="https://example.com/rss")
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.db_shows.fetch_all_shows",
        lambda cur: [show],
    )
    bundle = tmp_path / "bundle"
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.export_bundle_dir",
        lambda export_date: bundle,
    )

    from services.podcast.transcript_export.exporter import run_export

    run_export(prod=False, dry_run=False)
    manifest = read_manifest(bundle / "manifest.json")
    shows_path = bundle / manifest.shows_payload
    assert shows_path.is_file()
    rows = list(iter_show_jsonl_rows(shows_path))
    assert len(rows) == 1
    assert rows[0].id == 3
    assert rows[0].title == "Test Show"


def test_import_inserts_new_shows_before_transcripts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "2026-05-19"
    bundle.mkdir()
    show = ShowRow(id=9, title="New Show")
    shows_path = bundle / shows_payload_filename("2026-05-19")
    with shows_path.open("w", encoding="utf-8") as fp:
        write_show_jsonl_row(fp, show)
    payload_path = bundle / payload_filename("2026-05-19")
    payload_path.write_text("", encoding="utf-8")
    manifest = ExportManifest(
        schema_version=2,
        export_date="2026-05-19",
        since_ts=None,
        until_ts=datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc),
        row_count=0,
        payload=payload_filename("2026-05-19"),
        shows_payload=shows_payload_filename("2026-05-19"),
        shows_count=1,
    )
    write_manifest(bundle / "manifest.json", manifest)

    inserted: list[list] = []

    def fake_insert(cur, rows):
        inserted.append(rows)
        return len(rows)

    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.db_shows.insert_new_shows",
        fake_insert,
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
        "services.podcast.transcript_import.importer.manifest_path_for_date",
        lambda d: bundle / "manifest.json",
    )
    monkeypatch.setattr(
        "services.podcast.transcript_import.importer.getcursor",
        _fake_import_cursor,
    )
    monkeypatch.setenv("PODCAST_SYNC_IMPORT_STATE_FILE", str(tmp_path / "import_state.json"))

    from services.podcast.transcript_import.importer import run_import

    run_import(prod=True, export_date="2026-05-19", dry_run=False, force=True)
    assert len(inserted) == 1
    assert inserted[0][0].id == 9


def test_export_watermark_advanced_after_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "export_state.json"
    monkeypatch.setenv("PODCAST_SYNC_STATE_FILE", str(state_path))
    monkeypatch.setenv("PODCAST_SYNC_STORAGE_KIND", "skip")
    save_export_state(ExportState(last_exported_at=None))

    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.upload_export",
        lambda _m, _p, _s=None: None,
    )
    monkeypatch.setattr(
        "services.podcast.transcript_export.exporter.db_shows.fetch_all_shows",
        lambda cur: [],
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
        lambda export_date: bundle,
    )

    from services.podcast.transcript_export.exporter import run_export

    run_export(prod=False, dry_run=False)
    state = load_export_state(state_path)
    assert state.last_exported_at is not None
    assert state.last_exported_at == datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)


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
        return [("ep1", "transcript text", ts)]


def _fake_export_cursor(*args, **kwargs):
    return _FakeCursor()


class _FakeImportCursor:
    def __enter__(self):
        return object()

    def __exit__(self, *args):
        return False


def _fake_import_cursor(*args, **kwargs):
    return _FakeImportCursor()
