from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool
from services.podcast.transcript_sync import db_export, db_shows
from services.podcast.transcript_sync.format import (
    SCHEMA_VERSION,
    ExportManifest,
    episodes_payload_filename,
    make_bundle_id,
    shows_payload_filename,
    write_episode_jsonl_row,
    write_manifest,
    write_show_jsonl_row,
)
from services.podcast.transcript_sync.state import ExportState, load_export_state, save_export_state
from storage.podcast_sync import export_bundle_dir, upload_export

load_dotenv()

log = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_export(
    *,
    prod: bool,
    since_override: datetime | None = None,
    dry_run: bool = False,
) -> ExportManifest | None:
    until_ts = _utc_now()
    bundle_id = make_bundle_id(until_ts)
    export_date = until_ts.date().isoformat()

    state = load_export_state()
    since_ts = since_override if since_override is not None else state.last_exported_at

    with getcursor() as cur:
        total = db_export.count_exportable(cur, since_ts)

    log.info(
        "export window since=%s until=%s episode_rows=%d dry_run=%s",
        since_ts,
        until_ts,
        total,
        dry_run,
    )

    if dry_run:
        return None

    bundle_dir = export_bundle_dir(bundle_id)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    payload_path = bundle_dir / episodes_payload_filename(bundle_id)
    shows_path = bundle_dir / shows_payload_filename(bundle_id)
    manifest_path = bundle_dir / "manifest.json"

    row_count = 0
    max_updated: datetime | None = None
    after_ts: datetime | None = None
    after_id: str | None = None
    podcast_ids: set[int] = set()

    with payload_path.open("w", encoding="utf-8") as out_fp:
        while True:
            with getcursor() as cur:
                batch = db_export.fetch_export_batch(
                    cur,
                    since_ts=since_ts,
                    until_ts=until_ts,
                    after_ts=after_ts,
                    after_id=after_id,
                )
            if not batch:
                break
            for row in batch:
                write_episode_jsonl_row(out_fp, row)
                row_count += 1
                if row.source_show_id is not None:
                    podcast_ids.add(row.source_show_id)
                if max_updated is None or row.transcript_updated_at > max_updated:
                    max_updated = row.transcript_updated_at
            last = batch[-1]
            after_ts = last.transcript_updated_at
            after_id = last.source_episode_id

    shows: list = []
    if podcast_ids:
        with getcursor() as cur:
            shows = db_shows.fetch_shows_by_ids(cur, sorted(podcast_ids))

    with shows_path.open("w", encoding="utf-8") as shows_fp:
        for show in shows:
            write_show_jsonl_row(shows_fp, show)

    manifest = ExportManifest(
        schema_version=SCHEMA_VERSION,
        bundle_id=bundle_id,
        export_date=export_date,
        since_ts=since_ts,
        until_ts=until_ts,
        row_count=row_count,
        payload=episodes_payload_filename(bundle_id),
        shows_payload=shows_payload_filename(bundle_id),
        shows_count=len(shows),
    )
    write_manifest(manifest_path, manifest)
    log.info(
        "wrote %d episode rows to %s and %d shows to %s",
        row_count,
        payload_path,
        len(shows),
        shows_path,
    )

    try:
        upload_export(manifest_path, payload_path, shows_path)
    except Exception:
        log.exception("upload failed; export state watermark not advanced")
        raise

    new_watermark = max_updated if max_updated is not None else until_ts
    save_export_state(ExportState(last_exported_at=new_watermark))
    log.info("advanced export watermark to %s", new_watermark)
    return manifest


def main(prod: bool = False, since_override: datetime | None = None, dry_run: bool = False) -> None:
    _setup_logging()
    prefix = "prod" if prod else "dev"
    init_pool(prefix=prefix)
    log.info("Initialized DB pool with %s prefix.", prefix.upper())
    try:
        run_export(prod=prod, since_override=since_override, dry_run=dry_run)
    finally:
        close_pool()
