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
    payload_filename,
    shows_payload_filename,
    write_jsonl_row,
    write_manifest,
    write_show_jsonl_row,
)
from services.podcast.transcript_sync.state import ExportState, load_export_state, save_export_state
from services.podcast.transcript_sync.storage import export_bundle_dir, upload_export

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
    export_date = until_ts.date().isoformat()

    state = load_export_state()
    since_ts = since_override if since_override is not None else state.last_exported_at

    with getcursor() as cur:
        total = db_export.count_exportable(cur, since_ts)
        shows = db_shows.fetch_all_shows(cur)

    log.info(
        "export window since=%s until=%s transcript_rows=%d shows=%d dry_run=%s",
        since_ts,
        until_ts,
        total,
        len(shows),
        dry_run,
    )

    if dry_run:
        return None

    bundle_dir = export_bundle_dir(export_date)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    payload_path = bundle_dir / payload_filename(export_date)
    shows_path = bundle_dir / shows_payload_filename(export_date)
    manifest_path = bundle_dir / "manifest.json"

    with shows_path.open("w", encoding="utf-8") as shows_fp:
        for show in shows:
            write_show_jsonl_row(shows_fp, show)

    row_count = 0
    max_updated: datetime | None = None
    after_ts: datetime | None = None
    after_id: str | None = None

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
                write_jsonl_row(out_fp, row)
                row_count += 1
                if max_updated is None or row.transcript_updated_at > max_updated:
                    max_updated = row.transcript_updated_at
            last = batch[-1]
            after_ts = last.transcript_updated_at
            after_id = last.id

    manifest = ExportManifest(
        schema_version=SCHEMA_VERSION,
        export_date=export_date,
        since_ts=since_ts,
        until_ts=until_ts,
        row_count=row_count,
        payload=payload_filename(export_date),
        shows_payload=shows_payload_filename(export_date),
        shows_count=len(shows),
    )
    write_manifest(manifest_path, manifest)
    log.info(
        "wrote %d transcript rows to %s and %d shows to %s",
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
