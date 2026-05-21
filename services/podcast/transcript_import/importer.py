from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool
from services.podcast.transcript_sync import db_import, db_shows
from services.podcast.transcript_sync.format import (
    ExportManifest,
    iter_jsonl_rows,
    iter_show_jsonl_rows,
    read_manifest,
)
from services.podcast.transcript_sync.state import ImportState, load_import_state, save_import_state
from services.podcast.transcript_sync.storage import (
    MANIFEST_NAME,
    download_export_for_date,
    find_latest_export_date,
    manifest_path_for_date,
)

load_dotenv()

log = logging.getLogger(__name__)
BATCH_SIZE = db_import.DEFAULT_BATCH_SIZE


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _resolve_export_date(export_date: str | None) -> str:
    if export_date:
        return export_date
    latest = find_latest_export_date()
    if latest is None:
        raise FileNotFoundError("no export bundles found under PODCAST_SYNC_LOCAL_DIR")
    return latest


def _already_processed(
    export_date: str,
    row_count: int,
    shows_count: int,
    force: bool,
) -> bool:
    if force:
        return False
    state = load_import_state()
    return (
        state.export_date == export_date
        and state.row_count == row_count
        and state.shows_count == shows_count
    )


def _load_bundle(export_date: str) -> tuple[ExportManifest, Path, Path, Path | None]:
    bundle_dir = manifest_path_for_date(export_date).parent
    manifest_path = bundle_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        bundle_dir.mkdir(parents=True, exist_ok=True)
        download_export_for_date(export_date, bundle_dir)
    manifest = read_manifest(manifest_path)
    payload_path = bundle_dir / manifest.payload
    if not payload_path.is_file():
        raise FileNotFoundError(f"missing payload: {payload_path}")
    shows_path = None
    if manifest.shows_payload:
        shows_path = bundle_dir / manifest.shows_payload
        if not shows_path.is_file():
            raise FileNotFoundError(f"missing shows payload: {shows_path}")
    return manifest, manifest_path, payload_path, shows_path


def run_import(
    *,
    prod: bool,
    export_date: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    date = _resolve_export_date(export_date)
    manifest, manifest_path, payload_path, shows_path = _load_bundle(date)
    shows_count = manifest.shows_count

    log.info(
        "import export_date=%s transcript_rows=%d shows=%d dry_run=%s force=%s",
        manifest.export_date,
        manifest.row_count,
        shows_count,
        dry_run,
        force,
    )

    if _already_processed(manifest.export_date, manifest.row_count, shows_count, force):
        log.info("export %s already applied; skipping", manifest.export_date)
        return

    if manifest.row_count == 0 and shows_count == 0:
        log.info("empty export; nothing to apply")
        if not dry_run:
            save_import_state(
                ImportState(
                    export_date=manifest.export_date,
                    row_count=0,
                    shows_count=0,
                    manifest_path=str(manifest_path),
                )
            )
        return

    shows_inserted = 0
    if shows_path is not None:
        show_rows = list(iter_show_jsonl_rows(shows_path))
        if dry_run:
            shows_inserted = len(show_rows)
            log.info("dry-run: would import up to %d shows", shows_inserted)
        else:
            with getcursor(commit=True) as cur:
                shows_inserted = db_shows.insert_new_shows(cur, show_rows)
            log.info("inserted %d new shows", shows_inserted)

    totals = {"seen": 0, "updated": 0, "registered": 0}
    batch: list = []

    for row in iter_jsonl_rows(payload_path):
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            totals = _apply_transcript_batch(totals, batch, dry_run=dry_run)
            batch = []

    if batch:
        totals = _apply_transcript_batch(totals, batch, dry_run=dry_run)

    log.info(
        "import complete shows_inserted=%d seen=%d updated=%d registered=%d",
        shows_inserted,
        totals["seen"],
        totals["updated"],
        totals["registered"],
    )

    if not dry_run:
        save_import_state(
            ImportState(
                export_date=manifest.export_date,
                row_count=manifest.row_count,
                shows_count=shows_count,
                manifest_path=str(manifest_path),
            )
        )


def _apply_transcript_batch(totals: dict[str, int], batch: list, *, dry_run: bool) -> dict[str, int]:
    if dry_run:
        totals["seen"] += len(batch)
        return totals
    with getcursor(commit=True) as cur:
        result = db_import.apply_batch(cur, batch)
    totals["seen"] += result.seen
    totals["updated"] += result.updated
    totals["registered"] += result.registered
    return totals


def main(
    prod: bool = True,
    export_date: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    _setup_logging()
    prefix = "prod" if prod else "dev"
    init_pool(prefix=prefix)
    log.info("Initialized DB pool with %s prefix.", prefix.upper())
    try:
        run_import(
            prod=prod,
            export_date=export_date,
            dry_run=dry_run,
            force=force,
        )
    finally:
        close_pool()
