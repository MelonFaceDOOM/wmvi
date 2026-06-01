from __future__ import annotations

import logging
from pathlib import Path

from dotenv import load_dotenv

from db.db import close_pool, getcursor, init_pool
from services.podcast.transcript_sync import db_import_state
from services.podcast.transcript_sync.bundle_import import ImportStats, apply_bundle, dry_run_bundle
from services.podcast.transcript_sync.format import ExportManifest, read_manifest
from storage.podcast_sync import (
    MANIFEST_NAME,
    download_export_bundle,
    export_bundle_dir,
    list_pending_bundle_ids,
)

load_dotenv()

log = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _load_bundle(bundle_id: str) -> tuple[ExportManifest, Path, Path | None]:
    bundle_dir = export_bundle_dir(bundle_id)
    manifest_path = bundle_dir / MANIFEST_NAME
    if not manifest_path.is_file():
        bundle_dir.mkdir(parents=True, exist_ok=True)
        download_export_bundle(bundle_id, bundle_dir)
    manifest = read_manifest(manifest_path)
    episodes_path = bundle_dir / manifest.payload
    if not episodes_path.is_file():
        raise FileNotFoundError(f"missing episodes payload: {episodes_path}")
    shows_path = None
    if manifest.shows_payload:
        candidate = bundle_dir / manifest.shows_payload
        if candidate.is_file():
            shows_path = candidate
    return manifest, episodes_path, shows_path


def _import_one_bundle(
    bundle_id: str,
    *,
    dry_run: bool,
) -> ExportManifest:
    manifest, episodes_path, shows_path = _load_bundle(bundle_id)

    log.info(
        "import bundle_id=%s schema=%s episode_rows=%d shows=%d dry_run=%s",
        manifest.bundle_id,
        manifest.schema_version,
        manifest.row_count,
        manifest.shows_count,
        dry_run,
    )

    if manifest.row_count == 0 and manifest.shows_count == 0:
        log.info("empty bundle %s; nothing to apply", bundle_id)
        return manifest

    if dry_run:
        stats = dry_run_bundle(shows_path=shows_path, episodes_path=episodes_path)
    else:
        with getcursor(commit=True) as cur:
            stats = apply_bundle(
                cur,
                shows_path=shows_path,
                episodes_path=episodes_path,
            )

    _log_import_stats(bundle_id, stats)
    return manifest


def _log_import_stats(bundle_id: str, stats: ImportStats) -> None:
    log.info(
        "bundle %s: episodes_seen=%d shows_upserted=%d episodes_inserted=%d "
        "transcripts_applied=%d transcripts_updated=%d posts_registered=%d "
        "skipped_no_show_rss=%d skipped_show_not_in_map=%d "
        "skipped_no_transcript_key=%d skipped_id_collision=%d",
        bundle_id,
        stats.episodes_seen,
        stats.shows_upserted,
        stats.episodes_inserted,
        stats.transcripts_applied,
        stats.transcripts_updated,
        stats.posts_registered,
        stats.skipped_no_show_rss,
        stats.skipped_show_not_in_map,
        stats.skipped_no_transcript_key,
        stats.skipped_id_collision,
    )


def run_import(
    *,
    prod: bool,
    bundle_id: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    if bundle_id:
        pending = [bundle_id]
    else:
        with getcursor() as cur:
            last_imported_at = db_import_state.get_last_imported_at(cur)
        pending = list_pending_bundle_ids(last_imported_at, force=force)
        if not pending:
            log.info(
                "no pending export bundles (last_imported_at=%s force=%s)",
                last_imported_at,
                force,
            )
            return
        log.info(
            "importing %d bundle(s) since last_imported_at=%s",
            len(pending),
            last_imported_at,
        )

    max_until = None
    for bid in pending:
        manifest = _import_one_bundle(bid, dry_run=dry_run)
        if manifest.until_ts and (max_until is None or manifest.until_ts > max_until):
            max_until = manifest.until_ts

    if not dry_run and max_until is not None:
        with getcursor(commit=True) as cur:
            db_import_state.set_last_imported_at(cur, max_until)
        log.info("advanced last_imported_at to %s", max_until)


def main(
    prod: bool = False,
    bundle_id: str | None = None,
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
            bundle_id=bundle_id,
            dry_run=dry_run,
            force=force,
        )
    finally:
        close_pool()
