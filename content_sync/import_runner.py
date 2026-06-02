from __future__ import annotations

import logging
from pathlib import Path

from content_sync import db_sync_state
from content_sync.format import (
    MANIFEST_NAME,
    SIDECAR_PODCAST_SHOWS,
    SIDECAR_YOUTUBE_SEGMENTS,
    iter_jsonl_rows,
    read_manifest,
)
from content_sync.platforms import get_handlers
from content_sync.platforms.base import ImportStats
from storage.content_sync import download_bundle, get_local_dir, list_pending_bundle_ids
from storage.nitwitch_paths import CONTENT_SYNC_SUBDIR

log = logging.getLogger(__name__)


def _staging_dir(bundle_id: str) -> Path:
    d = get_local_dir() / CONTENT_SYNC_SUBDIR / "_import_staging" / bundle_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def _load_sidecar(bundle_dir: Path, manifest, name: str) -> list[dict]:
    fname = manifest.sidecars.get(name)
    if not fname:
        return []
    path = bundle_dir / fname
    if not path.is_file():
        return []
    return list(iter_jsonl_rows(path))


def _import_one_bundle(bundle_dir: Path, *, dry_run: bool) -> ImportStats:
    manifest = read_manifest(bundle_dir / MANIFEST_NAME)
    total = ImportStats()

    if dry_run:
        for platform, pinfo in manifest.platforms.items():
            path = bundle_dir / pinfo.file
            if path.is_file():
                count = sum(1 for _ in iter_jsonl_rows(path))
                log.info("dry-run %s: %d rows", platform, count)
        return total

    handlers = {h.platform: h for h in get_handlers(list(manifest.platforms.keys()))}

    from db.db import getcursor

    with getcursor(commit=True) as cur:
        for platform, pinfo in manifest.platforms.items():
            handler = handlers.get(platform)
            if handler is None:
                log.warning("no handler for platform %s; skipping", platform)
                continue
            path = bundle_dir / pinfo.file
            rows = list(iter_jsonl_rows(path)) if path.is_file() else []
            sidecars: dict[str, list[dict]] = {}
            if platform == "podcast_episode":
                sidecars[SIDECAR_PODCAST_SHOWS] = _load_sidecar(
                    bundle_dir, manifest, SIDECAR_PODCAST_SHOWS
                )
            elif platform == "youtube_video":
                sidecars[SIDECAR_YOUTUBE_SEGMENTS] = _load_sidecar(
                    bundle_dir, manifest, SIDECAR_YOUTUBE_SEGMENTS
                )

            stats = handler.import_bundle(cur, rows=rows, sidecars=sidecars)
            total.merge(stats)
            log.info(
                "imported %s: seen=%d upserted=%d transcripts=%d registered=%d segments=%d",
                platform,
                stats.rows_seen,
                stats.rows_upserted,
                stats.transcripts_updated,
                stats.posts_registered,
                stats.segments_replaced,
            )

    return total


def run_import(
    *,
    bundle_id: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    from db.db import getcursor

    if bundle_id:
        pending = [bundle_id]
    else:
        with getcursor() as cur:
            last = db_sync_state.get_last_imported_bundle_at(cur)
        pending = list_pending_bundle_ids(last, force=force)
        if not pending:
            log.info("no pending content sync bundles (last=%s force=%s)", last, force)
            return
        log.info("importing %d bundle(s) since last=%s", len(pending), last)

    max_until = None
    for bid in pending:
        bundle_dir = _staging_dir(bid)
        download_bundle(bid, bundle_dir)
        stats = _import_one_bundle(bundle_dir, dry_run=dry_run)
        log.info("bundle %s complete: %s", bid, stats)
        if not dry_run:
            manifest = read_manifest(bundle_dir / MANIFEST_NAME)
            if max_until is None or manifest.until_ts > max_until:
                max_until = manifest.until_ts

    if not dry_run and max_until is not None:
        with getcursor(commit=True) as cur:
            db_sync_state.set_last_imported_bundle_at(cur, max_until)
        log.info("advanced import watermark to %s", max_until)
