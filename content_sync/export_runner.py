from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from content_sync.format import (
    MANIFEST_NAME,
    SCHEMA_VERSION,
    ContentSyncManifest,
    PlatformFileInfo,
    make_bundle_id,
    platform_filename,
    sidecar_filename,
    write_jsonl_row,
    write_manifest,
)
from content_sync.platforms import get_handlers
from content_sync.state import ExportState, load_export_state, save_export_state
from storage.content_sync import export_bundle_dir, upload_bundle

log = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_export(
    *,
    since_override: datetime | None = None,
    dry_run: bool = False,
    platforms: list[str] | None = None,
) -> ContentSyncManifest | None:
    until_ts = _utc_now()
    bundle_id = make_bundle_id(until_ts)
    since_ts = since_override if since_override is not None else load_export_state().last_exported_at

    handlers = get_handlers(platforms)
    log.info(
        "content sync export since=%s until=%s platforms=%s dry_run=%s",
        since_ts,
        until_ts,
        [h.platform for h in handlers],
        dry_run,
    )

    if dry_run:
        return None

    bundle_dir = export_bundle_dir(bundle_id)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    manifest_platforms: dict[str, PlatformFileInfo] = {}
    sidecar_files: dict[str, str] = {}
    max_watermark: datetime | None = None

    from db.db import getcursor

    with getcursor() as cur:
        for handler in handlers:
            rows, sidecars = handler.export_delta(
                cur, since=since_ts, until=until_ts
            )
            out_path = bundle_dir / platform_filename(handler.platform)
            with out_path.open("w", encoding="utf-8") as fp:
                for row in rows:
                    write_jsonl_row(fp, row)
                    ts_raw = row.get("transcript_updated_at") or row.get("created_at_ts") or row.get("date_entered")
                    if ts_raw:
                        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                        if max_watermark is None or ts > max_watermark:
                            max_watermark = ts
            manifest_platforms[handler.platform] = PlatformFileInfo(
                row_count=len(rows),
                file=out_path.name,
            )
            log.info("exported %d rows for %s", len(rows), handler.platform)

            for sidecar_name, sidecar_rows in sidecars.items():
                if not sidecar_rows:
                    continue
                fname = sidecar_filename(sidecar_name, bundle_id)
                sidecar_files[sidecar_name] = fname
                sc_path = bundle_dir / fname
                with sc_path.open("w", encoding="utf-8") as fp:
                    for row in sidecar_rows:
                        write_jsonl_row(fp, row)

    manifest = ContentSyncManifest(
        schema_version=SCHEMA_VERSION,
        bundle_id=bundle_id,
        since_ts=since_ts,
        until_ts=until_ts,
        platforms=manifest_platforms,
        sidecars=sidecar_files,
    )
    write_manifest(bundle_dir / MANIFEST_NAME, manifest)

    try:
        upload_bundle(bundle_dir)
    except Exception:
        log.exception("upload failed; export watermark not advanced")
        raise

    new_watermark = max_watermark if max_watermark is not None else until_ts
    save_export_state(ExportState(last_exported_at=new_watermark))
    log.info("advanced export watermark to %s", new_watermark)
    return manifest
