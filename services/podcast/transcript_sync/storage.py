from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from services.storage import AzureBlobStorage, LocalFileStorage, StorageBackend

from .format import ExportManifest, read_manifest

log = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"


def get_storage_kind() -> str:
    return os.environ.get("PODCAST_SYNC_STORAGE_KIND", "skip").strip().lower()


def get_local_dir() -> Path:
    return Path(os.environ.get("PODCAST_SYNC_LOCAL_DIR", "./data/podcast_sync_exports"))


def get_blob_prefix() -> str:
    prefix = os.environ.get("PODCAST_SYNC_BLOB_PREFIX", "podcast_transcripts/")
    return prefix if prefix.endswith("/") else prefix + "/"


def export_bundle_dir(export_date: str) -> Path:
    return get_local_dir() / "podcast_transcripts" / export_date


def manifest_path_for_date(export_date: str) -> Path:
    return export_bundle_dir(export_date) / MANIFEST_NAME


def get_sync_storage_backend() -> StorageBackend | None:
    kind = get_storage_kind()
    if kind == "skip":
        return None
    if kind == "local":
        return LocalFileStorage(get_local_dir())
    if kind == "azure":
        return AzureBlobStorage.from_env()
    raise ValueError(f"Unsupported PODCAST_SYNC_STORAGE_KIND: {kind!r}")


def _blob_rel_path(export_date: str, filename: str) -> str:
    return f"{get_blob_prefix()}{export_date}/{filename}"


def _upload_file(
    storage: StorageBackend | None,
    *,
    kind: str,
    export_date: str,
    local_path: Path,
    content_type: str,
) -> None:
    if storage is None:
        return
    rel = f"podcast_transcripts/{export_date}/{local_path.name}"
    if content_type.startswith("text/") or content_type == "application/json":
        storage.write_text(rel, local_path.read_text(encoding="utf-8"))
    else:
        storage.write_bytes(rel, local_path.read_bytes(), content_type=content_type)


def upload_export(
    local_manifest_path: Path,
    local_payload_path: Path,
    local_shows_path: Path | None = None,
) -> None:
    """
    Upload manifest + JSONL (+ optional shows JSONL) to remote storage.

    - skip: no-op (files remain under PODCAST_SYNC_LOCAL_DIR only)
    - local: copy into PODCAST_SYNC_LOCAL_DIR mirror (same tree as staging)
    - azure: PUT blobs via AzureBlobStorage
    """
    kind = get_storage_kind()
    export_date = local_manifest_path.parent.name

    if kind == "skip":
        log.info(
            "PODCAST_SYNC_STORAGE_KIND=skip; leaving export at %s",
            local_manifest_path.parent,
        )
        return

    backend: StorageBackend | None = None
    if kind == "local":
        backend = LocalFileStorage(get_local_dir())
    elif kind == "azure":
        backend = AzureBlobStorage.from_env()
    else:
        raise ValueError(f"Unsupported PODCAST_SYNC_STORAGE_KIND: {kind!r}")

    _upload_file(
        backend,
        kind=kind,
        export_date=export_date,
        local_path=local_manifest_path,
        content_type="application/json",
    )
    _upload_file(
        backend,
        kind=kind,
        export_date=export_date,
        local_path=local_payload_path,
        content_type="application/x-ndjson",
    )
    if local_shows_path is not None and local_shows_path.is_file():
        _upload_file(
            backend,
            kind=kind,
            export_date=export_date,
            local_path=local_shows_path,
            content_type="application/x-ndjson",
        )

    if kind == "local":
        log.info("Copied export to local sync dir: %s", get_local_dir())
    elif kind == "azure":
        log.info("Uploaded export to Azure blob prefix %s", get_blob_prefix())


def download_export_for_date(export_date: str, dest_dir: Path) -> tuple[Path, Path]:
    """
    Download manifest + JSONL for export_date into dest_dir.

    - skip / local: read from PODCAST_SYNC_LOCAL_DIR staging tree
    - azure: NotImplementedError until read/list exists on StorageBackend
    """
    kind = get_storage_kind()
    bundle_src = export_bundle_dir(export_date)

    if kind in ("skip", "local"):
        if not bundle_src.is_dir():
            raise FileNotFoundError(f"export bundle not found: {bundle_src}")
        dest_dir.mkdir(parents=True, exist_ok=True)
        for item in bundle_src.iterdir():
            if item.is_file():
                shutil.copy2(item, dest_dir / item.name)
        manifest = dest_dir / MANIFEST_NAME
        if not manifest.is_file():
            raise FileNotFoundError(f"missing manifest in {bundle_src}")
        m = read_manifest(manifest)
        payload = dest_dir / m.payload
        if not payload.is_file():
            raise FileNotFoundError(f"missing payload {payload}")
        if m.shows_payload:
            shows = dest_dir / m.shows_payload
            if not shows.is_file():
                raise FileNotFoundError(f"missing shows payload {shows}")
        return manifest, payload

    if kind == "azure":
        raise NotImplementedError(
            "Azure blob download is not implemented yet. "
            "Set PODCAST_SYNC_STORAGE_KIND=local and stage files under "
            "PODCAST_SYNC_LOCAL_DIR, or extend services.storage with read_bytes."
        )

    raise ValueError(f"Unsupported PODCAST_SYNC_STORAGE_KIND: {kind!r}")


def find_latest_export_date() -> str | None:
    """Return the lexicographically greatest export_date directory name, if any."""
    root = get_local_dir() / "podcast_transcripts"
    if not root.is_dir():
        return None
    dates = sorted(
        p.name
        for p in root.iterdir()
        if p.is_dir() and (p / MANIFEST_NAME).is_file()
    )
    return dates[-1] if dates else None
