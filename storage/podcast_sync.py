from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from storage.backends import AzureBlobStorage, LocalFileStorage, SftpStorage, StorageBackend
from storage.nitwitch_paths import NITWITCH_SFTP_ROOT, PODCAST_TRANSCRIPTS_SUBDIR

from services.podcast.transcript_sync.format import parse_bundle_id, read_manifest

log = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"


def get_local_dir() -> Path:
    return Path(os.environ.get("PODCAST_SYNC_LOCAL_DIR", "./data/podcast_sync_exports"))


def get_azure_blob_prefix() -> str:
    """
    Virtual folder prefix inside the Azure Storage container for podcast export
    bundles (manifest + JSONL). Only used when export/import kind is ``azure``.
    Ignored for sftp, nitwitch HTTP, and local staging.
    """
    prefix = os.environ.get(
        "PODCAST_SYNC_AZURE_BLOB_PREFIX", "podcast_transcripts/"
    )
    return prefix if prefix.endswith("/") else prefix + "/"


def get_export_storage_kind() -> str:
    return os.environ.get("PODCAST_EXPORT_STORAGE_KIND", "skip").strip().lower()


def get_import_storage_kind() -> str:
    return os.environ.get("PODCAST_IMPORT_STORAGE_KIND", "local").strip().lower()


def export_bundle_dir(bundle_id: str) -> Path:
    return get_local_dir() / PODCAST_TRANSCRIPTS_SUBDIR / bundle_id


def manifest_path_for_bundle(bundle_id: str) -> Path:
    return export_bundle_dir(bundle_id) / MANIFEST_NAME


# Backward-compatible aliases
def manifest_path_for_date(export_date: str) -> Path:
    return manifest_path_for_bundle(export_date)


def _export_backend(kind: str) -> StorageBackend | None:
    if kind == "skip":
        return None
    if kind == "local":
        return LocalFileStorage(get_local_dir())
    if kind == "azure":
        return AzureBlobStorage.from_env()
    if kind == "sftp":
        return SftpStorage.from_env(NITWITCH_SFTP_ROOT)
    raise ValueError(f"Unsupported PODCAST_EXPORT_STORAGE_KIND: {kind!r}")


def _bundle_rel_path(bundle_id: str, filename: str, *, kind: str) -> str:
    segment = f"{PODCAST_TRANSCRIPTS_SUBDIR}/{bundle_id}/{filename}"
    if kind == "azure":
        return f"{get_azure_blob_prefix()}{bundle_id}/{filename}"
    return segment


def _upload_file(
    storage: StorageBackend,
    *,
    kind: str,
    bundle_id: str,
    local_path: Path,
    content_type: str,
) -> None:
    rel = _bundle_rel_path(bundle_id, local_path.name, kind=kind)
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
    - local: upload into PODCAST_SYNC_LOCAL_DIR mirror
    - azure: PUT blobs via AzureBlobStorage
    - sftp: PUT under NITWITCH_SFTP_ROOT on SSH host
    """
    kind = get_export_storage_kind()
    bundle_id = local_manifest_path.parent.name

    if kind == "skip":
        log.info(
            "PODCAST_EXPORT_STORAGE_KIND=skip; leaving export at %s",
            local_manifest_path.parent,
        )
        return

    backend = _export_backend(kind)
    assert backend is not None

    _upload_file(
        backend,
        kind=kind,
        bundle_id=bundle_id,
        local_path=local_manifest_path,
        content_type="application/json",
    )
    _upload_file(
        backend,
        kind=kind,
        bundle_id=bundle_id,
        local_path=local_payload_path,
        content_type="application/x-ndjson",
    )
    if local_shows_path is not None and local_shows_path.is_file():
        _upload_file(
            backend,
            kind=kind,
            bundle_id=bundle_id,
            local_path=local_shows_path,
            content_type="application/x-ndjson",
        )

    if kind == "local":
        log.info("Copied export to local sync dir: %s", get_local_dir())
    elif kind == "azure":
        log.info("Uploaded export to Azure blob prefix %s", get_azure_blob_prefix())
    elif kind == "sftp":
        log.info("Uploaded export to SFTP root %s", NITWITCH_SFTP_ROOT)


def _copy_local_bundle(bundle_id: str, dest_dir: Path) -> tuple[Path, Path]:
    bundle_src = export_bundle_dir(bundle_id)
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


def _download_azure_bundle(bundle_id: str, dest_dir: Path) -> tuple[Path, Path]:
    backend = AzureBlobStorage.from_env()
    prefix = get_azure_blob_prefix()
    dest_dir.mkdir(parents=True, exist_ok=True)

    manifest_rel = f"{prefix}{bundle_id}/{MANIFEST_NAME}"
    manifest_bytes = backend.read_bytes(manifest_rel)
    manifest_path = dest_dir / MANIFEST_NAME
    manifest_path.write_bytes(manifest_bytes)
    m = read_manifest(manifest_path)

    for filename in (m.payload, m.shows_payload):
        if not filename:
            continue
        rel = f"{prefix}{bundle_id}/{filename}"
        (dest_dir / filename).write_bytes(backend.read_bytes(rel))

    payload = dest_dir / m.payload
    if not payload.is_file():
        raise FileNotFoundError(f"missing payload {payload}")
    return manifest_path, payload


def download_export_bundle(bundle_id: str, dest_dir: Path) -> tuple[Path, Path]:
    """Download manifest + JSONL for bundle_id into dest_dir."""
    kind = get_import_storage_kind()

    if kind in ("skip", "local"):
        return _copy_local_bundle(bundle_id, dest_dir)

    if kind == "nitwitch":
        from services.podcast.transcript_import.nitwitch_dl import download_bundle

        download_bundle(bundle_id, dest_dir)
        manifest = dest_dir / MANIFEST_NAME
        m = read_manifest(manifest)
        return manifest, dest_dir / m.payload

    if kind == "azure":
        return _download_azure_bundle(bundle_id, dest_dir)

    raise ValueError(f"Unsupported PODCAST_IMPORT_STORAGE_KIND: {kind!r}")


def download_export_for_date(export_date: str, dest_dir: Path) -> tuple[Path, Path]:
    """Backward-compatible alias (export_date is the bundle folder name)."""
    return download_export_bundle(export_date, dest_dir)


def list_export_bundle_ids() -> list[str]:
    """List bundle folder names from the configured import source."""
    kind = get_import_storage_kind()

    if kind == "nitwitch":
        from services.podcast.transcript_import.nitwitch_dl import list_export_bundle_ids as nitwitch_list

        return nitwitch_list()

    root = get_local_dir() / PODCAST_TRANSCRIPTS_SUBDIR
    if not root.is_dir():
        return []
    return sorted(
        p.name
        for p in root.iterdir()
        if p.is_dir() and (p / MANIFEST_NAME).is_file()
    )


def list_pending_bundle_ids(
    last_imported_at: datetime | None,
    *,
    force: bool = False,
) -> list[str]:
    """Bundle ids with export time strictly after last_imported_at (sorted)."""
    from services.podcast.transcript_import.nitwitch_dl import bundle_sort_key

    all_ids = list_export_bundle_ids()
    if force or last_imported_at is None:
        pending = all_ids
    else:
        if last_imported_at.tzinfo is None:
            last_imported_at = last_imported_at.replace(tzinfo=timezone.utc)
        else:
            last_imported_at = last_imported_at.astimezone(timezone.utc)
        pending = [
            bid
            for bid in all_ids
            if parse_bundle_id(bid) > last_imported_at
        ]
    pending.sort(key=bundle_sort_key)
    return pending


def find_latest_export_date() -> str | None:
    """Return the lexicographically greatest bundle id, if any."""
    ids = list_export_bundle_ids()
    return ids[-1] if ids else None


def get_sync_storage_backend() -> StorageBackend | None:
    return _export_backend(get_export_storage_kind())
