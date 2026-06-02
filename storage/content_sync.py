from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from storage.backends import AzureBlobStorage, LocalFileStorage, SftpStorage, StorageBackend
from storage.nitwitch_paths import CONTENT_SYNC_SUBDIR, NITWITCH_SFTP_ROOT

from content_sync.format import MANIFEST_NAME, bundle_sort_key, parse_bundle_id, read_manifest

log = logging.getLogger(__name__)


def get_local_dir() -> Path:
    return Path(os.environ.get("CONTENT_SYNC_LOCAL_DIR", "./data/content_sync"))


def get_azure_blob_prefix() -> str:
    prefix = os.environ.get("CONTENT_SYNC_AZURE_BLOB_PREFIX", "content_sync/")
    return prefix if prefix.endswith("/") else prefix + "/"


def get_export_storage_kind() -> str:
    return os.environ.get("CONTENT_SYNC_EXPORT_STORAGE_KIND", "skip").strip().lower()


def get_import_storage_kind() -> str:
    return os.environ.get("CONTENT_SYNC_IMPORT_STORAGE_KIND", "local").strip().lower()


def export_bundle_dir(bundle_id: str) -> Path:
    return get_local_dir() / CONTENT_SYNC_SUBDIR / bundle_id


def _export_backend(kind: str) -> StorageBackend | None:
    if kind == "skip":
        return None
    if kind == "local":
        return LocalFileStorage(get_local_dir())
    if kind == "azure":
        return AzureBlobStorage.from_env()
    if kind == "sftp":
        return SftpStorage.from_env(NITWITCH_SFTP_ROOT)
    raise ValueError(f"Unsupported CONTENT_SYNC_EXPORT_STORAGE_KIND: {kind!r}")


def _bundle_rel_path(bundle_id: str, filename: str, *, kind: str) -> str:
    segment = f"{CONTENT_SYNC_SUBDIR}/{bundle_id}/{filename}"
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


def upload_bundle(bundle_dir: Path) -> None:
    """Upload all files in bundle_dir to configured remote storage."""
    kind = get_export_storage_kind()
    bundle_id = bundle_dir.name

    if kind == "skip":
        log.info(
            "CONTENT_SYNC_EXPORT_STORAGE_KIND=skip; leaving export at %s",
            bundle_dir,
        )
        return

    backend = _export_backend(kind)
    assert backend is not None

    for local_path in sorted(bundle_dir.iterdir()):
        if not local_path.is_file():
            continue
        ctype = (
            "application/json"
            if local_path.suffix == ".json"
            else "application/x-ndjson"
        )
        _upload_file(
            backend,
            kind=kind,
            bundle_id=bundle_id,
            local_path=local_path,
            content_type=ctype,
        )

    if kind == "local":
        log.info("Copied export to local sync dir: %s", get_local_dir())
    elif kind == "azure":
        log.info("Uploaded export to Azure blob prefix %s", get_azure_blob_prefix())
    elif kind == "sftp":
        log.info("Uploaded export to SFTP root %s", NITWITCH_SFTP_ROOT)


def _copy_local_bundle(bundle_id: str, dest_dir: Path) -> Path:
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
    return manifest


def _download_azure_bundle(bundle_id: str, dest_dir: Path) -> Path:
    backend = AzureBlobStorage.from_env()
    prefix = get_azure_blob_prefix()
    dest_dir.mkdir(parents=True, exist_ok=True)

    manifest_rel = f"{prefix}{bundle_id}/{MANIFEST_NAME}"
    manifest_path = dest_dir / MANIFEST_NAME
    manifest_path.write_bytes(backend.read_bytes(manifest_rel))
    manifest = read_manifest(manifest_path)

    names = [manifest.platforms[p].file for p in manifest.platforms]
    names.extend(manifest.sidecars.values())
    for filename in names:
        if not filename:
            continue
        rel = f"{prefix}{bundle_id}/{filename}"
        (dest_dir / filename).write_bytes(backend.read_bytes(rel))
    return manifest_path


def download_bundle(bundle_id: str, dest_dir: Path) -> Path:
    kind = get_import_storage_kind()

    if kind in ("skip", "local"):
        return _copy_local_bundle(bundle_id, dest_dir)

    if kind == "nitwitch":
        from storage.nitwitch_http import download_bundle_files

        dest_dir.mkdir(parents=True, exist_ok=True)
        download_bundle_files(
            CONTENT_SYNC_SUBDIR,
            bundle_id,
            dest_dir,
            [MANIFEST_NAME],
        )
        local_manifest = read_manifest(dest_dir / MANIFEST_NAME)
        filenames = [local_manifest.platforms[p].file for p in local_manifest.platforms]
        filenames.extend(local_manifest.sidecars.values())
        download_bundle_files(
            CONTENT_SYNC_SUBDIR,
            bundle_id,
            dest_dir,
            filenames,
        )
        return dest_dir / MANIFEST_NAME

    if kind == "azure":
        return _download_azure_bundle(bundle_id, dest_dir)

    raise ValueError(f"Unsupported CONTENT_SYNC_IMPORT_STORAGE_KIND: {kind!r}")


def list_export_bundle_ids() -> list[str]:
    kind = get_import_storage_kind()

    if kind == "nitwitch":
        from storage.nitwitch_http import list_bundle_ids

        return list_bundle_ids(CONTENT_SYNC_SUBDIR)

    root = get_local_dir() / CONTENT_SYNC_SUBDIR
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
