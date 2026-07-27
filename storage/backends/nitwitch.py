from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Optional

from storage.nitwitch_upload import load_upload_config, upload_file

from .base import StorageBackend

log = logging.getLogger(__name__)


def _remote_name(rel_path: str) -> str:
    """Return a bare filename; reject nested paths (nitwitch flat upload)."""
    name = rel_path.strip().replace("\\", "/")
    if not name or "/" in name or name in (".", ".."):
        raise ValueError(
            f"Nitwitch upload requires a bare filename, got: {rel_path!r}"
        )
    return name


class NitwitchUploadStorage(StorageBackend):
    """Write files via nitwitch WebDAV PUT (``storage.nitwitch_upload``)."""

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            load_upload_config()
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        self.write_bytes(
            rel_path,
            text.encode("utf-8"),
            content_type="application/json; charset=utf-8",
        )

    def write_bytes(
        self,
        rel_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        remote = _remote_name(rel_path)
        suffix = Path(remote).suffix or ".bin"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(data)
        try:
            url = upload_file(tmp_path, remote_name=remote)
            log.info("NitwitchUploadStorage wrote %s -> %s", remote, url)
        finally:
            tmp_path.unlink(missing_ok=True)
