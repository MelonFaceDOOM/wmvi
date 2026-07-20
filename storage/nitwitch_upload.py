"""Upload files to nitwitch via WebDAV PUT + Basic Auth.

Env (repo ``.env``)::

    NITWITCH_UPLOAD_URL=https://nitwitch.com/u/<secret>/
    NITWITCH_UPLOAD_USER=...
    NITWITCH_UPLOAD_PASSWORD=...

Browse uploaded files at ``NITWITCH_UPLOADS_DL_BASE_URL`` (public listing).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import urljoin

import requests

log = logging.getLogger(__name__)

_HTTP_TIMEOUT_S = 300


def _normalize_base(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def load_upload_config() -> tuple[str, str, str]:
    """Return ``(base_url, user, password)`` from env. Raises ``ValueError`` if incomplete."""
    base = (os.environ.get("NITWITCH_UPLOAD_URL") or "").strip()
    user = (os.environ.get("NITWITCH_UPLOAD_USER") or "").strip()
    password = (os.environ.get("NITWITCH_UPLOAD_PASSWORD") or "").strip()
    missing = [
        name
        for name, val in (
            ("NITWITCH_UPLOAD_URL", base),
            ("NITWITCH_UPLOAD_USER", user),
            ("NITWITCH_UPLOAD_PASSWORD", password),
        )
        if not val
    ]
    if missing:
        raise ValueError(
            "Missing upload env var(s): "
            + ", ".join(missing)
            + ". Set them in the repo .env (see storage/nitwitch_upload.py)."
        )
    return _normalize_base(base), user, password


def upload_file(
    local_path: Path,
    *,
    remote_name: str | None = None,
    timeout_s: float = _HTTP_TIMEOUT_S,
) -> str:
    """PUT ``local_path`` to nitwitch WebDAV. Returns the final upload URL."""
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"Nothing to upload: {path}")

    name = (remote_name or path.name).strip()
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise ValueError(f"Invalid remote filename: {remote_name!r}")

    base, user, password = load_upload_config()
    url = urljoin(base, name)
    data = path.read_bytes()
    log.info("nitwitch upload: PUT %s (%d bytes)", url, len(data))
    resp = requests.put(
        url,
        data=data,
        auth=(user, password),
        timeout=timeout_s,
        headers={"Content-Type": "application/octet-stream"},
    )
    resp.raise_for_status()
    log.info("nitwitch upload: ok %s status=%s", url, resp.status_code)
    return url
