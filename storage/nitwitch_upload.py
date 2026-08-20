"""Upload files to nitwitch via WebDAV PUT + Basic Auth.

Env (repo ``.env``)::

    NITWITCH_UPLOAD_URL=https://nitwitch.com/u/<secret>/
    # or e.g. https://nitwitch.com/upload/
    NITWITCH_UPLOAD_USER=...
    NITWITCH_UPLOAD_PASSWORD=...

Optional TLS (corp MITM / custom CA) — mirrors curl ``--cacert``::

    NITWITCH_UPLOAD_CACERT=cert.pem

Relative paths resolve against the repo root. Absolute paths are used as-is.
If unset and ``cert.pem`` exists at the repo root, that file is used automatically.

Browse uploaded files at ``NITWITCH_UPLOADS_DL_BASE_URL`` (public listing).

Equivalent curl (Windows corp network)::

    curl --ssl-no-revoke --cacert cert.pem -u "user:pass" -T local.json \\
      "https://nitwitch.com/upload/remote.json"

``--ssl-no-revoke`` is a curl/Schannel flag; ``requests`` does not perform CRL
checks the same way. The important piece for Python is ``verify=<cacert>``.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import urljoin

import requests

log = logging.getLogger(__name__)

_HTTP_TIMEOUT_S = 300
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACERT_NAME = "cert.pem"
# HuggingFace Hub / sentence-transformers use these instead of verify=.
_PYTHON_SSL_CA_ENV_VARS = ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE")


def _normalize_base(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def resolve_cacert_path(raw: str | None = None) -> Path | None:
    """Resolve optional CA bundle path for ``requests`` ``verify=``.

    Order:
      1. Explicit ``raw`` (or ``NITWITCH_UPLOAD_CACERT`` if ``raw`` is None)
      2. Else ``cert.pem`` at repo root if that file exists
      3. Else ``None`` (system/default trust store)
    """
    if raw is None:
        raw = (os.environ.get("NITWITCH_UPLOAD_CACERT") or "").strip() or None

    if raw:
        path = Path(raw)
        if not path.is_absolute():
            path = REPO_ROOT / path
        path = path.resolve()
        if not path.is_file():
            raise FileNotFoundError(
                f"NITWITCH_UPLOAD_CACERT not found: {path} "
                f"(set a valid path or remove the env var)"
            )
        return path

    default = REPO_ROOT / DEFAULT_CACERT_NAME
    if default.is_file():
        return default.resolve()
    return None


def apply_python_ssl_cacert_env() -> Path | None:
    """Point requests/OpenSSL at the same corp CA used for nitwitch uploads.

    HuggingFace Hub does not read ``NITWITCH_UPLOAD_CACERT``; it uses
    ``REQUESTS_CA_BUNDLE`` / ``SSL_CERT_FILE``. Already-set vars are left alone.
    """
    path = resolve_cacert_path()
    if path is None:
        return None
    value = str(path)
    for key in _PYTHON_SSL_CA_ENV_VARS:
        if not (os.environ.get(key) or "").strip():
            os.environ[key] = value
    log.info("python SSL CA bundle: %s", value)
    return path


def load_upload_config() -> tuple[str, str, str, Path | bool]:
    """Return ``(base_url, user, password, verify)``.

    ``verify`` is a CA bundle ``Path`` or ``True`` (default trust store).
    Raises ``ValueError`` if required auth env vars are incomplete.
    """
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
    cacert = resolve_cacert_path()
    verify: Path | bool = cacert if cacert is not None else True
    return _normalize_base(base), user, password, verify


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

    base, user, password, verify = load_upload_config()
    url = urljoin(base, name)
    data = path.read_bytes()
    verify_arg: str | bool = str(verify) if isinstance(verify, Path) else verify
    log.info(
        "nitwitch upload: PUT %s (%d bytes) verify=%s",
        url,
        len(data),
        verify_arg if verify_arg is not True else "default",
    )
    resp = requests.put(
        url,
        data=data,
        auth=(user, password),
        timeout=timeout_s,
        headers={"Content-Type": "application/octet-stream"},
        verify=verify_arg,
    )
    resp.raise_for_status()
    log.info("nitwitch upload: ok %s status=%s", url, resp.status_code)
    return url
