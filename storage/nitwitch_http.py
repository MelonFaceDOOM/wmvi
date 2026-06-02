from __future__ import annotations

import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import requests

from storage.nitwitch_paths import NITWITCH_DL_BASE_URL

log = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"
_HTTP_TIMEOUT_S = 60

_BUNDLE_HREF_RE = re.compile(
    r'href=["\']((?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z|\d{4}-\d{2}-\d{2}))/?["\']',
    re.I,
)


def _normalize_base(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def subdir_base_url(subdir: str, *, base_url: str = NITWITCH_DL_BASE_URL) -> str:
    return urljoin(_normalize_base(base_url), f"{subdir}/")


def parse_bundle_ids_from_index(html: str) -> list[str]:
    return sorted(set(_BUNDLE_HREF_RE.findall(html)))


def list_bundle_ids(
    subdir: str,
    *,
    base_url: str = NITWITCH_DL_BASE_URL,
) -> list[str]:
    url = subdir_base_url(subdir, base_url=base_url)
    resp = requests.get(url, timeout=_HTTP_TIMEOUT_S)
    resp.raise_for_status()
    bundle_ids = parse_bundle_ids_from_index(resp.text)
    log.info("nitwitch index: found %d bundle(s) at %s", len(bundle_ids), url)
    return bundle_ids


def _download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=_HTTP_TIMEOUT_S)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    log.info("nitwitch: downloaded %s -> %s (%d bytes)", url, dest, len(resp.content))


def download_bundle_files(
    subdir: str,
    bundle_id: str,
    dest_dir: Path,
    filenames: list[str],
    *,
    base_url: str = NITWITCH_DL_BASE_URL,
) -> None:
    """Download manifest and listed files into dest_dir."""
    bundle_base = urljoin(subdir_base_url(subdir, base_url=base_url), f"{bundle_id}/")
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in filenames:
        if not name:
            continue
        _download_file(urljoin(bundle_base, name), dest_dir / name)
    log.info("nitwitch: bundle %s ready in %s", bundle_id, dest_dir)
