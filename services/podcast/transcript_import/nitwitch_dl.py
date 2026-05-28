from __future__ import annotations

import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import requests

from services.podcast.transcript_sync.format import parse_bundle_id, read_manifest
from storage.nitwitch_paths import NITWITCH_DL_BASE_URL, PODCAST_TRANSCRIPTS_SUBDIR

log = logging.getLogger(__name__)

MANIFEST_NAME = "manifest.json"
_HTTP_TIMEOUT_S = 60

# Timestamped run folders (schema v3) and legacy YYYY-MM-DD folders.
_BUNDLE_HREF_RE = re.compile(
    r'href=["\']((?:\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z|\d{4}-\d{2}-\d{2}))/?["\']',
    re.I,
)


def _normalize_base(url: str) -> str:
    return url if url.endswith("/") else url + "/"


def podcast_transcripts_base_url(*, base_url: str = NITWITCH_DL_BASE_URL) -> str:
    return urljoin(_normalize_base(base_url), f"{PODCAST_TRANSCRIPTS_SUBDIR}/")


def parse_bundle_ids_from_index(html: str) -> list[str]:
    """Parse Apache-style directory index for export bundle folder links."""
    return sorted(set(_BUNDLE_HREF_RE.findall(html)))


def list_export_bundle_ids(*, base_url: str = NITWITCH_DL_BASE_URL) -> list[str]:
    url = podcast_transcripts_base_url(base_url=base_url)
    resp = requests.get(url, timeout=_HTTP_TIMEOUT_S)
    resp.raise_for_status()
    bundle_ids = parse_bundle_ids_from_index(resp.text)
    log.info("nitwitch index: found %d bundle(s) at %s", len(bundle_ids), url)
    return bundle_ids


def find_latest_export_bundle_id(*, base_url: str = NITWITCH_DL_BASE_URL) -> str | None:
    ids = list_export_bundle_ids(base_url=base_url)
    return ids[-1] if ids else None


def _download_file(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=_HTTP_TIMEOUT_S)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    log.info("nitwitch: downloaded %s -> %s (%d bytes)", url, dest, len(resp.content))


def download_bundle(bundle_id: str, dest_dir: Path, *, base_url: str = NITWITCH_DL_BASE_URL) -> None:
    """
    Fetch manifest.json and payload files for bundle_id into dest_dir via HTTP.
    """
    bundle_base = urljoin(podcast_transcripts_base_url(base_url=base_url), f"{bundle_id}/")
    dest_dir.mkdir(parents=True, exist_ok=True)

    manifest_url = urljoin(bundle_base, MANIFEST_NAME)
    manifest_path = dest_dir / MANIFEST_NAME
    _download_file(manifest_url, manifest_path)

    manifest = read_manifest(manifest_path)
    _download_file(urljoin(bundle_base, manifest.payload), dest_dir / manifest.payload)
    if manifest.shows_payload:
        _download_file(
            urljoin(bundle_base, manifest.shows_payload),
            dest_dir / manifest.shows_payload,
        )

    log.info("nitwitch: bundle %s ready in %s", bundle_id, dest_dir)


def bundle_sort_key(bundle_id: str) -> tuple:
    """Order bundles by parsed time, then id string."""
    return (parse_bundle_id(bundle_id), bundle_id)
