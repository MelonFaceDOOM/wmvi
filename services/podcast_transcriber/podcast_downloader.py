from __future__ import annotations

import re
import urllib.parse
from typing import Iterable, Tuple

import requests

from dotenv import load_dotenv
from db.db import init_pool, close_pool, getcursor

load_dotenv()

# ----------------------------
# Config
# ----------------------------

TIMEOUT = 20
MIN_BYTES = 512 * 1024  # 512 KB minimum to count as "real"

AUDIO_PATH_RE = re.compile(
    r"(?:https?://)?([a-z0-9.-]+\.[a-z]{2,}/[^\s\"']+\.(?:mp3|m4a|wav|aac))",
    re.I,
)
"""
This matches:
https://audioboom.com/posts/8525412.mp3
audioboom.com/posts/8525412.mp3
"""

HEADERS_LADDER = [
    {},
    {"User-Agent": "Mozilla/5.0"},
    {"User-Agent": "Mozilla/5.0", "Accept": "audio/*"},
    {
        "User-Agent": "Mozilla/5.0",
        "Accept": "audio/*",
        "Range": "bytes=0-",
    },
]

# ---------------------------
# API
# ---------------------------


class DownloadFailed(Exception):
    def __init__(self, raw_url: str, attempts: list[tuple[str, str]]):
        self.raw_url = raw_url
        self.attempts = attempts
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        lines = [f"Download failed for {self.raw_url}"]
        for url, reason in self.attempts:
            lines.append(f"  - {url}: {reason}")
        return "\n".join(lines)


def download_episode(raw_url: str, output_path: str) -> None:
    """
    Resolve tracking URLs and download audio to output_path.

    Raises DownloadFailed on failure.
    """
    attempts: list[tuple[str, str]] = []

    for candidate in normalize_url(raw_url):
        ok, size, reason = try_download(candidate, output_path)
        if ok:
            print("url used:", candidate)
            return
        attempts.append((candidate, reason))

    raise DownloadFailed(raw_url, attempts)

# ----------------------------
# Helpers
# ----------------------------


def _hr_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"

# ----------------------------
# Normalization
# ----------------------------


def extract_audio_fragments(raw_url: str) -> list[str]:
    decoded = urllib.parse.unquote(raw_url)

    # Split on slashes and rebuild progressively
    parts = decoded.split("/")
    candidates = []

    for i in range(len(parts)):
        tail = "/".join(parts[i:])
        m = re.match(
            r"([a-z0-9.-]+\.[a-z]{2,}/[^\s\"']+\.(?:mp3|m4a|wav|aac))",
            tail,
            re.I,
        )
        if m:
            candidates.append(m.group(1))

    return candidates


def promote_fragment(fragment: str) -> str:
    if fragment.startswith("http"):
        return fragment
    return "https://" + fragment


def prioritize_candidates(candidates: list[str]) -> list[str]:
    # choose deepest candidates first
    return list(dict.fromkeys(reversed(candidates)))


def find_candidate_urls(raw_url: str) -> list[str]:
    fragments = extract_audio_fragments(raw_url)

    candidates = []
    for frag in fragments:
        url = promote_fragment(frag)
        url = url.split("?", 1)[0]   # drop tracking params
        candidates.append(url)

    return candidates


def walk_redirects(url: str) -> list[str]:
    urls = []
    try:
        r = requests.get(
            url,
            allow_redirects=True,
            timeout=TIMEOUT,
            stream=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        for h in r.history:
            if "Location" in h.headers:
                urls.append(h.headers["Location"])
        urls.append(r.url)
    except Exception:
        pass
    return urls


def normalize_url(raw_url: str) -> list[str]:
    candidates: list[str] = []

    # 1) Extract embedded audio fragments
    candidates.extend(find_candidate_urls(raw_url))

    # 2) Walk redirects and repeat extraction
    for u in walk_redirects(raw_url):
        candidates.extend(find_candidate_urls(u))

    # 3) Fallback: original URL
    candidates.append(raw_url)

    # Deduplicate + deepest-first
    seen = set()
    out = []
    for u in prioritize_candidates(candidates):
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out

# ----------------------------
# Download attempt
# ----------------------------


def try_download(url: str, output_path: str) -> Tuple[bool, int, str]:
    """
    Attempts to download audio to output_path.

    Returns: (success, size_bytes, failure_reason)
    """
    for headers in HEADERS_LADDER:
        try:
            with requests.get(
                url,
                headers=headers,
                timeout=TIMEOUT,
                stream=True,
                allow_redirects=True,
            ) as r:
                if r.status_code >= 400:
                    continue

                ct = r.headers.get("Content-Type", "").lower()

                total = 0
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if not chunk:
                            continue
                        f.write(chunk)
                        total += len(chunk)

                if total < MIN_BYTES:
                    continue

                if ct and not ct.startswith("audio/"):
                    return False, total, f"non-audio content-type: {ct}"

                return True, total, ""

        except requests.RequestException as e:
            last_error = str(e)
            continue

    return False, 0, last_error if "last_error" in locals() else "download failed"


# ----------------------------
# DB fetch (for testing)
# ----------------------------

FETCH_SQL = """
    SELECT DISTINCT
           split_part(download_url, '/', 3) AS domain,
           download_url
    FROM podcasts.episodes
    WHERE download_url IS NOT NULL
    ORDER BY domain
"""


def fetch_one_url_per_domain() -> Iterable[Tuple[str, str]]:
    with getcursor() as cur:
        cur.execute(FETCH_SQL)
        seen = set()
        for domain, url in cur.fetchall():
            if domain not in seen:
                seen.add(domain)
                yield domain, url


# ----------------------------
# run a test on each domain
# results show success with some failures that can be investigated later
# ----------------------------

def try_dls() -> None:
    init_pool()
    try:
        for domain, raw_url in fetch_one_url_per_domain():
            print("DOMAIN:", domain)
            print("RAW_URL:", raw_url)
            normalized = normalize_url(raw_url)
            for candidate in normalized:
                ok, size, reason = try_download(candidate)
                print(ok, candidate)
                if ok:
                    print(f" ----- DOMAIN {domain} SUCCEEDED ------ ")
                    break
            else:
                print(f" ----- DOMAIN {domain} FAILED ------ ")
            print(50*"-")

    finally:
        close_pool()


if __name__ == "__main__":
    try_dls()
