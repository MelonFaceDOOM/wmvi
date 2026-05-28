from __future__ import annotations

from urllib.parse import urlparse, urlunparse


def normalize_rss_url(url: str | None) -> str | None:
    """
    Canonical form for cross-DB show matching (scheme/host lowercased, no trailing path slash).
    Returns None for missing/blank input.
    """
    if url is None:
        return None
    s = url.strip()
    if not s:
        return None

    parsed = urlparse(s)
    if not parsed.scheme or not parsed.netloc:
        return s

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or ""
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunparse(
        (
            scheme,
            netloc,
            path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )
