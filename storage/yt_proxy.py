from __future__ import annotations

import os
import re
from urllib.parse import quote


def normalize_proxy_url(raw: str) -> str:
    """
    Normalize YT_PROXY_URL for yt-dlp / requests.

    Accepts:
      - http(s)://user:pass@host:port  (unchanged)
      - host:port:user:pass            (Proxidize dashboard line)
    """
    url = raw.strip()
    if not url:
        return ""
    if re.match(r"^https?://", url, re.I):
        return url
    parts = url.split(":", 3)
    if len(parts) != 4:
        raise ValueError(
            "YT_PROXY_URL must be http(s)://user:pass@host:port "
            f"or host:port:user:pass (Proxidize); got: {raw!r}"
        )
    host, port, user, password = parts
    if not host or not port or not user:
        raise ValueError(f"invalid proxy URL components in {raw!r}")
    return f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"


def yt_dlp_proxy_args() -> list[str]:
    """Return yt-dlp --proxy args when YT_PROXY_URL is set (e.g. Proxidize static proxy)."""
    raw = os.environ.get("YT_PROXY_URL", "").strip()
    if not raw:
        return []
    return ["--proxy", normalize_proxy_url(raw)]
