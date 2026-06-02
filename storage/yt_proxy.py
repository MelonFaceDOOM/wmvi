from __future__ import annotations

import os


def yt_dlp_proxy_args() -> list[str]:
    """Return yt-dlp --proxy args when YT_PROXY_URL is set (e.g. Proxidize static proxy)."""
    url = os.environ.get("YT_PROXY_URL", "").strip()
    if not url:
        return []
    return ["--proxy", url]
