from __future__ import annotations

import re
from dataclasses import dataclass

_PERMANENT_PATTERNS = (
    re.compile(r"video unavailable", re.I),
    re.compile(r"account associated with this video has been terminated", re.I),
    re.compile(r"private video", re.I),
    re.compile(r"this video is not available", re.I),
    re.compile(r"this video has been removed", re.I),
    re.compile(r"this video is no longer available", re.I),
    re.compile(r"video has been removed", re.I),
    re.compile(r"copyright", re.I),
)

_AUTH_PATTERNS = (
    re.compile(r"sign in to confirm", re.I),
    re.compile(r"not a bot", re.I),
    re.compile(r"--cookies-from-browser", re.I),
    re.compile(r"http error 403", re.I),
)

_PROXY_PATTERNS = (
    re.compile(r"proxy authentication required", re.I),
    re.compile(r"unable to connect to proxy", re.I),
    re.compile(r"tunnel connection failed", re.I),
    re.compile(r"\b407\b"),
)


@dataclass(frozen=True)
class DownloadFailureInfo:
    summary: str
    category: str  # permanent | auth | proxy | retryable
    detail: str = ""


class DownloadFailed(Exception):
    def __init__(self, info: DownloadFailureInfo | str, *, category: str = "retryable"):
        if isinstance(info, str):
            info = DownloadFailureInfo(summary=info, category=category, detail=info)
        self.info = info
        super().__init__(info.summary)


def _matches_any(patterns: tuple[re.Pattern[str], ...], text: str) -> bool:
    return any(p.search(text) for p in patterns)


def classify_yt_dlp_stderr(stderr: str) -> DownloadFailureInfo:
    summary = _extract_error_summary(stderr)
    haystack = f"{summary}\n{stderr}"

    if _matches_any(_PROXY_PATTERNS, haystack):
        category = "proxy"
    elif _matches_any(_AUTH_PATTERNS, haystack):
        category = "auth"
    elif _matches_any(_PERMANENT_PATTERNS, haystack):
        category = "permanent"
    else:
        category = "retryable"

    return DownloadFailureInfo(summary=summary, category=category, detail=stderr)


def _extract_error_summary(stderr: str) -> str:
    for line in reversed(stderr.splitlines()):
        stripped = line.strip()
        if stripped.startswith("ERROR:"):
            return stripped.removeprefix("ERROR:").strip()

    for line in reversed(stderr.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("[debug]"):
            return stripped

    return "yt-dlp failed (no error details)"
