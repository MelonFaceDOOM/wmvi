from __future__ import annotations

from services.youtube.transcriber.yt_download_errors import classify_yt_dlp_stderr


def test_classify_permanent_video_unavailable() -> None:
    stderr = (
        "ERROR: [youtube] iTEbKLEwQ9o: Video unavailable. "
        "This video is no longer available because the YouTube account "
        "associated with this video has been terminated."
    )
    info = classify_yt_dlp_stderr(stderr)
    assert info.category == "permanent"
    assert "Video unavailable" in info.summary


def test_classify_proxy_auth_failure() -> None:
    stderr = (
        "OSError: Tunnel connection failed: 407 Proxy Authentication Required\n"
        "yt_dlp.networking.exceptions.ProxyError: ('Unable to connect to proxy', ...)"
    )
    info = classify_yt_dlp_stderr(stderr)
    assert info.category == "proxy"
    assert "407" in info.summary or "proxy" in info.summary.lower()


def test_classify_auth_bot_check() -> None:
    stderr = "ERROR: Sign in to confirm you're not a bot"
    info = classify_yt_dlp_stderr(stderr)
    assert info.category == "auth"


def test_classify_retryable_unknown() -> None:
    stderr = "ERROR: [youtube] abc: Unable to download video data: HTTP Error 500"
    info = classify_yt_dlp_stderr(stderr)
    assert info.category == "retryable"
