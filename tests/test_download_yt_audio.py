from __future__ import annotations

from pathlib import Path

from services.youtube.transcriber.download_yt_audio import (
    DENO_MIN_VERSION,
    YT_EXTRACTOR_ARGS,
    parse_deno_version,
    resolve_deno_bin,
    yt_dlp_youtube_args,
)


def test_parse_deno_version() -> None:
    assert parse_deno_version("deno 2.4.3 (stable, release, x86_64-unknown-linux-gnu)\n") == (
        2,
        4,
        3,
    )
    assert parse_deno_version("not a version") is None
    assert parse_deno_version("deno 2.2.0") < DENO_MIN_VERSION


def test_resolve_deno_bin_env_override(monkeypatch, tmp_path: Path) -> None:
    fake = tmp_path / "deno"
    fake.write_text("#!/bin/sh\n", encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("YT_DENO_BIN", str(fake))
    monkeypatch.setattr(
        "services.youtube.transcriber.download_yt_audio.shutil.which",
        lambda _name: None,
    )
    assert resolve_deno_bin() == str(fake)


def test_yt_dlp_youtube_args_use_deno_and_extractor(monkeypatch, tmp_path: Path) -> None:
    deno = tmp_path / "deno"
    monkeypatch.setattr(
        "services.youtube.transcriber.download_yt_audio.resolve_deno_bin",
        lambda: str(deno),
    )
    monkeypatch.setattr(
        "services.youtube.transcriber.download_yt_audio.yt_dlp_proxy_args",
        lambda: ["--proxy", "http://proxy.example:8080"],
    )
    cookies = tmp_path / "cookies.txt"
    args = yt_dlp_youtube_args(cookies=cookies, user_agent="Mozilla/5.0")
    assert args[args.index("--js-runtimes") + 1] == f"deno:{deno}"
    assert args[args.index("--extractor-args") + 1] == YT_EXTRACTOR_ARGS
    assert "web_embedded" in YT_EXTRACTOR_ARGS
    assert "-tv_downgraded" in YT_EXTRACTOR_ARGS
    assert args[args.index("--add-headers") + 1] == "User-Agent:Mozilla/5.0"
    assert "--proxy" in args
