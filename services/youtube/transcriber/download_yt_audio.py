import os
import pwd
import re
import shutil
import subprocess
import sys
import logging
from pathlib import Path

from dotenv import load_dotenv

from storage.yt_proxy import yt_dlp_proxy_args
from .yt_download_errors import DownloadFailed, DownloadFailureInfo, classify_yt_dlp_stderr

load_dotenv()

log = logging.getLogger(__name__)

# Deno is the yt-dlp EJS default; Node must be >= 22 (v20 is reported unsupported).
DENO_MIN_VERSION = (2, 3, 0)
# tv_downgraded is in YouTube's default client set and fails logged-in cookies
# with "The page needs to be reloaded". web_embedded is an extra client.
YT_EXTRACTOR_ARGS = "youtube:player_client=default,web_embedded,-tv_downgraded"


def resolve_yt_dlp_bin() -> str:
    env_bin = os.environ.get("YT_DLP_BIN")
    if env_bin:
        return env_bin

    venv_bin = Path(sys.prefix) / "bin" / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)

    exe_bin = Path(sys.executable).parent / "yt-dlp"
    if exe_bin.exists():
        return str(exe_bin)

    path_bin = shutil.which("yt-dlp")
    if path_bin:
        return path_bin

    raise RuntimeError("yt-dlp not found; set YT_DLP_BIN or install it in the active venv")


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def get_youtube_cookies_path() -> Path:
    path = get_project_root() / "private" / "youtube-cookies.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_youtube_user_agent_path() -> Path:
    path = get_project_root() / "private" / "youtube-agent.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_firefox_user_agent() -> str:
    path = get_youtube_user_agent_path()
    if not path.exists():
        raise RuntimeError(f"Missing YouTube user-agent file: {path}")
    return path.read_text(encoding="utf-8").strip()


def parse_deno_version(version_stdout: str) -> tuple[int, int, int] | None:
    m = re.search(r"deno\s+(\d+)\.(\d+)\.(\d+)", version_stdout, re.I)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def resolve_deno_bin() -> str:
    """Locate deno for yt-dlp --js-runtimes (PATH, ~/.deno, repo owner, YT_DENO_BIN)."""
    candidates: list[Path] = []
    override = os.environ.get("YT_DENO_BIN", "").strip()
    if override:
        candidates.append(Path(override))
    which = shutil.which("deno")
    if which:
        candidates.append(Path(which))
    candidates.append(Path.home() / ".deno" / "bin" / "deno")
    try:
        owner = pwd.getpwuid(get_project_root().stat().st_uid)
        candidates.append(Path(owner.pw_dir) / ".deno" / "bin" / "deno")
    except Exception:
        pass

    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.is_file() and os.access(path, os.X_OK):
            return str(path)

    raise RuntimeError(
        "deno not found (need >= 2.3.0). Install from https://deno.land, "
        "symlink to /usr/local/bin/deno for systemd, or set YT_DENO_BIN in .env"
    )


def js_runtime_arg() -> str:
    return f"deno:{resolve_deno_bin()}"


def yt_dlp_youtube_args(*, cookies: Path, user_agent: str) -> list[str]:
    """Shared yt-dlp flags for YouTube (cookies, UA, Deno EJS, player clients, proxy)."""
    return [
        "--no-playlist",
        "--js-runtimes",
        js_runtime_arg(),
        "--cookies",
        str(cookies),
        "--add-headers",
        f"User-Agent:{user_agent}",
        "--extractor-args",
        YT_EXTRACTOR_ARGS,
        *yt_dlp_proxy_args(),
    ]


YT_COOKIES_PATH = get_youtube_cookies_path()


def _log_download_failure(url: str, info: DownloadFailureInfo) -> None:
    if info.category == "proxy":
        log.error("yt-dlp proxy failure for %s: %s", url, info.summary)
    elif info.category == "auth":
        log.error("yt-dlp auth failure for %s: %s", url, info.summary)
    elif info.category == "permanent":
        log.warning("yt-dlp permanent failure for %s: %s", url, info.summary)
    else:
        log.warning("yt-dlp download failed for %s: %s", url, info.summary)
    log.debug("yt-dlp stderr for %s:\n%s", url, info.detail)


def download_yt_audio(url: str, audio_path: str) -> None:
    audio_path = Path(audio_path)
    outtmpl = str(audio_path.with_suffix(""))

    cookies_exists = YT_COOKIES_PATH.exists()
    cookies_size = YT_COOKIES_PATH.stat().st_size if cookies_exists else 0
    yt_dlp_bin = resolve_yt_dlp_bin()
    deno_bin = resolve_deno_bin()
    ua = load_firefox_user_agent()

    log.debug(
        "yt-dlp setup: bin=%s deno=%s cookies=%s cookies_exists=%s cookies_size=%s ua_len=%s url=%s",
        yt_dlp_bin,
        deno_bin,
        YT_COOKIES_PATH,
        cookies_exists,
        cookies_size,
        len(ua),
        url,
    )

    cmd = [
        yt_dlp_bin,
        *yt_dlp_youtube_args(cookies=YT_COOKIES_PATH, user_agent=ua),
        "-f", "bestaudio/best",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "--force-overwrites",
        "--output", outtmpl + ".%(ext)s",
        url,
    ]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.stderr:
            log.debug("yt-dlp stderr (success) for %s:\n%s", url, result.stderr)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        info = classify_yt_dlp_stderr(stderr)
        _log_download_failure(url, info)
        raise DownloadFailed(info) from e

    produced = None
    for ext in ("mp3", "m4a", "opus", "webm"):
        candidate = audio_path.with_suffix("." + ext)
        if candidate.exists():
            produced = candidate
            break

    if not produced:
        info = DownloadFailureInfo(
            summary="yt-dlp reported success but no audio file was produced",
            category="retryable",
        )
        _log_download_failure(url, info)
        raise DownloadFailed(info)

    produced.replace(audio_path)
