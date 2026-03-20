import subprocess
from pathlib import Path
import os
import shutil
import sys
from dotenv import load_dotenv
load_dotenv()

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

YT_DLP_BIN = resolve_yt_dlp_bin()
YT_COOKIES_PATH = get_youtube_cookies_path()
FIREFOX_UA = load_firefox_user_agent()

class DownloadFailed(Exception):
    pass


def download_yt_audio(url: str, audio_path: str) -> None:
    audio_path = Path(audio_path)
    outtmpl = str(audio_path.with_suffix(""))

    cmd = [
        YT_DLP_BIN,
        "--no-playlist",
        "--js-runtimes", "node",
        "--cookies", str(YT_COOKIES_PATH),
        "--add-headers", f"User-Agent:{FIREFOX_UA}",
        "-f", "bestaudio/best",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "--force-overwrites",
        "--output", outtmpl + ".%(ext)s",
        url,
    ]

    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise DownloadFailed(f"yt-dlp failed for {url}\n{e.stderr}") from e

    produced = None
    for ext in ("mp3", "m4a", "opus", "webm"):
        candidate = audio_path.with_suffix("." + ext)
        if candidate.exists():
            produced = candidate
            break

    if not produced:
        raise DownloadFailed(
            f"yt-dlp reported success but no audio file was produced for {url}"
        )

    produced.replace(audio_path)