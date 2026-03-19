import subprocess
from pathlib import Path
import os
import shutil
import sys

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

YT_DLP_BIN = resolve_yt_dlp_bin()

class DownloadFailed(Exception):
    pass


def download_yt_audio(url: str, audio_path: str) -> None:
    """
    Download audio-only from a YouTube video into audio_path.

    Produces a file suitable for faster-whisper.
    Raises DownloadFailed on failure.
    """
    audio_path = Path(audio_path)

    # yt-dlp wants a template *without* extension
    outtmpl = str(audio_path.with_suffix(""))

    cmd = [
        YT_DLP_BIN,
        "--no-playlist",
        "--js-runtimes", "node",
        "--cookies-from-browser", "firefox",
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
        raise DownloadFailed(
            f"yt-dlp failed for {url}\n{e.stderr}"
        ) from e

    # yt-dlp determines final extension; find it
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

    # Normalize to requested output path
    produced.replace(audio_path)
