import subprocess
from pathlib import Path


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
        "yt-dlp",
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
