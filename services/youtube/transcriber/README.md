# YouTube transcriber service

Transcribes videos in `youtube.video` (download via yt-dlp, Whisper, save segments). Runs on the **transcription** venv.

## Run manually

From repo root:

```bash
# DEV database (default)
python -m services.youtube.transcriber

# PROD database
python -m services.youtube.transcriber --prod

# One-shot: up to N videos, no session scheduler
python -m services.youtube.transcriber --limit 5
```

## Install (systemd)

```bash
sudo -E python -m services install youtube/transcriber
```

See [services/readme.md](../../readme.md).

## Setup and troubleshooting

Cookies, proxy, Deno, xrdp, and failure triage: **[transcription/youtube.md](../../../transcription/youtube.md)**

GPU env and checklist: [transcription/README.md](../../../transcription/README.md)
