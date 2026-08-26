# Transcription (GPU runtime)

WMVI runs YouTube and podcast transcription on a dedicated GPU machine using a separate Python virtual environment (heavy ML deps: Whisper, PyTorch). Main app services use the default venv — see [Virtual Environments](../README.md#virtual-environments) in the root README.

## Quick start

1. [setup.md](setup.md) — create venv, system packages, `.env`
2. Verify:

   ```bash
   python transcription/transcription_checklist.py --group core
   python transcription/transcription_checklist.py --group youtube   # if using YouTube transcriber
   python transcription/transcription_checklist.py --group podcast   # if using podcast transcriber
   ```

3. Install systemd units: [services/readme.md](../services/readme.md)

## When you need a browser

Only **YouTube cookie refresh** requires a graphical session on gpu-pc:

1. [gpu-remote-desktop.md](gpu-remote-desktop.md) — connect via Windows Remote Desktop (xrdp)
2. [youtube.md](youtube.md) — export cookies and user-agent

Podcast transcription does **not** need Firefox, cookies, or xrdp.

## Documentation map

| Doc | Scope |
|-----|--------|
| [setup.md](setup.md) | Shared GPU env: venv, ffmpeg, Deno, checklist |
| [gpu-remote-desktop.md](gpu-remote-desktop.md) | xrdp install and connect |
| [youtube.md](youtube.md) | YouTube cookies, proxy, yt-dlp troubleshooting |
| [services/youtube/transcriber/README.md](../services/youtube/transcriber/README.md) | Run / install YouTube transcriber |
| [services/podcast/transcriber/README.md](../services/podcast/transcriber/README.md) | Run / install podcast transcriber |

## Checklist groups

`transcription_checklist.py` groups checks by platform:

| Group | Checks |
|-------|--------|
| `core` | `.env`, Python, ffmpeg, imports, CUDA, optional Whisper smoke |
| `db` | DEV/PROD Postgres, SSH tunnel |
| `youtube` | yt-dlp, Deno, cookie files, proxy probe, download smoke |
| `podcast` | Download one episode URL from DB |

```bash
python transcription/transcription_checklist.py --list
python transcription/transcription_checklist.py --group core
```

Heavy checks (Whisper smoke, yt-dlp download) are marked in `--list`; run them when validating a fresh setup.

## Dependencies

- [requirements-transcription.txt](requirements-transcription.txt) — pip packages for the transcription venv
