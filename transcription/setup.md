# Transcription environment setup (gpu-pc)

Initial setup for the shared transcription runtime used by YouTube and podcast transcribers. Run from the WMVI repo root on the GPU machine.

For YouTube cookie export (one-time / periodic), see [youtube.md](youtube.md) after completing this guide.

## 1. Python virtual environment

Use Python **3.11+**. Typical path: `venvs/transcription` under the repo root.

```bash
cd /path/to/wmvi
python3.11 -m venv venvs/transcription
source venvs/transcription/bin/activate
pip install -r transcription/requirements-transcription.txt
```

The service installer expects `runtime = transcription` in `service.toml` to point at this interpreter — see [services/readme.md](../services/readme.md#python-runtimes-venvs).

## 2. System packages

```bash
sudo apt install ffmpeg
sudo apt install firefox-esr   # only required for YouTube cookie export via xrdp
```

**Deno ≥ 2.3.0** (required for yt-dlp YouTube JS challenges; Node 20 is not supported):

```bash
curl -fsSL https://deno.land/install.sh | sh
export PATH="$HOME/.deno/bin:$PATH"
deno --version   # first line like: deno 2.3.0 ...
sudo ln -sf "$HOME/.deno/bin/deno" /usr/local/bin/deno   # systemd PATH does not include ~/.deno
```

Optional: set `YT_DENO_BIN=/path/to/deno` in `.env` instead of the symlink.

Optional: [gpu-remote-desktop.md](gpu-remote-desktop.md) for xrdp + XFCE if the GPU box is headless.

## 3. WMVI `.env`

Copy/configure `.env` at repo root. On gpu-pc, typical entries:

- `SERVICE_ENV=dev` (or `prod` if this box writes to prod — uncommon)
- `DEV_PGHOST`, `DEV_PGPORT`, `DEV_PGDATABASE`, `DEV_PGUSER`, `DEV_PGPASSWORD`
- `YT_PROXY_URL` — optional Proxidize HTTP proxy for YouTube yt-dlp only ([youtube.md](youtube.md))
- `YT_DENO_BIN` — optional path to Deno if systemd cannot see `~/.deno/bin/deno`

Use SSH tunnel vars if Postgres is reached via tunnel (`USE_SSH_TUNNEL`, `SSH_HOST`, etc.) — checklist `db` group validates these.

## 4. Verify

With the transcription venv active and `.env` in place:

```bash
python transcription/transcription_checklist.py --list
python transcription/transcription_checklist.py --group core
python transcription/transcription_checklist.py --group db
```

Platform-specific (after platform secrets exist):

```bash
python transcription/transcription_checklist.py --group youtube   # needs private/youtube-*.txt
python transcription/transcription_checklist.py --group podcast   # needs DB episode with download_url
```

## 5. Install transcription services

From repo root:

```bash
sudo -E python -m services install youtube/transcriber
sudo -E python -m services install podcast/transcriber
```

See [services/readme.md](../services/readme.md) for install scope, `--user`, and upgrades.

## 6. YouTube-only: cookies and proxy

Not part of initial pip/apt setup. When the YouTube transcriber reports auth failures or after provisioning a new gpu-pc:

1. [gpu-remote-desktop.md](gpu-remote-desktop.md)
2. [youtube.md](youtube.md)
