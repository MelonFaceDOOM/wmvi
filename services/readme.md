# Services

This project contains multiple “services” (scrapers, cleaners, processors, etc.) under `services/<service_name>/`.
Each service is runnable as a Python module and can be managed via systemd.

## Layout

- `services/<service_name>/` — individual service package
  - `service.toml` — service metadata (type/runtime/timer/etc.)
  - `__main__.py` — entrypoint for `python -m services.<service_name> ...`
- `services/cli/` — CLI + shared install logic
  - `lib/` — shared utilities (config parsing, rendering, systemd helpers, discovery, etc.)
  - `systemd/` — systemd unit templates used by the installer
- `services/__main__.py` — CLI entrypoint (`python -m services ...`)

## Service definition

Each service directory must contain:

- `service.toml` — declares how the service runs:
  - `service.type`: `longrunning` (daemon) or `oneshot` (run-to-completion)
  - `service.runtime`: which Python runtime/venv to use (must exist in `RUNTIMES`)
  - optional `[timer]`: for periodic oneshot jobs (maps to a `.timer` unit)

Each service must also have:

- `__main__.py` — entrypoint for `python -m services.<service_name> ...`

## systemd templates

Shared unit templates live in:

- `services/cli/systemd/oneshot.service.in`
- `services/cli/systemd/longrunning.service.in`
- `services/cli/systemd/timer.in`

The installer renders these templates and writes the resulting units to systemd.

## Python runtimes (venvs)

Available runtimes are defined by `RUNTIMES` in the installer.
Each service selects one via `service.toml` (`service.runtime`).

`RUNTIMES` entries are typically paths to Python interpreters (relative to the repo root),
and may be overridden via environment variables (set in .env)

## Environment (dev/prod)

The service installer may add an extra argument when starting services based on `SERVICE_ENV`
(e.g. `--prod` for production). `SERVICE_ENV` is loaded from the project `.env`.

- `SERVICE_ENV=dev` → no extra args
- `SERVICE_ENV=prod` → adds `--prod`

## Installing and uninstalling

Run commands from the project root.

### System-wide units (root)

System scope writes to `/etc/systemd/system` and requires root:

- install: writes `.service` (and `.timer` if configured), runs `systemctl daemon-reload`, then enables/starts
- uninstall: disables/stops timer first (if present), then service, deletes unit files, reloads systemd

### User units (`--user`)

User scope writes to `~/.config/systemd/user` and uses `systemctl --user`.

## CLI usage

```bash
# Install/uninstall (system scope)
sudo -E python -m services install label_en
sudo -E python -m services uninstall label_en
might need to install python-dotenv globally

# Install/uninstall (user scope)
python -m services install --user label_en
python -m services uninstall --user label_en

# Inspect
python -m services list-available
python -m services list-installed
python -m services list-installed --user

# Bulk stop/start (installed units only, one scope per invocation)
sudo -E python -m services stop-all
sudo -E python -m services start-all
python -m services stop-all --user
python -m services start-all --user
```

`stop-all` and `start-all` only affect services that already have unit files in the chosen scope (`/etc/systemd/system` by default, or `~/.config/systemd/user` with `--user`). They do not install new services. System scope requires root (`sudo -E`); user scope uses `--user` on both commands.

If you use **both** system and user units, run stop/start separately for each scope (`list-installed` vs `list-installed --user` to see what is installed where).

### Upgrading (code-only)

When unit files on disk are unchanged (no `install`, no template or `service.toml` changes):

```bash
cd /path/to/wmvi
sudo -E python -m services stop-all
git pull
# venv / dependency updates as needed
sudo -E python -m scripts.migrate_db --prod   # when schema changed
sudo -E python -m services start-all          # daemon-reload + enable/start
```

User scope: add `--user` to the `python -m services` lines and omit `sudo`.

`stop-all` uses `disable --now` so timers and longrunning services stay down during `git pull`. `start-all` runs `daemon-reload` then `enable --now`, which starts new processes that load the pulled code.

Re-run `python -m services install <name>` when unit definitions must change (templates, `service.toml`, `SERVICE_ENV`, venv paths). Installing a new service from the repo still requires `install` once.

## Storage and podcast transcript sync

Shared connectors live under `storage/` (backends, SSH tunnel, podcast export/import I/O). `services/storage.py` re-exports blob/local backends for older imports.

### SSH (DB tunnel + SFTP export)

Used when the database or SFTP target is reachable only via SSH (e.g. GPU box → nitwitch Postgres).

```bash
USE_SSH_TUNNEL=1              # or PROD_USE_SSH_TUNNEL=1
SSH_HOST=nitwitch.example
SSH_USERNAME=melon
SSH_PKEY=/home/melon/.ssh/wmvi_nitwitch_ed25519
SSH_PORT=22                   # optional
# Optional: OpenSSH connect timeout (seconds) and max wait for local forward to listen.
# SSH_CONNECT_TIMEOUT=30
# SSH_TUNNEL_READY_TIMEOUT=60
# SSH_BIN=C:/Windows/System32/OpenSSH/ssh.exe
```

`init_pool()` in `db/db.py` runs OpenSSH `ssh -L` to `{PREFIX}_PGHOST:{PREFIX}_PGPORT` and connects via `127.0.0.1:<local port>`. Set `{PREFIX}_PGSSLMODE=disable` if local Postgres has no SSL.

Generate a key (not stored in the repo): `ssh-keygen -t ed25519 -f ~/.ssh/wmvi_nitwitch_ed25519`, then install the public key in `authorized_keys` on the SSH host.

### Podcast export (GPU / local DB → nitwitch files)

```bash
PODCAST_EXPORT_STORAGE_KIND=sftp   # skip | local | azure | sftp
PODCAST_SYNC_LOCAL_DIR=./data/podcast_sync_exports
```

Export writes a schema **v4** bundle under `podcast_transcripts/{bundle_id}/` (UTC stamp like `2026-05-22T14-30-45Z`): `podcast_episodes_{bundle}.jsonl` (show `rss_url`, episode identity fields, transcript) and `podcast_shows_{bundle}.jsonl` (only shows referenced by those episodes). Matching on import is by canonical `rss_url` + `compute_episode_id` (not source DB ids). Upload path: `/mnt/md0/nitwitch_dl/transcription_exports/` (mirrored at https://nitwitch.com/dl/transcription_exports/podcast_transcripts/).

```bash
USE_SSH_TUNNEL=1 python -m services.podcast.transcript_export --prod
```

### Podcast import (nitwitch HTTP → Azure prod DB)

```bash
PODCAST_IMPORT_STORAGE_KIND=nitwitch   # local | azure | nitwitch
# PROD_* → Azure Postgres (direct; no tunnel)
```

```bash
python -m services.podcast.transcript_import
```

Import order per bundle: upsert shows (`rss_url`) → insert episodes (computed ids) → apply transcripts (requires guid or `download_url`). Applies bundles with `until_ts` after `sm.podcast_transcript_import_state.last_imported_at` (migration `022`). `--force` re-imports all bundles; `--bundle` for one run. v3 bundles are rejected.

```bash
python -m scripts.migrate_db --test
python -m scripts.migrate_db --prod
```

Nitwitch URLs are hardcoded in `storage/nitwitch_paths.py` and `services/podcast/transcript_import/nitwitch_dl.py` (not `.env`).

### Azure blob (optional)

```bash
AZURE_STORAGE_ACCOUNT=...
AZURE_STORAGE_KEY=...
AZURE_STORAGE_CONTAINER=...
# Virtual folder inside the container (azure export/import only)
PODCAST_SYNC_AZURE_BLOB_PREFIX=podcast_transcripts/
```


