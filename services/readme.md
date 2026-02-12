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



