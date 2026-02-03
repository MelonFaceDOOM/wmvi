# Services

This project contains multiple “services” (scrapers, cleaners, processors, etc.) under `services/<service_name>/`.
Each service is runnable as a Python module and can be managed via systemd.

## Service definition

Each service directory must contain:

- `service.toml` — declares how the service runs:
  - `service.type`: `longrunning` (daemon) or `oneshot` (run-to-completion)
  - `service.runtime`: which Python runtime/venv to use (must exist in `RUNTIMES` in `services/install.py`)
  - optional `[timer]`: for periodic oneshot jobs (maps to a `.timer` unit)

Each service must also have:

- `__main__.py` — entrypoint for `python -m services.<service_name> ...`

## systemd templates

Shared unit templates live in:

- `services/systemd/oneshot.service.in`
- `services/systemd/longrunning.service.in`
- `services/systemd/timer.in`

The installer renders these templates and writes the resulting units to systemd.

## Python runtimes (venvs)

Available runtimes are defined in `services/install.py` as `RUNTIMES`.
A service selects one via `service.toml` (`service.runtime`).

## Installing and uninstalling

Run commands from the project root.

System-wide units (writes to `/etc/systemd/system`) require root:
- install: writes `.service` (and `.timer` if configured), runs `systemctl daemon-reload`, then enables/starts.
- uninstall: disables/stops timer first (if present), then service, deletes unit files, reloads systemd.

User units are supported with `--user` (writes to `~/.config/systemd/user`).

## CLI usage

```bash
# Install/uninstall (system scope)
sudo -E python -m services install label_en
sudo -E python -m services uninstall label_en

# Inspect
python -m services list-available
python -m services list-installed

