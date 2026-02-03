from __future__ import annotations

import argparse
import sys
from pathlib import Path

from services.lib.discover import discover_services
from services.lib.config import load_toml, parse_service_config
from services.lib.systemd import (
    SystemdNotAvailable,
    InstalledStatus,
    unit_file_exists,
    is_active,
    is_enabled,
)

RUNTIMES = {
    "base": "venvs/base/bin/python",
    "transcription": "venvs/transcription/bin/python",
}


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    raise SystemExit(1)


def compute_installed_status(
    *,
    service_name: str,
    has_timer_declared: bool,
    user: bool,
) -> InstalledStatus:
    has_service_unit = unit_file_exists(service_name, "service", user=user)
    has_timer_unit = unit_file_exists(service_name, "timer", user=user)

    # If not installed, don't waste systemctl calls
    service_enabled = service_active = None
    timer_enabled = timer_active = None

    if has_service_unit:
        service_enabled = is_enabled(f"{service_name}.service", user=user)
        service_active = is_active(f"{service_name}.service", user=user)

    # Timer states are only meaningful if timer is present (and usually only for oneshot+timer services)
    if has_timer_declared or has_timer_unit:
        if has_timer_unit:
            timer_enabled = is_enabled(f"{service_name}.timer", user=user)
            timer_active = is_active(f"{service_name}.timer", user=user)
        else:
            timer_enabled = timer_active = None

    return InstalledStatus(
        name=service_name,
        has_service_unit=has_service_unit,
        has_timer_unit=has_timer_unit,
        service_enabled=service_enabled,
        service_active=service_active,
        timer_enabled=timer_enabled,
        timer_active=timer_active,
    )


def list_installed(project_root: Path, user: bool) -> int:
    try:
        discovered = discover_services(project_root)
    except Exception as e:
        die(str(e))

    if not discovered:
        print("No services with service.toml found")
        return 0

    # Confirm systemd exists early so the output isn't half-useful
    try:
        # any call that touches systemctl will raise if missing
        _ = is_active("default.target", user=user)
    except SystemdNotAvailable as e:
        die(str(e))
    except Exception:
        # default.target may be unknown in --user context; ignore
        pass

    installed_rows: list[tuple[str, object, object]] = []

    for svc in discovered:
        service_name = svc.name

        try:
            data = load_toml(svc.toml_path)
            cfg = parse_service_config(
                data=data, service_name=service_name, runtimes=RUNTIMES)
        except Exception as e:
            # Still let it show up if unit files exist, but flag config parse error
            cfg = None
            cfg_err = str(e)
        else:
            cfg_err = None

        has_timer_declared = bool(getattr(cfg, "timer", None))
        st = compute_installed_status(
            service_name=service_name,
            has_timer_declared=has_timer_declared,
            user=user,
        )

        # Installed definition (for your project): unit file exists where installer writes it
        if not (st.has_service_unit or st.has_timer_unit):
            continue

        installed_rows.append((service_name, cfg, (st, cfg_err)))

    if not installed_rows:
        print("No INSTALLED services found (no unit files in the expected systemd dir).")
        return 0

    print("INSTALLED services:")
    for name, cfg, payload in installed_rows:
        st, cfg_err = payload

        # Basic header
        print(f"- {name}")

        # Config summary (if parsable)
        if cfg is not None:
            print(f"    type: {cfg.type}")
            print(f"    runtime: {cfg.runtime}")
            print(f"    description: {cfg.description}")
            print(f"    declares timer: {'yes' if cfg.timer else 'no'}")
        else:
            print("    [warn] service.toml could not be parsed")
            print(f"    [warn] {cfg_err}")

        # Unit file presence
        print(
            f"    unit files: service={'yes' if st.has_service_unit else 'no'}, timer="
            f"{'yes' if st.has_timer_unit else 'no'}"
        )

        # systemctl states (only if unit exists)
        if st.has_service_unit:
            print(
                f"    service: enabled="
                f"{st.service_enabled}, active={st.service_active}"
            )
        if st.has_timer_unit:
            print(f"    timer: enabled="
                  f"{st.timer_enabled}, active={st.timer_active}"
              )

    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="List INSTALLED services (unit files exist)")
    ap.add_argument(
        "--user",
        action="store_true",
        help="Query user services (~/.config/systemd/user) via systemctl --user",
    )
    args = ap.parse_args()

    project_root = Path.cwd().resolve()
    raise SystemExit(list_installed(project_root, user=args.user))


if __name__ == "__main__":
    main()
