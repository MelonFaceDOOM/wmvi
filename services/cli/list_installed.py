from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from services.cli.lib.discover import discover_services
from services.cli.lib.config import load_toml, parse_service_config
from services.cli.lib.systemd import (
    SystemdNotAvailable,
    InstalledStatus,
    unit_file_exists,
    is_active,
    is_enabled,
)
from dotenv import load_dotenv

load_dotenv()


RUNTIMES = {
    "base": os.getenv("BASE_INTERPRETER", "venvs/base/bin/python"),
    "transcription": os.getenv(
        "TRANSCRIPTION_INTERPRETER", "venvs/transcription/bin/python"
    ),
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


@dataclass(frozen=True)
class InstalledServiceInfo:
    """
    Represents a service that is considered "installed" because at least one unit
    file exists in the target systemd unit directory.
    """
    name: str
    cfg: object | None
    cfg_err: str | None
    status: InstalledStatus


def get_installed_services(project_root: Path, user: bool) -> list[InstalledServiceInfo]:
    """
    Discover services with service.toml, compute systemd unit presence + enabled/active status,
    and return only those that are installed (unit file exists).
    """
    try:
        discovered = discover_services(project_root)
    except Exception as e:
        die(str(e))

    if not discovered:
        return []

    # Confirm systemd exists early so the output isn't half-useful
    try:
        _ = is_active("default.target", user=user)
    except SystemdNotAvailable as e:
        die(str(e))
    except Exception:
        # default.target may be unknown in --user context; ignore
        pass

    out: list[InstalledServiceInfo] = []

    for svc in discovered:
        service_name = svc.name

        try:
            data = load_toml(svc.toml_path)
            cfg = parse_service_config(
                data=data, service_name=service_name, runtimes=RUNTIMES)
            cfg_err = None
        except Exception as e:
            # Still let it show up if unit files exist, but flag config parse error
            cfg = None
            cfg_err = str(e)

        has_timer_declared = bool(
            getattr(cfg, "timer", None)) if cfg is not None else False
        st = compute_installed_status(
            service_name=service_name,
            has_timer_declared=has_timer_declared,
            user=user,
        )

        # Installed definition: unit file exists where installer writes it
        if not (st.has_service_unit or st.has_timer_unit):
            continue

        out.append(
            InstalledServiceInfo(
                name=service_name,
                cfg=cfg,
                cfg_err=cfg_err,
                status=st,
            )
        )

    out.sort(key=lambda x: x.name)
    return out


def list_installed(project_root: Path, user: bool) -> int:
    installed = get_installed_services(project_root, user=user)

    if not installed:
        print("No INSTALLED services found (no unit files in the expected systemd dir).")
        return 0

    print("INSTALLED services:")
    for row in installed:
        name = row.name
        cfg = row.cfg
        cfg_err = row.cfg_err
        st = row.status

        print(f"- {name}")

        # Config summary (if parsable)
        if cfg is not None:
            print(f"    type: {cfg.type}")
            print(f"    runtime: {cfg.runtime}")
            print(f"    description: {cfg.description}")
            print(f"    declares timer: {'yes' if cfg.timer else 'no'}")
        else:
            print("    [warn] service.toml could not be parsed")
            if cfg_err:
                print(f"    [warn] {cfg_err}")

        # Unit file presence
        print(
            f"    unit files: service={
                'yes' if st.has_service_unit else 'no'}, "
            f"timer={'yes' if st.has_timer_unit else 'no'}"
        )

        # systemctl states (only if unit exists)
        if st.has_service_unit:
            print(f"    service: enabled={
                  st.service_enabled}, active={st.service_active}")
        if st.has_timer_unit:
            print(f"    timer: enabled={
                  st.timer_enabled}, active={st.timer_active}")

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
