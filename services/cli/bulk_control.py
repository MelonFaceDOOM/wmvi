from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from services.cli.lib.systemd import SystemdNotAvailable, systemctl_cmd
from services.cli.list_installed import get_installed_services


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    raise SystemExit(1)


def _try_run(cmd: list[str]) -> int:
    print("+", " ".join(cmd))
    p = subprocess.run(cmd, check=False)
    return int(p.returncode or 0)


def stop_all(project_root: Path, user: bool) -> int:
    """
    Stop/disable all installed services.
    - If a timer unit exists, disable/stop the timer first (prevents retrigger).
    - Then disable/stop the service unit (if present).
    Best-effort: continues on errors and returns nonzero if any command failed.
    """
    installed = get_installed_services(project_root, user=user)
    if not installed:
        print("No installed services found.")
        return 0

    try:
        systemctl = systemctl_cmd(user)
    except SystemdNotAvailable as e:
        die(str(e))

    rc = 0

    # 1) Stop timers first
    for row in installed:
        st = row.status
        if st.has_timer_unit:
            rc |= _try_run(
                systemctl + ["disable", "--now", f"{row.name}.timer"])

    # 2) Stop services
    for row in installed:
        st = row.status
        if st.has_service_unit:
            rc |= _try_run(
                systemctl + ["disable", "--now", f"{row.name}.service"])

    # 3) Clean up failed states (optional but keeps status output clean)
    for row in installed:
        st = row.status
        if st.has_service_unit:
            rc |= _try_run(systemctl + ["reset-failed", f"{row.name}.service"])
        if st.has_timer_unit:
            rc |= _try_run(systemctl + ["reset-failed", f"{row.name}.timer"])

    print("[done] stop-all complete")
    return rc


def start_all(project_root: Path, user: bool) -> int:
    """
    Enable/start all installed services.
    - If a timer unit exists, enable/start the timer.
    - Otherwise enable/start the service.
    Best-effort: continues on errors and returns nonzero if any command failed.
    """
    installed = get_installed_services(project_root, user=user)
    if not installed:
        print("No installed services found.")
        return 0

    try:
        systemctl = systemctl_cmd(user)
    except SystemdNotAvailable as e:
        die(str(e))

    rc = 0

    for row in installed:
        st = row.status
        if st.has_timer_unit:
            rc |= _try_run(
                systemctl + ["enable", "--now", f"{row.name}.timer"])
        elif st.has_service_unit:
            rc |= _try_run(
                systemctl + ["enable", "--now", f"{row.name}.service"])

    print("[done] start-all complete")
    return rc


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Bulk start/stop installed services")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_stop = sub.add_parser(
        "stop-all", help="Disable/stop all installed services")
    ap_stop.add_argument("--user", action="store_true",
                         help="Use user systemd units")

    ap_start = sub.add_parser(
        "start-all", help="Enable/start all installed services")
    ap_start.add_argument("--user", action="store_true",
                          help="Use user systemd units")

    args = ap.parse_args()
    project_root = Path.cwd().resolve()

    if args.cmd == "stop-all":
        raise SystemExit(stop_all(project_root, user=args.user))
    elif args.cmd == "start-all":
        raise SystemExit(start_all(project_root, user=args.user))
    else:
        die(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
