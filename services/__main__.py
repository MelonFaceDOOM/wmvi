from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="python -m services",
        description="Manage project services (discover/install/status via systemd).",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    # list-available
    sub.add_parser("list-available",
                   help="List services discovered via services/*/service.toml")

    # list-installed
    ap_list_installed = sub.add_parser(
        "list-installed", help="List installed services (unit files exist)")
    ap_list_installed.add_argument(
        "--user",
        action="store_true",
        help="Use user systemd units (~/.config/systemd/user) via systemctl --user",
    )

    # install
    ap_install = sub.add_parser(
        "install", help="Install a service from service.toml + shared templates")
    ap_install.add_argument("service", help="Service name (services/<name>/)")
    ap_install.add_argument(
        "--user",
        action="store_true",
        help="Install as user systemd unit (~/.config/systemd/user)",
    )

    # uninstall
    ap_uninstall = sub.add_parser(
        "uninstall", help="Uninstall a service (stop/disable + delete unit files)")
    ap_uninstall.add_argument("service", help="Service name (unit prefix)")
    ap_uninstall.add_argument(
        "--user",
        action="store_true",
        help="Uninstall user systemd unit (~/.config/systemd/user)",
    )

    # stop installed
    ap_stop_installed = sub.add_parser(
        "stop-installed", help="Stop all installed services.")
    ap_stop_installed.add_argument(
        "--user",
        action="store_true",
        help="Install as user systemd unit (~/.config/systemd/user)",
    )

    # start installed
    ap_start_installed = sub.add_parser(
        "start-installed", help="Start all installed services.")
    ap_start_installed.add_argument(
        "--user",
        action="store_true",
        help="Uninstall user systemd unit (~/.config/systemd/user)",
    )

    args = ap.parse_args()

    # Dispatch
    if args.cmd == "list-available":
        from services.cli.list_available import list_available

        raise SystemExit(list_available(Path.cwd().resolve()))

    elif args.cmd == "list-installed":
        from services.cli.list_installed import list_installed

        raise SystemExit(list_installed(Path.cwd().resolve(), user=args.user))

    elif args.cmd == "install":
        from services.cli.install import install

        name = args.service.replace("-", "_")
        install(service_name=name, user=args.user)
        return

    elif args.cmd == "uninstall":
        from services.cli.install import uninstall

        name = args.service.replace("-", "_")
        uninstall(service_name=name, user=args.user)
        return

    elif args.cmd == "stop-installed":
        from services.cli.bulk_control import stop_all
        stop_all(Path.cwd().resolve(), user=args.user)
        return

    elif args.cmd == "start-installed":
        from services.cli.bulk_control import start_all
        start_all(Path.cwd().resolve(), user=args.user)
        return

    else:
        print(f"[error] unknown command: {args.cmd}", file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
