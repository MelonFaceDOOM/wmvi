from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

from services.cli.lib.config import load_toml, parse_service_config
from services.cli.lib.render import load_template, render_template, write_text
from services.cli.lib.systemd import (
    SystemdNotAvailable,
    systemctl_cmd,
    unit_paths,
)
import subprocess
from dotenv import load_dotenv
load_dotenv()


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    raise SystemExit(1)


RUNTIMES = {
    "base": os.getenv("BASE_INTERPRETER", "venvs/base/bin/python"),
    "transcription": os.getenv("TRANSCRIPTION_INTERPRETER", "venvs/transcription/bin/python"),
}


SYSTEMD_TEMPLATES_DIR = Path("services/cli/systemd")

SERVICE_ENV = os.getenv("SERVICE_ENV", "dev").strip().lower()
if SERVICE_ENV not in ("prod", "dev"):
    die("SERVICE_ENV (set in .env) must be prod or dev")


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def resolve_python(project_root: Path, runtime: str) -> Path:
    rel = RUNTIMES.get(runtime)
    if rel is None:
        die(f"Unknown runtime '{runtime}'. Known: {sorted(RUNTIMES.keys())}")
    python_path = project_root / rel
    if not python_path.exists():
        die(f"Python for runtime '{runtime}' not found: {python_path}")
    return python_path


def templates_root(project_root: Path) -> Path:
    root = project_root / SYSTEMD_TEMPLATES_DIR
    if not root.exists():
        die(f"Missing templates dir: {root}")
    return root


def pick_service_template(cfg_type: str, templates: Path) -> Path:
    if cfg_type == "oneshot":
        p = templates / "oneshot.service.in"
    elif cfg_type == "longrunning":
        p = templates / "longrunning.service.in"
    else:
        die(f"Unknown service type '{cfg_type}'")
    if not p.exists():
        die(f"Missing template file: {p}")
    return p


def timer_template(templates: Path) -> Path:
    p = templates / "timer.in"
    if not p.exists():
        die(f"Missing template file: {p}")
    return p


def make_replacements(
    *,
    project_root: Path,
    env_file: Path,
    service_name: str,
    description: str,
    python_bin: Path,
    args: str,
) -> dict[str, str]:
    return {
        "SERVICE_NAME": service_name,
        "SERVICE_MODULE": f"services.{service_name}",
        "DESCRIPTION": description,
        "PROJECT_ROOT": str(project_root),
        "PYTHON": str(python_bin),
        "ENV_FILE": str(env_file),
        "ARGS": args,
    }


def install(
    *,
    service_name: str,
    user: bool,
) -> None:
    project_root = Path.cwd().resolve()

    service_dir = project_root / "services" / service_name
    if not service_dir.exists():
        die(f"Service directory not found: {service_dir}")

    toml_path = service_dir / "service.toml"
    if not toml_path.exists():
        die(f"Missing service.toml: {toml_path}")

    env_file = project_root / ".env"
    if not env_file.exists():
        die(".env file not found in project root")

    data = load_toml(toml_path)
    cfg = parse_service_config(
        data=data, service_name=service_name, runtimes=RUNTIMES)

    py = resolve_python(project_root, cfg.runtime)
    templates = templates_root(project_root)

    # Render service unit
    svc_tpl_path = pick_service_template(cfg.type, templates)
    svc_tpl = load_template(svc_tpl_path)

    # build an args str (only includes system env for now)
    args_str = "--prod" if SERVICE_ENV == "prod" else ""

    print(f"[info] installing {service_name}: env={
          SERVICE_ENV} args={args_str or '<none>'}")

    repl = make_replacements(
        project_root=project_root,
        env_file=env_file,
        service_name=service_name,
        description=cfg.description,
        python_bin=py,
        args=args_str
    )

    svc_rendered = render_template(svc_tpl, repl)

    # Write units
    paths = unit_paths(service_name, user=user)
    try:
        write_text(paths["service"], svc_rendered)
    except PermissionError as e:
        target_dir = paths["service"].parent
        die(
            f"Permission denied writing to {target_dir}. "
            f"Use --user to install to ~/.config/systemd/user, "
            f"or run with sudo for system-wide install. ({e})"
        )
    print(f"[ok] wrote {paths['service']}")

    if cfg.timer is not None:
        t_tpl_path = timer_template(templates)
        t_tpl = load_template(t_tpl_path)

        timer_repl = dict(repl)
        timer_repl.update(
            {
                "ON_BOOT_SEC": cfg.timer.on_boot_sec,
                "ON_UNIT_INACTIVE_SEC": cfg.timer.on_unit_inactive_sec,
                "PERSISTENT": str(cfg.timer.persistent).lower(),
            }
        )
        t_rendered = render_template(t_tpl, timer_repl)
        try:
            write_text(paths["timer"], t_rendered)
        except PermissionError as e:
            target_dir = paths["service"].parent
            die(
                f"Permission denied writing to {target_dir}. "
                f"Use --user to install to ~/.config/systemd/user, "
                f"or run with sudo for system-wide install. ({e})"
            )

        print(f"[ok] wrote {paths['timer']}")

    # Reload + enable/start
    try:
        systemctl = systemctl_cmd(user)
    except SystemdNotAvailable as e:
        die(str(e))

    run(systemctl + ["daemon-reload"])

    if cfg.timer is not None:
        run(systemctl + ["enable", "--now", f"{service_name}.timer"])
    else:
        run(systemctl + ["enable", "--now", f"{service_name}.service"])

    print("[done] service installed")


def require_root_for_system_units(*, user: bool, action: str) -> None:
    if user:
        return
    if os.geteuid() != 0:
        die(
            f"{action} targets /etc/systemd/system and requires root. "
            f"Run: sudo -E python -m services {action} <name> "
            f"or use --user for ~/.config/systemd/user."
        )


def uninstall(
    *,
    service_name: str,
    user: bool,
) -> None:
    """
    Stop/disable unit(s) if present, delete unit files, daemon-reload.
    Safe to run even if not installed.
    """
    require_root_for_system_units(user=user, action="uninstall")

    project_root = Path.cwd().resolve()
    # We do not require the service dir or service.toml to uninstall; we operate on unit files.
    paths = unit_paths(service_name, user=user)

    try:
        systemctl = systemctl_cmd(user)
    except SystemdNotAvailable as e:
        die(str(e))

    # Best-effort stop/disable (don't fail if not installed)
    def try_run(cmd: list[str]) -> None:
        print("+", " ".join(cmd))
        subprocess.run(cmd, check=False)

    # Stop timers first (prevents re-trigger)
    try_run(systemctl + ["disable", "--now", f"{service_name}.timer"])
    try_run(systemctl + ["disable", "--now", f"{service_name}.service"])

    # Clean up "failed" state so status output is clearer
    try_run(systemctl + ["reset-failed", f"{service_name}.service"])
    try_run(systemctl + ["reset-failed", f"{service_name}.timer"])

    # Remove unit files if they exist
    removed_any = False
    for unit_type, p in paths.items():
        if p.exists():
            p.unlink()
            removed_any = True
            print(f"[ok] deleted {p}")

    # Reload systemd if we changed anything
    if removed_any:
        try_run(systemctl + ["daemon-reload"])

    print("[done] uninstall complete")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Install/uninstall a service from service.toml + shared templates")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_install = sub.add_parser("install", help="Install a service")
    ap_install.add_argument("service", help="Service name (services/<name>/)")
    ap_install.add_argument("--user", action="store_true",
                            help="Install as user unit (~/.config/systemd/user)")

    ap_uninstall = sub.add_parser(
        "uninstall", help="Uninstall a service (stop/disable + delete unit files)")
    ap_uninstall.add_argument("service", help="Service name (unit prefix)")
    ap_uninstall.add_argument("--user", action="store_true",
                              help="Uninstall user unit (~/.config/systemd/user)")

    args = ap.parse_args()
    name = args.service.replace("-", "_")

    if args.cmd == "install":
        install(service_name=name, user=args.user)
    elif args.cmd == "uninstall":
        uninstall(service_name=name, user=args.user)
    else:
        die(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
