from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
import tomllib


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    sys.exit(1)


def check_systemd(user: bool) -> list[str]:
    if not shutil.which("systemctl"):
        die("systemctl not found; systemd does not appear to be available")

    cmd = ["systemctl"]
    if user:
        cmd.append("--user")
    return cmd


def discover_services(project_root: Path) -> list[Path]:
    """
    Return service directories under services/ that contain a service.toml.
    Skips services/systemd/.
    """
    services_dir = project_root / "services"
    if not services_dir.exists():
        die("services/ directory not found")

    out: list[Path] = []
    for d in sorted(p for p in services_dir.iterdir() if p.is_dir()):
        if d.name == "systemd":
            continue
        if (d / "service.toml").exists():
            out.append(d)
    return out


def load_service_cfg(service_dir: Path) -> dict:
    toml_path = service_dir / "service.toml"
    try:
        data = tomllib.loads(toml_path.read_text())
    except Exception as e:
        raise RuntimeError(f"failed to parse {toml_path}: {e}") from e

    svc = data.get("service", {})
    return {
        "name": service_dir.name,
        "description": svc.get("description", service_dir.name),
        "type": svc.get("type", "longrunning"),
        "has_timer": bool(data.get("timer")),
    }


def systemctl_is_active(systemctl: list[str], unit: str) -> tuple[bool, str]:
    """
    Returns (is_active, state_string).
    state_string is the literal stdout from `systemctl is-active`, e.g.:
    active, inactive, failed, activating, deactivating, unknown.
    """
    p = subprocess.run(
        systemctl + ["is-active", unit],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    state = (p.stdout or "").strip()
    return (state == "active"), state


def list_running_services(project_root: Path, user: bool) -> int:
    systemctl = check_systemd(user)
    service_dirs = discover_services(project_root)

    running: list[dict] = []
    errors: list[str] = []

    for service_dir in service_dirs:
        try:
            cfg = load_service_cfg(service_dir)
        except Exception as e:
            errors.append(f"{service_dir.name}: {e}")
            continue

        unit = f"{cfg['name']}.service"
        is_active, state = systemctl_is_active(systemctl, unit)

        if is_active:
            cfg["state"] = state
            running.append(cfg)

    if errors:
        print("[warn] some services could not be inspected:")
        for msg in errors:
            print(" ", msg)
        print()

    if not running:
        print("No RUNNING services found.")
        return 0

    # Pretty output
    print("RUNNING services:")
    for cfg in running:
        print(f"- {cfg['name']}")
        print(f"    state: {cfg['state']}")
        print(f"    type: {cfg['type']}")
        print(f"    timer: {'yes' if cfg['has_timer'] else 'no'}")
        print(f"    description: {cfg['description']}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="List RUNNING services discovered via services/*/service.toml"
    )
    ap.add_argument(
        "--user",
        action="store_true",
        help="Query user services via systemctl --user",
    )
    args = ap.parse_args()

    project_root = Path.cwd().resolve()
    raise SystemExit(list_running_services(project_root, user=args.user))


if __name__ == "__main__":
    main()
