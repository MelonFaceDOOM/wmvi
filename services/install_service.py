from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    sys.exit(1)


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def check_systemd(user: bool) -> list[str]:
    if not shutil.which("systemctl"):
        die("systemctl not found; systemd does not appear to be available")

    cmd = ["systemctl"]
    if user:
        cmd.append("--user")
    return cmd


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Install a single systemd service from per-service templates"
    )
    ap.add_argument("service", help="Service name (e.g. term_matcher)")
    ap.add_argument("--timer", action="store_true", help="Install timer unit")
    ap.add_argument(
        "--user",
        action="store_true",
        help="Install as user service (~/.config/systemd/user)",
    )
    args = ap.parse_args()

    service_name = args.service
    service_module = service_name.replace("-", "_")

    # Project root = cwd (explicit design choice)
    project_root = Path.cwd().resolve()

    env_file = project_root / ".env"
    if not env_file.exists():
        die(".env file not found in project root")

    python_bin = sys.executable

    service_dir = project_root / "services" / service_module
    if not service_dir.exists():
        die(f"Service directory not found: {service_dir}")

    service_tpl = service_dir / f"{service_name}.service.in"
    timer_tpl = service_dir / f"{service_name}.timer.in"

    if not service_tpl.exists():
        die(f"Missing service template: {service_tpl}")

    if args.timer and not timer_tpl.exists():
        die(f"--timer passed but timer template missing: {timer_tpl}")

    systemctl = check_systemd(args.user)

    if args.user:
        systemd_dir = Path.home() / ".config/systemd/user"
    else:
        systemd_dir = Path("/etc/systemd/system")

    systemd_dir.mkdir(parents=True, exist_ok=True)

    replacements = {
        "{{SERVICE_NAME}}": service_name,
        "{{SERVICE_MODULE}}": service_module,
        "{{PROJECT_ROOT}}": str(project_root),
        "{{PYTHON}}": python_bin,
        "{{ENV_FILE}}": str(env_file),
    }

    def render(src: Path, dst: Path) -> None:
        text = src.read_text()
        for k, v in replacements.items():
            text = text.replace(k, v)
        dst.write_text(text)
        print(f"[ok] wrote {dst}")

    service_unit = systemd_dir / f"{service_name}.service"
    render(service_tpl, service_unit)

    if args.timer:
        timer_unit = systemd_dir / f"{service_name}.timer"
        render(timer_tpl, timer_unit)

    run(systemctl + ["daemon-reload"])
    run(systemctl + ["enable", "--now", f"{service_name}.service"])

    if args.timer:
        run(systemctl + ["enable", "--now", f"{service_name}.timer"])

    print("[done] service installed")


if __name__ == "__main__":
    main()
