from __future__ import annotations

import argparse
import sys
from pathlib import Path

from services.cli.lib.discover import discover_services
from services.cli.lib.config import load_toml, parse_service_config


RUNTIMES = {
    "base": "venvs/base/bin/python",
    "transcription": "venvs/transcription/bin/python",
}


def die(msg: str) -> None:
    print(f"[error] {msg}", file=sys.stderr)
    raise SystemExit(1)


def list_available(project_root: Path) -> int:
    try:
        discovered = discover_services(project_root)
    except Exception as e:
        die(str(e))

    if not discovered:
        print("No services with service.toml found")
        return 0

    for svc in discovered:
        try:
            data = load_toml(svc.toml_path)
            cfg = parse_service_config(
                data=data, service_name=svc.name, runtimes=RUNTIMES)
        except Exception as e:
            print(svc.name)
            print(f"  [error] {e}")
            print()
            continue

        print(cfg.name)
        print(f"  type: {cfg.type}")
        print(f"  runtime: {cfg.runtime}")
        print(f"  description: {cfg.description}")

        if cfg.timer:
            print("  timer:")
            print(f"    on_boot_sec: {cfg.timer.on_boot_sec}")
            print(f"    on_unit_inactive_sec:"
                  f"{cfg.timer.on_unit_inactive_sec}")
            print(f"    persistent: {cfg.timer.persistent}")
        else:
            print("  timer: no")

        print()

    return 0


def main() -> None:
    ap = argparse.ArgumentParser(
        description="List available services (service.toml discovered)")
    ap.parse_args()  # no args currently
    project_root = Path.cwd().resolve()
    raise SystemExit(list_available(project_root))


if __name__ == "__main__":
    main()
