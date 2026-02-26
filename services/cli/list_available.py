from __future__ import annotations

import argparse
import sys
from pathlib import Path
import os

from services.cli.lib.discover import discover_services
from services.cli.lib.config import load_toml, parse_service_config

# NEW: shared naming helpers (see abstraction plan below)
from services.cli.lib.naming import (
    normalize_service_id,
    unit_name_from_service_id,
    module_from_service_id,
)
from dotenv import load_dotenv
load_dotenv()

RUNTIMES = {
    "base": os.getenv("BASE_INTERPRETER", "venvs/base/bin/python"),
    "transcription": os.getenv("TRANSCRIPTION_INTERPRETER", "venvs/transcription/bin/python"),
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
        # svc.name should already be a service_id like "youtube/monitor"
        service_id = normalize_service_id(svc.name)
        unit_name = unit_name_from_service_id(service_id)
        module = module_from_service_id(service_id)

        try:
            data = load_toml(svc.toml_path)
            # IMPORTANT: pass service_id (user-facing identifier), not unit_name
            cfg = parse_service_config(data=data, service_name=service_id, runtimes=RUNTIMES)
        except Exception as e:
            print(service_id)
            print(f"  [error] {e}")
            print()
            continue

        print(cfg.name)  # should match service_id
        print(f"  unit: {unit_name}")
        print(f"  module: {module}")
        print(f"  type: {cfg.type}")
        print(f"  runtime: {cfg.runtime}")
        print(f"  description: {cfg.description}")

        if cfg.timer:
            print("  timer:")
            print(f"    on_boot_sec: {cfg.timer.on_boot_sec}")
            print(f"    on_unit_inactive_sec: {cfg.timer.on_unit_inactive_sec}")
            print(f"    persistent: {cfg.timer.persistent}")
        else:
            print("  timer: no")

        print()

    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="List available services (service.toml discovered)")
    ap.parse_args()
    project_root = Path.cwd().resolve()
    raise SystemExit(list_available(project_root))


if __name__ == "__main__":
    main()