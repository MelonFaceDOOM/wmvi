from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
try:
    import tomllib  # py3.11+
except ModuleNotFoundError:  # py3.10 and earlier
    import tomli as tomllib


DEFAULT_RUNTIME = "base"
DEFAULT_SERVICE_TYPE = "longrunning"


@dataclass(frozen=True)
class TimerConfig:
    on_boot_sec: str = "2min"
    on_unit_inactive_sec: str = "5min"
    persistent: bool = False


@dataclass(frozen=True)
class ServiceConfig:
    name: str
    description: str
    type: str
    runtime: str
    timer: Optional[TimerConfig]


def load_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text())
    except Exception as e:
        raise ValueError(f"failed to parse TOML at {path}: {e}") from e


def parse_service_config(
    *,
    data: dict[str, Any],
    service_name: str,
    runtimes: dict[str, str],
) -> ServiceConfig:
    svc = data.get("service", {})
    runtime = svc.get("runtime", DEFAULT_RUNTIME)
    svc_type = svc.get("type", DEFAULT_SERVICE_TYPE)
    description = svc.get("description", service_name)

    if runtime not in runtimes:
        raise ValueError(f"unknown runtime '"
                f"{runtime}' for service '{service_name}'")

    if svc_type not in {"oneshot", "longrunning"}:
        raise ValueError(f"unknown service type '"
                         f"{svc_type}' for service '{service_name}'")

    timer_cfg = None
    t = data.get("timer")
    if isinstance(t, dict):
        timer_cfg = TimerConfig(
            on_boot_sec=t.get("on_boot_sec", "2min"),
            on_unit_inactive_sec=t.get("on_unit_inactive_sec", "5min"),
            persistent=bool(t.get("persistent", False)),
        )

    return ServiceConfig(
        name=service_name,
        description=description,
        type=svc_type,
        runtime=runtime,
        timer=timer_cfg,
    )
