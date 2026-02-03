from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class SystemdNotAvailable(RuntimeError):
    pass


def systemctl_cmd(user: bool) -> list[str]:
    if not shutil.which("systemctl"):
        raise SystemdNotAvailable(
            "systemctl not found; systemd does not appear to be available")
    cmd = ["systemctl"]
    if user:
        cmd.append("--user")
    return cmd


def unit_dir(user: bool) -> Path:
    """
    Where *your installer* writes unit files.
    - system: /etc/systemd/system
    - user:   ~/.config/systemd/user
    """
    if user:
        return Path.home() / ".config/systemd/user"
    return Path("/etc/systemd/system")


def unit_paths(service_name: str, user: bool) -> dict[str, Path]:
    d = unit_dir(user)
    return {
        "service": d / f"{service_name}.service",
        "timer": d / f"{service_name}.timer",
    }


def unit_file_exists(service_name: str, unit_type: str, user: bool) -> bool:
    paths = unit_paths(service_name, user)
    p = paths.get(unit_type)
    if p is None:
        raise ValueError(f"unknown unit_type: {unit_type}")
    return p.exists()


def _run_quiet(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return p.returncode, (p.stdout or "").strip()


def is_active(unit: str, user: bool) -> str:
    """
    Returns the raw `systemctl is-active` state string, e.g.:
    active, inactive, failed, activating, deactivating, unknown
    """
    cmd = systemctl_cmd(user) + ["is-active", unit]
    _, out = _run_quiet(cmd)
    return out or "unknown"


def is_enabled(unit: str, user: bool) -> str:
    """
    Returns the raw `systemctl is-enabled` state string, e.g.:
    enabled, disabled, static, indirect, masked, generated, transient, unknown
    """
    cmd = systemctl_cmd(user) + ["is-enabled", unit]
    _, out = _run_quiet(cmd)
    return out or "unknown"


@dataclass(frozen=True)
class InstalledStatus:
    name: str
    has_service_unit: bool
    has_timer_unit: bool
    service_enabled: Optional[str]
    service_active: Optional[str]
    timer_enabled: Optional[str]
    timer_active: Optional[str]
