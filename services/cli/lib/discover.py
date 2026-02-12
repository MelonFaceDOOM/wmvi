from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class DiscoveredService:
    name: str
    dir: Path
    toml_path: Path


def iter_service_dirs(services_root: Path) -> Iterable[Path]:
    """
    Yield candidate service directories under services_root.
    Skips known non-service directories.
    """
    skip_names = {"systemd", "lib", "__pycache__"}

    for p in services_root.iterdir():
        if not p.is_dir():
            continue
        if p.name in skip_names:
            continue
        yield p


def discover_services(project_root: Path) -> list[DiscoveredService]:
    """
    Discover services by finding services/<name>/service.toml.
    """
    services_root = project_root / "services"
    if not services_root.exists():
        raise FileNotFoundError(
            f"services/ directory not found: {services_root}")

    out: list[DiscoveredService] = []
    for d in sorted(iter_service_dirs(services_root), key=lambda x: x.name):
        toml_path = d / "service.toml"
        if not toml_path.exists():
            continue
        out.append(DiscoveredService(name=d.name, dir=d, toml_path=toml_path))
    return out
