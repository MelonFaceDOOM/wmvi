from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def _default_export_state_path() -> Path:
    return Path(os.environ.get("PODCAST_SYNC_STATE_FILE", "./data/podcast_sync_export_state.json"))


def _default_import_state_path() -> Path:
    return Path(os.environ.get("PODCAST_SYNC_IMPORT_STATE_FILE", "./data/podcast_sync_import_state.json"))


@dataclass
class ExportState:
    last_exported_at: datetime | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExportState":
        raw = data.get("last_exported_at")
        if raw is None:
            return cls()
        return cls(
            last_exported_at=datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_exported_at": (
                self.last_exported_at.isoformat() if self.last_exported_at else None
            ),
        }


@dataclass
class ImportState:
    export_date: str | None = None
    row_count: int | None = None
    shows_count: int | None = None
    manifest_path: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ImportState":
        return cls(
            export_date=data.get("export_date"),
            row_count=data.get("row_count"),
            shows_count=data.get("shows_count"),
            manifest_path=data.get("manifest_path"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "export_date": self.export_date,
            "row_count": self.row_count,
            "shows_count": self.shows_count,
            "manifest_path": self.manifest_path,
        }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def load_export_state(path: Path | None = None) -> ExportState:
    p = path or _default_export_state_path()
    return ExportState.from_dict(_read_json(p))


def save_export_state(state: ExportState, path: Path | None = None) -> None:
    p = path or _default_export_state_path()
    _write_json(p, state.to_dict())


def load_import_state(path: Path | None = None) -> ImportState:
    p = path or _default_import_state_path()
    return ImportState.from_dict(_read_json(p))


def save_import_state(state: ImportState, path: Path | None = None) -> None:
    p = path or _default_import_state_path()
    _write_json(p, state.to_dict())
