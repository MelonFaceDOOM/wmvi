from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ExportState:
    last_exported_at: datetime | None = None


def _state_path() -> Path:
    return Path(
        os.environ.get(
            "CONTENT_SYNC_STATE_FILE",
            "./data/content_sync/export_state.json",
        )
    )


def load_export_state() -> ExportState:
    path = _state_path()
    if not path.is_file():
        return ExportState()
    data = json.loads(path.read_text(encoding="utf-8"))
    raw = data.get("last_exported_at")
    if not raw:
        return ExportState()
    return ExportState(
        last_exported_at=datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    )


def save_export_state(state: ExportState) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_exported_at": (
            state.last_exported_at.isoformat() if state.last_exported_at else None
        ),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
