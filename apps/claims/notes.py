"""Append-only NOTES.md helpers for corpus pipeline stages."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_note(notes_path: Path, title: str, lines: list[str]) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    block = f"\n## {title} {stamp}\n" + "".join(f"- {line}\n" for line in lines)
    notes_path.parent.mkdir(parents=True, exist_ok=True)
    if notes_path.is_file():
        notes_path.write_text(notes_path.read_text(encoding="utf-8") + block, encoding="utf-8")
    else:
        notes_path.write_text(f"# notes\n{block}", encoding="utf-8")


def fmt_kv(payload: dict[str, Any]) -> list[str]:
    return [f"{k}: {v}" for k, v in payload.items() if v is not None]
