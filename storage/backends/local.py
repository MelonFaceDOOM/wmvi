from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import StorageBackend


class LocalFileStorage(StorageBackend):
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def is_accessible(self) -> tuple[bool, Optional[str]]:
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            test_path = self.base_dir / ".write_test"
            test_path.write_text("ok", encoding="utf-8")
            test_path.unlink()
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def write_text(self, rel_path: str, text: str) -> None:
        dest = self.base_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")

    def write_bytes(
        self,
        rel_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        dest = self.base_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    def read_bytes(self, rel_path: str) -> bytes:
        return (self.base_dir / rel_path).read_bytes()

    def list_names(self, prefix: str) -> list[str]:
        root = self.base_dir / prefix
        if not root.is_dir():
            return []
        return sorted(p.name for p in root.iterdir())
