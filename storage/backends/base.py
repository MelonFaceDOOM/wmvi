from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class StorageBackend(ABC):
    @abstractmethod
    def is_accessible(self) -> tuple[bool, Optional[str]]:
        raise NotImplementedError

    @abstractmethod
    def write_text(self, rel_path: str, text: str) -> None:
        raise NotImplementedError

    def write_bytes(
        self,
        rel_path: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement write_bytes"
        )

    def read_bytes(self, rel_path: str) -> bytes:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement read_bytes"
        )

    def list_names(self, prefix: str) -> list[str]:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement list_names"
        )
