from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass
class ImportStats:
    rows_seen: int = 0
    rows_upserted: int = 0
    transcripts_updated: int = 0
    posts_registered: int = 0
    segments_replaced: int = 0
    skipped: int = 0
    extra: dict[str, int] = field(default_factory=dict)

    def merge(self, other: "ImportStats") -> None:
        self.rows_seen += other.rows_seen
        self.rows_upserted += other.rows_upserted
        self.transcripts_updated += other.transcripts_updated
        self.posts_registered += other.posts_registered
        self.segments_replaced += other.segments_replaced
        self.skipped += other.skipped
        for k, v in other.extra.items():
            self.extra[k] = self.extra.get(k, 0) + v


class PlatformHandler(Protocol):
    platform: str

    def export_delta(
        self,
        cur,
        *,
        since: datetime | None,
        until: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """
        Return (platform rows as JSON-serializable dicts, sidecar exports).

        Sidecar dict keys are logical names (e.g. youtube_segments); values are
        lists of row dicts. Caller writes files.
        """
        ...

    def import_bundle(
        self,
        cur,
        *,
        rows: list[dict[str, Any]],
        sidecars: dict[str, list[dict[str, Any]]],
    ) -> ImportStats:
        ...
