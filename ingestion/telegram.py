from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from ingestion.ingestion import flush_and_link_dual_key
from ingestion.row_model import InsertableRow


@dataclass(frozen=True, slots=True)
class TelegramPostRow(InsertableRow):
    TABLE = "sm.telegram_post"
    PK = ("channel_id", "message_id")

    channel_id: int
    message_id: int

    link: str
    created_at_ts: datetime
    text: str
    filtered_text: str

    views: int | None = None
    forwards: int | None = None
    replies: int | None = None
    reactions_total: int | None = None

    is_pinned: bool = False
    has_media: bool = False
    raw_type: str | None = None


def flush_telegram_batch(rows: list[TelegramPostRow], job_id: int, cur=None) -> tuple[int, int]:
    """
    Insert a batch of telegram posts and link *inserted* ones to a scrape job.

    Returns: (inserted, skipped)
    """
    if not rows:
        return 0, 0
    if not isinstance(rows[0], TelegramPostRow):
        raise TypeError("rows must be list of TelegramPostRow")

    inserted, skipped, _pairs = flush_and_link_dual_key(
        rows=rows,
        job_id=job_id,
        platform="telegram_post",
        cur=cur,
    )
    return inserted, skipped