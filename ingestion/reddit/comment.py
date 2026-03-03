from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from ingestion.ingestion import flush_and_link_single_key
from ingestion.row_model import InsertableRow


@dataclass(frozen=True, slots=True)
class RedditCommentRow(InsertableRow):
    TABLE = "sm.reddit_comment"
    PK = ("id",)

    id: str
    link_id: str
    body: str
    permalink: str
    created_at_ts: datetime
    filtered_text: str
    subreddit_id: str
    total_awards_received: int
    subreddit: str
    score: int
    gilded: int
    parent_comment_id: str | None = None
    subreddit_type: str | None = None
    stickied: bool = False
    is_submitter: bool = False
    gildings: Any | None = field(default=None, metadata={"json": True})
    all_awardings: Any | None = field(default=None, metadata={"json": True})


def flush_reddit_comment_batch(rows: list[RedditCommentRow], job_id: int, cur=None):
    ins, skip, _ids = flush_and_link_single_key(rows=rows, job_id=job_id, platform="reddit_comment", cur=cur)
    return ins, skip

# ------- some parsing helpers. dunno if they will go somewhere else later ------

def parse_link_id(raw: str | None) -> str:
    """
    Normalize a Reddit submission id to 't3_<id>' form.

    - If already starts with 't3_', return as-is.
    - If bare (no 't*_' prefix), assume it is a submission id and prepend 't3_'.
    - If it starts with 't1_' (comment), or is empty/None, raise ValueError.
    """
    if raw is None:
        raise ValueError("Missing link_id for submission")

    s = str(raw).strip()
    if not s:
        raise ValueError("Empty link_id for submission")

    if s.startswith("t1_"):
        raise ValueError(
            "comment link value provided for link_id, which should always be a submission id"
        )

    return s if s.startswith("t3_") else f"t3_{s}"


def parse_comment_id(raw: str | None) -> str | None:
    """
    Normalize a Reddit comment id to 't1_<id>' form or return None.

    Expected inputs:
    - If already starts with 't1_', return as-is.
    - If starts bare or starts with 't3_' (parent is a submission), return None.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.startswith("t3_"):
        return None
    return s if s.startswith("t1_") else f"t1_{s}"
