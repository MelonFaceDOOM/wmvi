from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from ingestion.ingestion import flush_and_link_single_key
from ingestion.row_model import InsertableRow


@dataclass(frozen=True, slots=True)
class RedditSubmissionRow(InsertableRow):
    TABLE = "sm.reddit_submission"
    PK = ("id",)

    id: str
    url: str
    domain: str
    title: str
    created_at_ts: datetime
    filtered_text: str
    subreddit_id: str
    subreddit: str
    upvote_ratio: float
    score: int
    gilded: int
    num_comments: int
    num_crossposts: int
    shared_url: str | None = None
    permalink: str | None = None
    selftext: str | None = None
    url_overridden_by_dest: str | None = None
    pinned: bool = False
    stickied: bool = False
    over_18: bool = False
    is_created_from_ads_ui: bool = False
    is_self: bool = False
    is_video: bool = False
    media: dict[str, Any] | None = field(default=None, metadata={"json": True})
    gildings: dict[str, Any] | None = field(default=None, metadata={"json": True})
    all_awardings: list[Any] | None = field(default=None, metadata={"json": True})


def flush_reddit_submission_batch(rows: list[RedditSubmissionRow], job_id: int, cur=None):
    ins, skip, _ids = flush_and_link_single_key(rows=rows, job_id=job_id, platform="reddit_submission", cur=cur)
    return ins, skip
