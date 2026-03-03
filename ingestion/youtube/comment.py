from __future__ import annotations
from datetime import datetime
from db.db import getcursor
from ingestion.row_model import InsertableRow
from ingestion.ingestion import ensure_scrape_job, flush_and_link_dual_key

from dataclasses import dataclass, field

@dataclass(frozen=True, slots=True)
class YoutubeCommentRow(InsertableRow):
    TABLE = "youtube.comment"
    PK = ("video_id", "comment_id")

    video_id: str
    comment_id: str
    comment_url: str
    text: str
    filtered_text: str
    created_at_ts: datetime
    parent_comment_id: str | None = None
    like_count: int | None = None
    reply_count: int | None = None

def flush_youtube_comment_batch(rows: list[YoutubeCommentRow], job_id: int, cur=None):
    return flush_and_link_dual_key(rows=rows, job_id=job_id, platform="youtube_comment", cur=cur)


def save_comments(
    comments: list[YoutubeCommentRow],
    *,
    term_name: str,
    cur=None,
) -> tuple[int, int, set[tuple[str, str]]]:
    """
    Persist normalized comments via ingestion layer.
    Returns (inserted, skipped, inserted_comment_ids which is (video_id, comment_id)).

    If cur is provided, uses it. Otherwise opens its own committing cursor.
    """
    if not comments:
        return 0, 0, set()

    # Convert dict -> YoutubeCommentRow if needed
    first = comments[0]
    if not isinstance(first, YoutubeCommentRow):
        raise TypeError("comments must be a YoutubeCommentRow")

    job_id = ensure_scrape_job(
        name=f"youtube comments monitor: {term_name}",
        description=f"YouTube comment scrape for term {term_name!r}",
        platforms=["youtube_comment"],
    )

    if cur is None:
        with getcursor(commit=True) as cur2:
            return flush_youtube_comment_batch(rows=comments, job_id=job_id, cur=cur2)

    return flush_youtube_comment_batch(rows=comments, job_id=job_id, cur=cur)