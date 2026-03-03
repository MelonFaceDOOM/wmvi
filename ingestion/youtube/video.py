from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from datetime import datetime

from db.db import getcursor
from ingestion.ingestion import ensure_scrape_job, flush_and_link_single_key
from ingestion.row_model import InsertableRow

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class YoutubeVideoRow(InsertableRow):
    TABLE = "youtube.video"
    PK = ("video_id",)

    video_id: str
    url: str
    title: str
    created_at_ts: datetime
    channel_id: str
    description: str | None = None
    channel_title: str | None = None
    duration_iso: str | None = None
    view_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None

def flush_youtube_video_batch(rows: list[YoutubeVideoRow], job_id: int, cur=None):
    ins, skip, ids = flush_and_link_single_key(rows=rows, job_id=job_id, platform="youtube_video", cur=cur)
    return ins, skip, ids


def sample_video_debug(videos: list[dict], limit: int = 3) -> list[dict[str, Any]]:
    """
    Return a tiny sample of fields useful for debugging insert failures.
    (Accepts dict input since failures usually happen before conversion.)
    """
    out: list[dict[str, Any]] = []
    for v in videos[:limit]:
        title = v.get("title")
        out.append(
            {
                "video_id": v.get("video_id"),
                "created_at_ts": v.get("created_at_ts"),
                "channel_id": v.get("channel_id"),
                "title": (title[:80] + "...") if isinstance(title, str) and len(title) > 80 else title,
                "keys": sorted(list(v.keys()))[:30],
            }
        )
    return out


def save_videos(
    videos: list[YoutubeVideoRow],
    *,
    term_name: str,
    cur=None,
) -> tuple[int, int, set[str]]:
    """
    Persist normalized videos via ingestion layer.
    Returns (inserted, skipped, inserted_video_ids).

    If cur is provided, uses it. Otherwise opens its own committing cursor.
    """
    if not videos:
        return 0, 0, set()

    # Convert dict -> YoutubeVideoRow if needed
    first = videos[0]
    if not isinstance(first, YoutubeVideoRow):
        raise TypeError("videos must be a list of YoutubeVideoRow")

    job_id = ensure_scrape_job(
        name=f"youtube monitor: {term_name}",
        description=f"Continuous YouTube monitor scrape for term {term_name!r}",
        platforms=["youtube_video"],
    )

    def _run(cur2):
        inserted, skipped, inserted_ids = flush_youtube_video_batch(
            rows=videos,  # type: ignore[arg-type]
            job_id=job_id,
            cur=cur2,
        )
        return inserted, skipped, inserted_ids

    if cur is None:
        with getcursor(commit=True) as cur2:
            return _run(cur2)
    return _run(cur)


def save_all_videos_on_pages(pages, term_name: str):
    """
    Loop over pages, save vids to DB, return:
      (newly_inserted_videos, inserted_count, skipped_count)
    """
    new_vids: list[YoutubeVideoRow] = []
    inserted_total = 0
    skipped_total = 0

    for page in pages:
        vids = page.get("videos") or []  # expects videos to be YotubeVideoRow objects
        if not vids:
            reason = page.get("stopped_reason")
            if reason:
                log.info("Stopped reason for term %r: %s", term_name, reason)
            else:
                log.warning("No videos found on search page.")
            continue

        inserted, skipped, inserted_ids = save_videos(vids, term_name=term_name)
        inserted_total += inserted
        skipped_total += skipped

        if inserted_ids:
            new_vids.extend([v for v in vids if v.video_id in inserted_ids])

    return new_vids, inserted_total, skipped_total