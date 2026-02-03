from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import bulk_link_single_key, insert_batch_return_inserted

YOUTUBE_VIDEO_COLS = [
    "video_id",
    "url",
    "title",
    "description",
    "created_at_ts",
    "channel_id",
    "channel_title",
    "duration_iso",
    "view_count",
    "like_count",
    "comment_count",
]

YOUTUBE_VIDEO_INSERT_SQL = f"""
    INSERT INTO youtube.video (
        {", ".join(YOUTUBE_VIDEO_COLS)}
    ) VALUES %s
    ON CONFLICT (video_id) DO NOTHING
    RETURNING video_id
"""


def flush_youtube_video_batch(
    rows: list[dict],
    job_id: int,
    cur=None
) -> tuple[int, int, set[str]]:
    """
    Insert a batch of YouTube videos and link them to a scrape job.

    Inputs:
      - rows: list[dict] keyed by YOUTUBE_VIDEO_COLS
      - job_id: scrape.scrape_job.id associated with this batch

    In a single transaction:
      1) Bulk insert rows into youtube.video (or your target table) using
         ON CONFLICT DO NOTHING and RETURNING video_id so we can identify
         which rows were newly inserted.
      2) Ensure scrape.post_scrape links exist for *all* attempted video_ids
         (including those skipped due to existing rows), which is desired
         because a post can be linked to multiple scrape jobs.

    Returns:
      (inserted, skipped, inserted_ids) where:
        - inserted: number of rows actually inserted
        - skipped:  number of rows skipped due to ON CONFLICT
        - inserted_ids: set of video_id strings that were newly inserted

    Assumes:
      - DB pool has been initialized (db.init_pool)
      - YOUTUBE_VIDEO_INSERT_SQL inserts into the correct table and uses:
            ON CONFLICT (...) DO NOTHING
      - YOUTUBE_VIDEO_COLS includes "video_id"
    """
    if not rows:
        return 0, 0, set()

    video_ids = [str(d["video_id"])
                 for d in rows if d.get("video_id") is not None]
    if not video_ids:
        return 0, 0, set()

    def _run(cur):
        inserted, skipped, inserted_ids = insert_batch_return_inserted(
            insert_sql=YOUTUBE_VIDEO_INSERT_SQL,
            rows=rows,
            returning_cols=["video_id"],
            cols=YOUTUBE_VIDEO_COLS,
            json_cols=None,
            cur=cur,
        )

        bulk_link_single_key(
            job_id=job_id,
            platform="youtube_video",
            key1_values=video_ids,
            cur=cur,
        )
        inserted_ids = {
            x[0] if isinstance(x, tuple) else x
            for x in inserted_ids
        }
        return inserted, skipped, inserted_ids

    if not cur:
        with getcursor() as cur2:
            return _run(cur2)

    return _run(cur)
