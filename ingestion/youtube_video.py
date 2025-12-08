from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch, bulk_link_single_key



YOUTUBE_VIDEO_COLS = [
    "video_id",
    "url",
    "title",
    "filtered_text",
    "description",
    "created_at_ts",
    "channel_id",
    "channel_title",
    "duration_iso",
    "view_count",
    "like_count",
    "comment_count",
    "is_en",
]

YOUTUBE_VIDEO_INSERT_SQL = """
    INSERT INTO sm.youtube_video (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT (video_id) DO NOTHING
""".format(
    cols=", ".join(YOUTUBE_VIDEO_COLS),
    vals=", ".join(f"%({c})s" for c in YOUTUBE_VIDEO_COLS),
)


def flush_youtube_video_batch(rows: list[dict], job_id: int) -> tuple[int, int]:
    """
    `rows` is a list of dicts keyed by YOUTUBE_VIDEO_COLS.
    job_id is the id of the scrape_job associated with this batch
    In a single transaction:
        Insert a batch of yt vids into sm.youtube_video.
        For each inserted yt vid, ensure scrape.post_scrape link exists.

    Returns:
        (inserted, skipped) where:
          - inserted = number of rows actually inserted
          - skipped  = number of rows skipped due to ON CONFLICT

    Assumes:
      - DB pool has been initialized (db.init_pool)
    """
    if not rows:
        return 0, 0

    with getcursor(commit=True) as cur:
        # getcursor() will auto-rollback if anything fails
        # and autocommit if everything succeeds
        # 1) Insert videos
        inserted, skipped = insert_batch(
            insert_sql=YOUTUBE_VIDEO_INSERT_SQL,
            rows=rows,
            cur=cur
        )

        # 2) Batched linkage of post_registry to scrape job
        # If any videos already existed,
        # and therefore didn't get entered in step 1,
        # they will still be linked to the scrape job here,
        # which is desired (1 post can be linked to multiple scrape jobs)
        
        video_ids = [str(d["video_id"]) for d in rows]
        bulk_link_single_key(
            job_id=job_id,
            platform="youtube_video",
            key1_values=video_ids,
            cur=cur,
        )

        return inserted, skipped
