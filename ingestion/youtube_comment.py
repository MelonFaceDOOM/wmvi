from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch, bulk_link_dual_key

YOUTUBE_COMMENT_COLS = [
    "video_id",
    "comment_id",
    "comment_url",
    "text",
    "filtered_text",
    "created_at_ts",
    "like_count",
    "raw",
    "is_en",
]

YOUTUBE_COMMENT_INSERT_SQL = """
    INSERT INTO sm.youtube_comment (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT (video_id, comment_id) DO NOTHING
""".format(
    cols=", ".join(YOUTUBE_COMMENT_COLS),
    vals=", ".join(f"%({c})s" for c in YOUTUBE_COMMENT_COLS),
)


def flush_youtube_comment_batch(rows: list[dict], job_id: int) -> tuple[int, int]:
    """
    `rows` is a list of dicts keyed by YOUTUBE_COMMENT_COLS.
    job_id is the id of the scrape_job associated with this batch
    In a single transaction:
        Insert a batch of youtube comments into sm.youtube_comment.
        For each inserted youtube comment, ensure scrape.post_scrape link exists.

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
        # 1) Insert comments
        inserted, skipped = insert_batch(
            insert_sql=YOUTUBE_COMMENT_INSERT_SQL,
            rows=rows,
            json_cols=["raw"],
            cur=cur,
        )

        # 2) Batched linkage of post_registry to scrape job
        # If any youtube comments already existed,
        # and therefore didn't get entered in step 1,
        # they will still be linked to the scrape job here,
        # which is desired (1 post can be linked to multiple scrape jobs)
        
        key1_list: list[str] = []
        key2_list: list[str] = []
        for d in rows:
            vid = d.get("video_id")
            cid = d.get("comment_id")
            if vid is None or cid is None:
                continue
            key1_list.append(str(vid))
            key2_list.append(str(cid))

        bulk_link_dual_key(
            job_id=job_id,
            platform="youtube_comment",
            key1_values=key1_list,
            key2_values=key2_list,
            cur=cur,
        )

        return inserted, skipped
