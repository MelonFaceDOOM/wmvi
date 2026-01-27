from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch, bulk_link_single_key

REDDIT_SUB_COLS = [
    "id",
    "url",
    "domain",
    "title",
    "permalink",
    "created_at_ts",
    "filtered_text",
    "url_overridden_by_dest",
    "subreddit_id",
    "subreddit",
    "upvote_ratio",
    "score",
    "gilded",
    "num_comments",
    "num_crossposts",
    "pinned",
    "stickied",
    "over_18",
    "is_created_from_ads_ui",
    "is_self",
    "is_video",
    "media",
    "gildings",
    "all_awardings",
]

REDDIT_SUB_INSERT_SQL = """
    INSERT INTO sm.reddit_submission (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT DO NOTHING
""".format(
    cols=", ".join(REDDIT_SUB_COLS),
    vals=", ".join(f"%({c})s" for c in REDDIT_SUB_COLS),
)


def flush_reddit_submission_batch(rows: list[dict], job_id: int) -> tuple[int, int]:
    """
    `rows` is a list of dicts keyed by REDDIT_SUB_COLS.
    job_id is the id of the scrape_job associated with this batch
    In a single transaction:
        Insert a batch of submissions into sm.reddit_submission.
        For each inserted submission, ensure scrape.post_scrape link exists.

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
        # 1) Insert submissions
        inserted, skipped = insert_batch(
            insert_sql=REDDIT_SUB_INSERT_SQL,
            rows=rows,
            json_cols=["media", "gildings", "all_awardings"],
            cur=cur
        )

        # 2) Batched linkage of post_registry to scrape job
        # If any submissions already existed,
        # and therefore didn't get entered in step 1,
        # they will still be linked to the scrape job here,
        # which is desired (1 post can be linked to multiple scrape jobs)

        submission_ids = [str(d["id"]) for d in rows]
        bulk_link_single_key(
            job_id=job_id,
            platform="reddit_submission",
            key1_values=submission_ids,
            cur=cur,
        )

        return inserted, skipped
