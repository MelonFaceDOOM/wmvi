from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch, bulk_link_single_key

REDDIT_COMMENT_COLS = [
    "id",
    "parent_comment_id",
    "link_id",
    "body",
    "permalink","created_at_ts",
    "filtered_text",
    "subreddit_id",
    "subreddit_type",
    "total_awards_received",
    "subreddit",
    "score",
    "gilded",
    "stickied",
    "is_submitter",
    "gildings",
    "all_awardings",
    "is_en",
]

REDDIT_COMMENT_INSERT_SQL = """
    INSERT INTO sm.reddit_comment (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT DO NOTHING
""".format(
    cols=", ".join(REDDIT_COMMENT_COLS),
    vals=", ".join(f"%({c})s" for c in REDDIT_COMMENT_COLS),
)


def flush_reddit_comment_batch(rows: list[dict], job_id: int) -> tuple[int, int]:
    """
    `rows` is a list of dicts keyed by REDDIT_COMMENT_COLS.
    job_id is the id of the scrape_job associated with this batch
    In a single transaction:
        Insert a batch of comments into sm.reddit_comment.
        For each inserted comment, ensure scrape.post_scrape link exists.

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
            insert_sql=REDDIT_COMMENT_INSERT_SQL,
            rows=rows,
            json_cols=["gildings", "all_awardings"],
            cur=cur
        )

        # 2) Batched linkage of post_registry to scrape job
        # If any comments already existed,
        # and therefore didn't get entered in step 1,
        # they will still be linked to the scrape job here,
        # which is desired (1 post can be linked to multiple scrape jobs)
        
        comment_ids = [str(d["id"]) for d in rows]
        bulk_link_single_key(
            job_id=job_id,
            platform="reddit_comment",
            key1_values=comment_ids,
            cur=cur,
        )
        return inserted, skipped
        
        
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