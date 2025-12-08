from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch, bulk_link_dual_key

TELEGRAM_COLS = [
    "channel_id",
    "message_id",
    "link",
    "text",
    "filtered_text",
    "created_at_ts",
    "views",
    "forwards",
    "replies",
    "reactions_total",
    "is_pinned",
    "has_media",
    "raw_type",
    "is_en",
]

TELEGRAM_INSERT_SQL = """
    INSERT INTO sm.telegram_post (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT (channel_id, message_id) DO NOTHING
""".format(
    cols=", ".join(TELEGRAM_COLS),
    vals=", ".join(f"%({c})s" for c in TELEGRAM_COLS),
)

def flush_telegram_batch(rows: list[dict], job_id: int) -> tuple[int, int]:
    """
    `rows` is a list of dicts keyed by TELEGRAM_COLS.
    job_id is the id of the scrape_job associated with this batch
    In a single transaction:
        Insert a batch of telegram posts into sm.telegram_post.
        For each inserted telegram post, ensure scrape.post_scrape link exists.

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
        # 1) Insert posts
        inserted, skipped = insert_batch(
            insert_sql=TELEGRAM_INSERT_SQL,
            rows=rows,
            cur=cur
        )

        # 2) Batched linkage of post_registry to scrape job
        # If any telegram posts already existed,
        # and therefore didn't get entered in step 1,
        # they will still be linked to the scrape job here,
        # which is desired (1 post can be linked to multiple scrape jobs)
        key1_list: list[str] = []
        key2_list: list[str] = []
        for d in rows:
            ch = d.get("channel_id")
            msg = d.get("message_id")
            if ch is None or msg is None:
                continue
            key1_list.append(str(ch))
            key2_list.append(str(msg))

        if not key1_list:
            return inserted, skipped
            
        bulk_link_dual_key(
            job_id=job_id,
            platform="telegram_post",
            key1_values=key1_list,
            key2_values=key2_list,
            cur=cur,
        )

        return inserted, skipped