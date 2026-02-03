from __future__ import annotations

from db.db import getcursor
from ingestion.ingestion import insert_batch_return_inserted, bulk_link_dual_key

"""Expects data cols to match these, even though yt uses some diff values
i.e. published_at (yt) should be converted to created_at_ts"""

YOUTUBE_COMMENT_COLS = [
    "video_id",
    "comment_id",
    "parent_comment_id",
    "comment_url",
    "text",
    "filtered_text",
    "created_at_ts",
    "like_count",
    "reply_count",
]

YOUTUBE_COMMENT_INSERT_SQL = f"""
    INSERT INTO youtube.comment(
        {", ".join(YOUTUBE_COMMENT_COLS)}
    ) VALUES %s
    ON CONFLICT (video_id, comment_id) DO NOTHING
    RETURNING video_id, comment_id
"""


def flush_youtube_comment_batch(
    rows: list[dict],
    job_id: int,
    cur=None
) -> tuple[int, int, set[tuple[str, str]]]:
    """
    Insert a batch of YouTube comments and link them to a scrape job.

    In a single transaction:
      1) Bulk insert rows into youtube.comment using ON CONFLICT DO NOTHING
         and RETURNING (video_id, comment_id) so we can identify which were new.
      2) Ensure scrape.post_scrape links exist for *all attempted* (video_id, comment_id)
         pairs (inserted + skipped), which is desired.

    Returns:
      (inserted, skipped, inserted_keys)
        inserted_keys is a set of (video_id, comment_id) for newly inserted rows.
    """
    if not rows:
        return 0, 0, set()

    # Sanity check: if parent_comment_id exists, comment_id must exist (your original intent)
    for d in rows:
        if d.get("parent_comment_id") and not d.get("comment_id"):
            raise ValueError("Reply comment missing comment_id")

    # Build linkage keys for all attempted rows
    key1_list: list[str] = []
    key2_list: list[str] = []
    for d in rows:
        vid = d.get("video_id")
        cid = d.get("comment_id")
        if vid is None or cid is None:
            continue
        key1_list.append(str(vid))
        key2_list.append(str(cid))

    if not key1_list:
        return 0, 0, set()

    def _run(cur):
        inserted, skipped, inserted_keys = insert_batch_return_inserted(
            insert_sql=YOUTUBE_COMMENT_INSERT_SQL,
            rows=rows,
            returning_cols=("video_id", "comment_id"),
            cols=YOUTUBE_COMMENT_COLS,
            json_cols=["raw"],
            cur=cur,
        )

        bulk_link_dual_key(
            job_id=job_id,
            platform="youtube_comment",
            key1_values=key1_list,
            key2_values=key2_list,
            cur=cur,
        )

        # type narrow: inserted_keys contains tuples[str,...] already, but we want tuples[str,str]
        inserted_keys2: set[tuple[str, str]] = set()
        for k in inserted_keys:
            if len(k) == 2:
                inserted_keys2.add((k[0], k[1]))

        return inserted, skipped, inserted_keys2
    if not cur:
        with getcursor() as cur2:
            return _run(cur2)
    return _run(cur)
