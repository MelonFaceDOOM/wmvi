from __future__ import annotations

from datetime import datetime
from typing import Any

from content_sync.format import PLATFORM_YOUTUBE_COMMENT
from content_sync.platforms.base import ImportStats


def _row_to_dict(record: tuple) -> dict[str, Any]:
    (
        video_id,
        comment_id,
        comment_url,
        text,
        filtered_text,
        created_at_ts,
        parent_comment_id,
        like_count,
        reply_count,
    ) = record
    return {
        "platform": PLATFORM_YOUTUBE_COMMENT,
        "key1": str(video_id),
        "key2": str(comment_id),
        "video_id": str(video_id),
        "comment_id": str(comment_id),
        "comment_url": str(comment_url),
        "text": str(text),
        "filtered_text": str(filtered_text),
        "created_at_ts": created_at_ts.isoformat()
        if isinstance(created_at_ts, datetime)
        else created_at_ts,
        "parent_comment_id": parent_comment_id,
        "like_count": like_count,
        "reply_count": reply_count,
    }


class YoutubeCommentHandler:
    platform = PLATFORM_YOUTUBE_COMMENT

    def export_delta(
        self,
        cur,
        *,
        since: datetime | None,
        until: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        conditions = ["c.date_entered <= %s"]
        params: list[Any] = [until]
        if since is not None:
            conditions.append("c.date_entered > %s")
            params.append(since)

        sql = f"""
            SELECT
                c.video_id,
                c.comment_id,
                c.comment_url,
                c.text,
                c.filtered_text,
                c.created_at_ts,
                c.parent_comment_id,
                c.like_count,
                c.reply_count
            FROM youtube.comment c
            WHERE {' AND '.join(conditions)}
            ORDER BY c.date_entered, c.video_id, c.comment_id
        """
        cur.execute(sql, params)
        rows = [_row_to_dict(r) for r in cur.fetchall()]
        return rows, {}

    def import_bundle(
        self,
        cur,
        *,
        rows: list[dict[str, Any]],
        sidecars: dict[str, list[dict[str, Any]]],
    ) -> ImportStats:
        stats = ImportStats()
        stats.rows_seen = len(rows)

        for row in rows:
            cur.execute(
                """
                INSERT INTO youtube.comment (
                    video_id,
                    comment_id,
                    comment_url,
                    text,
                    filtered_text,
                    created_at_ts,
                    parent_comment_id,
                    like_count,
                    reply_count
                )
                VALUES (
                    %(video_id)s,
                    %(comment_id)s,
                    %(comment_url)s,
                    %(text)s,
                    %(filtered_text)s,
                    %(created_at_ts)s::timestamptz,
                    %(parent_comment_id)s,
                    %(like_count)s,
                    %(reply_count)s
                )
                ON CONFLICT (video_id, comment_id) DO UPDATE SET
                    comment_url = EXCLUDED.comment_url,
                    text = EXCLUDED.text,
                    filtered_text = EXCLUDED.filtered_text,
                    created_at_ts = EXCLUDED.created_at_ts,
                    parent_comment_id = EXCLUDED.parent_comment_id,
                    like_count = EXCLUDED.like_count,
                    reply_count = EXCLUDED.reply_count
                """,
                row,
            )
            if cur.rowcount:
                stats.rows_upserted += 1

        return stats
