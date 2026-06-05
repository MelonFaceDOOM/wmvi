from __future__ import annotations

from datetime import datetime
from typing import Any

from db.post_registry_utils import ensure_post_registered
from content_sync.format import PLATFORM_YOUTUBE_VIDEO, SIDECAR_YOUTUBE_SEGMENTS
from content_sync.platforms.base import ImportStats

_UPDATE_TRANSCRIPT_SQL = """
UPDATE youtube.video
SET transcript = %s,
    transcript_updated_at = %s::timestamptz
WHERE video_id = %s
  AND (
        transcript IS NULL
     OR transcript_updated_at IS NULL
     OR %s::timestamptz > transcript_updated_at
  )
RETURNING video_id
"""


def _serialize_ts(val: Any) -> Any:
    if isinstance(val, datetime):
        return val.isoformat()
    return val


def _row_to_dict(record: tuple) -> dict[str, Any]:
    (
        video_id,
        url,
        title,
        description,
        created_at_ts,
        channel_id,
        channel_title,
        duration_iso,
        view_count,
        like_count,
        comment_count,
        duration_seconds,
        transcript,
        transcript_updated_at,
        date_entered,
    ) = record
    return {
        "platform": PLATFORM_YOUTUBE_VIDEO,
        "key1": str(video_id),
        "key2": "",
        "video_id": str(video_id),
        "url": str(url),
        "title": str(title),
        "description": description,
        "created_at_ts": _serialize_ts(created_at_ts),
        "channel_id": str(channel_id),
        "channel_title": channel_title,
        "duration_iso": duration_iso,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "duration_seconds": duration_seconds,
        "transcript": str(transcript) if transcript is not None else None,
        "transcript_updated_at": _serialize_ts(transcript_updated_at),
        "date_entered": _serialize_ts(date_entered),
    }


def _youtube_video_export_where(
    *,
    since: datetime | None,
    until: datetime,
) -> tuple[str, list[Any]]:
    """
    Export videos when any of:
      - newly ingested on GPU (date_entered window),
      - transcript appeared or was updated (transcript_updated_at window),
      - referenced by a comment in the comment export window (parent FK).
    """
    params: list[Any] = []
    branches: list[str] = []

    ingest_parts = ["v.date_entered <= %s"]
    params.append(until)
    if since is not None:
        ingest_parts.append("v.date_entered > %s")
        params.append(since)
    branches.append("(" + " AND ".join(ingest_parts) + ")")

    transcript_parts = [
        "v.transcript IS NOT NULL",
        "btrim(v.transcript) <> ''",
        "COALESCE(v.transcript_updated_at, v.date_entered) <= %s",
    ]
    params.append(until)
    if since is not None:
        transcript_parts.append(
            "COALESCE(v.transcript_updated_at, v.date_entered) > %s"
        )
        params.append(since)
    branches.append("(" + " AND ".join(transcript_parts) + ")")

    comment_parts = ["c.date_entered <= %s"]
    params.append(until)
    if since is not None:
        comment_parts.append("c.date_entered > %s")
        params.append(since)
    branches.append(
        "EXISTS ("
        "SELECT 1 FROM youtube.comment c "
        "WHERE c.video_id = v.video_id AND "
        + " AND ".join(comment_parts)
        + ")"
    )

    return "(" + " OR ".join(branches) + ")", params


class YoutubeVideoHandler:
    platform = PLATFORM_YOUTUBE_VIDEO

    def count_export_delta(
        self,
        cur,
        *,
        since: datetime | None,
        until: datetime,
    ) -> tuple[int, dict[str, int]]:
        where, params = _youtube_video_export_where(since=since, until=until)
        cur.execute(
            f"SELECT COUNT(*)::bigint FROM youtube.video v WHERE {where}",
            params,
        )
        video_count = int(cur.fetchone()[0])
        sidecars: dict[str, int] = {}
        if video_count:
            cur.execute(
                f"""
                SELECT COUNT(*)::bigint
                FROM youtube.transcript_segments s
                WHERE EXISTS (
                    SELECT 1
                    FROM youtube.video v
                    WHERE v.video_id = s.video_id
                      AND {where}
                )
                """,
                params,
            )
            sidecars[SIDECAR_YOUTUBE_SEGMENTS] = int(cur.fetchone()[0])
        return video_count, sidecars

    def export_delta(
        self,
        cur,
        *,
        since: datetime | None,
        until: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        where, params = _youtube_video_export_where(since=since, until=until)

        sql = f"""
            SELECT
                v.video_id,
                v.url,
                v.title,
                v.description,
                v.created_at_ts,
                v.channel_id,
                v.channel_title,
                v.duration_iso,
                v.view_count,
                v.like_count,
                v.comment_count,
                v.duration_seconds,
                v.transcript,
                v.transcript_updated_at,
                v.date_entered
            FROM youtube.video v
            WHERE {where}
            ORDER BY v.date_entered, v.video_id
        """
        cur.execute(sql, params)
        rows = [_row_to_dict(r) for r in cur.fetchall()]

        video_ids = [r["video_id"] for r in rows]
        segments: list[dict[str, Any]] = []
        if video_ids:
            cur.execute(
                """
                SELECT video_id, seg_idx, start_s, end_s, text
                FROM youtube.transcript_segments
                WHERE video_id = ANY(%s)
                ORDER BY video_id, seg_idx
                """,
                (video_ids,),
            )
            for vid, seg_idx, start_s, end_s, text in cur.fetchall():
                segments.append(
                    {
                        "video_id": str(vid),
                        "seg_idx": int(seg_idx),
                        "start_s": float(start_s),
                        "end_s": float(end_s),
                        "text": text,
                    }
                )

        return rows, {SIDECAR_YOUTUBE_SEGMENTS: segments}

    def import_bundle(
        self,
        cur,
        *,
        rows: list[dict[str, Any]],
        sidecars: dict[str, list[dict[str, Any]]],
    ) -> ImportStats:
        stats = ImportStats()
        stats.rows_seen = len(rows)
        updated_video_ids: set[str] = set()

        for row in rows:
            cur.execute(
                """
                INSERT INTO youtube.video (
                    video_id, url, title, description, created_at_ts,
                    channel_id, channel_title, duration_iso,
                    view_count, like_count, comment_count, duration_seconds
                )
                VALUES (
                    %(video_id)s, %(url)s, %(title)s, %(description)s,
                    %(created_at_ts)s::timestamptz,
                    %(channel_id)s, %(channel_title)s, %(duration_iso)s,
                    %(view_count)s, %(like_count)s, %(comment_count)s,
                    %(duration_seconds)s
                )
                ON CONFLICT (video_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    created_at_ts = EXCLUDED.created_at_ts,
                    channel_id = EXCLUDED.channel_id,
                    channel_title = EXCLUDED.channel_title,
                    duration_iso = EXCLUDED.duration_iso,
                    view_count = EXCLUDED.view_count,
                    like_count = EXCLUDED.like_count,
                    comment_count = EXCLUDED.comment_count,
                    duration_seconds = EXCLUDED.duration_seconds
                """,
                row,
            )
            stats.rows_upserted += 1

            ts = row.get("transcript_updated_at")
            transcript = row.get("transcript")
            if not transcript or not ts:
                continue
            cur.execute(
                _UPDATE_TRANSCRIPT_SQL,
                (transcript, ts, row["video_id"], ts),
            )
            if cur.fetchone():
                updated_video_ids.add(row["video_id"])
                ensure_post_registered(
                    cur, platform=PLATFORM_YOUTUBE_VIDEO, key1=row["video_id"]
                )
                stats.posts_registered += 1

        stats.transcripts_updated = len(updated_video_ids)

        seg_rows = sidecars.get(SIDECAR_YOUTUBE_SEGMENTS) or []
        seg_by_video: dict[str, list[dict[str, Any]]] = {}
        for s in seg_rows:
            seg_by_video.setdefault(s["video_id"], []).append(s)

        for video_id, segs in seg_by_video.items():
            if video_id not in updated_video_ids:
                continue
            cur.execute(
                "DELETE FROM youtube.transcript_segments WHERE video_id = %s",
                (video_id,),
            )
            cur.executemany(
                """
                INSERT INTO youtube.transcript_segments (
                    video_id, seg_idx, start_s, end_s, text
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (
                        video_id,
                        int(s["seg_idx"]),
                        s["start_s"],
                        s["end_s"],
                        s["text"],
                    )
                    for s in sorted(segs, key=lambda x: x["seg_idx"])
                ],
            )
            stats.segments_replaced += 1

        return stats
