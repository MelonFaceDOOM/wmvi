from __future__ import annotations

from datetime import datetime
from typing import Any

from .format import TranscriptRow

DEFAULT_BATCH_SIZE = 200


def count_exportable(cur, since_ts: datetime | None) -> int:
    if since_ts is None:
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM podcasts.episodes
            WHERE transcript IS NOT NULL
              AND btrim(transcript) <> ''
            """
        )
    else:
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM podcasts.episodes
            WHERE transcript IS NOT NULL
              AND btrim(transcript) <> ''
              AND transcript_updated_at > %s
            """,
            (since_ts,),
        )
    return int(cur.fetchone()[0])


def fetch_export_batch(
    cur,
    *,
    since_ts: datetime | None,
    until_ts: datetime,
    batch_size: int = DEFAULT_BATCH_SIZE,
    after_ts: datetime | None = None,
    after_id: str | None = None,
) -> list[TranscriptRow]:
    """
    Keyset-paginated fetch of episodes with transcripts updated in (since_ts, until_ts].
    """
    params: list[Any] = []
    conditions = [
        "transcript IS NOT NULL",
        "btrim(transcript) <> ''",
        "transcript_updated_at <= %s",
    ]
    params.append(until_ts)

    if since_ts is not None:
        conditions.append("transcript_updated_at > %s")
        params.append(since_ts)

    if after_ts is not None and after_id is not None:
        conditions.append(
            "(transcript_updated_at, id) > (%s, %s)"
        )
        params.extend([after_ts, after_id])

    sql = f"""
        SELECT id, transcript, transcript_updated_at
        FROM podcasts.episodes
        WHERE {' AND '.join(conditions)}
        ORDER BY transcript_updated_at, id
        LIMIT %s
    """
    params.append(batch_size)
    cur.execute(sql, params)
    rows: list[TranscriptRow] = []
    for episode_id, transcript, updated_at in cur.fetchall():
        rows.append(
            TranscriptRow(
                id=str(episode_id),
                transcript=str(transcript),
                transcript_updated_at=updated_at,
            )
        )
    return rows
