from __future__ import annotations

from datetime import datetime
from typing import Any

from .format import EpisodeExportRow
from .rss_url import normalize_rss_url

DEFAULT_BATCH_SIZE = 200

_EXPORT_SELECT = """
    SELECT
        e.id,
        e.guid,
        e.download_url,
        e.created_at_ts,
        e.title,
        e.description,
        e.transcript,
        e.transcript_updated_at,
        e.podcast_id,
        s.rss_url
    FROM podcasts.episodes e
    INNER JOIN podcasts.shows s ON s.id = e.podcast_id
"""


def count_exportable(cur, since_ts: datetime | None) -> int:
    conditions = [
        "e.transcript IS NOT NULL",
        "btrim(e.transcript) <> ''",
        "s.rss_url IS NOT NULL",
        "btrim(s.rss_url) <> ''",
    ]
    params: list[Any] = []
    if since_ts is not None:
        conditions.append("e.transcript_updated_at > %s")
        params.append(since_ts)

    sql = f"""
        SELECT COUNT(*)::bigint
        FROM podcasts.episodes e
        INNER JOIN podcasts.shows s ON s.id = e.podcast_id
        WHERE {' AND '.join(conditions)}
    """
    cur.execute(sql, params)
    return int(cur.fetchone()[0])


def fetch_export_batch(
    cur,
    *,
    since_ts: datetime | None,
    until_ts: datetime,
    batch_size: int = DEFAULT_BATCH_SIZE,
    after_ts: datetime | None = None,
    after_id: str | None = None,
) -> list[EpisodeExportRow]:
    """
    Episodes with transcripts updated in (since_ts, until_ts], show must have rss_url.
    """
    params: list[Any] = []
    conditions = [
        "e.transcript IS NOT NULL",
        "btrim(e.transcript) <> ''",
        "e.transcript_updated_at <= %s",
        "s.rss_url IS NOT NULL",
        "btrim(s.rss_url) <> ''",
    ]
    params.append(until_ts)

    if since_ts is not None:
        conditions.append("e.transcript_updated_at > %s")
        params.append(since_ts)

    if after_ts is not None and after_id is not None:
        conditions.append("(e.transcript_updated_at, e.id) > (%s, %s)")
        params.extend([after_ts, after_id])

    sql = f"""
        {_EXPORT_SELECT}
        WHERE {' AND '.join(conditions)}
        ORDER BY e.transcript_updated_at, e.id
        LIMIT %s
    """
    params.append(batch_size)
    cur.execute(sql, params)

    rows: list[EpisodeExportRow] = []
    for (
        episode_id,
        guid,
        download_url,
        created_at_ts,
        title,
        description,
        transcript,
        updated_at,
        podcast_id,
        rss_url,
    ) in cur.fetchall():
        canonical_rss = normalize_rss_url(str(rss_url))
        if canonical_rss is None:
            continue
        rows.append(
            EpisodeExportRow(
                show_rss_url=canonical_rss,
                guid=str(guid) if guid is not None else None,
                download_url=str(download_url) if download_url is not None else None,
                created_at_ts=created_at_ts,
                title=str(title) if title is not None else None,
                description=str(description) if description is not None else None,
                transcript=str(transcript),
                transcript_updated_at=updated_at,
                source_show_id=int(podcast_id),
                source_episode_id=str(episode_id),
            )
        )
    return rows
