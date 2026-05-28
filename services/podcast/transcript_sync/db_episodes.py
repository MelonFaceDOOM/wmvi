from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from psycopg2.extras import execute_values

from .format import EpisodeExportRow


@dataclass(frozen=True)
class EpisodeInsertRow:
    id: str
    podcast_id: int
    guid: str | None
    title: str | None
    description: str | None
    created_at_ts: object
    download_url: str | None


_INSERT_EPISODES_SQL = """
INSERT INTO podcasts.episodes (
    id,
    podcast_id,
    guid,
    title,
    description,
    created_at_ts,
    download_url
)
VALUES %s
ON CONFLICT (id) DO NOTHING
RETURNING id
"""


def insert_new_episodes(cur, rows: Sequence[EpisodeInsertRow]) -> int:
    if not rows:
        return 0

    values = [
        (
            r.id,
            r.podcast_id,
            r.guid,
            r.title,
            r.description,
            r.created_at_ts,
            r.download_url,
        )
        for r in rows
    ]
    template = "(%s, %s, %s, %s, %s, %s::timestamptz, %s)"
    execute_values(
        cur,
        _INSERT_EPISODES_SQL,
        values,
        template=template,
        page_size=len(values),
        fetch=True,
    )
    return len(cur.fetchall())


def episode_insert_from_export(
    podcast_id: int,
    episode_id: str,
    row: EpisodeExportRow,
) -> EpisodeInsertRow:
    guid = (row.guid or "").strip() or None
    download_url = (row.download_url or "").strip() or None
    return EpisodeInsertRow(
        id=episode_id,
        podcast_id=podcast_id,
        guid=guid,
        title=row.title,
        description=row.description,
        created_at_ts=row.created_at_ts,
        download_url=download_url,
    )
