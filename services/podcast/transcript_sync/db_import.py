from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

from psycopg2.extras import execute_values

from db.post_registry_utils import ensure_post_registered

log = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500

_UPDATE_SQL = """
UPDATE podcasts.episodes p
SET transcript = v.transcript,
    transcript_updated_at = v.transcript_updated_at
FROM (VALUES %s) AS v(id, transcript, transcript_updated_at)
WHERE p.id = v.id
  AND (
        p.transcript IS NULL
     OR p.transcript_updated_at IS NULL
     OR v.transcript_updated_at > p.transcript_updated_at
  )
RETURNING p.id;
"""


@dataclass
class TranscriptApplyRow:
    episode_id: str
    transcript: str
    transcript_updated_at: object


@dataclass
class BatchImportResult:
    seen: int
    updated: int
    registered: int


def apply_transcript_batch(
    cur,
    rows: Sequence[TranscriptApplyRow],
) -> BatchImportResult:
    if not rows:
        return BatchImportResult(seen=0, updated=0, registered=0)

    values = [
        (r.episode_id, r.transcript, r.transcript_updated_at)
        for r in rows
    ]
    template = "(%s, %s, %s::timestamptz)"
    execute_values(
        cur,
        _UPDATE_SQL,
        values,
        template=template,
        page_size=len(values),
        fetch=True,
    )
    updated_ids = {str(row[0]) for row in cur.fetchall()}
    registered = 0
    for episode_id in updated_ids:
        ensure_post_registered(
            cur,
            platform="podcast_episode",
            key1=episode_id,
        )
        registered += 1

    return BatchImportResult(
        seen=len(rows),
        updated=len(updated_ids),
        registered=registered,
    )
