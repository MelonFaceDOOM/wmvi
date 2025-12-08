from __future__ import annotations

from typing import List, Dict, Tuple

from db.db import getcursor
from ingestion.ingestion import insert_batch

"""
unlike other ingestion modules,
podcasts do not end up in post_registry
and they are not linked to scrape job
podcasts are just podcasts
"""

# -------------------------
# podcasts.shows
# -------------------------

PODCAST_SHOWS_COLS = [
    "id",
    "date_entered",
    "title",
    "rss_url",
]

PODCAST_SHOWS_INSERT_SQL = """
    INSERT INTO podcasts.shows (
        id,
        date_entered,
        title,
        rss_url
    )
    OVERRIDING SYSTEM VALUE
    VALUES (
        %(id)s,
        %(date_entered)s,
        %(title)s,
        %(rss_url)s
    )
    ON CONFLICT DO NOTHING
"""


def flush_podcast_shows_batch(rows: List[Dict]) -> Tuple[int, int]:
    """
    Insert a batch of podcast shows into podcasts.shows.

    rows: list of dicts with keys PODCAST_SHOWS_COLS:

    Returns:
        (inserted, skipped) where:
          - inserted = number of rows actually inserted
          - skipped  = number of rows skipped due to ON CONFLICT (id)

    Assumes:
      - DB pool has been initialized (db.init_pool)
    """
    if not rows:
        return 0, 0

    with getcursor(commit=True) as cur:
        inserted, skipped = insert_batch(
            insert_sql=PODCAST_SHOWS_INSERT_SQL,
            rows=rows,
            cur=cur,
        )
        return inserted, skipped


# -------------------------
# podcasts.episodes
# -------------------------

PODCAST_EPISODE_COLS = [
    "id",
    "date_entered",
    "audio_path",
    "guid",
    "title",
    "description",
    "created_at_ts",
    "download_url",
    "podcast_id",
]

PODCAST_EPISODES_INSERT_SQL = """
    INSERT INTO podcasts.episodes (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT DO NOTHING
""".format(
    cols=", ".join(PODCAST_EPISODE_COLS),
    vals=", ".join(f"%({c})s" for c in PODCAST_EPISODE_COLS),
)


def flush_podcast_episodes_batch(rows: List[Dict]) -> Tuple[int, int]:
    """
    Insert a batch of episodes into podcasts.episodes.

    rows: list of dicts with keys PODCAST_EPISODE_COLS:
        id, date_entered, audio_path, guid, title, description,
        created_at_ts, download_url, podcast_id

    Caller is responsible for:
        - building created_at_ts (e.g., from legacy pub_date)

    Returns:
        (inserted, skipped) as above.
    """
    if not rows:
        return 0, 0

    with getcursor(commit=True) as cur:
        inserted, skipped = insert_batch(
            insert_sql=PODCAST_EPISODES_INSERT_SQL,
            rows=rows,
            cur=cur,
        )
        return inserted, skipped


# -------------------------
# podcasts.transcript_segments
# -------------------------

PODCAST_SEGMENT_COLS = [
    "id",
    "episode_id",
    "seg_idx",
    "start_s",
    "end_s",
    "text",
    "filtered_text",
]

PODCAST_SEGMENTS_INSERT_SQL = """
    INSERT INTO podcasts.transcript_segments (
        {cols}
    ) VALUES (
        {vals}
    )
    ON CONFLICT (episode_id, seg_idx) DO NOTHING
""".format(
    cols=", ".join(PODCAST_SEGMENT_COLS),
    vals=", ".join(f"%({c})s" for c in PODCAST_SEGMENT_COLS),
)


def flush_podcast_transcript_segments_batch(rows: List[Dict]) -> Tuple[int, int]:
    """
    Insert a batch of transcript segments into podcasts.transcript_segments.

    rows: list of dicts with keys PODCAST_SEGMENT_COLS:
        id, episode_id, seg_idx, start_s, end_s, text, filtered_text

    Caller is responsible for pre-populating 'filtered_text'.

    Returns:
        (inserted, skipped) as above.
    """
    if not rows:
        return 0, 0

    with getcursor(commit=True) as cur:
        inserted, skipped = insert_batch(
            insert_sql=PODCAST_SEGMENTS_INSERT_SQL,
            rows=rows,
            cur=cur,
        )
        return inserted, skipped
