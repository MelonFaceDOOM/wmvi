from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from . import db_episodes, db_import, db_shows
from .format import EpisodeExportRow, ShowRow, iter_episode_jsonl_rows, iter_show_jsonl_rows
from .resolve import (
    BundleResolveState,
    has_transcript_match_key,
    normalized_show_rss,
    target_episode_id,
)

log = logging.getLogger(__name__)
EPISODE_BATCH_SIZE = db_import.DEFAULT_BATCH_SIZE


@dataclass
class ImportStats:
    episodes_seen: int = 0
    shows_upserted: int = 0
    episodes_inserted: int = 0
    transcripts_applied: int = 0
    transcripts_updated: int = 0
    posts_registered: int = 0
    skipped_no_show_rss: int = 0
    skipped_show_not_in_map: int = 0
    skipped_no_transcript_key: int = 0
    skipped_id_collision: int = 0

    def merge(self, other: "ImportStats") -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))


def _resolve_rows(
    episode_rows: list[EpisodeExportRow],
    rss_to_podcast_id: dict[str, int],
) -> tuple[list[db_episodes.EpisodeInsertRow], list[db_import.TranscriptApplyRow], ImportStats]:
    stats = ImportStats()
    resolve_state = BundleResolveState()
    to_insert: list[db_episodes.EpisodeInsertRow] = []
    transcripts: list[db_import.TranscriptApplyRow] = []

    for row in episode_rows:
        stats.episodes_seen += 1
        rss = normalized_show_rss(row)
        if rss is None:
            stats.skipped_no_show_rss += 1
            continue

        podcast_id = rss_to_podcast_id.get(rss)
        if podcast_id is None:
            stats.skipped_show_not_in_map += 1
            continue

        ep_id = target_episode_id(podcast_id, row)
        if not resolve_state.accept_episode_id(ep_id):
            stats.skipped_id_collision += 1
            continue

        to_insert.append(
            db_episodes.episode_insert_from_export(podcast_id, ep_id, row)
        )

        if not has_transcript_match_key(row):
            stats.skipped_no_transcript_key += 1
            continue

        transcripts.append(
            db_import.TranscriptApplyRow(
                episode_id=ep_id,
                transcript=row.transcript,
                transcript_updated_at=row.transcript_updated_at,
            )
        )

    return to_insert, transcripts, stats


def dry_run_bundle(
    *,
    shows_path: Path | None,
    episodes_path: Path,
) -> ImportStats:
    show_rows = list(iter_show_jsonl_rows(shows_path)) if shows_path and shows_path.is_file() else []
    episode_rows = list(iter_episode_jsonl_rows(episodes_path))
    stats = ImportStats()
    stats.episodes_seen = len(episode_rows)
    stats.shows_upserted = len(
        {k for r in show_rows if (k := normalize_show_rss_from_row(r)) is not None}
    )
    stats.skipped_no_transcript_key = sum(
        1 for r in episode_rows if not has_transcript_match_key(r)
    )
    log.info("dry-run: %d episode rows, %d show rows", len(episode_rows), len(show_rows))
    return stats


def apply_bundle(
    cur,
    *,
    shows_path: Path | None,
    episodes_path: Path,
) -> ImportStats:
    stats = ImportStats()

    show_rows = list(iter_show_jsonl_rows(shows_path)) if shows_path and shows_path.is_file() else []
    episode_rows = list(iter_episode_jsonl_rows(episodes_path))

    rss_to_podcast_id = db_shows.upsert_shows(cur, show_rows)
    stats.shows_upserted = len(rss_to_podcast_id)

    to_insert, transcripts, resolve_stats = _resolve_rows(episode_rows, rss_to_podcast_id)
    stats.merge(resolve_stats)

    for i in range(0, len(to_insert), EPISODE_BATCH_SIZE):
        batch = to_insert[i : i + EPISODE_BATCH_SIZE]
        stats.episodes_inserted += db_episodes.insert_new_episodes(cur, batch)

    for i in range(0, len(transcripts), EPISODE_BATCH_SIZE):
        batch = transcripts[i : i + EPISODE_BATCH_SIZE]
        stats.transcripts_applied += len(batch)
        result = db_import.apply_transcript_batch(cur, batch)
        stats.transcripts_updated += result.updated
        stats.posts_registered += result.registered

    return stats


def normalize_show_rss_from_row(row: ShowRow) -> str | None:
    from .rss_url import normalize_rss_url

    return normalize_rss_url(row.rss_url)
