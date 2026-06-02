from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
import tempfile

from content_sync.format import PLATFORM_PODCAST_EPISODE, SIDECAR_PODCAST_SHOWS
from content_sync.platforms.base import ImportStats
from services.podcast.transcript_sync import db_export, db_shows
from services.podcast.transcript_sync.bundle_import import apply_bundle
from services.podcast.transcript_sync.format import EpisodeExportRow, ShowRow


def _episode_row_to_dict(row: EpisodeExportRow) -> dict[str, Any]:
    d = row.to_jsonl_dict()
    d["platform"] = PLATFORM_PODCAST_EPISODE
    d["key1"] = ""  # filled on import after compute_episode_id
    d["key2"] = ""
    return d


def _show_row_to_dict(row: ShowRow) -> dict[str, Any]:
    return row.to_jsonl_dict()


class PodcastEpisodeHandler:
    platform = PLATFORM_PODCAST_EPISODE

    def export_delta(
        self,
        cur,
        *,
        since: datetime | None,
        until: datetime,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows_out: list[dict[str, Any]] = []
        podcast_ids: set[int] = set()
        after_ts: datetime | None = None
        after_id: str | None = None

        while True:
            batch = db_export.fetch_export_batch(
                cur,
                since_ts=since,
                until_ts=until,
                after_ts=after_ts,
                after_id=after_id,
            )
            if not batch:
                break
            for row in batch:
                rows_out.append(_episode_row_to_dict(row))
                if row.source_show_id is not None:
                    podcast_ids.add(row.source_show_id)
            last = batch[-1]
            after_ts = last.transcript_updated_at
            after_id = last.source_episode_id

        shows: list[dict[str, Any]] = []
        if podcast_ids:
            for show in db_shows.fetch_shows_by_ids(cur, sorted(podcast_ids)):
                shows.append(_show_row_to_dict(show))

        return rows_out, {SIDECAR_PODCAST_SHOWS: shows}

    def import_bundle(
        self,
        cur,
        *,
        rows: list[dict[str, Any]],
        sidecars: dict[str, list[dict[str, Any]]],
    ) -> ImportStats:
        stats = ImportStats()
        stats.rows_seen = len(rows)

        show_rows = sidecars.get(SIDECAR_PODCAST_SHOWS) or []
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            episodes_path = tmp_path / "episodes.jsonl"
            shows_path = tmp_path / "shows.jsonl" if show_rows else None

            with episodes_path.open("w", encoding="utf-8") as fp:
                for data in rows:
                    payload = {
                        k: v
                        for k, v in data.items()
                        if k not in ("platform", "key1", "key2")
                    }
                    fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

            if show_rows and shows_path is not None:
                with shows_path.open("w", encoding="utf-8") as fp:
                    for data in show_rows:
                        fp.write(json.dumps(data, ensure_ascii=False) + "\n")

            pod_stats = apply_bundle(
                cur,
                shows_path=shows_path,
                episodes_path=episodes_path,
            )

        stats.rows_upserted = pod_stats.episodes_inserted
        stats.transcripts_updated = pod_stats.transcripts_updated
        stats.posts_registered = pod_stats.posts_registered
        stats.skipped = (
            pod_stats.skipped_no_show_rss
            + pod_stats.skipped_show_not_in_map
            + pod_stats.skipped_no_transcript_key
            + pod_stats.skipped_id_collision
        )
        return stats
