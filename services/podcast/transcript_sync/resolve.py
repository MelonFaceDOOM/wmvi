from __future__ import annotations

from dataclasses import dataclass, field

from ingestion.podcast import compute_episode_id

from .format import EpisodeExportRow
from .rss_url import normalize_rss_url


def has_transcript_match_key(row: EpisodeExportRow) -> bool:
    """Transcripts require guid or download_url (not fallback token alone)."""
    if (row.guid or "").strip():
        return True
    if (row.download_url or "").strip():
        return True
    return False


def target_episode_id(podcast_id: int, row: EpisodeExportRow) -> str:
    return compute_episode_id(
        podcast_id=podcast_id,
        guid=row.guid,
        download_url=row.download_url,
        created_at_ts=row.created_at_ts,
        title=row.title,
    )


@dataclass
class BundleResolveState:
    """Per-bundle dedupe for computed episode ids (first row wins)."""

    seen_episode_ids: set[str] = field(default_factory=set)

    def accept_episode_id(self, episode_id: str) -> bool:
        if episode_id in self.seen_episode_ids:
            return False
        self.seen_episode_ids.add(episode_id)
        return True


def normalized_show_rss(row: EpisodeExportRow) -> str | None:
    return normalize_rss_url(row.show_rss_url)
