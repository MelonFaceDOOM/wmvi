from __future__ import annotations

from content_sync.platforms.base import ImportStats, PlatformHandler
from content_sync.platforms.podcast_episode import PodcastEpisodeHandler
from content_sync.platforms.youtube_comment import YoutubeCommentHandler
from content_sync.platforms.youtube_video import YoutubeVideoHandler

HANDLERS: dict[str, PlatformHandler] = {
    YoutubeVideoHandler.platform: YoutubeVideoHandler(),
    YoutubeCommentHandler.platform: YoutubeCommentHandler(),
    PodcastEpisodeHandler.platform: PodcastEpisodeHandler(),
}


def get_handlers(platforms: list[str] | None = None) -> list[PlatformHandler]:
    if platforms is None:
        return list(HANDLERS.values())
    out: list[PlatformHandler] = []
    for name in platforms:
        handler = HANDLERS.get(name)
        if handler is None:
            raise ValueError(f"unknown content sync platform: {name!r}")
        out.append(handler)
    return out
