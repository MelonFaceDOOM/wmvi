"""Post text helpers shared by claim extraction consumers."""

from __future__ import annotations

from typing import Any


def stable_task_id(row: dict[str, Any]) -> str:
    """Stable id for a post/chunk row (prefers explicit task_id)."""
    tid = row.get("task_id")
    if tid is not None and str(tid).strip():
        return str(tid)
    src = row.get("source_post_id")
    idx = row.get("sentence_boundary_chunk_index")
    if src is not None and idx is not None:
        return f"{src}:{idx}"
    return str(row.get("post_id", "unknown"))


def format_input_text(row: dict[str, Any], text: str) -> str:
    """Wrap body text with platform-specific title/context prefixes."""
    platform = str(row.get("platform", "unknown"))
    if platform == "reddit_submission":
        return f"Submission title: {row.get('reddit_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "reddit_comment":
        return f"Reddit comment context title: {row.get('reddit_comment_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "youtube_video":
        return f"YouTube video title: {row.get('youtube_video_title') or 'Unknown'}\n\n{text}"
    if platform == "podcast_episode":
        return f"Podcast name: {row.get('podcast_name') or 'Unknown'}\n\n{text}"
    return text
