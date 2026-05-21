"""
Logical variable keys for building model input (backend knows JSON shape; UI shows display names only).
"""

from __future__ import annotations

from typing import Any, Callable

from apps.claim_extractor.model_common import stable_task_id

Extractor = Callable[[dict[str, Any], dict[str, Any]], str]


def _claim_text(_post: dict[str, Any], claim: dict[str, Any]) -> str:
    return str(claim.get("claim") or "")


def _text_coreference_resolved(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    t = post.get("text_coreference_resolved")
    if not isinstance(t, str) or not t.strip():
        t = post.get("text")
    return t if isinstance(t, str) else ""


def _plain_text(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    t = post.get("text")
    return t if isinstance(t, str) else ""


def _platform(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return str(post.get("platform") or "")


def _reddit_submission_title(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return str(post.get("reddit_submission_title") or "")


def _reddit_comment_submission_title(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return str(post.get("reddit_comment_submission_title") or "")


def _youtube_video_title(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return str(post.get("youtube_video_title") or "")


def _podcast_name(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return str(post.get("podcast_name") or "")


def _task_id(post: dict[str, Any], _claim: dict[str, Any]) -> str:
    return stable_task_id(post)


VAR_EXTRACTORS: dict[str, Extractor] = {
    "claim": _claim_text,
    "text_coreference_resolved": _text_coreference_resolved,
    "text": _plain_text,
    "platform": _platform,
    "reddit_submission_title": _reddit_submission_title,
    "reddit_comment_submission_title": _reddit_comment_submission_title,
    "youtube_video_title": _youtube_video_title,
    "podcast_name": _podcast_name,
    "task_id": _task_id,
}

VAR_DISPLAY_NAMES: dict[str, str] = {
    "claim": "Claim text",
    "text_coreference_resolved": "Post text (coref-resolved, else plain)",
    "text": "Post text (plain)",
    "platform": "Platform",
    "reddit_submission_title": "Reddit submission title",
    "reddit_comment_submission_title": "Reddit parent submission title",
    "youtube_video_title": "YouTube video title",
    "podcast_name": "Podcast name",
    "task_id": "Task ID",
}


def list_var_keys() -> list[str]:
    return sorted(VAR_EXTRACTORS.keys())


def display_name(key: str) -> str:
    return VAR_DISPLAY_NAMES.get(key, key)


def extract_var(key: str, post_row: dict[str, Any], claim_dict: dict[str, Any]) -> str:
    if key not in VAR_EXTRACTORS:
        raise ValueError(f"Unknown variable key: {key!r}")
    return VAR_EXTRACTORS[key](post_row, claim_dict)
