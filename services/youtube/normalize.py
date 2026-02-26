from __future__ import annotations

from typing import List, Any, Dict, Optional
from datetime import datetime, timezone
from filtering.anonymization import redact_pii


def normalize_video(item: Dict[str, Any]) -> Dict[str, Any]:
    snip = item.get("snippet", {}) or {}
    stats = item.get("statistics", {}) or {}
    content = item.get("contentDetails", {}) or {}

    created_at_ts = clean_created_at_ts(snip.get("publishedAt"))
    vid = item.get("id")

    return {
        "video_id": vid,
        "url": f"https://www.youtube.com/watch?v={vid}",
        "title": snip.get("title"),
        "description": snip.get("description"),
        "created_at_ts": created_at_ts,
        "channel_id": snip.get("channelId"),
        "channel_title": snip.get("channelTitle"),
        "duration_iso": content.get("duration"),
        "view_count": to_int(stats.get("viewCount")),
        "like_count": to_int(stats.get("likeCount")),
        "comment_count": to_int(stats.get("commentCount")),
    }

def normalize_comment_thread(item: Dict[str, Any], video_id: str) -> Dict[str, Any]:
    snip = item.get("snippet", {}) or {}
    tlc = snip.get("topLevelComment") or {}
    tlc_snip = tlc.get("snippet", {}) or {}

    created_at_ts = clean_created_at_ts(tlc_snip.get("publishedAt"))

    comment_id = tlc.get("id")
    text = tlc_snip.get("textDisplay") or tlc_snip.get("textOriginal") or ""
    filtered = redact_pii(text)

    return {
        "video_id": video_id,
        "comment_id": comment_id,
        "comment_url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
        "created_at_ts": created_at_ts,
        "text": text,
        "filtered_text": filtered,
        "like_count": to_int(tlc_snip.get("likeCount")),
        "reply_count": to_int(snip.get("totalReplyCount")),
    }


def normalize_comment_reply(item: Dict[str, Any], *, video_id: str) -> Dict[str, Any]:
    snip = item.get("snippet", {}) or {}
    created_at_ts = clean_created_at_ts(snip.get("publishedAt"))
    comment_id = item.get("id")
    text = snip.get("textDisplay") or snip.get("textOriginal") or ""
    filtered = redact_pii(text)

    return {
        "video_id": video_id,
        "comment_id": comment_id,
        "video_url": f"https://www.youtube.com/watch?v={video_id}",
        "comment_url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
        "parent_comment_id": snip.get("parentId"),
        "text": text,
        "filtered_text": filtered,
        "created_at_ts": created_at_ts,
        "like_count": to_int(snip.get("likeCount")),
    }


def normalize_comment_threads(raw_items: List[Dict[str, Any]], *, video_id: str) -> List[Dict[str, Any]]:
    return [normalize_comment_thread(it, video_id) for it in raw_items]


def normalize_comment_replies(raw_items: List[Dict[str, Any]], *, video_id: str) -> List[Dict[str, Any]]:
    return [normalize_comment_reply(it, video_id=video_id) for it in raw_items]


def to_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def clean_created_at_ts(published_at: Optional[str]) -> Optional[datetime]:
    if not published_at:
        return None
    try:
        return datetime.fromisoformat(published_at.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None