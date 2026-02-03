import json
from typing import List, Dict, Any, Optional, Iterator, Tuple
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
from filtering.anonymization import redact_pii

# ---------------------------------------------------------------------
# Exceptions and Errors
# ---------------------------------------------------------------------


class YTQuotaExceeded(RuntimeError):
    """Daily quota exhausted (403 quotaExceeded)."""


class YTUnexpectedError(RuntimeError):
    """Unexpected YouTube API error."""


def _yt_error_reason(e: HttpError) -> Optional[str]:
    try:
        data = json.loads(e.content.decode("utf-8"))
        return data["error"]["errors"][0].get("reason")
    except Exception:
        return None


def _raise_on_fatal_http_error(e: HttpError) -> None:
    reason = _yt_error_reason(e)
    if reason in ("quotaExceeded", "dailyLimitExceeded"):
        raise YTQuotaExceeded(reason)
    raise YTUnexpectedError(str(e))


# ---------------------------------------------------------------------
# Search + enrichment
# ---------------------------------------------------------------------

def _search_page(
    yt,
    *,
    term_name: str,
    region: Optional[str],
    published_after: str,
    published_before: str,
    page_token: Optional[str],
) -> Tuple[List[str], Optional[str]]:
    params = dict(
        part="id",
        q=term_name,
        type="video",
        maxResults=50,
        order="date",
        publishedAfter=published_after,
        publishedBefore=published_before,
    )
    if region and region.upper() != "GLOBAL":
        params["regionCode"] = region
    if page_token:
        params["pageToken"] = page_token

    try:
        resp = yt.search().list(**params).execute()
    except HttpError as e:
        _raise_on_fatal_http_error(e)

    ids = [it["id"]["videoId"] for it in resp.get("items", [])]
    return ids, resp.get("nextPageToken")


def _enrich_videos(
    yt,
    video_ids: List[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        try:
            resp = yt.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch),
                maxResults=50,
            ).execute()
        except HttpError as e:
            _raise_on_fatal_http_error(e)

        out.extend(resp.get("items", []))

    return out


# ---------------------------------------------------------------------
# Public video scraper (generator)
# ---------------------------------------------------------------------

def iter_videos(
    yt,
    *,
    term_name: str,
    region: Optional[str],
    published_after: str,
    published_before: str,
    max_pages: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Generator yielding pages of normalized videos.

    Yields dicts of the form:
        {
            "videos": [normalized_video, ...],
            "page_index": int,
            "is_last_page": bool,
            "stopped_reason": None | "max_pages" | "exhausted"
        }

    Raises:
        YTQuotaExceeded
        YTUnexpectedError
    """
    page_token: Optional[str] = None
    page_index = 0

    while True:
        if max_pages is not None and page_index >= max_pages:
            yield {
                "videos": [],
                "page_index": page_index,
                "is_last_page": True,
                "stopped_reason": "max_pages",
            }
            return

        ids, next_token = _search_page(
            yt,
            term_name=term_name,
            region=region,
            published_after=published_after,
            published_before=published_before,
            page_token=page_token,
        )

        if not ids:
            yield {
                "videos": [],
                "page_index": page_index,
                "is_last_page": True,
                "stopped_reason": "exhausted",
            }
            return

        enriched = _enrich_videos(yt, ids)
        normalized = [normalize_video(v) for v in enriched]
        yield {
            "videos": normalized,
            "page_index": page_index,
            "is_last_page": next_token is None,
            "stopped_reason": None,
        }

        page_index += 1
        if not next_token:
            return

        page_token = next_token


# ---------------------------------------------------------------------
# Comments (separate, opt-in)
# ---------------------------------------------------------------------

def fetch_comment_threads(
    yt,
    *,
    video_id: str,
    max_threads: int,
    order: str = "relevance",
    page_token: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch a single page of top-level comment threads.
    """
    try:
        resp = yt.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(100, max_threads),
            order=order,
            pageToken=page_token,
            textFormat="plainText",
        ).execute()
    except HttpError as e:
        reason = _yt_error_reason(e)
        if reason == "commentsDisabled":
            return [], None
        _raise_on_fatal_http_error(e)

    items = resp.get("items", [])
    threads = [normalize_comment_thread(it, video_id) for it in items]
    return threads, resp.get("nextPageToken")


def fetch_comment_replies(
    yt,
    *,
    video_id: str,
    parent_comment_id: str,
    max_replies: int,
    page_token: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch replies for a specific top-level comment.
    """
    try:
        resp = yt.comments().list(
            part="snippet",
            parentId=parent_comment_id,
            maxResults=min(100, max_replies),
            pageToken=page_token,
            textFormat="plainText",
        ).execute()
    except HttpError as e:
        _raise_on_fatal_http_error(e)

    items = resp.get("items", [])
    replies = [normalize_comment_reply(it, video_id=video_id) for it in items]
    return replies, resp.get("nextPageToken")


# ---------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------
def to_int(x):
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def clean_created_at_ts(published_at):
    created_at_ts = None
    if published_at:
        try:
            created_at_ts = datetime.fromisoformat(
                published_at.replace("Z", "+00:00")
            ).astimezone(timezone.utc)
        except Exception:
            created_at_ts = None
    return created_at_ts


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
    text = snip.get("textDisplay") or snip.get("textOriginal")
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
