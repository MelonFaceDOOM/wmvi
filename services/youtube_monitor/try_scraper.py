
"""
Verification script for main features in yt_scrape.py

Extended:
- attempts DB inserts for:
  - normalized videos
  - normalized top-level comment threads
  - normalized comment replies (one page from one thread with replies)
- uses commit=False and explicitly rolls back at the end

NOTE: Uses ingestion flush functions (per request).
"""

from __future__ import annotations

from datetime import datetime, timezone
import pprint
import sys
from typing import Any, Dict, List, Tuple, Optional

from db.db import init_pool, getcursor, close_pool

from ingestion.ingestion import ensure_scrape_job
from ingestion.youtube_video import flush_youtube_video_batch
from ingestion.youtube_comment import flush_youtube_comment_batch
from .yt import youtube_client
from .yt_scrape import (
    iter_videos,
    fetch_comment_threads,
    YTQuotaExceeded,
    YTUnexpectedError,
)

# ---------------------------
# CONFIG — EDIT THESE
# ---------------------------

DB_PREFIX = "dev"

TERM = "covid vaccine"
REGION = None          # e.g. "US", "CA", or None
MAX_PAGES = 2          # keep this small

# Narrow window to keep results reasonable
PUBLISHED_AFTER = "2024-12-01T00:00:00Z"
PUBLISHED_BEFORE = "2025-01-01T00:00:00Z"

# Comment verification
COMMENT_THREAD_PAGE_SIZE = 100
REPLY_PAGE_SIZE = 100

# ---------------------------
# Helpers
# ---------------------------

pp = pprint.PrettyPrinter(indent=2, width=120)


def iso_to_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def check_dates(videos: list[dict]) -> None:
    lo = iso_to_dt(PUBLISHED_AFTER)
    hi = iso_to_dt(PUBLISHED_BEFORE)
    for v in videos:
        ts = v.get("published_at") or v.get("created_at_ts")
        if not ts:
            print("missing published_at/created_at_ts:", v.get("video_id"))
            continue
        if isinstance(ts, str):
            dt = iso_to_dt(ts)
        else:
            dt = ts
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)

        if not (lo <= dt <= hi):
            print("date OUT OF RANGE:", dt.isoformat(), v.get("video_id"))


def clean_created_at_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_comment_replies_page(
    yt,
    *,
    parent_comment_id: str,
    max_results: int = 100,
    page_token: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch a single page of replies for a top-level comment id.

    Uses: comments.list(parentId=...)
    Returns: (items, nextPageToken)
    """
    resp = yt.comments().list(
        part="snippet",
        parentId=parent_comment_id,
        maxResults=min(100, max_results),
        pageToken=page_token,
        textFormat="plainText",
    ).execute()

    items = resp.get("items", []) or []
    return items, resp.get("nextPageToken")


def normalize_comment_reply(item: Dict[str, Any], *, video_id: str) -> Dict[str, Any]:
    """
    Normalize a reply comment into the shape expected by youtube.comment ingestion.
    """
    snip = item.get("snippet", {}) or {}
    created_at_ts = clean_created_at_ts(snip.get("publishedAt"))

    def to_int(x):
        try:
            return int(x) if x is not None else None
        except Exception:
            return None

    comment_id = item.get("id")
    text = snip.get("textDisplay") or snip.get("textOriginal") or ""

    return {
        "video_id": video_id,
        "comment_id": comment_id,
        "parent_comment_id": snip.get("parentId"),
        "comment_url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
        "text": text,
        "filtered_text": text,
        "created_at_ts": created_at_ts,
        "like_count": to_int(snip.get("likeCount")),
        "reply_count": None,  # replies-to-replies not tracked here
        "raw": item,
    }


# ---------------------------
# Main verification
# ---------------------------

def main():
    init_pool(prefix=DB_PREFIX)
    yt = youtube_client()

    print("\n=== VERIFY: iter_videos ===")
    print(f"TERM={TERM!r}")
    print(f"REGION={REGION}")
    print(f"WINDOW={PUBLISHED_AFTER} -> {PUBLISHED_BEFORE}")
    print(f"MAX_PAGES={MAX_PAGES}")

    all_videos: list[dict] = []

    try:
        for page in iter_videos(
            yt,
            term_name=TERM,
            region=REGION,
            published_after=PUBLISHED_AFTER,
            published_before=PUBLISHED_BEFORE,
            max_pages=MAX_PAGES,
        ):
            print("\n--- PAGE ---")
            print("page_index:", page["page_index"])
            print("is_last_page:", page["is_last_page"])
            print("stopped_reason:", page["stopped_reason"])

            vids = page["videos"]
            print("videos in page:", len(vids))

            if vids:
                print("sample video:")
                pp.pprint({
                    k: vids[0].get(k)
                    for k in (
                        "video_id",
                        "published_at",
                        "created_at_ts",
                        "view_count",
                        "like_count",
                        "comment_count",
                    )
                })

            check_dates(vids)
            all_videos.extend(vids)

    except YTQuotaExceeded:
        print("\nQUOTA EXCEEDED - STOPPING")
        sys.exit(0)

    except YTUnexpectedError as e:
        print("\nUNEXPECTED ERROR")
        print(e)
        sys.exit(1)

    print("\n=== VERIFY: enrichment ===")
    print("total videos collected:", len(all_videos))
    missing_stats = [v for v in all_videos if v.get("view_count") is None]
    print("videos missing view_count:", len(missing_stats))

    # ---------------------------
    # Choose a video to fetch comments for
    # ---------------------------

    print("\n=== VERIFY: comment counts ===")
    with_comments = [v for v in all_videos if (
        v.get("comment_count") or 0) > 0]
    print("videos with comments:", len(with_comments))

    with_comments.sort(key=lambda v: v.get("comment_count") or 0, reverse=True)
    for v in with_comments[:5]:
        print(f"video_id={v.get('video_id')} comment_count={
              v.get('comment_count')}")

    threads: list[dict] = []
    reply_rows: list[dict] = []

    if with_comments:
        top = with_comments[0]
        print("\n=== VERIFY: fetch_comment_threads ===")
        print(f"Fetching top-level threads for video {
              top['video_id']} (comment_count={top['comment_count']})")

        try:
            threads, next_token = fetch_comment_threads(
                yt,
                video_id=top["video_id"],
                max_threads=COMMENT_THREAD_PAGE_SIZE,
                order="relevance",
            )
        except YTQuotaExceeded:
            print("\nQUOTA EXCEEDED DURING COMMENT FETCH")
            threads = []
            next_token = None

        print("threads fetched:", len(threads))
        print("next_page_token:", bool(next_token))

        if threads:
            print("sample thread:")
            pp.pprint({
                k: threads[0].get(k)
                for k in ("comment_id", "published_at", "created_at_ts", "like_count", "reply_count", "comment_url")
            })

            # Fetch one page of replies for the first thread that has replies
            thread_with_replies = None
            for t in threads:
                if (t.get("reply_count") or 0) > 0 and t.get("comment_id"):
                    thread_with_replies = t
                    break

            if thread_with_replies:
                parent_id = thread_with_replies["comment_id"]
                vid = thread_with_replies["video_id"]
                print("\n=== VERIFY: fetch_comment_replies_page ===")
                print(f"Fetching replies for parent_comment_id={
                      parent_id} video_id={vid}")

                try:
                    raw_replies, _tok = fetch_comment_replies_page(
                        yt,
                        parent_comment_id=parent_id,
                        max_results=REPLY_PAGE_SIZE,
                    )
                    reply_rows = [normalize_comment_reply(
                        r, video_id=vid) for r in raw_replies]
                except Exception as e:
                    print("reply fetch failed:", e)
                    reply_rows = []

                print("replies fetched:", len(reply_rows))
                if reply_rows:
                    print("sample reply:")
                    pp.pprint({
                        k: reply_rows[0].get(k)
                        for k in ("comment_id", "parent_comment_id", "created_at_ts", "like_count", "comment_url")
                    })

    # ---------------------------
    # DB insert verification (rollback at end)
    # ---------------------------

    print("\n=== VERIFY: DB inserts via ingestion flush (rollback) ===")

    # Create/lookup scrape jobs
    job_videos = ensure_scrape_job(
        name=f"yt_verify_videos: {TERM}",
        description=f"Verification insert for videos term={TERM!r}",
        platforms=["youtube_video"],
    )
    job_comments = ensure_scrape_job(
        name=f"yt_verify_comments: {TERM}",
        description=f"Verification insert for comments term={TERM!r}",
        platforms=["youtube_comment"],
    )

    try:
        with getcursor(commit=False) as cur:
            # Videos
            v_ins, v_skip, v_inserted_ids = flush_youtube_video_batch(
                rows=all_videos,
                job_id=job_videos,
                cur=cur
            )
            print(f"attempted videos: {len(all_videos)}")
            print(f"videos inserted (would be): {v_ins}")
            print(f"videos skipped (would be): {v_skip}")
            print(f"inserted_ids sample: {list(v_inserted_ids)[:5]}")

            # Top-level comments
            c_ins = c_skip = 0
            c_inserted_keys: set[tuple[str, str]] = set()
            if threads:
                c_ins, c_skip, c_inserted_keys = flush_youtube_comment_batch(
                    rows=threads,
                    job_id=job_comments,
                    cur=cur
                )
                print(f"attempted top-level comments: {len(threads)}")
                print(f"top-level comments inserted (would be): {c_ins}")
                print(f"top-level comments skipped (would be): {c_skip}")
                print(f"inserted_keys sample: {list(c_inserted_keys)[:5]}")

            # Replies
            r_ins = r_skip = 0
            r_inserted_keys: set[tuple[str, str]] = set()
            if reply_rows:
                r_ins, r_skip, r_inserted_keys = flush_youtube_comment_batch(
                    rows=reply_rows,
                    job_id=job_comments,
                    cur=cur
                )
                print(f"attempted replies: {len(reply_rows)}")
                print(f"replies inserted (would be): {r_ins}")
                print(f"replies skipped (would be): {r_skip}")
                print(f"inserted_reply_keys sample: {
                      list(r_inserted_keys)[:5]}")

            # Explicit rollback
            cur.connection.rollback()

    except TypeError as e:
        # If your flush_youtube_*_batch functions do not accept `cur=...`,
        # this will fail. In that case, you cannot guarantee commit=False
        # without modifying those ingestion functions to accept a cursor/commit flag.
        print("\nERROR: ingestion flush function signature mismatch.")
        print("Likely cause: flush_youtube_video_batch/flush_youtube_comment_batch do not accept cur=...")
        print("Exception:", e)
        print("\nFix options:")
        print("- Add an optional `cur=None` parameter to the flush funcs (preferred),")
        print("  and use that cursor instead of opening a new transaction internally; OR")
        print("- Add a `commit` flag / pass-through to getcursor(commit=...) inside the flush funcs.")
        sys.exit(2)

    finally:
        close_pool()

    print("\n=== VERIFICATION COMPLETE ===")


if __name__ == "__main__":
    main()
