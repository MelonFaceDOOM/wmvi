"""
low level scraping tools for interacting with yt api
includes budget tracking
"""

from __future__ import annotations

import os
import json
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Tuple, TypeVar
from zoneinfo import ZoneInfo

from googleapiclient.discovery import build

from .normalize import (
    normalize_video,
    normalize_comment_threads,
    normalize_comment_replies
)

from .time import ensure_utc

import logging
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# YT client
# ---------------------------------------------------------------------

YT_API_KEY = os.getenv("YT_API_KEY")

def youtube_client(api_key: str | None = None) -> Any:
    """
    Construct a googleapiclient YouTube Data API v3 client.

    Uses YT_API_KEY from environment by default.
    """
    key = api_key or YT_API_KEY
    if not key:
        raise RuntimeError("Missing YT_API_KEY")
    return build("youtube", "v3", developerKey=key)


# ---------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------

class YTQuotaExceeded(RuntimeError):
    """YouTube daily quota exhausted (quotaExceeded/dailyLimitExceeded)."""


class YTBudgetExceeded(RuntimeError):
    """Local budget exceeded (we refused to make the call)."""


class YTUnexpectedError(RuntimeError):
    def __init__(self, msg: str, *, status: int | None = None, reason: str | None = None) -> None:
        super().__init__(msg)
        self.status = status
        self.reason = reason
    """Unexpected / non-retryable error from YouTube API layer."""


# ---------------------------------------------------------------------
# Day key helper (Pacific reset)
# ---------------------------------------------------------------------
UTC = timezone.utc
PACIFIC = ZoneInfo("America/Los_Angeles")

def pacific_day_key(now_utc: datetime) -> str:
    """
    Day key for budget reset. YouTube quota resets at midnight Pacific.
    Input may be naive or aware; we normalize to UTC first.
    """
    now_utc = ensure_utc(now_utc)
    return now_utc.astimezone(PACIFIC).date().isoformat()


# ---------------------------------------------------------------------
# Budget tracking (in-memory)
# ---------------------------------------------------------------------

@dataclass
class BudgetSnapshot:
    day_key: str
    used_units: int = 0
    calls: int = 0


class BudgetTracker:
    """
    In-memory budget tracker. Resets counters when the Pacific date changes.
    Thread-safe (in-process) via lock.
    One BudgetTracker is meant to be shared across multiple YTQuotaClients to enable parallelization
    """

    def __init__(
        self,
        *,
        budget_units_per_day: int,
        # dynamic now_fn is used so tests can pretend it is midnight
        now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        initial: BudgetSnapshot | None = None,
    ) -> None:
        self.budget_units_per_day = int(budget_units_per_day)
        self.now_fn = now_fn
        self._lock = threading.Lock()

        now = self.now_fn()
        today = pacific_day_key(now)

        if initial is None:
            self._state = BudgetSnapshot(day_key=today)
        else:
            self._state = initial
            if self._state.day_key != today:
                self._state = BudgetSnapshot(day_key=today)

    def _ensure_today(self) -> None:
        today = pacific_day_key(self.now_fn())
        if self._state.day_key != today:
            # create a fresh state (new day = no units used yet)
            self._state = BudgetSnapshot(day_key=today)

    def snapshot(self) -> BudgetSnapshot:
        """Returns a copy of the current state"""
        with self._lock:
            self._ensure_today()
            return BudgetSnapshot(
                day_key=self._state.day_key,
                used_units=self._state.used_units,
                calls=self._state.calls,
            )

    def used_units_today(self) -> int:
        with self._lock:
            self._ensure_today()
            return self._state.used_units

    def remaining_units_today(self) -> int:
        with self._lock:
            self._ensure_today()
            return max(0, self.budget_units_per_day - self._state.used_units)

    def can_afford(self, requested_units: int) -> bool:
        requested_units = max(1, int(requested_units))
        with self._lock:
            self._ensure_today()
            return (self._state.used_units + requested_units) <= self.budget_units_per_day

    def charge(self, units: int, *, label: str = "") -> None:
        units = max(1, int(units))
        with self._lock:
            self._ensure_today()
            if (self._state.used_units + units) > self.budget_units_per_day:
                raise YTBudgetExceeded(
                    f"Budget exceeded: label={label!r} cost={units} "
                    f"used={self._state.used_units} budget={self.budget_units_per_day}"
                )
            self._state.used_units += units
            self._state.calls += 1


# ---------------------------------------------------------------------
# Error classification (injectable)
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class ClassifiedError:
    kind: str  # "quota" | "retryable" | "fatal"
    reason: str | None = None
    status: int | None = None


def _yt_reason_from_exc(err: BaseException) -> str | None:
    """
    Duck-typed reason extraction from googleapiclient.errors.HttpError-like objects.

    Extracts Google API "reason" from googleapiclient HttpError-like exceptions.
    Assumes:
      - googleapiclient.errors.HttpError exposes:
          - err.resp.status (HTTP status)
          - err.content (bytes JSON body)  # see google-api-python-client HttpError source
      - Error body typically looks like:
          {"error": {"errors": [{"reason": "...", "message": "...", ...}], "code": 403, "message": "..."}}

    The error object structure (resp.status/content) /is shown here:
     - https://github.com/googleapis/google-api-python-client/blob/main/googleapiclient/errors.py?utm_source=chatgpt.com
    JSON Response structure ({"error": {"errors": ...}}) is shown here:
     - https://developers.google.com/workspace/gmail/api/guides/handle-errors
    """
    content = getattr(err, "content", None)

    try:
        if isinstance(content, (bytes, bytearray)):
            data = json.loads(content.decode("utf-8"))
        elif isinstance(content, str):
            data = json.loads(content)
        else:
            return None
    except Exception:
        return None

    err_obj = data.get("error")
    if not isinstance(err_obj, dict):
        return None

    # returns first reason found; might be worth looking at all reasons in the futrue
    errors = err_obj.get("errors")
    if isinstance(errors, list):
        for e in errors:
            if isinstance(e, dict):
                reason = e.get("reason")
                if isinstance(reason, str) and reason:
                    return reason

    return None


def default_classify_error(err: BaseException) -> ClassifiedError:
    """
    Default classifier for googleapiclient.errors.HttpError, without importing it.
    Uses duck-typing:
      - err.resp.status
      - err.content bytes (JSON with error.errors[*].reason)

    403 reason values are documented under:
      - YouTube Data API Errors + Global domain errors tables.
    """
    status = getattr(getattr(err, "resp", None), "status", None)
    reason = _yt_reason_from_exc(err)

    # Quota exhaustion (treat separately from "retryable" throttling)
    if reason in ("quotaExceeded", "dailyLimitExceeded", "dailyLimitExceededUnreg"):
        return ClassifiedError(kind="quota", reason=reason, status=status)

    # Treat typical transient errors as retryable
    if status in (429, 500, 502, 503, 504):
        return ClassifiedError(kind="retryable", reason=reason, status=status)

    # 403: split retryable vs non-retryable based on documented reason values
    if status == 403:
        RETRYABLE_403_REASONS = {
            # "Too many requests" style limits (retry with backoff)
            "userRateLimitExceeded",
            "rateLimitExceeded",
            "servingLimitExceeded",
            "concurrentLimitExceeded",
            "limitExceeded",
        }

        NONRETRYABLE_403_REASONS = {
            # Auth/config/account/permission issues (won't fix by retrying)
            "forbidden",
            "accessNotConfigured",
            "accountDeleted",
            "accountDisabled",
            "accountUnverified",
            "insufficientPermissions",
            "insufficientAudience",
            "insufficientAuthorizedParty",
            "lockedDomainForbidden",
            "sslRequired",
            "unknownAuth",
            "downloadServiceForbidden",
            # Unregistered variants (need to configure/register, not retry)
            "rateLimitExceededUnreg",
            "userRateLimitExceededUnreg",
            # Some YouTube API tables also include these as 403 "forbidden" flavors
            # (generally permanent until permissions/account state changes)
            "accountDelegationForbidden",
            "authenticatedUserAccountClosed",
            "authenticatedUserAccountSuspended",
            "authenticatedUserNotChannel",
            "channelClosed",
            "channelNotFound",
            "channelSuspended",
            "cmsUserAccountNotFound",
            "insufficientCapabilities",
        }

        if reason in RETRYABLE_403_REASONS:
            return ClassifiedError(kind="retryable", reason=reason, status=status)

        if reason in NONRETRYABLE_403_REASONS:
            return ClassifiedError(kind="fatal", reason=reason, status=status)

        # Unknown 403 reason: default to fatal (conservative)
        return ClassifiedError(kind="fatal", reason=reason, status=status)

    if status is None:
        # possibly transient network transport issues
        return ClassifiedError(kind="retryable", reason=reason, status=status)

    return ClassifiedError(kind="fatal", reason=reason, status=status)


# ---------------------------------------------------------------------
# YTQuotaClient (API calls + budget + retries)
# ---------------------------------------------------------------------

# Generic type used in the YTQuotaClient.call().
# Specifies that both the output of the callable func as well as
# the output for the wrapper (call()) will be the same type.
T = TypeVar("T")


class YTQuotaClient:
    """
    Wraps a googleapiclient YouTube client and:
      - charges local budget per call
      - retries transient failures
      - raises YTQuotaExceeded on daily quota exhaustion
    """

    COSTS: Dict[str, int] = {
        "search.list": 100,
        "videos.list": 1,
        "commentThreads.list": 1,
        "comments.list": 1,
    }

    def __init__(
        self,
        yt: Any,
        *,
        tracker: BudgetTracker,
        # dynamic classifier is used so tests can force diff errors
        classify_error: Callable[[BaseException], ClassifiedError] = default_classify_error,
        sleep_fn: Callable[[float], None] = lambda s: __import__("time").sleep(s),
        max_retries: int = 5,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        jitter: float = 0.25,
        charge_on_retry: bool = True,
    ) -> None:
        self.yt = yt
        self.tracker = tracker
        self.classify_error = classify_error
        self.sleep_fn = sleep_fn
        self.max_retries = int(max_retries)
        self.base_backoff_s = float(base_backoff_s)
        self.max_backoff_s = float(max_backoff_s)
        self.jitter = float(jitter)
        self.charge_on_retry = bool(charge_on_retry)

    @classmethod
    def from_api_key(cls, *, tracker: BudgetTracker, api_key: str | None = None, **kwargs):
        # allows it to build yt client on its own
        # i.e.: qyt = YTQuotaClient.from_api_key(tracker=tracker)
        yt = youtube_client(api_key=api_key)
        return cls(yt, tracker=tracker, **kwargs)

    def cost_for(self, method: str) -> int:
        return int(self.COSTS.get(method, 1))

    def can_afford(self, method: str, *, cost_units: int | None = None) -> bool:
        cost = int(cost_units if cost_units is not None else self.cost_for(method))
        return self.tracker.can_afford(cost)

    def _sleep_backoff(self, attempt: int) -> None:
        backoff = min(self.max_backoff_s, self.base_backoff_s * (2 ** attempt))
        if self.jitter > 0:
            backoff *= random.uniform(1.0 - self.jitter, 1.0 + self.jitter)
        self.sleep_fn(backoff)

    def call(
        self,
        method: str,
        exec_fn: Callable[[], T],
        *,
        cost_units: int | None = None,
        label: str = "",
    ) -> T:
        cost = int(cost_units if cost_units is not None else self.cost_for(method))

        attempts = 0
        while True:
            if attempts == 0 or self.charge_on_retry:
                self.tracker.charge(cost, label=label or method)

            try:
                return exec_fn()
            except Exception as e:
                info = self.classify_error(e)

                if info.kind == "quota":
                    raise YTQuotaExceeded(info.reason or "quota") from e

                if info.kind == "retryable" and attempts < self.max_retries:
                    self._sleep_backoff(attempts)
                    attempts += 1
                    continue

                raise YTUnexpectedError(
                    f"YT call failed method={method} status={info.status} reason={info.reason}",
                    status=info.status,
                    reason=info.reason,
                ) from e

    # -----------------------------------------------------------------
    # High-level API helpers (what yt_scrape used to do)
    # -----------------------------------------------------------------

    def search_page(
            self,
            *,
            term_name: str,
            region: str | None,
            published_after: str | datetime,
            published_before: str | datetime | None = None,
            page_token: str | None,
            max_results: int = 50,
            order: str = "date",
    ) -> Tuple[List[str], str | None]:
        """
        Equivalent to yt.search().list(part="id", ...).execute()
        Returns (video_ids, nextPageToken).
        """
        published_after_s = dt_to_iso(published_after)
        published_before_s = dt_to_iso(published_before) if published_before is not None else None
        params: Dict[str, Any] = dict(
            part="id",
            q=term_name,
            type="video",
            maxResults=max_results,
            order=order,
            publishedAfter=published_after_s,
        )
        if published_before is not None:
            params["publishedBefore"] = published_before_s
        if region and region.upper() != "GLOBAL":
            params["regionCode"] = region
        if page_token:
            params["pageToken"] = page_token

        resp = self.call(
            "search.list",
            lambda: self.yt.search().list(**params).execute(),
            label=f"search.list term={term_name!r}",
        )

        items = resp.get("items", []) or []
        ids = [it["id"]["videoId"] for it in items if it.get("id", {}).get("videoId")]
        return ids, resp.get("nextPageToken")

    def enrich_videos(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Equivalent to yt.videos().list(part="snippet,statistics,contentDetails", id=...).
        Returns raw API 'items' (caller normalizes).
        """
        out: List[Dict[str, Any]] = []
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            if not batch:
                continue

            params = dict(
                part="snippet,statistics,contentDetails",
                id=",".join(batch),
                maxResults=50,
            )
            resp = self.call(
                "videos.list",
                lambda: self.yt.videos().list(**params).execute(),
                label=f"videos.list n={len(batch)}",
            )
            out.extend(resp.get("items", []) or [])
        return out

    def fetch_comment_threads_normalized(
            self,
            *,
            video_id: str,
            max_threads: int,
            order: str = "relevance",
            page_token: str | None = None,
    ) -> Tuple[List[dict], str | None]:
        # TODO loop over pages using nxt token if we want more comments
        #  for now forcing just 1 page is good economy
        raw, nxt = self.fetch_comment_threads(
            video_id=video_id,
            max_threads=max_threads,
            order=order,
            page_token=page_token,
        )
        if not raw:
            return [], None
        return normalize_comment_threads(raw, video_id=video_id), nxt

    def fetch_comment_replies_normalized(
            self,
            *,
            video_id: str,
            parent_comment_id: str,
            max_replies: int,
            page_token: str | None = None,
    ) -> Tuple[List[dict], str | None]:
        raw, nxt = self.fetch_comment_replies(
            video_id=video_id,
            parent_comment_id=parent_comment_id,
            max_replies=max_replies,
            page_token=page_token,
        )
        if not raw:
            return [], None
        return normalize_comment_replies(raw, video_id=video_id), nxt

    def fetch_comment_threads(
        self,
        *,
        video_id: str,
        max_threads: int,
        order: str = "relevance",
        page_token: str | None = None,
    ) -> Tuple[List[Dict[str, Any]], str | None]:
        """
        Fetch a single page of top-level comment threads.
        Returns (raw_items, nextPageToken). Special-cases commentsDisabled.
        """
        params: Dict[str, Any] = dict(
            part="snippet",
            videoId=video_id,
            maxResults=min(100, max_threads),
            order=order,
            textFormat="plainText",
        )
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = self.call(
                "commentThreads.list",
                lambda: self.yt.commentThreads().list(**params).execute(),
                label=f"commentThreads.list video={video_id}",
            )
        except YTUnexpectedError as e:
            if e.reason in ("commentsDisabled", "videoNotFound"):
                return [], None
            raise

        items = resp.get("items", []) or []
        return items, resp.get("nextPageToken")

    def fetch_comment_replies(
        self,
        *,
        video_id: str,
        parent_comment_id: str,
        max_replies: int,
        page_token: str | None = None,
    ) -> Tuple[List[Dict[str, Any]], str | None]:
        """
        Fetch replies for a specific top-level comment.
        Returns (raw_items, nextPageToken).
        """
        params: Dict[str, Any] = dict(
            part="snippet",
            parentId=parent_comment_id,
            maxResults=min(100, max_replies),
            textFormat="plainText",
        )
        if page_token:
            params["pageToken"] = page_token

        resp = self.call(
            "comments.list",
            lambda: self.yt.comments().list(**params).execute(),
            label=f"comments.list video={video_id} parent={parent_comment_id}",
        )
        items = resp.get("items", []) or []
        return items, resp.get("nextPageToken")


# ---------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------

def dt_to_iso(x: str | datetime) -> str:
    """
    Convert a datetime to an RFC3339-ish string suitable for YouTube API params.
    - If datetime is naive, assume UTC.
    - Always return an offset-bearing string (UTC).
    If x is already a string, return as-is.
    """
    if isinstance(x, datetime):
        x = ensure_utc(x)
        return x.isoformat()
    return x

def iter_videos(
    yt: YTQuotaClient,
    *,
    term_name: str,
    region: str | None,
    published_after: str | datetime,
    published_before: str | datetime | None = None,
    max_pages: int | None = None,
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
        YTBudgetExceeded
    """
    page_token: str | None = None
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

        ids, next_token = yt.search_page(
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

        enriched = yt.enrich_videos(ids)
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
