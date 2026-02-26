from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

UTC = timezone.utc
PACIFIC = ZoneInfo("America/Los_Angeles")


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(dt: datetime) -> datetime:
    """
    Normalize a datetime to timezone-aware UTC.

    - If dt is naive, assume it is already UTC (attach tzinfo=UTC).
    - If dt is aware, convert to UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def next_midnight_pacific(now_utc: datetime) -> datetime:
    """
    Return the next midnight in America/Los_Angeles, expressed as UTC.

    Why: YouTube quota resets at midnight Pacific, so when quota is hit
    we pause until the next Pacific day boundary.
    """
    now_utc = ensure_utc(now_utc)
    now_pt = now_utc.astimezone(PACIFIC)

    tomorrow = now_pt.date() + timedelta(days=1)
    midnight_pt = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=PACIFIC)

    return midnight_pt.astimezone(UTC)


def publication_span_seconds(videos: Iterable[dict]) -> float:
    """
    Span (seconds) between oldest/newest created_at_ts in a list of normalized videos.
    Expects created_at_ts to be timezone-aware UTC datetimes.
    """
    dts: list[datetime] = []
    for v in videos:
        dt = v.get("created_at_ts")
        if isinstance(dt, datetime):
            dts.append(dt)

    if len(dts) < 2:
        return 0.0

    dts.sort()
    return (dts[-1] - dts[0]).total_seconds()


def newest_published_dt(videos: list[dict]) -> datetime | None:
    """
    Return the newest created_at_ts in a list of normalized videos.
    Expects created_at_ts to be timezone-aware UTC datetimes.
    """
    newest: datetime | None = None
    for v in videos:
        dt = v.get("created_at_ts")
        if not isinstance(dt, datetime):
            continue
        if newest is None or dt > newest:
            newest = dt
    return newest