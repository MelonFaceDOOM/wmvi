# scripts/yt_backfill_status.py
"""
Report YouTube backfill progress (per-term + summary) based on youtube.search_status.oldest_found_ts.

What it prints:
- total terms + how many have started (oldest_found_ts set)
- per-term farthest-back date (oldest_found_ts)
- % completion per term, relative to BACKFILL_START_UTC..BACKFILL_END_UTC
  (100% means oldest_found_ts <= BACKFILL_START_UTC)

Assumptions:
- youtube.search_status has columns: term_id, oldest_found_ts
- terms come from taxonomy subset (same query as your backfiller)

No CLI args: edit constants below.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from db.db import init_pool, close_pool, getcursor
from services.youtube.time import ensure_utc  # your helper
from services.youtube.scraping import load_search_terms  # returns [(term_id, term_name), ...]

# ----------------------------
# CONFIG (edit as needed)
# ----------------------------

PROD = False  # set True to check PROD

SEARCH_TERM_LIST_NAME = "core_search_terms"

BACKFILL_START_UTC = datetime(2024, 1, 1, tzinfo=timezone.utc)
BACKFILL_END_UTC = datetime.now(timezone.utc)  # same convention as backfiller


# ----------------------------
# DB queries
# ----------------------------

def load_oldest_status() -> Dict[int, Optional[datetime]]:
    """
    term_id -> oldest_found_ts (UTC aware) or None if NULL/missing
    """
    out: Dict[int, Optional[datetime]] = {}
    with getcursor() as cur:
        cur.execute(
            """
            SELECT term_id, oldest_found_ts
            FROM youtube.search_status
            """
        )
        for term_id, ts in cur.fetchall():
            out[int(term_id)] = ensure_utc(ts) if ts is not None else None
    return out


# ----------------------------
# Progress math
# ----------------------------

def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def pct_complete(oldest_ts: Optional[datetime]) -> float:
    """
    0%  = no progress (None) OR oldest_ts == BACKFILL_END_UTC
    100% = oldest_ts <= BACKFILL_START_UTC
    """
    if oldest_ts is None:
        return 0.0

    oldest_ts = ensure_utc(oldest_ts)
    start = ensure_utc(BACKFILL_START_UTC)
    end = ensure_utc(BACKFILL_END_UTC)

    total = (end - start).total_seconds()
    if total <= 0:
        return 100.0

    # how much of the range is covered from END backwards to oldest_ts
    covered = (end - oldest_ts).total_seconds()
    return 100.0 * clamp01(covered / total)


def fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "<none>"
    return ensure_utc(dt).isoformat(timespec="seconds")


# ----------------------------
# Main report
# ----------------------------

@dataclass
class TermReport:
    term_id: int
    term_name: str
    oldest_ts: Optional[datetime]
    pct: float

def print_report(reports: list[TermReport]) -> None:
    # Keep only started terms (oldest_ts not None).
    started = [r for r in reports if r.oldest_ts is not None]

    skipped = len(reports) - len(started)

    if not started:
        print("No started terms (all oldest_found_ts are NULL).")
        print(f"(skipped {skipped} terms)")
        return

    # Sort: least complete first, then name
    started.sort(key=lambda r: (r.pct, r.term_name.lower()))

    # Compute column widths
    name_w = max(len("term_name"), max(len(r.term_name) for r in started))
    id_w = max(len("term_id"), max(len(str(r.term_id)) for r in started))
    ts_w = max(len("oldest_found_ts_utc"), max(len(fmt_dt(r.oldest_ts)) for r in started))
    pct_w = len("pct_complete")

    # Header
    print(
        f"{'term_name':<{name_w}}  "
        f"{'term_id':>{id_w}}  "
        f"{'oldest_found_ts_utc':<{ts_w}}  "
        f"{'pct_complete':>{pct_w}}"
    )
    print(
        f"{'-'*name_w}  "
        f"{'-'*id_w}  "
        f"{'-'*ts_w}  "
        f"{'-'*pct_w}"
    )

    # Rows
    for r in started:
        print(
            f"{r.term_name:<{name_w}}  "
            f"{r.term_id:>{id_w}}  "
            f"{fmt_dt(r.oldest_ts):<{ts_w}}  "
            f"{r.pct:>{pct_w}.1f}%"
        )

    print()
    print(f"(printed {len(started)} started terms; skipped {skipped} with oldest_found_ts NULL)")

def main() -> None:
    init_pool(prefix="prod" if PROD else "dev")
    try:
        terms: List[Tuple[int, str]] = load_search_terms(SEARCH_TERM_LIST_NAME)
        status = load_oldest_status()

        reports: List[TermReport] = []
        started = 0

        for term_id, term_name in terms:
            oldest = status.get(term_id)  # might be None or missing
            if oldest is not None:
                started += 1
            p = pct_complete(oldest)
            reports.append(TermReport(term_id=term_id, term_name=term_name, oldest_ts=oldest, pct=p))
        print_report(reports)
    finally:
        close_pool()


if __name__ == "__main__":
    main()