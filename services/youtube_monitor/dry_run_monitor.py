"""
Dry-run YouTube monitor quota estimator.

This script simulates quota usage WITHOUT calling the YouTube API.

It answers:
- How many units per day will my monitor burn?
- Which terms are expensive?
- Do I have headroom for comments?
"""

from dataclasses import dataclass
from typing import Dict, List
from math import ceil

# -----------------------------
# Quota constants (YouTube)
# -----------------------------

SEARCH_PAGE_UNITS = 100
VIDEOS_LIST_UNITS = 1       # per 50 videos
COMMENT_PAGE_UNITS = 1

DAILY_QUOTA_LIMIT = 10_000
SECONDS_PER_DAY = 86_400

# -----------------------------
# Monitor assumptions
# -----------------------------

DEFAULT_SEARCH_PAGES = 1        # pages per scrape
VIDEOS_PER_PAGE = 50

# comment heuristics
FRACTION_WITH_COMMENTS = 0.4    # 40% of videos exceed comment threshold
AVG_COMMENT_PAGES = 1           # 1 page per qualifying video

# -----------------------------
# Input: per-term scheduling
# -----------------------------


@dataclass
class TermConfig:
    term: str
    scrapes_per_day: float


# Example terms — replace with DB-derived values later
TERMS: List[TermConfig] = [
    TermConfig("covid vaccine", 12.0),
    TermConfig("mRNA vaccine dangers", 6.0),
    TermConfig("pfizer myocarditis", 4.0),
    TermConfig("vaccine hoax", 1.0),
    TermConfig("vaccines autism", 1.0),
]

# -----------------------------
# Estimation logic
# -----------------------------


def estimate_term_quota(term: TermConfig) -> Dict[str, float]:
    scrapes = term.scrapes_per_day

    # Search
    search_units = scrapes * DEFAULT_SEARCH_PAGES * SEARCH_PAGE_UNITS

    # Enrichment (1 call per page)
    enrich_units = scrapes * DEFAULT_SEARCH_PAGES * VIDEOS_LIST_UNITS

    # Comments (expected value)
    videos_seen = scrapes * DEFAULT_SEARCH_PAGES * VIDEOS_PER_PAGE
    comment_videos = videos_seen * FRACTION_WITH_COMMENTS
    comment_units = (comment_videos / VIDEOS_PER_PAGE) * \
        AVG_COMMENT_PAGES * COMMENT_PAGE_UNITS

    total_units = search_units + enrich_units + comment_units

    return {
        "search_units": search_units,
        "enrich_units": enrich_units,
        "comment_units": comment_units,
        "total_units": total_units,
    }


def main():
    print("\n=== YouTube Monitor Dry-Run Quota Estimate ===\n")

    totals = {
        "search_units": 0.0,
        "enrich_units": 0.0,
        "comment_units": 0.0,
        "total_units": 0.0,
    }

    for term in TERMS:
        est = estimate_term_quota(term)

        for k in totals:
            totals[k] += est[k]

        print(f"TERM: {term.term!r}")
        print(f"  scrapes/day     : {term.scrapes_per_day:.2f}")
        print(f"  search units    : {est['search_units']:.1f}")
        print(f"  enrich units    : {est['enrich_units']:.1f}")
        print(f"  comment units   : {est['comment_units']:.1f}")
        print(f"  TOTAL units     : {est['total_units']:.1f}")
        print()

    print("=== DAILY TOTAL ===")
    for k, v in totals.items():
        print(f"{k:15s}: {v:8.1f}")

    print("\n=== QUOTA CHECK ===")
    pct = (totals["total_units"] / DAILY_QUOTA_LIMIT) * 100
    print(f"Quota used        : {pct:.1f}%")
    print(f"Quota remaining   : {
          DAILY_QUOTA_LIMIT - totals['total_units']:.1f}")

    if totals["total_units"] > DAILY_QUOTA_LIMIT:
        print("\n🚨 WARNING: quota exceeded in simulation")
    elif pct > 80:
        print("\n⚠️  Warning: quota usage >80%")
    else:
        print("\n✅ Quota usage looks safe")

    print("\n(This is a dry run. No API calls were made.)")


if __name__ == "__main__":
    main()
