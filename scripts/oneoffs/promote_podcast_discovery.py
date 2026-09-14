"""Promote shows from a podcast discovery report into podcasts.shows.

Discovery remains read-only; this separate one-off is the explicit promotion
step. It is a dry run unless --apply is supplied.

Examples:
    python -m scripts.oneoffs.promote_podcast_discovery REPORT.json --prod
    python -m scripts.oneoffs.promote_podcast_discovery REPORT.json --prod --apply
    python -m scripts.oneoffs.promote_podcast_discovery REPORT.json \
        --prod --tiers priority recurring --apply
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from psycopg2.extras import execute_values

from db.db import close_pool, getcursor, init_pool
from services.podcast.transcript_sync.rss_url import normalize_rss_url

load_dotenv()

TIERS = ("priority", "recurring", "watchlist")


def load_candidates(
    report_path: Path,
    *,
    tiers: set[str],
) -> list[tuple[str, str, str]]:
    """Return deduplicated (title, RSS URL, tier) rows from a discovery report."""
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    shows = payload.get("shows")
    if not isinstance(shows, list):
        raise ValueError("report must contain a top-level 'shows' list")

    by_rss: dict[str, tuple[str, str, str]] = {}
    for index, show in enumerate(shows):
        if not isinstance(show, dict):
            raise ValueError(f"shows[{index}] must be an object")
        tier = str(show.get("monitoring_tier") or "")
        if tier not in tiers:
            continue
        title = str(show.get("title") or "").strip()
        rss_url = normalize_rss_url(show.get("rss_url"))
        if not title or not rss_url:
            raise ValueError(f"shows[{index}] is missing title or rss_url")
        by_rss[rss_url] = (title, rss_url, tier)
    return list(by_rss.values())


def fetch_existing(urls: list[str]) -> set[str]:
    if not urls:
        return set()
    with getcursor(commit=False) as cur:
        cur.execute(
            "SELECT rss_url FROM podcasts.shows WHERE rss_url = ANY(%s)",
            (urls,),
        )
        return {
            canonical
            for (rss_url,) in cur.fetchall()
            if (canonical := normalize_rss_url(rss_url))
        }


def insert_candidates(rows: list[tuple[str, str, str]]) -> int:
    if not rows:
        return 0
    with getcursor(commit=True) as cur:
        inserted = execute_values(
            cur,
            """
            INSERT INTO podcasts.shows (title, rss_url)
            VALUES %s
            ON CONFLICT (rss_url) DO NOTHING
            RETURNING rss_url
            """,
            [(title, rss_url) for title, rss_url, _tier in rows],
            fetch=True,
        )
    return len(inserted or [])


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Promote show candidates from a discovery JSON report into "
            "podcasts.shows (dry run by default)"
        )
    )
    parser.add_argument("report", type=Path, help="Podcast discovery JSON report")
    parser.add_argument(
        "--tiers",
        nargs="+",
        choices=TIERS,
        default=list(TIERS),
        help="Tiers to promote (default: all)",
    )
    parser.add_argument("--prod", action="store_true", help="Use the prod DB")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert new shows; without this flag the command is a dry run",
    )
    args = parser.parse_args()

    rows = load_candidates(args.report, tiers=set(args.tiers))
    tier_counts = Counter(tier for _title, _rss, tier in rows)

    init_pool(prefix="prod" if args.prod else "dev")
    try:
        existing = fetch_existing([rss_url for _title, rss_url, _tier in rows])
        new_rows = [row for row in rows if row[1] not in existing]

        print(f"report: {args.report}")
        print(f"database: {'prod' if args.prod else 'dev'}")
        print(f"selected shows: {len(rows)} ({dict(tier_counts)})")
        print(f"already present: {len(existing)}")
        print(f"new shows: {len(new_rows)}")

        if not args.apply:
            print("dry run: no rows inserted; rerun with --apply to insert")
            return 0

        inserted = insert_candidates(new_rows)
        print(f"inserted: {inserted}")
        return 0
    finally:
        close_pool()


if __name__ == "__main__":
    raise SystemExit(main())
