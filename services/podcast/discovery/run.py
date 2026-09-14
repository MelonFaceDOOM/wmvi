"""CLI entrypoint for podcast discovery.

Usage:
    python -m services.podcast.discovery.run
    python -m services.podcast.discovery.run --include-apple
    python -m services.podcast.discovery.run --output candidates.json
    python -m services.podcast.discovery.run --compare-db --prod
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from services.podcast.transcript_sync.rss_url import normalize_rss_url

from .relevance import filter_relevant, score_shows
from .report import default_report_path, write_report
from .search import (
    DEFAULT_KEYWORDS,
    DEFAULT_PEOPLE,
    DiscoveredShow,
    search_all,
)

log = logging.getLogger(__name__)


def dedupe_and_rank(raw: list[DiscoveredShow]) -> list[DiscoveredShow]:
    """Merge duplicates by normalized RSS URL, accumulate keywords, sort by hit count."""
    by_url: dict[str, DiscoveredShow] = {}
    for show in raw:
        canonical = normalize_rss_url(show.rss_url)
        if canonical is None:
            continue
        if canonical in by_url:
            existing = by_url[canonical]
            existing.matching_keywords |= show.matching_keywords
            existing.source_apis |= show.source_apis
            if not existing.description and show.description:
                existing.description = show.description
            if not existing.title and show.title:
                existing.title = show.title
        else:
            by_url[canonical] = DiscoveredShow(
                title=show.title,
                rss_url=canonical,
                description=show.description,
                source_apis=set(show.source_apis),
                matching_keywords=set(show.matching_keywords),
            )
    ranked = sorted(by_url.values(), key=lambda s: len(s.matching_keywords), reverse=True)
    return ranked


def _fetch_existing_rss_urls() -> set[str]:
    from db.db import getcursor

    with getcursor() as cur:
        cur.execute(
            "SELECT rss_url FROM podcasts.shows WHERE rss_url IS NOT NULL AND rss_url <> ''"
        )
        urls: set[str] = set()
        for (rss_url,) in cur.fetchall():
            canonical = normalize_rss_url(rss_url)
            if canonical:
                urls.add(canonical)
        return urls


def mark_existing(shows: list[DiscoveredShow]) -> int:
    """Annotate candidates already present in podcasts.shows; do not remove them."""
    existing = _fetch_existing_rss_urls()
    count = 0
    for s in shows:
        s.already_monitored = normalize_rss_url(s.rss_url) in existing
        count += int(s.already_monitored)
    return count


def _log_acceptance(shows: list[DiscoveredShow]) -> None:
    n = len(shows)
    anti = sum(1 for s in shows if s.likely_anti_vax)
    pct = (100.0 * anti / n) if n else 0.0
    log.info(
        "Acceptance: %d vaccine-likely shows, %d likely anti-vax (%.1f%%)",
        n,
        anti,
        pct,
    )
    if n and pct < 15:
        log.warning("Anti-vax share %.1f%% is below the 15%% target", pct)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a report of shows worth reviewing for vaccine monitoring"
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=None,
        help=f"Override search keywords (default: {len(DEFAULT_KEYWORDS)} curated terms)",
    )
    parser.add_argument(
        "--people",
        nargs="+",
        default=None,
        help=f"Override person search (default: {len(DEFAULT_PEOPLE)} names)",
    )
    parser.add_argument(
        "--skip-people",
        action="store_true",
        help="Skip person-name queries",
    )
    parser.add_argument(
        "--include-apple",
        action="store_true",
        help="Also query Apple Podcasts Search (used automatically if PodcastIndex keys are missing)",
    )
    parser.add_argument(
        "--compare-db",
        action="store_true",
        help="Mark shows already present in podcasts.shows (never inserts)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Report path (default: timestamped JSON under discovery/results/)",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Use prod DB for --compare-db (default: dev)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    need_db = args.compare_db
    if need_db:
        from db.db import close_pool, init_pool

        init_pool(prefix="prod" if args.prod else "dev")

    try:
        log.info("Starting podcast discovery search …")
        people = [] if args.skip_people else args.people
        raw = search_all(
            args.keywords,
            people=people,
            include_apple=args.include_apple,
        )
        log.info("Raw results: %d shows from API queries", len(raw))

        ranked = dedupe_and_rank(raw)
        log.info("After dedup: %d unique shows", len(ranked))

        scored = score_shows(ranked)
        rss_ok = sum(1 for s in scored if s.rss_ok)
        log.info("RSS fetched for %d/%d feeds", rss_ok, len(scored))
        kept = filter_relevant(scored)

        log.info("Shows recommended for review: %d", len(kept))
        _log_acceptance(kept)

        if args.compare_db:
            already_tracked = mark_existing(kept)
            log.info("Already monitored: %d/%d", already_tracked, len(kept))

        output = args.output or default_report_path()
        write_report(
            output,
            kept,
            raw_results=len(raw),
            unique_feeds=len(ranked),
            rss_feeds_fetched=rss_ok,
        )
        log.info("Saved show report: %s", output)
    finally:
        if need_db:
            close_pool()


if __name__ == "__main__":
    main()
