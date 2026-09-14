"""Write a show-centric podcast discovery report."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .search import DiscoveredShow

RESULTS_DIR = Path(__file__).parent / "results"


def default_report_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return RESULTS_DIR / f"podcast_discovery_{stamp}.json"


def _show_dict(show: DiscoveredShow) -> dict:
    return {
        "title": show.title,
        "rss_url": show.rss_url,
        "description": show.description,
        "monitoring_tier": show.monitoring_tier,
        "monitoring_reason": show.monitoring_reason,
        "likely_anti_vax": show.likely_anti_vax,
        "already_monitored": show.already_monitored,
        "activity": {
            "latest_episode_published": show.latest_episode_published,
            "active_within_last_year": show.active_recently,
        },
        "discovery": {
            "sources": sorted(show.source_apis),
            "matching_queries": sorted(show.matching_keywords),
        },
        "episode_evidence": {
            "recent_episodes_scanned": show.episode_n,
            "vaccine_matches": show.episode_hits,
            "anti_vax_signal_matches": show.anti_hits,
            "examples": show.evidence,
        },
    }


def write_report(
    path: Path,
    shows: list[DiscoveredShow],
    *,
    raw_results: int,
    unique_feeds: int,
    rss_feeds_fetched: int,
) -> None:
    anti_n = sum(1 for show in shows if show.likely_anti_vax)
    tier_counts = {
        tier: sum(1 for show in shows if show.monitoring_tier == tier)
        for tier in ("priority", "recurring", "watchlist")
    }
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "purpose": (
            "Podcast shows worth reviewing for ongoing vaccine monitoring. "
            "Episode entries are supporting evidence, not output candidates."
        ),
        "summary": {
            "raw_search_results": raw_results,
            "unique_feeds_considered": unique_feeds,
            "rss_feeds_fetched": rss_feeds_fetched,
            "recommended_shows": len(shows),
            "likely_anti_vax_shows": anti_n,
            "likely_anti_vax_percent": round(100 * anti_n / len(shows), 1)
            if shows
            else 0.0,
            "tiers": tier_counts,
        },
        "tier_definitions": {
            "priority": (
                "Vaccine-focused/skeptic show identity, or at least five recent "
                "episodes matching vaccine terms; show must be active."
            ),
            "recurring": (
                "Active show with at least two recent episodes matching vaccine terms."
            ),
            "watchlist": (
                "Active show with one recent matching episode plus at least two independent "
                "discovery-query matches; useful for broad political/news shows."
            ),
        },
        "shows": [_show_dict(show) for show in shows],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
