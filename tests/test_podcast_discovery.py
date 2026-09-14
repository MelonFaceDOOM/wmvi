import json

from services.podcast.discovery.relevance import filter_relevant
from services.podcast.discovery.report import write_report
from services.podcast.discovery.search import DiscoveredShow
from scripts.oneoffs.promote_podcast_discovery import load_candidates


def _show(
    title: str,
    *,
    episode_hits: int,
    anti_hits: int = 0,
    queries: set[str] | None = None,
) -> DiscoveredShow:
    return DiscoveredShow(
        title=title,
        rss_url=f"https://example.com/{title}.xml",
        description="",
        source_apis={"test"},
        matching_keywords=queries or {"vaccine"},
        episode_n=30,
        episode_hits=episode_hits,
        anti_hits=anti_hits,
        rss_ok=True,
        active_recently=True,
    )


def test_filter_relevant_keeps_broad_show_as_watchlist():
    show = _show(
        "Broad Political Show",
        episode_hits=1,
        anti_hits=1,
        queries={"rfk", "Alex Berenson"},
    )

    assert filter_relevant([show]) == [show]
    assert show.monitoring_tier == "watchlist"
    # One skeptical episode is not enough to assign the show's overall stance.
    assert show.likely_anti_vax is False


def test_filter_relevant_rejects_single_weak_match():
    show = _show("General News Show", episode_hits=1)

    assert filter_relevant([show]) == []


def test_filter_relevant_rejects_inactive_show():
    show = _show("Vaccine Show", episode_hits=20)
    show.active_recently = False

    assert filter_relevant([show]) == []


def test_report_contains_shows_and_episode_evidence(tmp_path):
    show = _show("Vaccine Show", episode_hits=6, anti_hits=2)
    show.monitoring_tier = "priority"
    show.monitoring_reason = "six recent matches"
    show.evidence = [
        {
            "title": "A vaccine episode",
            "published": "2026-09-01",
            "likely_anti_vax": False,
        }
    ]
    path = tmp_path / "report.json"

    write_report(
        path,
        [show],
        raw_results=10,
        unique_feeds=8,
        rss_feeds_fetched=7,
    )

    report = json.loads(path.read_text())
    assert report["summary"]["recommended_shows"] == 1
    assert report["shows"][0]["title"] == "Vaccine Show"
    assert report["shows"][0]["episode_evidence"]["examples"][0]["title"] == (
        "A vaccine episode"
    )


def test_load_candidates_filters_tiers_and_deduplicates(tmp_path):
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "shows": [
                    {
                        "title": "Priority",
                        "rss_url": "HTTPS://EXAMPLE.COM/feed/",
                        "monitoring_tier": "priority",
                    },
                    {
                        "title": "Same feed",
                        "rss_url": "https://example.com/feed",
                        "monitoring_tier": "priority",
                    },
                    {
                        "title": "Watchlist",
                        "rss_url": "https://example.com/watch.xml",
                        "monitoring_tier": "watchlist",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    rows = load_candidates(report, tiers={"priority"})

    assert rows == [("Same feed", "https://example.com/feed", "priority")]
