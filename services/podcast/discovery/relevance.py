"""Confirm search hits actually discuss vaccines by scanning RSS episode text.

Apple/PodcastIndex title search is fuzzy: many returned shows never mention
vaccines. We fetch each feed and score recent episode titles/descriptions
against a compact matcher derived from taxonomy.vaccine_term.
"""
from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import feedparser
import requests

from .search import DiscoveredShow

log = logging.getLogger(__name__)

USER_AGENT = "wmvi-podcast-discovery/1.0"
RSS_TIMEOUT_S = 8
MAX_EPISODES = 30
RSS_WORKERS = 20
MAX_EVIDENCE = 5
ACTIVE_WITHIN_DAYS = 365

# Episode-text matcher: taxonomy search-terms + vaccine stems, not product SKUs
# or bare "cdc"/"hhs"/"flu" (those fire on unrelated news).
_VACCINE_RE = re.compile(
    r"""
    vaccin\w*
    | \bvaxx?\b
    | immuniz\w*
    | \bmmr\b
    | \bmrna\b
    | \bvaers\b
    | vaccine\s+injur
    | vaccine\s+mandat
    | vaccine\s+shed
    | medical\s+freedom
    | measles\s+outbreak
    | measles\s+vaccin
    | covid\s+vaccin
    | covid\s+shot
    | the\s+jab\b
    | anti-?vax
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Skeptic / anti-vax discourse (not merely "an episode about vaccines").
_ANTI_RE = re.compile(
    r"""
    anti-?vax
    | vaccine\s+injur
    | \bvaers\b
    | vaccine\s+shed
    | medical\s+freedom
    | health\s+freedom
    | died\s+suddenly
    | myocarditis
    | spike\s+protein
    | the\s+jab\b
    | nvic\b
    | highwire
    | health\s+defense
    | (?:autism.{0,20}vaccin|vaccin.{0,20}autism)
    | mmr\s+autism
    | informed\s+consent
    | del\s+bigtree
    | mercola
    | mccullough
    | wakefield
    | tenpenny
    | robert\s+malone
    """,
    re.IGNORECASE | re.VERBOSE,
)

_TITLE_VACCINE_RE = re.compile(
    r"vaccin|\bvaxx?\b|immuniz|\bmmr\b|\bvaers\b|nvic|"
    r"health defense|medical freedom|mccullough report|"
    r"the highwire|del bigtree|rfk\s*jr",
    re.IGNORECASE,
)

_TITLE_ANTI_RE = re.compile(
    r"the highwire|health defense|\bnvic\b|medical freedom|mccullough|"
    r"anti-?vax|vaccine injur|unreported truth|mercola|bigtree|"
    r"rfk\s*jr|tenpenny",
    re.IGNORECASE,
)

_TITLE_PRO_OR_DEBUNK_RE = re.compile(
    r"vax talk|osterholm|public health|science vs|this week in virology|"
    r"vaccine education|vec vaccine",
    re.IGNORECASE,
)

_NOISE_TITLE_RE = re.compile(
    r"optimism vaccine|behavioural vaccine|neurotic vaccine|"
    r"humor highwire|beyond prequalification",
    re.IGNORECASE,
)

def _entry_blob(entry) -> str:
    parts = [
        getattr(entry, "title", None) or "",
        getattr(entry, "summary", None) or "",
        getattr(entry, "summary_detail", {}).get("value", "")
        if isinstance(getattr(entry, "summary_detail", None), dict)
        else "",
    ]
    return " ".join(p for p in parts if p)


def _entry_published(entry) -> str:
    parsed = (
        getattr(entry, "published_parsed", None)
        or getattr(entry, "updated_parsed", None)
    )
    if parsed:
        return datetime(*parsed[:6], tzinfo=timezone.utc).isoformat()
    return ""


def fetch_episode_records(
    rss_url: str,
    *,
    max_episodes: int = MAX_EPISODES,
) -> list[dict[str, str]] | None:
    try:
        resp = requests.get(
            rss_url,
            timeout=RSS_TIMEOUT_S,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.debug("RSS fetch failed %s: %s", rss_url, exc)
        return None
    parsed = feedparser.parse(resp.content)
    records: list[dict[str, str]] = []
    for entry in (parsed.entries or [])[:max_episodes]:
        records.append(
            {
                "title": str(getattr(entry, "title", None) or "").strip(),
                "published": _entry_published(entry),
                "text": _entry_blob(entry),
            }
        )
    return records


def score_blobs(blobs: list[str]) -> tuple[int, int]:
    hits = 0
    anti = 0
    for blob in blobs:
        if _VACCINE_RE.search(blob):
            hits += 1
        if _ANTI_RE.search(blob):
            anti += 1
    return hits, anti


def monitoring_tier(show: DiscoveredShow) -> str | None:
    """Return why a show is worth further monitoring review.

    Episode matches are evidence for selecting a feed; episodes themselves are
    never output as candidates. A broad show can enter the watchlist with one
    recent vaccine episode when independent searches found it more than once.
    """
    if _NOISE_TITLE_RE.search(show.title or ""):
        return None
    if not show.active_recently:
        return None
    if _TITLE_ANTI_RE.search(show.title or ""):
        return "priority"
    if _TITLE_VACCINE_RE.search(show.title or "") and show.episode_hits >= 1:
        return "priority"
    if show.episode_hits >= 5:
        return "priority"
    if show.episode_hits >= 2:
        return "recurring"
    if show.episode_hits >= 1 and len(show.matching_keywords) >= 2:
        return "watchlist"
    return None


def monitoring_reason(show: DiscoveredShow, tier: str) -> str:
    if _TITLE_ANTI_RE.search(show.title or ""):
        return "show identity is strongly associated with vaccine-skeptic discourse"
    if _TITLE_VACCINE_RE.search(show.title or ""):
        return "vaccine-focused show with recent matching episode evidence"
    if tier == "priority":
        return f"{show.episode_hits} of {show.episode_n} recent episodes match vaccine terms"
    if tier == "recurring":
        return f"{show.episode_hits} of {show.episode_n} recent episodes match vaccine terms"
    return (
        f"one recent vaccine episode plus {len(show.matching_keywords)} "
        "independent discovery-query matches"
    )


def is_likely_anti_vax(show: DiscoveredShow) -> bool:
    if _TITLE_PRO_OR_DEBUNK_RE.search(show.title or ""):
        return False
    if _TITLE_ANTI_RE.search(show.title or ""):
        return True
    if show.episode_hits and show.anti_hits >= 3:
        if show.anti_hits / show.episode_hits >= 0.5:
            return True
    return False


def _score_one(show: DiscoveredShow) -> DiscoveredShow:
    records = fetch_episode_records(show.rss_url)
    if records is None:
        show.rss_ok = False
        show.likely_anti_vax = bool(_TITLE_ANTI_RE.search(show.title))
        return show
    show.rss_ok = True
    show.episode_n = len(records)
    published = [record["published"] for record in records if record["published"]]
    show.latest_episode_published = max(published, default="")
    if show.latest_episode_published:
        latest = datetime.fromisoformat(show.latest_episode_published)
        show.active_recently = latest >= datetime.now(timezone.utc) - timedelta(
            days=ACTIVE_WITHIN_DAYS
        )
    else:
        # Some valid feeds omit dates; recent entries still make them reviewable.
        show.active_recently = bool(records)
    hits, anti = score_blobs([record["text"] for record in records])
    show.episode_hits = hits
    show.anti_hits = anti
    show.evidence = [
        {
            "title": record["title"],
            "published": record["published"],
            "likely_anti_vax": bool(_ANTI_RE.search(record["text"])),
        }
        for record in records
        if _VACCINE_RE.search(record["text"])
    ][:MAX_EVIDENCE]
    show.likely_anti_vax = is_likely_anti_vax(show)
    return show


def score_shows(shows: list[DiscoveredShow], *, workers: int = RSS_WORKERS) -> list[DiscoveredShow]:
    if not shows:
        return []
    out: list[DiscoveredShow] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_score_one, s): s for s in shows}
        for i, fut in enumerate(as_completed(futs), start=1):
            try:
                out.append(fut.result())
            except Exception as exc:
                show = futs[fut]
                log.warning("score failed %s: %s", show.rss_url, exc)
                out.append(show)
            if i % 25 == 0:
                log.info("RSS-scored %d/%d feeds", i, len(shows))
    return out


def filter_relevant(shows: list[DiscoveredShow]) -> list[DiscoveredShow]:
    kept: list[DiscoveredShow] = []
    for s in shows:
        tier = monitoring_tier(s)
        if tier is None:
            continue
        s.monitoring_tier = tier
        s.monitoring_reason = monitoring_reason(s, tier)
        s.likely_anti_vax = is_likely_anti_vax(s)
        kept.append(s)
    tier_rank = {"priority": 2, "recurring": 1, "watchlist": 0}
    kept.sort(
        key=lambda s: (
            tier_rank[s.monitoring_tier],
            s.episode_hits,
            len(s.matching_keywords),
            s.anti_hits,
        ),
        reverse=True,
    )
    return kept
