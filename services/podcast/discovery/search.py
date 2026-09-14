"""Query PodcastIndex (preferred) and Apple Podcasts Search.

Search terms are a curated subset of taxonomy.vaccine_term (core_search_terms
plus a few discourse phrases). Product SKUs, agency acronyms, and bare disease
names are omitted because they do not work as podcast-title queries.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# Curated from taxonomy.vaccine_term. Kept: type=search, core_search_terms
# (vaccine/vaccines/vaccination, measles, mmr/covid/autism combos, rfk, NVIC).
# Dropped as podcast-title queries: product SKUs (gardasil, prevnar, …),
# agency acronyms (cdc, hhs, acip), bare diseases (flu, polio, hpv), and
# "… injection/jab" variants. Those match episode text in relevance.py instead.
DEFAULT_KEYWORDS: list[str] = [
    "vaccine",
    "vaccines",
    "vaccination",
    "measles",
    "measles vaccine",
    "mmr vaccine",
    "mmr autism",
    "covid vaccine",
    "autism vaccine",
    "vaccines autism",
    "rfk",
    "national vaccine information center",
    "vaccine injury",
    "vaccine shedding",
    "medical freedom",
    "anti-vaccine",
    "the highwire",
    "nvic",
]

# taxonomy.vaccine_term names that are people, plus hosts Apple/PI may index.
DEFAULT_PEOPLE: list[str] = [
    "rfk",
    "martin kulldorff",
    "Del Bigtree",
    "Peter McCullough",
    "Robert Malone",
    "Joseph Mercola",
    "Alex Berenson",
    "Andrew Wakefield",
    "Sherri Tenpenny",
]

REQUEST_DELAY_S = 0.8


@dataclass
class DiscoveredShow:
    title: str
    rss_url: str
    description: str
    source_apis: set[str] = field(default_factory=set)
    matching_keywords: set[str] = field(default_factory=set)
    episode_n: int = 0
    episode_hits: int = 0
    anti_hits: int = 0
    likely_anti_vax: bool = False
    rss_ok: bool = False
    evidence: list[dict[str, str | bool]] = field(default_factory=list)
    latest_episode_published: str = ""
    active_recently: bool = False
    monitoring_tier: str = ""
    monitoring_reason: str = ""
    already_monitored: bool = False


# ---------------------------------------------------------------------------
# Apple Podcasts Search API
# ---------------------------------------------------------------------------

_APPLE_SEARCH_URL = "https://itunes.apple.com/search"


def search_apple(keyword: str, *, limit: int = 50) -> list[DiscoveredShow]:
    """Search Apple Podcasts for shows matching *keyword*."""
    params = {"term": keyword, "media": "podcast", "limit": limit}
    try:
        resp = requests.get(_APPLE_SEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Apple search failed for %r: %s", keyword, exc)
        return []

    results: list[DiscoveredShow] = []
    for item in resp.json().get("results", []):
        feed_url = item.get("feedUrl")
        if not feed_url:
            continue
        results.append(
            DiscoveredShow(
                title=item.get("collectionName") or item.get("trackName") or "",
                rss_url=feed_url,
                description=item.get("artistName") or "",
                source_apis={"apple"},
                matching_keywords={keyword},
            )
        )
    return results


# ---------------------------------------------------------------------------
# PodcastIndex API
# ---------------------------------------------------------------------------

_PI_SEARCH_URL = "https://api.podcastindex.org/api/1.0/search/byterm"
_PI_PERSON_URL = "https://api.podcastindex.org/api/1.0/search/byperson"


class PodcastIndexCredentialsError(RuntimeError):
    """Raised when PodcastIndex is requested but env credentials are missing."""


def _pi_credentials(
    api_key: str | None = None,
    api_secret: str | None = None,
) -> tuple[str, str]:
    api_key = api_key or os.environ.get("PODCAST_INDEX_KEY", "")
    api_secret = api_secret or os.environ.get("PODCAST_INDEX_SECRET", "")
    if not api_key or not api_secret:
        raise PodcastIndexCredentialsError(
            "PodcastIndex credentials not set. Export PODCAST_INDEX_KEY and "
            "PODCAST_INDEX_SECRET (free at https://api.podcastindex.org)."
        )
    return api_key, api_secret


def _pi_auth_headers(api_key: str, api_secret: str) -> dict[str, str]:
    epoch = str(int(time.time()))
    data = api_key + api_secret + epoch
    sha1 = hashlib.sha1(data.encode("utf-8")).hexdigest()
    return {
        "X-Auth-Key": api_key,
        "X-Auth-Date": epoch,
        "Authorization": sha1,
        "User-Agent": "wmvi-discovery/1.0",
    }


def _pi_get(
    url: str,
    params: dict,
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
) -> dict | None:
    api_key, api_secret = _pi_credentials(api_key, api_secret)
    headers = _pi_auth_headers(api_key, api_secret)
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("PodcastIndex request failed %s %s: %s", url, params, exc)
        return None
    return resp.json()


def search_podcastindex(
    keyword: str,
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    limit: int = 50,
) -> list[DiscoveredShow]:
    """Search PodcastIndex feeds by title / author / owner."""
    data = _pi_get(
        _PI_SEARCH_URL,
        {"q": keyword, "max": limit, "fulltext": 1},
        api_key=api_key,
        api_secret=api_secret,
    )
    if not data:
        return []

    results: list[DiscoveredShow] = []
    for feed in data.get("feeds", []):
        rss = feed.get("url") or feed.get("originalUrl") or ""
        if not rss:
            continue
        results.append(
            DiscoveredShow(
                title=feed.get("title") or "",
                rss_url=rss,
                description=feed.get("description") or "",
                source_apis={"podcastindex"},
                matching_keywords={keyword},
            )
        )
    return results


def search_podcastindex_people(
    person: str,
    *,
    api_key: str | None = None,
    api_secret: str | None = None,
    limit: int = 50,
) -> list[DiscoveredShow]:
    """Find feeds whose episodes mention *person*."""
    data = _pi_get(
        _PI_PERSON_URL,
        {"q": person, "max": limit, "fulltext": 1},
        api_key=api_key,
        api_secret=api_secret,
    )
    if not data:
        return []

    results: list[DiscoveredShow] = []
    for item in data.get("items", []):
        rss = item.get("feedUrl") or ""
        if not rss:
            continue
        results.append(
            DiscoveredShow(
                title=item.get("feedTitle") or "",
                rss_url=rss,
                description=item.get("feedAuthor") or item.get("description") or "",
                source_apis={"podcastindex"},
                matching_keywords={person},
            )
        )
    return results


# ---------------------------------------------------------------------------
# Combined search
# ---------------------------------------------------------------------------


def search_all(
    keywords: list[str] | None = None,
    *,
    people: list[str] | None = None,
    include_apple: bool = False,
    delay: float = REQUEST_DELAY_S,
) -> list[DiscoveredShow]:
    """Query PodcastIndex and/or Apple and return raw (un-deduped) results.

    If PodcastIndex keys are missing, Apple is used automatically so the CLI
    still produces a candidate list.
    """
    if keywords is None:
        keywords = DEFAULT_KEYWORDS
    if people is None:
        people = DEFAULT_PEOPLE

    use_pi = True
    try:
        _pi_credentials()
    except PodcastIndexCredentialsError:
        use_pi = False
        if not include_apple:
            log.warning(
                "PODCAST_INDEX_KEY/SECRET not set; falling back to Apple search. "
                "Pass --include-apple to make this explicit."
            )
            include_apple = True

    all_results: list[DiscoveredShow] = []
    if use_pi:
        for kw in keywords:
            log.info("PodcastIndex keyword: %r", kw)
            all_results.extend(search_podcastindex(kw))
            time.sleep(delay)
        for person in people:
            log.info("PodcastIndex person: %r", person)
            all_results.extend(search_podcastindex_people(person))
            time.sleep(delay)

    if include_apple:
        apple_terms = list(keywords)
        if not use_pi:
            for person in people:
                if person not in apple_terms:
                    apple_terms.append(person)
        for kw in apple_terms:
            log.info("Apple keyword: %r", kw)
            all_results.extend(search_apple(kw))
            time.sleep(delay)

    return all_results
