"""
Telegram Channel Discovery Probe

Tests 3 Telegram-API approaches for finding new channels to scrape:
  1. Forwarded-from analysis (who do our seed channels forward from?)
  2. Similar channels API (Telegram's channel recommendations)
  3. Public post/name search (channels.searchPosts + contacts.search + hashtags)

The DB is used only to confirm curated term names exist in taxonomy.vaccine_term.
Those names are the relevance-scoring terms. Approach 3 uses a small paid
searchPosts query budget (paginated; extra pages are free) plus free hashtag
and contacts.search lookups.

Progress is written to probe_results/checkpoint.json after each approach, each
search term, and each scored channel. Re-running resumes from that file.

Usage:
    python -m services.telegram_monitor.probe_channels --prod
    python -m services.telegram_monitor.probe_channels --prod --fresh
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from telethon import TelegramClient, errors
from telethon.tl.functions.channels import (
    CheckSearchPostsFloodRequest,
    GetChannelRecommendationsRequest,
    SearchPostsRequest,
)
from telethon.tl.functions.contacts import SearchRequest as ContactsSearch
from telethon.tl.types import (
    Channel,
    InputPeerChannel,
    InputPeerEmpty,
    PeerChannel,
)

from dotenv import load_dotenv

load_dotenv()

from .monitor import (
    API_ID,
    API_HASH,
    SESSION,
    CHANNEL_LIST,
    require_session_file,
    _setup_logging,
)
from .tg_scrape import probe_channel, norm_channel

log = logging.getLogger(__name__)

RESULTS_DIR = Path(__file__).parent / "probe_results"
CHECKPOINT_PATH = RESULTS_DIR / "checkpoint.json"
SCORE_TIMEOUT_S = 90
SCORE_ATTEMPTS = 2  # first try + one retry
RETRY_SCORE_ERRORS = {"timeout"}
# Stay under Telegram's resolve-username hammer. Longer waits skip the channel.
MAX_FLOOD_SLEEP_S = 90
SCORE_PAUSE_S = 2.0
SEARCH_VERSION = 2
SEARCH_POST_PAGES = 3
SEARCH_POST_PAGE_SIZE = 100
HASHTAG_PAGES = 3

# Full-text public post searches. Each *new query* costs 1 of ~10 daily slots;
# extra pages of the same query are free. Do not exceed the daily budget.
SEARCH_POST_QUERIES = [
    "vaccine injury",
    "measles vaccine",
    "mmr autism",
    "vaccine mandate",
    "died suddenly",
    "vaccine damage",
    "covid vaccine",
    "vaccine autism",
]

# Hashtag search is free (no searchPosts slot).
SEARCH_HASHTAGS = [
    "vaccine",
    "measles",
    "mmr",
    "vaers",
    "vaxx",
]

# contacts.search matches public channel *names*, free, no pagination.
SEARCH_CONTACTS_QUERIES = [
    "vaccine",
    "measles",
    "antivax",
    "vaxx",
    "mmr",
    "nvic",
    "vaers",
]

# Curated ~30 names. Must match taxonomy.vaccine_term.name (case-insensitive).
# Measles/MMR kept in full; the rest are high-signal for vaccine / anti-vax discussion.
CURATED_PROBE_TERMS = [
    "measles",
    "measles vaccine",
    "measles jab",
    "measles injection",
    "mmr vaccine",
    "mmr autism",
    "priorix",
    "vaccine",
    "vaccines",
    "vaxx",
    "vaccination",
    "vaccines autism",
    "autism vaccine",
    "childhood vaccines autism",
    "covid vaccine",
    "covid",
    "covid-19",
    "spikevax",
    "comirnaty",
    "oxford-astrazeneca",
    "gardasil",
    "hpv",
    "polio",
    "dtap",
    "rotavirus",
    "rfk",
    "national vaccine information center",
    "martin kulldorff",
    "retsef levi",
    "vaccine panel",
]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def fetch_vaccine_terms() -> list[str]:
    """Load taxonomy.vaccine_term.name values (requires an initialized DB pool)."""
    from db.db import getcursor

    with getcursor(commit=False) as cur:
        cur.execute(
            """
            SELECT name
            FROM taxonomy.vaccine_term
            ORDER BY name
            """
        )
        return [str(row[0]) for row in cur.fetchall() if row and row[0]]


def select_probe_terms(db_names: list[str]) -> list[str]:
    """Keep curated names that exist in the DB, using DB spelling."""
    by_lower = {n.lower(): n for n in db_names}
    selected: list[str] = []
    missing: list[str] = []
    for name in CURATED_PROBE_TERMS:
        canonical = by_lower.get(name.lower())
        if canonical is None:
            missing.append(name)
        else:
            selected.append(canonical)
    if missing:
        log.warning("curated terms not in taxonomy.vaccine_term: %s", missing)
    return selected


def _seed_usernames() -> set[str]:
    return {norm_channel(c).lower() for c in CHANNEL_LIST}


def _keyword_hit_rate(texts: list[str], pattern: re.Pattern[str] | None) -> float:
    if not texts or pattern is None:
        return 0.0
    hits = sum(1 for t in texts if pattern.search(t))
    return hits / len(texts)


def _english_rate(texts: list[str]) -> float:
    """Cheap heuristic: fraction of texts that are mostly ASCII letters."""
    if not texts:
        return 0.0
    en = 0
    for t in texts:
        if not t:
            continue
        ascii_letters = sum(1 for c in t if c.isascii() and c.isalpha())
        total_alpha = sum(1 for c in t if c.isalpha())
        if total_alpha > 0 and ascii_letters / total_alpha > 0.7:
            en += 1
    return en / len(texts)


def _channel_key(entity) -> str:
    """Stable string key for a channel entity."""
    username = getattr(entity, "username", None)
    if username:
        return username.lower()
    return str(getattr(entity, "id", "unknown"))


def _term_pattern(terms: list[str]) -> re.Pattern[str] | None:
    if not terms:
        return None
    return re.compile("|".join(re.escape(k) for k in terms), re.IGNORECASE)


def _flood_sleep_s(e: errors.FloodWaitError) -> int:
    return int(getattr(e, "seconds", 0) or 0) + 1


async def _sleep_flood_or_skip(e: errors.FloodWaitError, where: str) -> bool:
    """Sleep out a short FloodWait. Return False if the wait is too long (caller should skip)."""
    wait_s = _flood_sleep_s(e)
    if wait_s > MAX_FLOOD_SLEEP_S:
        log.warning(
            "%s: FloodWait %ds exceeds cap %ds; skipping instead of sleeping",
            where, e.seconds, MAX_FLOOD_SLEEP_S,
        )
        return False
    log.warning("%s: FloodWait %ds; sleeping %ds", where, e.seconds, wait_s)
    await asyncio.sleep(wait_s)
    return True


async def _safe_flood(coro):
    """Await a coroutine; on a short FloodWait, sleep and retry once."""
    try:
        return await coro
    except errors.FloodWaitError as e:
        if not await _sleep_flood_or_skip(e, "api"):
            raise
        return await coro


# ------------------------------------------------------------------
# Checkpoint
# ------------------------------------------------------------------

def _empty_keyword_partial() -> dict[str, Any]:
    return {
        "search_version": SEARCH_VERSION,
        "completed": False,
        "done_jobs": [],
        "active_job": None,
        "hit_counts": {},
        "hit_details": {},
        "keyword_breakdown": {},
    }


def _empty_state(terms: list[str]) -> dict[str, Any]:
    return {
        "terms": terms,
        "approaches": {},
        "keyword_search_partial": _empty_keyword_partial(),
        "all_channels": {},
        "scored": {},
    }


def load_checkpoint() -> dict[str, Any] | None:
    if not CHECKPOINT_PATH.exists():
        return None
    try:
        data = json.loads(CHECKPOINT_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        log.exception("failed to read %s", CHECKPOINT_PATH)
        return None
    if not isinstance(data, dict):
        return None
    data.setdefault("approaches", {})
    data.setdefault("keyword_search_partial", _empty_keyword_partial())
    data.setdefault("all_channels", {})
    data.setdefault("scored", {})
    data.setdefault("terms", [])
    return data


def save_checkpoint(state: dict[str, Any]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, default=str))
    tmp.replace(CHECKPOINT_PATH)


def merge_all_channels(approach_results: list[dict]) -> dict[str, dict]:
    all_channels: dict[str, dict] = {}
    for ar in approach_results:
        for ch in ar.get("channels") or []:
            key = ch["channel_key"]
            if key not in all_channels:
                all_channels[key] = {
                    "channel_key": key,
                    "id": ch.get("id"),
                    "username": ch.get("username"),
                    "title": ch.get("title"),
                    "discovery_methods": [],
                }
            method = ar["approach"]
            methods = all_channels[key]["discovery_methods"]
            if method not in methods:
                methods.append(method)
    return all_channels


def persist(state: dict[str, Any], write_report: bool = True) -> None:
    save_checkpoint(state)
    if not write_report:
        return
    approach_results = [
        state["approaches"][name]
        for name in ("forwards", "similar", "keyword_search")
        if name in state["approaches"]
    ]
    _write_reports(
        approach_results,
        state.get("scored") or {},
        RESULTS_DIR,
        state.get("terms") or [],
        partial=len(state.get("scored") or {}) < len(state.get("all_channels") or {}),
    )


# ------------------------------------------------------------------
# Approach 1: Forwarded-from analysis
# ------------------------------------------------------------------

async def approach_forwards(client: TelegramClient, msg_limit: int = 500) -> dict[str, Any]:
    """Check who the seed channels forward from."""
    log.info("=== Approach 1: Forwarded-from analysis (limit=%d per channel) ===", msg_limit)
    seeds = _seed_usernames()
    fwd_counts: Counter = Counter()
    fwd_details: dict[str, dict] = {}

    for ch in CHANNEL_LIST:
        log.info("  forwards: scanning %s", ch)
        try:
            entity = await probe_channel(client, ch)
            if not entity:
                continue
            async for msg in client.iter_messages(entity, limit=msg_limit):
                fwd = getattr(msg, "fwd_from", None)
                if not fwd:
                    continue
                from_id = getattr(fwd, "from_id", None)
                if not isinstance(from_id, PeerChannel):
                    continue
                try:
                    fwd_entity = await _safe_flood(client.get_entity(from_id))
                except (errors.ChannelPrivateError, errors.UsernameInvalidError):
                    continue
                except Exception:
                    continue
                key = _channel_key(fwd_entity)
                if key in seeds:
                    continue
                fwd_counts[key] += 1
                if key not in fwd_details:
                    fwd_details[key] = {
                        "id": getattr(fwd_entity, "id", None),
                        "username": getattr(fwd_entity, "username", None),
                        "title": getattr(fwd_entity, "title", None),
                    }
        except errors.FloodWaitError as e:
            if not await _sleep_flood_or_skip(e, f"forwards {ch}"):
                continue
        except Exception:
            log.exception("  error scanning forwards for %s", ch)
        await asyncio.sleep(SCORE_PAUSE_S)

    ranked = [
        {**fwd_details[k], "forward_count": c, "channel_key": k}
        for k, c in fwd_counts.most_common()
    ]
    log.info("  forwards: found %d unique channels", len(ranked))
    return {"approach": "forwards", "channels": ranked}


# ------------------------------------------------------------------
# Approach 2: Similar channels API
# ------------------------------------------------------------------

async def approach_similar(client: TelegramClient) -> dict[str, Any]:
    """Use GetChannelRecommendationsRequest for each seed."""
    log.info("=== Approach 2: Similar channels API ===")
    seeds = _seed_usernames()
    rec_counts: Counter = Counter()
    rec_details: dict[str, dict] = {}

    for ch in CHANNEL_LIST:
        log.info("  similar: querying %s", ch)
        try:
            entity = await probe_channel(client, ch)
            if not entity:
                continue
            result = await _safe_flood(
                client(GetChannelRecommendationsRequest(channel=entity))
            )
            chats = getattr(result, "chats", [])
            for chat in chats:
                if not isinstance(chat, Channel):
                    continue
                key = _channel_key(chat)
                if key in seeds:
                    continue
                rec_counts[key] += 1
                if key not in rec_details:
                    rec_details[key] = {
                        "id": chat.id,
                        "username": getattr(chat, "username", None),
                        "title": getattr(chat, "title", None),
                    }
        except errors.ChatAdminRequiredError:
            log.warning("  similar: admin required for %s, skipping", ch)
        except errors.FloodWaitError as e:
            if not await _sleep_flood_or_skip(e, f"similar {ch}"):
                continue
        except Exception:
            log.exception("  error getting similar for %s", ch)
        await asyncio.sleep(SCORE_PAUSE_S)

    ranked = [
        {**rec_details[k], "seed_overlap": c, "channel_key": k}
        for k, c in rec_counts.most_common()
    ]
    log.info("  similar: found %d unique channels", len(ranked))
    return {"approach": "similar", "channels": ranked}


# ------------------------------------------------------------------
# Approach 3: Public channel search
# ------------------------------------------------------------------

def _search_jobs() -> list[dict[str, str]]:
    jobs = [{"kind": "posts", "q": q} for q in SEARCH_POST_QUERIES]
    jobs += [{"kind": "hashtag", "q": h} for h in SEARCH_HASHTAGS]
    jobs += [{"kind": "contacts", "q": q} for q in SEARCH_CONTACTS_QUERIES]
    return jobs


def _job_key(job: dict[str, str]) -> str:
    return f"{job['kind']}:{job['q']}"


def _channel_record(entity) -> dict[str, Any] | None:
    if not isinstance(entity, Channel):
        return None
    key = _channel_key(entity)
    return {
        "id": getattr(entity, "id", None),
        "username": getattr(entity, "username", None),
        "title": getattr(entity, "title", None),
        "access_hash": getattr(entity, "access_hash", None),
        "broadcast": bool(getattr(entity, "broadcast", False)),
        "megagroup": bool(getattr(entity, "megagroup", False)),
        "channel_key": key,
    }


def _add_search_hit(
    *,
    entity,
    label: str,
    seeds: set[str],
    hit_counts: Counter,
    hit_details: dict[str, dict],
    keyword_breakdown: dict[str, list[str]],
) -> bool:
    rec = _channel_record(entity)
    if rec is None:
        return False
    key = rec["channel_key"]
    if key in seeds:
        return False
    hit_counts[key] += 1
    keyword_breakdown[key].append(label)
    if key not in hit_details:
        hit_details[key] = rec
    return True


def _offset_peer_from_partial(active: dict[str, Any] | None):
    if not active:
        return InputPeerEmpty()
    cid = active.get("offset_peer_id")
    ah = active.get("offset_peer_access_hash")
    if cid is None or ah is None:
        return InputPeerEmpty()
    return InputPeerChannel(int(cid), int(ah))


def _next_offset(result, last_msg) -> dict[str, Any] | None:
    if last_msg is None:
        return None
    peer = getattr(last_msg, "peer_id", None)
    if not isinstance(peer, PeerChannel):
        return None
    chats = {
        c.id: c
        for c in (getattr(result, "chats", None) or [])
        if isinstance(c, Channel)
    }
    ch = chats.get(peer.channel_id)
    if ch is None or getattr(ch, "access_hash", None) is None:
        return None
    return {
        "offset_rate": int(getattr(result, "next_rate", 0) or 0),
        "offset_id": int(last_msg.id),
        "offset_peer_id": int(ch.id),
        "offset_peer_access_hash": int(ch.access_hash),
    }


async def _posts_quota(client: TelegramClient, query: str | None = None) -> dict[str, Any]:
    flood = await client(CheckSearchPostsFloodRequest(query=query))
    return {
        "remains": int(getattr(flood, "remains", 0) or 0),
        "query_is_free": bool(getattr(flood, "query_is_free", False)),
        "total_daily": int(getattr(flood, "total_daily", 0) or 0),
        "stars_amount": getattr(flood, "stars_amount", None),
    }


async def _search_posts_page(
    client: TelegramClient,
    *,
    query: str | None = None,
    hashtag: str | None = None,
    offset_rate: int = 0,
    offset_peer=None,
    offset_id: int = 0,
):
    kwargs: dict[str, Any] = {}
    if query is not None:
        kwargs["query"] = query
    if hashtag is not None:
        kwargs["hashtag"] = hashtag
    return await client(
        SearchPostsRequest(
            offset_rate=offset_rate,
            offset_peer=offset_peer or InputPeerEmpty(),
            offset_id=offset_id,
            limit=SEARCH_POST_PAGE_SIZE,
            **kwargs,
        )
    )


def _ingest_search_result(
    result,
    *,
    label: str,
    seeds: set[str],
    hit_counts: Counter,
    hit_details: dict[str, dict],
    keyword_breakdown: dict[str, list[str]],
) -> tuple[int, int]:
    """Add chats + message peers. Returns (n_messages, n_new_or_hit_channels)."""
    n_hit = 0
    for chat in getattr(result, "chats", None) or []:
        if _add_search_hit(
            entity=chat,
            label=label,
            seeds=seeds,
            hit_counts=hit_counts,
            hit_details=hit_details,
            keyword_breakdown=keyword_breakdown,
        ):
            n_hit += 1
    msgs = getattr(result, "messages", None) or []
    return len(msgs), n_hit


async def approach_keyword_search(
    client: TelegramClient,
    partial: dict[str, Any] | None = None,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Discover public channels via searchPosts, hashtags, and contacts.search."""
    log.info(
        "=== Approach 3: public search (searchPosts v%d, %d queries / %d hashtags / %d names) ===",
        SEARCH_VERSION,
        len(SEARCH_POST_QUERIES),
        len(SEARCH_HASHTAGS),
        len(SEARCH_CONTACTS_QUERIES),
    )
    seeds = _seed_usernames()
    partial = dict(partial or _empty_keyword_partial())
    if partial.get("search_version") != SEARCH_VERSION:
        partial = _empty_keyword_partial()

    done = set(partial.get("done_jobs") or [])
    hit_counts: Counter = Counter(
        {k: int(v) for k, v in (partial.get("hit_counts") or {}).items()}
    )
    hit_details: dict[str, dict] = dict(partial.get("hit_details") or {})
    keyword_breakdown: dict[str, list[str]] = defaultdict(list)
    for key, kws in (partial.get("keyword_breakdown") or {}).items():
        keyword_breakdown[key].extend(kws)
    active = partial.get("active_job")

    def snapshot() -> dict[str, Any]:
        return {
            "search_version": SEARCH_VERSION,
            "completed": False,
            "done_jobs": sorted(done),
            "active_job": active,
            "hit_counts": dict(hit_counts),
            "hit_details": hit_details,
            "keyword_breakdown": {k: list(v) for k, v in keyword_breakdown.items()},
        }

    def save() -> None:
        if on_progress:
            on_progress(snapshot())

    jobs = _search_jobs()
    remaining = [j for j in jobs if _job_key(j) not in done]
    log.info("  search: %d jobs already done, %d remaining", len(done), len(remaining))

    skip_paid_posts = False

    for job in remaining:
        kind = job["kind"]
        q = job["q"]
        key = _job_key(job)
        label = key

        if kind == "posts" and skip_paid_posts:
            log.info("  search: deferring %s until a free searchPosts slot is available", key)
            continue

        if kind == "contacts":
            log.info("  search: contacts.search %r", q)
            try:
                result = await client(ContactsSearch(q=q, limit=50))
                n_hit = 0
                for chat in getattr(result, "chats", None) or []:
                    if _add_search_hit(
                        entity=chat,
                        label=label,
                        seeds=seeds,
                        hit_counts=hit_counts,
                        hit_details=hit_details,
                        keyword_breakdown=keyword_breakdown,
                    ):
                        n_hit += 1
                log.info("  search: contacts %r hit %d channels", q, n_hit)
            except errors.FloodWaitError as e:
                if not await _sleep_flood_or_skip(e, f"contacts {q!r}"):
                    log.warning("  search: giving up on contacts %r", q)
            except Exception:
                log.exception("  error during contacts.search %r", q)
            done.add(key)
            active = None
            save()
            await asyncio.sleep(SCORE_PAUSE_S)
            continue

        pages_total = SEARCH_POST_PAGES if kind == "posts" else HASHTAG_PAGES
        if not (active and active.get("key") == key):
            active = {
                "key": key,
                "kind": kind,
                "q": q,
                "pages_done": 0,
                "offset_rate": 0,
                "offset_id": 0,
                "offset_peer_id": None,
                "offset_peer_access_hash": None,
            }

        if kind == "posts" and int(active.get("pages_done") or 0) == 0:
            try:
                quota = await _posts_quota(client, query=q)
                log.info(
                    "  search: searchPosts quota remains=%s free=%s total=%s query=%r",
                    quota["remains"],
                    quota["query_is_free"],
                    quota["total_daily"],
                    q,
                )
                if not quota["query_is_free"] and quota["remains"] <= 0:
                    log.warning("  search: no free searchPosts slots left; skipping paid queries")
                    skip_paid_posts = True
                    continue
            except errors.PremiumAccountRequiredError:
                log.warning("  search: searchPosts requires Premium; skipping remaining post queries")
                skip_paid_posts = True
                continue
            except Exception:
                log.exception("  search: failed to read searchPosts quota")

        log.info("  search: %s %r (resume page %d/%d)", kind, q, int(active.get("pages_done") or 0) + 1, pages_total)

        while int(active.get("pages_done") or 0) < pages_total:
            page_n = int(active["pages_done"]) + 1
            try:
                result = await _search_posts_page(
                    client,
                    query=q if kind == "posts" else None,
                    hashtag=q if kind == "hashtag" else None,
                    offset_rate=int(active.get("offset_rate") or 0),
                    offset_peer=_offset_peer_from_partial(active),
                    offset_id=int(active.get("offset_id") or 0),
                )
            except errors.PremiumAccountRequiredError:
                log.warning("  search: searchPosts requires Premium; stopping post/hashtag pages")
                skip_paid_posts = kind == "posts"
                break
            except errors.FloodWaitError as e:
                if not await _sleep_flood_or_skip(e, f"{kind} {q!r}"):
                    log.warning("  search: giving up on %s %r", kind, q)
                    break
                continue
            except Exception:
                log.exception("  error during %s search %r page %d", kind, q, page_n)
                break

            msgs = getattr(result, "messages", None) or []
            n_msgs, n_hit = _ingest_search_result(
                result,
                label=label,
                seeds=seeds,
                hit_counts=hit_counts,
                hit_details=hit_details,
                keyword_breakdown=keyword_breakdown,
            )
            log.info(
                "  search: %s %r page %d/%d msgs=%d chat_hits=%d unique=%d",
                kind, q, page_n, pages_total, n_msgs, n_hit, len(hit_counts),
            )
            if not msgs:
                break
            nxt = _next_offset(result, msgs[-1])
            active["pages_done"] = page_n
            if nxt is None:
                save()
                break
            active.update(nxt)
            save()
            await asyncio.sleep(SCORE_PAUSE_S)

        done.add(key)
        active = None
        save()
        await asyncio.sleep(SCORE_PAUSE_S)

    ranked = [
        {
            **hit_details[k],
            "keyword_hits": c,
            "keywords_matched": sorted(set(keyword_breakdown[k])),
            "channel_key": k,
        }
        for k, c in hit_counts.most_common()
    ]
    all_job_keys = {_job_key(j) for j in jobs}
    completed = all_job_keys <= done
    log.info(
        "  search: found %d unique channels (complete=%s, jobs_done=%d/%d)",
        len(ranked), completed, len(done), len(all_job_keys),
    )
    final = snapshot()
    final["completed"] = completed
    final["active_job"] = None if completed else active
    if on_progress:
        on_progress(final)
    return {
        "approach": "keyword_search",
        "search_version": SEARCH_VERSION,
        "search_complete": completed,
        "channels": ranked,
    }


# ------------------------------------------------------------------
# Relevance scoring
# ------------------------------------------------------------------

async def _entity_for_score(client: TelegramClient, key: str, info: dict):
    """Prefer channel id (cached peer) over username resolve, which is heavily rate-limited."""
    chan_id = info.get("id")
    username = info.get("username") or (
        key if key and not str(key).isdigit() else None
    )
    if chan_id is not None:
        try:
            return await client.get_entity(PeerChannel(int(chan_id)))
        except errors.FloodWaitError:
            raise
        except Exception as exc:
            log.info("  scoring: id lookup failed for %s (%s); trying username", key, exc)
    if not username:
        return None
    return await probe_channel(client, username, reraise_flood=True)


async def _score_one_channel(
    client: TelegramClient,
    key: str,
    info: dict,
    pattern: re.Pattern[str] | None,
    sample_n: int,
) -> dict[str, Any]:
    try:
        entity = await _entity_for_score(client, key, info)
        if not entity:
            log.warning("  scoring: can't resolve %s", key)
            return {**info, "score_error": "unresolvable"}
    except errors.FloodWaitError:
        raise
    except Exception:
        log.warning("  scoring: error resolving %s", key)
        return {**info, "score_error": "resolve_error"}

    participants = getattr(entity, "participants_count", None)

    texts: list[str] = []
    oldest_dt = None
    newest_dt = None
    try:
        async for msg in client.iter_messages(entity, limit=sample_n):
            t = getattr(msg, "message", None)
            if t and t.strip():
                texts.append(t)
            dt = getattr(msg, "date", None)
            if dt:
                if oldest_dt is None or dt < oldest_dt:
                    oldest_dt = dt
                if newest_dt is None or dt > newest_dt:
                    newest_dt = dt
    except (errors.ChannelPrivateError, errors.ChatAdminRequiredError):
        return {**info, "score_error": "private", "subscribers": participants}
    except errors.FloodWaitError:
        raise
    except Exception:
        log.exception("  error scoring %s", key)
        return {**info, "score_error": "other"}

    span_days = 0.0
    if oldest_dt and newest_dt and oldest_dt != newest_dt:
        span_days = (newest_dt - oldest_dt).total_seconds() / 86400

    msgs_per_day = len(texts) / span_days if span_days > 0.1 else 0.0

    return {
        **info,
        "subscribers": participants,
        "sampled_texts": len(texts),
        "keyword_hit_rate": round(_keyword_hit_rate(texts, pattern), 3),
        "english_rate": round(_english_rate(texts), 3),
        "msgs_per_day": round(msgs_per_day, 2),
        "sample_span_days": round(span_days, 1),
    }


def _pop_retryable_scores(scored: dict[str, dict]) -> int:
    """Drop checkpoint rows we should try again (timeouts). Returns how many were queued."""
    retry_keys = [
        k for k, v in scored.items()
        if isinstance(v, dict) and v.get("score_error") in RETRY_SCORE_ERRORS
    ]
    for k in retry_keys:
        del scored[k]
    return len(retry_keys)


async def _score_one_channel_with_retry(
    client: TelegramClient,
    key: str,
    info: dict,
    pattern: re.Pattern[str] | None,
    sample_n: int,
) -> dict[str, Any]:
    last: dict[str, Any] | None = None
    for attempt in range(1, SCORE_ATTEMPTS + 1):
        try:
            return await asyncio.wait_for(
                _score_one_channel(client, key, info, pattern, sample_n),
                timeout=SCORE_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            log.warning(
                "  scoring: timeout after %ds on %s (attempt %d/%d)",
                SCORE_TIMEOUT_S, key, attempt, SCORE_ATTEMPTS,
            )
            last = {**info, "score_error": "timeout"}
        except errors.FloodWaitError as e:
            last = {**info, "score_error": "flood"}
            if not await _sleep_flood_or_skip(e, f"scoring {key}"):
                return last
        if attempt < SCORE_ATTEMPTS:
            log.info("  scoring: retrying %s", key)
    assert last is not None
    return last


async def score_channels(
    client: TelegramClient,
    all_channels: dict[str, dict],
    terms: list[str],
    scored: dict[str, dict],
    sample_n: int = 50,
    on_progress: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, dict]:
    """Sample messages from each discovered channel and compute relevance metrics."""
    n_retry = _pop_retryable_scores(scored)
    pending = [k for k in all_channels if k not in scored]
    total = len(all_channels)
    done = total - len(pending)
    log.info(
        "=== Scoring %d discovered channels (%d already scored, %d remaining, %d timeout retries, sample_n=%d) ===",
        total,
        done,
        len(pending),
        n_retry,
        sample_n,
    )
    pattern = _term_pattern(terms)

    for i, key in enumerate(pending, 1):
        info = all_channels[key]
        log.info("  scoring: [%d/%d remaining | %d/%d total] %s", i, len(pending), done + i, total, key)
        result = await _score_one_channel_with_retry(
            client, key, info, pattern, sample_n,
        )
        scored[key] = result
        if on_progress:
            on_progress(key, result)
        await asyncio.sleep(SCORE_PAUSE_S)

    return scored


# ------------------------------------------------------------------
# Report generation
# ------------------------------------------------------------------

def _write_reports(
    approach_results: list[dict],
    scored: dict[str, dict],
    out_dir: Path,
    terms: list[str],
    partial: bool = False,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    report = {
        "generated_at": ts,
        "partial": partial,
        "seed_channels": CHANNEL_LIST,
        "vaccine_terms": terms,
        "approaches": approach_results,
        "scored_channels": scored,
    }
    json_path = out_dir / "probe_report.json"
    json_path.write_text(json.dumps(report, indent=2, default=str))
    log.info("wrote %s%s", json_path, " (partial)" if partial else "")

    md_lines = [
        "# Telegram Channel Discovery Probe",
        "",
        f"Generated: {ts}",
        "",
        f"{'**PARTIAL** — scoring still in progress.' if partial else 'Complete.'}",
        "",
        f"Seed channels: {len(CHANNEL_LIST)}",
        "",
        f"Vaccine terms (search + relevance): {len(terms)}",
        "",
        ", ".join(terms),
        "",
    ]

    for ar in approach_results:
        name = ar["approach"]
        channels = ar["channels"]
        md_lines.append(f"## Approach: {name}")
        md_lines.append("")
        md_lines.append(f"Discovered {len(channels)} unique channels.")
        md_lines.append("")
        if channels:
            md_lines.append("| # | Channel | Key metric |")
            md_lines.append("|---|---------|------------|")
            for i, ch in enumerate(channels[:30], 1):
                label = ch.get("title") or ch.get("username") or ch.get("channel_key", "?")
                if "forward_count" in ch:
                    metric = f"forwards: {ch['forward_count']}"
                elif "seed_overlap" in ch:
                    metric = f"seed overlap: {ch['seed_overlap']}"
                elif "keyword_hits" in ch:
                    metric = f"keyword hits: {ch['keyword_hits']}"
                else:
                    metric = ""
                md_lines.append(f"| {i} | {label} | {metric} |")
            md_lines.append("")

    md_lines.append("## Scored channels (top 50 by keyword hit rate)")
    md_lines.append("")
    md_lines.append(f"Scored so far: {len(scored)}")
    md_lines.append("")

    top = sorted(
        scored.values(),
        key=lambda x: x.get("keyword_hit_rate") or 0,
        reverse=True,
    )[:50]

    if top:
        md_lines.append("| Channel | Subscribers | KW hit rate | English % | msgs/day | Discovery |")
        md_lines.append("|---------|------------|-------------|-----------|----------|-----------|")
        for ch in top:
            label = ch.get("title") or ch.get("username") or ch.get("channel_key", "?")
            subs = ch.get("subscribers", "?")
            kw = ch.get("keyword_hit_rate", "?")
            en = ch.get("english_rate", "?")
            mpd = ch.get("msgs_per_day", "?")
            methods = ", ".join(ch.get("discovery_methods", []))
            md_lines.append(f"| {label} | {subs} | {kw} | {en} | {mpd} | {methods} |")
        md_lines.append("")

    md_path = out_dir / "probe_report.md"
    md_path.write_text("\n".join(md_lines))
    log.info("wrote %s%s", md_path, " (partial)" if partial else "")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def run_probe(prod: bool = False, sample_n: int = 50, fresh: bool = False) -> None:
    _setup_logging()
    require_session_file(SESSION)

    from db.db import init_pool, close_pool
    init_pool(prefix="prod" if prod else "dev")

    client = TelegramClient(SESSION, int(API_ID), API_HASH)
    state: dict[str, Any] | None = None

    try:
        db_terms = fetch_vaccine_terms()
        if not db_terms:
            raise RuntimeError("taxonomy.vaccine_term returned 0 names")
        terms = select_probe_terms(db_terms)
        if not terms:
            raise RuntimeError("no curated probe terms found in taxonomy.vaccine_term")
        log.info("using %d curated vaccine terms (of %d in DB)", len(terms), len(db_terms))

        if fresh and CHECKPOINT_PATH.exists():
            CHECKPOINT_PATH.unlink()
            log.info("removed existing checkpoint (--fresh)")

        state = None if fresh else load_checkpoint()
        if state and state.get("terms") != terms:
            log.info("term list changed vs checkpoint; keeping forwards/similar, resetting search+scores")
            state["terms"] = terms
            state["approaches"].pop("keyword_search", None)
            state["keyword_search_partial"] = _empty_keyword_partial()
            state["scored"] = {}
            state["all_channels"] = {}
        if state is None:
            state = _empty_state(terms)
        else:
            state["terms"] = terms

        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telegram client not authorized. Run:\n"
                "  python -m services.telegram_monitor --login"
            )

        if "forwards" not in state["approaches"]:
            state["approaches"]["forwards"] = await approach_forwards(client)
            persist(state)
        else:
            log.info("resume: skipping forwards (%d channels)",
                     len(state["approaches"]["forwards"].get("channels") or []))

        if "similar" not in state["approaches"]:
            state["approaches"]["similar"] = await approach_similar(client)
            persist(state)
        else:
            log.info("resume: skipping similar (%d channels)",
                     len(state["approaches"]["similar"].get("channels") or []))

        ks = state["approaches"].get("keyword_search") or {}
        search_done = (
            ks.get("search_version") == SEARCH_VERSION
            and bool((state.get("keyword_search_partial") or {}).get("completed"))
        )
        if not search_done:
            if ks and ks.get("search_version") != SEARCH_VERSION:
                log.info("resume: replacing old keyword_search (searchGlobal) with searchPosts v%d", SEARCH_VERSION)
                state["approaches"].pop("keyword_search", None)
                state["keyword_search_partial"] = _empty_keyword_partial()

            def _save_kw_partial(partial: dict[str, Any]) -> None:
                assert state is not None
                state["keyword_search_partial"] = partial
                persist(state, write_report=False)

            state["approaches"]["keyword_search"] = await approach_keyword_search(
                client,
                partial=state.get("keyword_search_partial"),
                on_progress=_save_kw_partial,
            )
            persist(state)
        else:
            log.info("resume: skipping keyword_search (%d channels)",
                     len(ks.get("channels") or []))

        approach_results = [
            state["approaches"][name]
            for name in ("forwards", "similar", "keyword_search")
            if name in state["approaches"]
        ]
        state["all_channels"] = merge_all_channels(approach_results)
        log.info("Total unique channels across all approaches: %d", len(state["all_channels"]))
        persist(state)

        def _save_score(_key: str, _result: dict[str, Any]) -> None:
            assert state is not None
            n = len(state["scored"])
            persist(state, write_report=(n % 10 == 0 or n == len(state["all_channels"])))

        await score_channels(
            client,
            state["all_channels"],
            terms,
            state["scored"],
            sample_n=sample_n,
            on_progress=_save_score,
        )

        persist(state)
        log.info("probe complete: scored %d / %d",
                 len(state["scored"]), len(state["all_channels"]))

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.warning("interrupted; checkpoint saved at %s", CHECKPOINT_PATH)
        if state is not None:
            persist(state)
        raise
    finally:
        close_pool()
        if client.is_connected():
            await client.disconnect()


def _parse_args():
    ap = argparse.ArgumentParser(
        prog="python -m services.telegram_monitor.probe_channels",
        description="Probe for new Telegram channels to scrape.",
    )
    ap.add_argument("--prod", action="store_true", help="Load terms from prod DB")
    ap.add_argument("--sample-n", type=int, default=50,
                    help="Messages to sample per discovered channel for scoring")
    ap.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore existing checkpoint and start over",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(run_probe(prod=args.prod, sample_n=args.sample_n, fresh=args.fresh))
