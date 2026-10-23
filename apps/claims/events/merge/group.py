"""Mention clustering. exact_key_v1 keeps titles; filter_place_key_v2 drops vague waves."""

from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from apps.claims.events.merge.clean import (
    clean_place,
    generic_national_actors,
    iso_date_only,
    small_wave_countries,
)
from events.schema import slugify

STRATEGY_V1 = "exact_key_v1"
STRATEGY_V2 = "filter_place_key_v2"
STRATEGY_V3 = "high_precision_v3"
STRATEGY_V4 = "place_year_v4"
STRATEGIES = (STRATEGY_V1, STRATEGY_V2, STRATEGY_V3, STRATEGY_V4)

WAVE_TYPES = frozenset({"outbreak", "cases"})
NEWSISH_PLATFORMS = frozenset(
    {"reddit_submission", "youtube_video", "telegram_post", "podcast_episode"}
)
VAGUE_PLACE_RE = re.compile(
    r"\b(speaker'?s|poster'?s)\s+(area|state|county|city)\b"
    r"|\b(unspecified|unnamed)\s+(area|place|location)\b"
    r"|\bnationwide\b"
    r"|\bmultiple (places|locations|countries|states)\b"
    r"|\ball over (the )?(country|place|world)\b",
    re.I,
)
YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")
INSTITUTIONAL_KEEP_RE = re.compile(
    r"eliminat|vicp|compensat|vaccine court|mandate|recall|approv|licen|"
    r"appoint|fir(ed|ing)|ruling|exemption|schedule",
    re.I,
)

SIZE_BUCKETS = (
    ("1", lambda n: n == 1),
    ("2-5", lambda n: 2 <= n <= 5),
    ("6-20", lambda n: 6 <= n <= 20),
    ("21-100", lambda n: 21 <= n <= 100),
    ("100+", lambda n: n > 100),
)
OVERMERGE_MIN_SIZE = 20
NEAR_DUP_JACCARD = 0.5
QA_SLICE = 15

# Back-compat names used by merge_1 tests / old run.py
STRATEGY = STRATEGY_V1
KEY_FIELDS = ["event_type", "country", "admin1", "title_slug"]
UNPLACED_RULE = "type + title_slug (empty country and admin1)"
DATE_RULE = "not a key; attach as cluster attribute"


def title_slug(title: str) -> str:
    return slugify(title) or "untitled"


def actor_or_title_slug(mention: dict[str, Any]) -> str:
    actor = slugify(str(mention.get("actor") or ""))
    if actor:
        return actor
    return title_slug(str(mention.get("title") or ""))


def place_parts(mention: dict[str, Any]) -> tuple[str, str]:
    loc = mention.get("location") or {}
    if not isinstance(loc, dict):
        loc = {}
    country = str(loc.get("country") or "").strip()
    admin1 = str(loc.get("admin1") or "").strip()
    return country, admin1


def is_unplaced(country: str, admin1: str) -> bool:
    return not country and not admin1


def grouping_key(mention: dict[str, Any]) -> tuple[str, str, str, str]:
    country, admin1 = place_parts(mention)
    return (
        str(mention.get("event_type") or "").strip() or "other",
        country,
        admin1,
        title_slug(str(mention.get("title") or "")),
    )


def grouping_key_v2(mention: dict[str, Any]) -> tuple[str, str, str, str]:
    etype = str(mention.get("event_type") or "").strip() or "other"
    country = str(mention.get("clean_country") or "")
    admin1 = str(mention.get("clean_admin1") or "")
    if not country and not admin1:
        fallback = (
            str(mention.get("clean_admin2") or "")
            or str(mention.get("clean_locality") or "")
            or str(mention.get("clean_site") or "")
        )
        admin1 = fallback
    disc = "" if etype in WAVE_TYPES else actor_or_title_slug(mention)
    return (etype, country, admin1, disc)


def place_key(mention: dict[str, Any]) -> str:
    """State when the text has one. Otherwise the finest named place."""
    admin1 = str(mention.get("clean_admin1") or "")
    if admin1:
        return admin1
    return (
        str(mention.get("clean_admin2") or "")
        or str(mention.get("clean_locality") or "")
        or str(mention.get("clean_site") or "")
    )


def disease_slug(mention: dict[str, Any]) -> str:
    raw = mention.get("diseases") or []
    if not isinstance(raw, list):
        return ""
    parts = sorted({slugify(str(item)) for item in raw if str(item).strip()})
    return "+".join(parts)


def named_disc_v4(mention: dict[str, Any]) -> str:
    """Title, not actor, when the actor is a generic national name like CDC."""
    actor = slugify(str(mention.get("actor") or ""))
    if actor and actor not in generic_national_actors():
        return actor
    return title_slug(str(mention.get("title") or ""))


def grouping_key_v4(mention: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    etype = str(mention.get("event_type") or "").strip() or "other"
    country = str(mention.get("clean_country") or "")
    place = place_key(mention)
    if etype in WAVE_TYPES:
        return (etype, country, place, stated_year(mention), disease_slug(mention), "")
    return (etype, country, place, "", "", named_disc_v4(mention))


def cluster_id_for(key: tuple[str, ...]) -> str:
    raw = "|".join(key)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    label = slugify("-".join(p for p in key if p), max_len=60) or "cluster"
    return f"{label}-{digest}"


def stated_year(mention: dict[str, Any]) -> str:
    if str(mention.get("date_precision") or "unknown") == "unknown":
        return "unknown"
    start = iso_date_only(mention.get("stated_start")) or str(mention.get("stated_start") or "")
    return start[:4] if len(start) >= 4 else "unknown"


def has_place_hook(place: dict[str, str]) -> bool:
    return bool(
        place["country"]
        or place["admin1"]
        or place["admin2"]
        or place["locality"]
        or place["site"]
    )


def vague_place_title(title: str) -> bool:
    return bool(VAGUE_PLACE_RE.search(title or ""))


def has_subnational(place: dict[str, str]) -> bool:
    return bool(place["admin1"] or place["admin2"] or place["locality"] or place["site"])


def drop_reason(
    mention: dict[str, Any],
    place: dict[str, str],
    *,
    mode: str = STRATEGY_V2,
) -> str | None:
    etype = str(mention.get("event_type") or "")
    title = str(mention.get("title") or "")
    if etype in WAVE_TYPES:
        if not has_place_hook(place):
            return "vague_unplaced_wave"
        if vague_place_title(title):
            return "vague_place_title"
        if mode == STRATEGY_V3 and not has_subnational(place):
            if place["country"] not in small_wave_countries():
                return "national_status"
        return None
    if mode != STRATEGY_V3:
        return None
    if etype == "other":
        return "leftover_other"
    if etype == "notable_case":
        if not has_place_hook(place) and not str(mention.get("actor") or "").strip():
            return "unanchored_case"
        return None
    if etype == "institutional_action":
        if has_subnational(place):
            return None
        blob = f"{title} {mention.get('summary') or ''}"
        if INSTITUTIONAL_KEEP_RE.search(blob):
            return None
        disc = actor_or_title_slug(mention)
        if disc in generic_national_actors():
            return "national_institution"
        return None
    return None


def _posted_year(mention: dict[str, Any]) -> str | None:
    raw = mention.get("post_created_at") or ""
    text = str(raw).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return text[:4]
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y")
    except (TypeError, ValueError):
        return None


def should_impute_proxy(mention: dict[str, Any], place: dict[str, str]) -> bool:
    if str(mention.get("date_precision") or "unknown") != "unknown":
        return False
    if not (place["country"] or place["admin1"]):
        return False
    if str(mention.get("platform") or "") not in NEWSISH_PLATFORMS:
        return False
    if YEAR_RE.search(str(mention.get("title") or "")):
        return False
    return _posted_year(mention) is not None


def prepare_mentions(
    mentions: list[dict[str, Any]],
    *,
    mode: str = STRATEGY_V2,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    for raw in mentions:
        row = dict(raw)
        place = clean_place(row)
        start = iso_date_only(row.get("stated_start"))
        end = iso_date_only(row.get("stated_end"))
        if start:
            row["stated_start"] = start
        if end:
            row["stated_end"] = end
        row["clean_country"] = place["country"]
        row["clean_admin1"] = place["admin1"]
        row["clean_admin2"] = place["admin2"]
        row["clean_locality"] = place["locality"]
        row["clean_site"] = place["site"]
        reason = drop_reason(row, place, mode=mode)
        if reason:
            row["drop_reason"] = reason
            dropped.append(row)
            continue
        if should_impute_proxy(row, place):
            row["date_source"] = "posted_at_proxy"
            row["proxy_year"] = _posted_year(row)
        else:
            row["date_source"] = "stated" if stated_year(row) != "unknown" else "unknown"
            row["proxy_year"] = None
        kept.append(row)
    return kept, dropped


def cluster_mentions(
    mentions: list[dict[str, Any]],
    *,
    strategy: str = STRATEGY_V1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown merge strategy {strategy!r}")
    if strategy == STRATEGY_V4:
        key_fn = grouping_key_v4
    elif strategy == STRATEGY_V1:
        key_fn = grouping_key
    else:
        key_fn = grouping_key_v2
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in mentions:
        grouped[key_fn(row)].append(row)

    clusters: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    for key in sorted(grouped):
        rows = grouped[key]
        event_type, country, admin1, slug = key[0], key[1], key[2], key[-1]
        year_key = key[3] if len(key) > 4 else ""
        disease_key = key[4] if len(key) > 4 else ""
        cid = cluster_id_for(key)
        years = Counter(stated_year(m) for m in rows)
        sources = Counter(str(m.get("date_source") or "unknown") for m in rows)
        titles = []
        seen_titles: set[str] = set()
        for m in rows:
            t = str(m.get("title") or "").strip()
            if t and t not in seen_titles:
                seen_titles.add(t)
                titles.append(t)
            if len(titles) >= 8:
                break
        posts = {str(m.get("post_id")) for m in rows if m.get("post_id") is not None}
        if strategy in (STRATEGY_V2, STRATEGY_V3, STRATEGY_V4):
            unplaced = not any(
                str(m.get("clean_country") or "") or str(m.get("clean_admin1") or "")
                for m in rows
            )
        else:
            unplaced = is_unplaced(country, admin1)
        cluster = {
            "cluster_id": cid,
            "event_type": event_type,
            "country": country,
            "admin1": admin1,
            "title_slug": slug,
            "join_year": year_key if year_key and year_key != "unknown" else "",
            "disease": disease_key,
            "unplaced": unplaced,
            "n_mentions": len(rows),
            "n_posts": len(posts),
            "year_histogram": dict(years),
            "n_unknown_date": int(years.get("unknown", 0)),
            "date_source_histogram": dict(sources),
            "example_titles": titles,
            "example_mention_ids": [str(m.get("mention_id") or "") for m in rows[:3]],
        }
        clusters.append(cluster)
        for m in rows:
            members.append(
                {
                    "mention_id": str(m.get("mention_id") or ""),
                    "cluster_id": cid,
                    "post_id": m.get("post_id"),
                    "event_type": event_type,
                    "title": m.get("title") or "",
                    "date_precision": m.get("date_precision") or "unknown",
                    "stated_start": m.get("stated_start"),
                    "stated_year": stated_year(m),
                    "date_source": m.get("date_source"),
                    "proxy_year": m.get("proxy_year"),
                }
            )
    clusters.sort(key=lambda c: (-int(c["n_mentions"]), str(c["cluster_id"])))
    return clusters, members


def _dated_years(hist: dict[str, int]) -> list[str]:
    return sorted(y for y, n in hist.items() if y != "unknown" and n)


def is_mixed_year(cluster: dict[str, Any]) -> bool:
    return len(_dated_years(cluster.get("year_histogram") or {})) >= 2


def is_overmerge_suspect(cluster: dict[str, Any]) -> bool:
    if int(cluster["n_mentions"]) >= OVERMERGE_MIN_SIZE:
        return True
    years = _dated_years(cluster.get("year_histogram") or {})
    if len(years) < 2:
        return False
    return int(years[-1]) - int(years[0]) >= 2


def _token_set(slug: str) -> set[str]:
    return {p for p in slug.split("-") if p}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def near_duplicate_groups(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_place: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for c in clusters:
        if not str(c.get("title_slug") or ""):
            continue
        by_place[(c["event_type"], c["country"], c["admin1"])].append(c)
    out: list[dict[str, Any]] = []
    for place, rows in by_place.items():
        if len(rows) < 2:
            continue
        used: set[str] = set()
        for i, a in enumerate(rows):
            if a["cluster_id"] in used:
                continue
            tokens_a = _token_set(str(a["title_slug"]))
            group = [a]
            for b in rows[i + 1 :]:
                if b["cluster_id"] in used:
                    continue
                if _jaccard(tokens_a, _token_set(str(b["title_slug"]))) >= NEAR_DUP_JACCARD:
                    group.append(b)
            if len(group) < 2:
                continue
            for c in group:
                used.add(c["cluster_id"])
            out.append(
                {
                    "event_type": place[0],
                    "country": place[1],
                    "admin1": place[2],
                    "n_clusters": len(group),
                    "n_mentions": sum(int(c["n_mentions"]) for c in group),
                    "clusters": [
                        {
                            "cluster_id": c["cluster_id"],
                            "title_slug": c["title_slug"],
                            "n_mentions": c["n_mentions"],
                            "example_titles": c["example_titles"][:3],
                        }
                        for c in sorted(group, key=lambda x: -int(x["n_mentions"]))
                    ],
                }
            )
    out.sort(key=lambda g: (-int(g["n_mentions"]), -int(g["n_clusters"])))
    return out


def size_histogram(clusters: list[dict[str, Any]]) -> dict[str, int]:
    counts = {label: 0 for label, _ in SIZE_BUCKETS}
    for c in clusters:
        n = int(c["n_mentions"])
        for label, pred in SIZE_BUCKETS:
            if pred(n):
                counts[label] += 1
                break
    return counts


def _cluster_preview(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        "cluster_id": cluster["cluster_id"],
        "event_type": cluster["event_type"],
        "country": cluster["country"],
        "admin1": cluster["admin1"],
        "unplaced": cluster["unplaced"],
        "n_mentions": cluster["n_mentions"],
        "n_posts": cluster["n_posts"],
        "title_slug": cluster["title_slug"],
        "year_histogram": cluster["year_histogram"],
        "date_source_histogram": cluster.get("date_source_histogram") or {},
        "example_titles": cluster["example_titles"],
    }


def build_qa(
    clusters: list[dict[str, Any]],
    *,
    dropped: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    mixed = [c for c in clusters if is_mixed_year(c)]
    mixed.sort(key=lambda c: (-int(c["n_mentions"]), str(c["cluster_id"])))
    unplaced = [c for c in clusters if c["unplaced"]]
    unplaced.sort(key=lambda c: (-int(c["n_mentions"]), str(c["cluster_id"])))
    dups = near_duplicate_groups(clusters)
    drop_titles = Counter(
        (str(m.get("drop_reason") or ""), str(m.get("title") or ""))
        for m in (dropped or [])
    )
    dropped_preview = [
        {"reason": reason, "title": title, "n": n}
        for (reason, title), n in drop_titles.most_common(QA_SLICE)
    ]
    return {
        "biggest": [_cluster_preview(c) for c in clusters[:QA_SLICE]],
        "mixed_year": [_cluster_preview(c) for c in mixed[:QA_SLICE]],
        "near_duplicate_titles": dups[:QA_SLICE],
        "unplaced": [_cluster_preview(c) for c in unplaced[:QA_SLICE]],
        "dropped_titles": dropped_preview,
    }


def build_metrics(
    *,
    mentions: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
    config: dict[str, Any],
    dropped: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dups = near_duplicate_groups(clusters)
    n_over = sum(1 for c in clusters if is_overmerge_suspect(c))
    n_mixed = sum(1 for c in clusters if is_mixed_year(c))
    n_unplaced = sum(1 for c in clusters if c["unplaced"])
    n_singleton = sum(1 for c in clusters if int(c["n_mentions"]) == 1)
    n_imputed = sum(1 for m in mentions if m.get("date_source") == "posted_at_proxy")
    dropped = dropped or []
    return {
        "strategy": config.get("strategy") or STRATEGY,
        "n_mentions_in": int(config.get("n_mentions_in") or (len(mentions) + len(dropped))),
        "n_dropped": len(dropped),
        "drop_reason_histogram": dict(Counter(str(m.get("drop_reason") or "") for m in dropped)),
        "n_grouped": len(mentions),
        "n_imputed_proxy": n_imputed,
        "n_clusters": len(clusters),
        "size_histogram": size_histogram(clusters),
        "type_histogram": dict(Counter(c["event_type"] for c in clusters)),
        "n_unplaced_clusters": n_unplaced,
        "n_unplaced_mentions": sum(int(c["n_mentions"]) for c in clusters if c["unplaced"]),
        "n_mixed_year": n_mixed,
        "n_singleton": n_singleton,
        "n_overmerge_suspects": n_over,
        "n_undermerge_suspect_groups": len(dups),
        "top_clusters": [_cluster_preview(c) for c in clusters[:15]],
        "config": config,
    }


def attempt_config(*, mentions_path: str, strategy: str = STRATEGY_V1) -> dict[str, Any]:
    if strategy == STRATEGY_V4:
        return {
            "input": mentions_path,
            "strategy": STRATEGY_V4,
            "key": {
                "outbreak_cases": [
                    "event_type",
                    "country",
                    "admin1_or_finest_place",
                    "stated_year",
                    "disease",
                ],
                "named_types": [
                    "event_type",
                    "country",
                    "admin1_or_finest_place",
                    "title_slug_if_generic_actor_else_actor",
                ],
            },
            "date": "stated year splits outbreak and cases; unknown year stays in its own cluster",
            "notes": (
                "no new drop list. City or site is the place key when admin1 is empty. "
                "Generic national actors (CDC, Trump, …) do not join institutional acts. "
                "Disease tag splits waves."
            ),
        }
    if strategy == STRATEGY_V3:
        return {
            "input": mentions_path,
            "strategy": STRATEGY_V3,
            "key": {
                "outbreak_cases": ["event_type", "country", "admin1_or_finest_place"],
                "named_types": ["event_type", "country", "admin1", "actor_or_title_slug"],
            },
            "keep": (
                "subnational waves (or small-country waves); notable_case with place or actor; "
                "vaccine_injury and study_publication; institutional with admin1 or "
                "mandate/elimination/court/VICP/etc. in the title"
            ),
            "date": "not a key; posted_at_proxy year on news-like placed unknown-time mentions",
            "notes": (
                "high-precision timeline candidates for MMR-related events. "
                "Excluded buckets: vague_unplaced_wave, vague_place_title, national_status, "
                "national_institution, leftover_other, unanchored_case"
            ),
        }
    if strategy == STRATEGY_V2:
        return {
            "input": mentions_path,
            "strategy": STRATEGY_V2,
            "key": {
                "outbreak_cases": ["event_type", "country", "admin1_or_finest_place"],
                "named_types": ["event_type", "country", "admin1", "actor_or_title_slug"],
            },
            "unplaced": "dropped if outbreak/cases with no place hook or vague-place title",
            "date": "not a key; posted_at_proxy year on news-like placed unknown-time mentions",
            "notes": "filter + aliases + titles off for waves; do not split mixed stated years",
        }
    return {
        "input": mentions_path,
        "strategy": STRATEGY_V1,
        "key": KEY_FIELDS,
        "unplaced": UNPLACED_RULE,
        "date": DATE_RULE,
        "notes": "first pass; expect Texas 2025+2026 over-merge and named-injury under-merge",
    }
