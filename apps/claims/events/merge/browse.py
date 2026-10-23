"""Denormalized merge browse: events with year/place children, plus include-candidate buckets.

Writes ``browse.json`` (source of truth) and ``browse.html`` (open in a browser).

Usage (repo root)::

    python -m apps.claims.events.merge.browse \\
        --merge-dir apps/claims/events/merges/merge_3
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.events.extract import log_stderr
from apps.claims.events.merge.group import stated_year
from apps.claims.events.merge.paths import (
    BROWSE_FILE,
    BROWSE_HTML,
    CLUSTERS_FILE,
    CONFIG_FILE,
    DROPPED_FILE,
    MEMBERS_FILE,
)
from events.schema import slugify

SCHEMA = "merge_browse_v1"

COUNTRY_WAVE_INCLUDE = frozenset(
    {"New Zealand", "Romania", "Ireland", "Mongolia", "Mexico", "Philippines"}
)

SURVEILLANCE_RE = re.compile(
    r"\b(confirms?|case counts?|health alert|issues? (a )?measles (warning|alert)"
    r"|reported global measles|measles warning)\b",
    re.I,
)
MEASLES_FREE_RE = re.compile(r"measles[-\s]?free|\beliminat", re.I)
SPLIT_MMR_RE = re.compile(
    r"executive order|split (the )?mmr|separat\w+ .{0,20}(measles|mumps|rubella|mmr)",
    re.I,
)
ACIP_TITLE_RE = re.compile(r"\bacip\b", re.I)
PERSON_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b")
PERSON_STOP_FIRST = frozenset(
    {
        "Child",
        "Baby",
        "Boy",
        "Girl",
        "Little",
        "Three",
        "Two",
        "First",
        "Measles",
        "Possible",
        "Confirmed",
        "Recent",
        "Current",
        "United",
        "South",
        "North",
        "The",
        "This",
        "That",
        "Some",
        "Many",
        "Several",
        "Another",
        "Unvaccinated",
        "Ongoing",
        "Major",
        "Reported",
    }
)
OTHER_KEEP_RE = re.compile(
    r"clinic|shortage|exposure|campaign|measles part(?:y|ies)|vitamin a tox|"
    r"tainted batch|vaccination drive|walk-in",
    re.I,
)
MEASLESISH_RE = re.compile(r"\b(measles|mmr|vaccin)", re.I)


def event_label(cluster: dict[str, Any]) -> str:
    etype = str(cluster.get("event_type") or "event")
    country = str(cluster.get("country") or "").strip()
    admin1 = str(cluster.get("admin1") or "").strip()
    slug = str(cluster.get("title_slug") or "").strip()
    if admin1 and country:
        place = f"{admin1}, {country}"
    else:
        place = admin1 or country or "unplaced"
    year = str(cluster.get("join_year") or "").strip()
    disease = str(cluster.get("disease") or "").strip()
    bits = [etype, place]
    if year:
        bits.append(year)
    if disease:
        bits.append(disease)
    if slug:
        bits.append(slug)
    return " · ".join(bits)


def _loc(mention: dict[str, Any]) -> dict[str, str]:
    loc = mention.get("location") or {}
    if not isinstance(loc, dict):
        loc = {}
    return {
        "country": str(mention.get("clean_country") or loc.get("country") or "").strip(),
        "admin1": str(mention.get("clean_admin1") or loc.get("admin1") or "").strip(),
        "admin2": str(mention.get("clean_admin2") or loc.get("admin2") or "").strip(),
        "locality": str(mention.get("clean_locality") or loc.get("locality") or "").strip(),
        "site": str(mention.get("clean_site") or loc.get("site") or "").strip(),
        "label": str(loc.get("label") or "").strip(),
    }


def finer_place(mention: dict[str, Any]) -> tuple[str, str]:
    loc = _loc(mention)
    if loc["locality"]:
        return f"locality:{loc['locality']}", loc["locality"]
    if loc["admin2"]:
        return f"admin2:{loc['admin2']}", loc["admin2"]
    site = loc["site"]
    if site:
        return f"site:{site}", site
    return "", "place not specified"


def member_card(src: dict[str, Any], *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    extra = extra or {}
    loc = _loc(src)
    quotes = [str(q).strip() for q in (src.get("quotes") or []) if str(q).strip()][:2]
    year = extra.get("stated_year") or stated_year(src)
    return {
        "mention_id": str(src.get("mention_id") or extra.get("mention_id") or ""),
        "post_id": src.get("post_id") if src.get("post_id") is not None else extra.get("post_id"),
        "chunk_index": int(src.get("chunk_index") or 0),
        "event_type": str(src.get("event_type") or extra.get("event_type") or ""),
        "title": str(src.get("title") or extra.get("title") or ""),
        "summary": str(src.get("summary") or "").strip() or None,
        "actor": str(src.get("actor") or "").strip() or None,
        "quotes": quotes,
        "stated_start": src.get("stated_start") or extra.get("stated_start"),
        "stated_end": src.get("stated_end"),
        "date_precision": str(src.get("date_precision") or extra.get("date_precision") or "unknown"),
        "stated_year": year,
        "stated_date_text": str(src.get("stated_date_text") or "") or None,
        "date_source": extra.get("date_source") or src.get("date_source"),
        "proxy_year": extra.get("proxy_year") or src.get("proxy_year"),
        "location": loc,
        "case_count": str(src.get("case_count") or "") or None,
        "death_count": str(src.get("death_count") or "") or None,
        "role_in_post": str(src.get("role_in_post") or "") or None,
        "platform": str(src.get("platform") or extra.get("platform") or "") or None,
        "url": src.get("url") or None,
        "post_created_at": src.get("post_created_at"),
        "drop_reason": src.get("drop_reason") or extra.get("drop_reason"),
        "include_rule": extra.get("include_rule"),
    }


def _sorted_groups(groups: dict[str, list], *, unspecified: str) -> list[tuple[str, list]]:
    specified = [(k, rows) for k, rows in groups.items() if k != unspecified]
    specified.sort(key=lambda item: (-len(item[1]), str(item[0])))
    if unspecified in groups:
        specified.append((unspecified, groups[unspecified]))
    return specified


def _place_node(node_id: str, key: str, label: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": node_id,
        "kind": "place",
        "key": key,
        "label": label,
        "n_mentions": len(rows),
        "children": [],
        "members": rows,
    }


def _split_places(parent_id: str, cards: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    labels: dict[str, str] = {}
    for card in cards:
        key, label = finer_place(card)
        grouped[key].append(card)
        labels[key] = label
    specified = [k for k in grouped if k]
    if not specified or len(grouped) < 2:
        return [], cards
    children = []
    for key, rows in _sorted_groups(grouped, unspecified=""):
        slug = slugify(key or "unspecified", max_len=40) or "unspecified"
        children.append(_place_node(f"{parent_id}::place::{slug}", key, labels[key], rows))
    return children, []


def attach_children(cluster: dict[str, Any], cards: list[dict[str, Any]]) -> dict[str, Any]:
    node = {
        "id": cluster["cluster_id"],
        "kind": "event",
        "event_type": cluster.get("event_type"),
        "label": event_label(cluster),
        "country": cluster.get("country") or "",
        "admin1": cluster.get("admin1") or "",
        "title_slug": cluster.get("title_slug") or "",
        "unplaced": bool(cluster.get("unplaced")),
        "n_mentions": int(cluster.get("n_mentions") or len(cards)),
        "n_posts": cluster.get("n_posts"),
        "year_histogram": cluster.get("year_histogram") or {},
        "example_titles": cluster.get("example_titles") or [],
        "children": [],
        "members": [],
    }
    year_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for card in cards:
        year_groups[str(card.get("stated_year") or "unknown")].append(card)
    specified_years = [y for y in year_groups if y != "unknown"]
    split_years = bool(specified_years) and len(year_groups) >= 2

    if split_years:
        year_items = sorted(specified_years, reverse=True)
        if "unknown" in year_groups:
            year_items.append("unknown")
        children = []
        for year in year_items:
            rows = year_groups[year]
            year_id = f"{node['id']}::year::{year}"
            label = "year not stated" if year == "unknown" else year
            place_children, rest = _split_places(year_id, rows)
            children.append(
                {
                    "id": year_id,
                    "kind": "year",
                    "key": year,
                    "label": label,
                    "n_mentions": len(rows),
                    "children": place_children,
                    "members": rest,
                }
            )
        node["children"] = children
        return node

    place_children, rest = _split_places(str(node["id"]), cards)
    node["children"] = place_children
    node["members"] = rest
    return node


def named_person(title: str) -> str | None:
    if not MEASLESISH_RE.search(title or ""):
        return None
    for match in PERSON_RE.finditer(title or ""):
        name = match.group(1)
        first = name.split()[0]
        if first not in PERSON_STOP_FIRST:
            return name
    return None


def has_place_hook_raw(mention: dict[str, Any]) -> bool:
    loc = _loc(mention)
    return any(loc[k] for k in ("country", "admin1", "admin2", "locality", "site"))


def include_national_status(mention: dict[str, Any]) -> str | None:
    loc = _loc(mention)
    country = loc["country"]
    if country in COUNTRY_WAVE_INCLUDE:
        return f"country_wave:{country}"
    return None


def include_national_institution(mention: dict[str, Any]) -> str | None:
    title = str(mention.get("title") or "")
    blob = f"{title} {mention.get('summary') or ''}"
    if SURVEILLANCE_RE.search(blob):
        return None
    if MEASLES_FREE_RE.search(blob):
        return "measles_free_or_elimination"
    if SPLIT_MMR_RE.search(blob):
        return "split_mmr_or_executive_order"
    if ACIP_TITLE_RE.search(blob):
        return "acip_action"
    return None


def include_unanchored_case(mention: dict[str, Any]) -> str | None:
    name = named_person(str(mention.get("title") or ""))
    if name:
        return f"named_person:{name}"
    return None


def include_leftover_other(mention: dict[str, Any]) -> str | None:
    blob = f"{mention.get('title') or ''} {mention.get('summary') or ''}"
    match = OTHER_KEEP_RE.search(blob)
    if not match:
        return None
    if not has_place_hook_raw(mention):
        return None
    return f"ops:{match.group(0).lower()}"


INCLUDE_MATCHERS = {
    "national_status": include_national_status,
    "national_institution": include_national_institution,
    "unanchored_case": include_unanchored_case,
    "leftover_other": include_leftover_other,
}

BUCKET_META = (
    (
        "national_status",
        "Country-as-wave candidates",
        "Country-only outbreaks we would allowlist (NZ, Romania, Ireland, Mongolia, Mexico, Philippines). "
        "US/UK/Canada tallies stay out.",
    ),
    (
        "national_institution",
        "Named national policy acts",
        "Dropped CDC/WHO/Trump/ACIP rows that look like measles-free loss, an MMR split order, or an ACIP vote. "
        "Surveillance alerts stay out.",
    ),
    (
        "unanchored_case",
        "Named deaths / cases without actor",
        "notable_case with a person name in the title (Daisy Hildebrand, Kayley Fehr). Anonymous 'child dies' stays out.",
    ),
    (
        "leftover_other",
        "Ops / consequence leftovers",
        "extract type=other with a place and clinic/shortage/exposure/campaign/party/vitamin-A language.",
    ),
)


def _group_include(members: list[dict[str, Any]], bucket_id: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for card in members:
        grouped[str(card.get("include_rule") or "other")].append(card)
    out = []
    for rule, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        label = rule.split(":", 1)[-1].replace("_", " ")
        out.append(
            {
                "id": f"include::{bucket_id}::{slugify(rule, max_len=50) or 'group'}",
                "kind": "include_group",
                "key": rule,
                "label": label,
                "n_mentions": len(rows),
                "children": [],
                "members": rows,
            }
        )
    return out


def build_include_buckets(
    dropped: list[dict[str, Any]],
    mentions_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in dropped:
        reason = str(row.get("drop_reason") or "")
        mid = str(row.get("mention_id") or "")
        src = dict(mentions_by_id.get(mid) or row)
        src["drop_reason"] = reason
        by_reason[reason].append(src)

    buckets = []
    for reason, label, why in BUCKET_META:
        matcher = INCLUDE_MATCHERS[reason]
        kept_cards = []
        for src in by_reason.get(reason, []):
            rule = matcher(src)
            if not rule:
                continue
            kept_cards.append(member_card(src, extra={"drop_reason": reason, "include_rule": rule}))
        buckets.append(
            {
                "id": f"include::{reason}",
                "kind": "include_bucket",
                "drop_reason": reason,
                "label": label,
                "include_why": why,
                "n_dropped": len(by_reason.get(reason, [])),
                "n_suggested": len(kept_cards),
                "groups": _group_include(kept_cards, reason),
            }
        )
    return buckets


def build_browse(
    *,
    clusters: list[dict[str, Any]],
    members: list[dict[str, Any]],
    mentions_by_id: dict[str, dict[str, Any]],
    dropped: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    extra_by_id = {str(m.get("mention_id") or ""): m for m in members}
    by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in members:
        mid = str(row.get("mention_id") or "")
        src = mentions_by_id.get(mid) or row
        cid = str(row.get("cluster_id") or "")
        by_cluster[cid].append(member_card(src, extra=row))

    events = []
    n_children = 0
    n_event_members = 0
    for cluster in clusters:
        cid = str(cluster.get("cluster_id") or "")
        node = attach_children(cluster, by_cluster.get(cid) or [])
        events.append(node)
        n_children += _count_nodes(node) - 1
        n_event_members += int(node["n_mentions"])

    include_buckets = build_include_buckets(dropped, mentions_by_id)
    type_hist = dict(Counter(str(e.get("event_type") or "") for e in events))
    return {
        "schema": SCHEMA,
        "strategy": config.get("strategy"),
        "input": config.get("input"),
        "n_events": len(events),
        "n_event_members": n_event_members,
        "n_child_nodes": n_children,
        "type_histogram": type_hist,
        "include_summary": [
            {
                "drop_reason": b["drop_reason"],
                "label": b["label"],
                "n_dropped": b["n_dropped"],
                "n_suggested": b["n_suggested"],
            }
            for b in include_buckets
        ],
        "events": events,
        "include_buckets": include_buckets,
    }


def _count_nodes(node: dict[str, Any]) -> int:
    return 1 + sum(_count_nodes(child) for child in node.get("children") or [])


def render_html(browse: dict[str, Any]) -> str:
    payload = json.dumps(browse, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    return _HTML.replace("__BROWSE_JSON__", payload)


def write_merge_browse(merge_dir: Path, browse: dict[str, Any]) -> tuple[Path, Path]:
    json_path = merge_dir / BROWSE_FILE
    html_path = merge_dir / BROWSE_HTML
    claims_io.write_json(json_path, browse, indent=None)
    html_path.write_text(render_html(browse), encoding="utf-8")
    return json_path, html_path


def load_mentions_by_id(mentions_path: Path) -> dict[str, dict[str, Any]]:
    rows = claims_io.read_jsonl(mentions_path)
    return {str(m.get("mention_id") or ""): m for m in rows if m.get("mention_id")}


def build_from_merge_dir(merge_dir: Path, *, mentions_path: Path) -> dict[str, Any]:
    config = claims_io.read_json(merge_dir / CONFIG_FILE) if (merge_dir / CONFIG_FILE).is_file() else {}
    clusters = claims_io.read_jsonl(merge_dir / CLUSTERS_FILE)
    members = claims_io.read_jsonl(merge_dir / MEMBERS_FILE)
    dropped = claims_io.read_jsonl(merge_dir / DROPPED_FILE)
    mentions_by_id = load_mentions_by_id(mentions_path)
    return build_browse(
        clusters=clusters,
        members=members,
        mentions_by_id=mentions_by_id,
        dropped=dropped,
        config=config,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m apps.claims.events.merge.browse",
        description="Write browse.json + browse.html for a merge attempt.",
    )
    p.add_argument("--merge-dir", required=True, help="apps/claims/events/merges/merge_N")
    p.add_argument(
        "--mentions",
        default=None,
        help="mentions.jsonl (default: config.json input)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    merge_dir = Path(args.merge_dir)
    if not (merge_dir / CLUSTERS_FILE).is_file():
        raise SystemExit(f"Missing clusters: {merge_dir / CLUSTERS_FILE}")
    config = {}
    if (merge_dir / CONFIG_FILE).is_file():
        config = claims_io.read_json(merge_dir / CONFIG_FILE)
    mentions_path = Path(args.mentions) if args.mentions else Path(str(config.get("input") or ""))
    if not mentions_path.is_file():
        raise SystemExit(f"Missing mentions file: {mentions_path}")
    log_stderr(f"browse: merge {merge_dir}")
    log_stderr(f"browse: mentions {mentions_path}")
    browse = build_from_merge_dir(merge_dir, mentions_path=mentions_path)
    json_path, html_path = write_merge_browse(merge_dir, browse)
    log_stderr(
        f"browse: {browse['n_events']} events  "
        f"members={browse['n_event_members']}  "
        f"child_nodes={browse['n_child_nodes']}"
    )
    for row in browse["include_summary"]:
        log_stderr(
            f"browse: include {row['drop_reason']} "
            f"suggested={row['n_suggested']} / dropped={row['n_dropped']}"
        )
    claims_io.emit_json(
        {
            "browse_json": str(json_path),
            "browse_html": str(html_path),
            "n_events": browse["n_events"],
            "n_event_members": browse["n_event_members"],
            "include_summary": browse["include_summary"],
        }
    )
    return 0


_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Merge browse</title>
<style>
:root {
  --bg: #f6f5f1;
  --fg: #1c1c1a;
  --muted: #6b6a64;
  --line: #d9d6cc;
  --card: #fff;
  --accent: #2155c8;
  --warn: #8a4b12;
  --pad: 12px;
  --sidebar: 340px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161615;
    --fg: #eceae4;
    --muted: #9b9a93;
    --line: #2e2d2a;
    --card: #1e1e1c;
    --accent: #7ea0ff;
    --warn: #e0a066;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; }
body {
  font: 13px/1.45 ui-sans-serif, system-ui, sans-serif;
  color: var(--fg);
  background: var(--bg);
}
header {
  display: flex;
  gap: 10px;
  align-items: center;
  flex-wrap: wrap;
  padding: 10px 14px;
  border-bottom: 1px solid var(--line);
  background: var(--card);
}
header h1 { font-size: 15px; font-weight: 600; margin: 0; }
.tabs { display: flex; gap: 0; border: 1px solid var(--line); }
.tabs button, select, input[type=search] {
  font: inherit;
  color: var(--fg);
  background: var(--bg);
  border: 1px solid var(--line);
}
.tabs button {
  border: 0;
  padding: 6px 12px;
  cursor: pointer;
  background: transparent;
}
.tabs button[aria-selected="true"] { background: var(--bg); font-weight: 600; }
input[type=search] { min-width: 220px; padding: 6px 8px; flex: 1; }
select { padding: 6px 8px; }
.meta { color: var(--muted); font-size: 12px; }
.layout { display: flex; height: calc(100% - 54px); }
nav {
  width: var(--sidebar);
  border-right: 1px solid var(--line);
  overflow: auto;
  background: var(--card);
}
nav .item {
  display: block;
  width: 100%;
  text-align: left;
  border: 0;
  border-bottom: 1px solid var(--line);
  background: transparent;
  color: inherit;
  padding: 8px 12px;
  cursor: pointer;
  font: inherit;
}
nav .item:hover, nav .item.active { background: var(--bg); }
nav .item .k { color: var(--muted); font-size: 11px; }
nav .count { float: right; color: var(--muted); font-variant-numeric: tabular-nums; }
main { flex: 1; overflow: auto; padding: 16px 20px 48px; }
.node {
  border: 1px solid var(--line);
  background: var(--card);
  margin: 0 0 10px;
  padding: 10px 12px;
}
.node h2, .node h3 { margin: 0 0 6px; font-size: 16px; }
.node h3 { font-size: 14px; }
.kids { margin: 8px 0 0 14px; border-left: 2px solid var(--line); padding-left: 10px; }
.member {
  border-top: 1px solid var(--line);
  padding: 8px 0;
}
.member .title { font-weight: 600; }
.member .sum { margin: 4px 0; }
.member .q { margin: 4px 0 0; padding: 4px 8px; border-left: 2px solid var(--accent); color: var(--muted); }
.pills { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0; }
.pill {
  border: 1px solid var(--line);
  padding: 1px 7px;
  color: var(--muted);
  font-size: 11px;
}
a { color: var(--accent); }
.why { color: var(--warn); margin-bottom: 10px; }
#empty { color: var(--muted); padding: 24px; }
</style>
</head>
<body>
<header>
  <h1>Merge browse</h1>
  <div class="tabs" role="tablist">
    <button type="button" id="tab-events" aria-selected="true">Events</button>
    <button type="button" id="tab-include" aria-selected="false">Include candidates</button>
  </div>
  <select id="type-filter"></select>
  <input id="search" type="search" placeholder="Filter titles, places, actors…" />
  <span class="meta" id="header-meta"></span>
</header>
<div class="layout">
  <nav id="list"></nav>
  <main id="detail"><p id="empty">Select an event or include group.</p></main>
</div>
<script type="application/json" id="data">__BROWSE_JSON__</script>
<script>
const data = JSON.parse(document.getElementById("data").textContent);
let tab = "events";
let selected = null;

function textBlob(node) {
  const parts = [node.label, node.event_type, node.country, node.admin1, node.title_slug, node.include_why, node.key];
  (node.example_titles || []).forEach((t) => parts.push(t));
  (node.members || []).forEach((m) => {
    parts.push(m.title, m.summary, m.actor);
    (m.quotes || []).forEach((q) => parts.push(q));
  });
  (node.children || []).forEach((c) => parts.push(textBlob(c)));
  (node.groups || []).forEach((g) => parts.push(textBlob(g)));
  return parts.filter(Boolean).join(" ").toLowerCase();
}

function matches(node, q, type) {
  if (type && type !== "all" && node.event_type && node.event_type !== type) return false;
  if (!q) return true;
  return textBlob(node).includes(q);
}

function pills(card) {
  const loc = card.location || {};
  const place = [loc.site, loc.locality, loc.admin2, loc.admin1, loc.country].filter(Boolean).join(" · ");
  const bits = [];
  if (card.event_type) bits.push(card.event_type);
  if (place) bits.push(place);
  if (card.stated_year && card.stated_year !== "unknown") bits.push(card.stated_year);
  else if (card.proxy_year) bits.push("proxy " + card.proxy_year);
  if (card.actor) bits.push(card.actor);
  if (card.case_count) bits.push(card.case_count + " cases");
  if (card.death_count) bits.push(card.death_count + " deaths");
  if (card.drop_reason) bits.push(card.drop_reason);
  if (card.include_rule) bits.push(card.include_rule);
  return bits;
}

function memberEl(card) {
  const el = document.createElement("article");
  el.className = "member";
  const t = document.createElement("div");
  t.className = "title";
  if (card.url) {
    const a = document.createElement("a");
    a.href = card.url;
    a.target = "_blank";
    a.rel = "noreferrer";
    a.textContent = card.title || card.mention_id;
    t.appendChild(a);
  } else {
    t.textContent = card.title || card.mention_id;
  }
  el.appendChild(t);
  const pwrap = document.createElement("div");
  pwrap.className = "pills";
  pills(card).forEach((bit) => {
    const s = document.createElement("span");
    s.className = "pill";
    s.textContent = bit;
    pwrap.appendChild(s);
  });
  el.appendChild(pwrap);
  if (card.summary) {
    const s = document.createElement("div");
    s.className = "sum";
    s.textContent = card.summary;
    el.appendChild(s);
  }
  (card.quotes || []).forEach((q) => {
    const b = document.createElement("div");
    b.className = "q";
    b.textContent = q;
    el.appendChild(b);
  });
  return el;
}

function nodeEl(node, heading) {
  const wrap = document.createElement("section");
  wrap.className = "node";
  const h = document.createElement(heading);
  h.textContent = (node.label || node.id) + "  (" + (node.n_mentions || 0) + ")";
  wrap.appendChild(h);
  if (node.include_why) {
    const w = document.createElement("p");
    w.className = "why";
    w.textContent = node.include_why;
    wrap.appendChild(w);
  }
  if (node.example_titles && node.example_titles.length) {
    const ex = document.createElement("div");
    ex.className = "meta";
    ex.textContent = node.example_titles.slice(0, 6).join(" · ");
    wrap.appendChild(ex);
  }
  (node.members || []).forEach((m) => wrap.appendChild(memberEl(m)));
  if (node.children && node.children.length) {
    const kids = document.createElement("div");
    kids.className = "kids";
    node.children.forEach((c) => kids.appendChild(nodeEl(c, "h3")));
    wrap.appendChild(kids);
  }
  (node.groups || []).forEach((g) => wrap.appendChild(nodeEl(g, "h3")));
  return wrap;
}

function currentItems() {
  const q = document.getElementById("search").value.trim().toLowerCase();
  const type = document.getElementById("type-filter").value;
  if (tab === "events") {
    return data.events.filter((e) => matches(e, q, type));
  }
  const out = [];
  (data.include_buckets || []).forEach((b) => {
    const groups = (b.groups || []).filter((g) => matches(g, q, "all") || matches(b, q, "all"));
    if (!groups.length && !matches(b, q, "all")) return;
    out.push({ ...b, groups: groups.length ? groups : b.groups });
  });
  return out;
}

function renderList() {
  const nav = document.getElementById("list");
  nav.innerHTML = "";
  const items = currentItems();
  items.forEach((item) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "item" + (selected && selected.id === item.id ? " active" : "");
    const count = document.createElement("span");
    count.className = "count";
    count.textContent = item.n_mentions || item.n_suggested || 0;
    btn.appendChild(count);
    const k = document.createElement("div");
    k.className = "k";
    k.textContent = item.event_type || item.drop_reason || item.kind || "";
    btn.appendChild(k);
    btn.appendChild(document.createTextNode(item.label || item.id));
    btn.addEventListener("click", () => {
      selected = item;
      renderList();
      renderDetail();
    });
    nav.appendChild(btn);
  });
  document.getElementById("header-meta").textContent =
    (tab === "events" ? data.n_events + " events" : "include candidates") +
    " · showing " + items.length;
}

function renderDetail() {
  const main = document.getElementById("detail");
  main.innerHTML = "";
  if (!selected) {
    const p = document.createElement("p");
    p.id = "empty";
    p.textContent = "Select an event or include group.";
    main.appendChild(p);
    return;
  }
  main.appendChild(nodeEl(selected, "h2"));
}

function fillTypes() {
  const sel = document.getElementById("type-filter");
  const types = Object.keys(data.type_histogram || {}).sort();
  sel.innerHTML = "";
  const all = document.createElement("option");
  all.value = "all";
  all.textContent = "All types";
  sel.appendChild(all);
  types.forEach((t) => {
    const o = document.createElement("option");
    o.value = t;
    o.textContent = t + " (" + data.type_histogram[t] + ")";
    sel.appendChild(o);
  });
}

document.getElementById("tab-events").addEventListener("click", () => {
  tab = "events";
  selected = null;
  document.getElementById("tab-events").setAttribute("aria-selected", "true");
  document.getElementById("tab-include").setAttribute("aria-selected", "false");
  document.getElementById("type-filter").disabled = false;
  renderList();
  renderDetail();
});
document.getElementById("tab-include").addEventListener("click", () => {
  tab = "include";
  selected = null;
  document.getElementById("tab-events").setAttribute("aria-selected", "false");
  document.getElementById("tab-include").setAttribute("aria-selected", "true");
  document.getElementById("type-filter").disabled = true;
  renderList();
  renderDetail();
});
document.getElementById("search").addEventListener("input", () => {
  selected = null;
  renderList();
  renderDetail();
});
document.getElementById("type-filter").addEventListener("change", () => {
  selected = null;
  renderList();
  renderDetail();
});
fillTypes();
renderList();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
