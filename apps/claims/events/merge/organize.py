"""Build events from mentions. Updates attach. Finer events and inner events belong.

Usage (repo root)::

    python -m apps.claims.events.merge.organize \\
        --mentions apps/claims/events/runs/run_N/mentions.jsonl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.events.extract import log_stderr
from apps.claims.events.merge.clean import iso_date_only
from apps.claims.events.merge.group import disease_slug
from apps.claims.events.merge.paths import MERGES_DIR
from apps.claims.events.merge.strict import _prepare, classify
from apps.claims.events.mention import person_actor_rejected, place_slots_from

WAVE_TYPES = frozenset({"outbreak", "cases"})
PERSON_TYPES = frozenset({"notable_case", "vaccine_injury"})
IDENTITY_TYPES = frozenset({"institutional_action", "study_publication", "other"})
_SERIES_LEVELS = frozenset({"country", "global", "region"})
_CENSUS_RE = re.compile(
    r"\b(?:\d+|several|multiple|many|numerous)\b.{0,40}\boutbreaks\b"
    r"|\boutbreaks\b.{0,40}\b(?:across|several|multiple|jurisdictions)\b",
    re.I,
)


def _eid(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _year(row: dict[str, Any]) -> str | None:
    """The stated year. A multi-year window returns '' so the mention is held."""
    grain = str(row.get("date_grain") or "")
    precision = str(row.get("date_precision") or "")
    if grain == "unknown" and precision in ("day", "month", "year"):
        grain = precision
    start = iso_date_only(row.get("stated_start")) or ""
    end = iso_date_only(row.get("stated_end")) or ""
    if grain in ("day", "month", "year") and len(start) >= 4 and start[:4].isdigit():
        return start[:4]
    if precision == "range" and len(start) >= 4 and start[:4].isdigit():
        if len(end) >= 4 and end[:4].isdigit() and end[:4] != start[:4]:
            return ""
        return start[:4]
    return None


def _point_date(row: dict[str, Any]) -> str | None:
    """A day or a month can open the span. A year does not become January 1."""
    grain = str(row.get("date_grain") or "")
    precision = str(row.get("date_precision") or "")
    if grain == "unknown" and precision in ("day", "month"):
        grain = precision
    if grain not in ("day", "month"):
        return None
    return iso_date_only(row.get("stated_start"))


def _is_census(row: dict[str, Any]) -> bool:
    blob = f"{row.get('title') or ''} {row.get('summary') or ''}"
    return bool(_CENSUS_RE.search(blob))


def _slots(row: dict[str, Any]) -> list[str]:
    return place_slots_from(row.get("location") or {}, str(row.get("region") or ""))


def _scrub_person(row: dict[str, Any]) -> dict[str, Any]:
    slots = _slots(row)
    copied = dict(row)
    if person_actor_rejected(str(copied.get("actor") or ""), slots=slots):
        copied["actor"] = ""
    people = copied.get("people") or []
    if isinstance(people, list):
        copied["people"] = [
            person for person in people if not person_actor_rejected(str(person), slots=slots)
        ]
    return copied


def _has_real_name(row: dict[str, Any]) -> bool:
    scrubbed = _scrub_person(row)
    if str(scrubbed.get("actor") or "").strip():
        return True
    people = scrubbed.get("people") or []
    return isinstance(people, list) and any(str(person).strip() for person in people)


def _prepare_row(raw: dict[str, Any]) -> dict[str, Any]:
    row = _prepare(raw)
    scope = str(row.get("place_scope") or "").strip()
    if scope == "global":
        row["grain_level"] = "global"
        row["grain_path"] = "global"
    return row


def _bucket_kind(row: dict[str, Any]) -> str:
    level = str(row.get("grain_level") or "")
    etype = str(row.get("event_type") or "")
    mention_kind = str(row.get("mention_kind") or "event")
    if _is_census(row) and level in _SERIES_LEVELS:
        return "series"
    if level in _SERIES_LEVELS and (etype == "cases" or mention_kind == "update" or _is_census(row)):
        return "series"
    return "wave"


def _member_role(row: dict[str, Any], *, bucket_kind: str) -> str:
    mention_kind = str(row.get("mention_kind") or "event")
    if _is_census(row) or mention_kind == "update" or bucket_kind == "series" and str(row.get("event_type") or "") == "cases":
        return "update"
    if mention_kind == "event":
        return "defines"
    return "update"


def _place_label(path: str) -> str:
    if path == "global":
        return "Worldwide"
    parts = [part for part in str(path or "").split(" / ") if part]
    return parts[-1] if parts else ""


def _event_title(*, kind: str, disease: str, path: str, year: str) -> str:
    place = _place_label(path)
    disease_label = disease.replace("+", " ").replace("-", " ")
    if kind == "series":
        base = f"{place} {disease_label} cases".strip()
    else:
        base = f"{place} {disease_label}".strip()
    return f"{base} {year}".strip()


def _prefixes(path: str) -> list[str]:
    parts = [part for part in str(path or "").split(" / ") if part]
    return [" / ".join(parts[:i]) for i in range(len(parts) - 1, 0, -1)]


def _path_and_prefixes(path: str) -> list[str]:
    parts = [part for part in str(path or "").split(" / ") if part]
    if not parts:
        return []
    return [" / ".join(parts[:i]) for i in range(len(parts), 0, -1)]


def _hold(row: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "mention_id": str(row.get("mention_id") or ""),
        "reason": reason,
        "event_type": str(row.get("event_type") or ""),
        "title": str(row.get("title") or ""),
        "grain_path": str(row.get("grain_path") or ""),
    }


def _member(event_id: str, row: dict[str, Any], role: str) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "mention_id": str(row.get("mention_id") or ""),
        "role": role,
        "title": str(row.get("title") or ""),
        "event_type": str(row.get("event_type") or ""),
        "mention_kind": str(row.get("mention_kind") or ""),
        "stated_start": iso_date_only(row.get("stated_start")) or "",
        "date_grain": str(row.get("date_grain") or ""),
        "span_status": str(row.get("span_status") or "open"),
        "grain_path": str(row.get("grain_path") or ""),
    }


def _span(rows: list[dict[str, Any]]) -> tuple[str, str, bool]:
    points = [point for row in rows if (point := _point_date(row))]
    start = min(points) if points else ""
    closed_rows = [row for row in rows if str(row.get("span_status") or "") == "closed"]
    if not closed_rows:
        return start, "", True
    closed_points = [point for row in closed_rows if (point := _point_date(row))]
    end = max(closed_points) if closed_points else (max(points) if points else "")
    return start, end, False


def _index_waves(events: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event["kind"] not in ("wave", "series"):
            continue
        indexed[(event["disease"], event["grain_path"], event["year"])].append(event)
    return indexed


def _pick(group: list[dict[str, Any]]) -> str:
    waves = [event for event in group if event["kind"] == "wave"]
    if waves:
        return str(waves[0]["event_id"])
    series = [event for event in group if event["kind"] == "series"]
    if series:
        return str(series[0]["event_id"])
    return ""


def _parent_for_wave(event: dict[str, Any], indexed: dict[tuple[str, str, str], list[dict[str, Any]]]) -> str:
    disease = str(event["disease"])
    year = str(event["year"])
    path = str(event["grain_path"])
    if event["kind"] == "wave":
        same = indexed.get((disease, path, year), [])
        series = [item for item in same if item["kind"] == "series" and item["event_id"] != event["event_id"]]
        if series and event["grain_level"] in _SERIES_LEVELS:
            return str(series[0]["event_id"])
    for prefix in _prefixes(path):
        chosen = _pick(indexed.get((disease, prefix, year), []))
        if chosen and chosen != event["event_id"]:
            return chosen
    return ""


def _parent_for_place(
    *,
    disease: str,
    path: str,
    year: str,
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> str:
    if not disease or not path or not year:
        return ""
    for prefix in _path_and_prefixes(path):
        chosen = _pick(indexed.get((disease, prefix, year), []))
        if chosen:
            return chosen
    return ""


def _ids_at_path(
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]],
    disease: str,
    path: str,
) -> list[str]:
    found: list[str] = []
    for (event_disease, event_path, _year), group in indexed.items():
        if event_disease == disease and event_path == path:
            chosen = _pick(group)
            if chosen:
                found.append(chosen)
    return list(dict.fromkeys(found))


def _single_place_event(
    *,
    disease: str,
    path: str,
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> str:
    if not disease or not path:
        return ""
    found = _ids_at_path(indexed, disease, path)
    if len(found) == 1:
        return found[0]
    return ""


def organize_mentions(mentions: list[dict[str, Any]]) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    held: list[dict[str, Any]] = []

    prepared = [_prepare_row(raw) for raw in mentions]
    buckets: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    undated: list[dict[str, Any]] = []
    inners: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []

    for row in prepared:
        etype = str(row.get("event_type") or "") or "other"
        if etype in WAVE_TYPES:
            if str(row.get("place_scope") or "") == "unplaced" or not str(row.get("grain_path") or ""):
                held.append(_hold(row, "unplaced"))
                continue
            disease = disease_slug(row)
            if not disease:
                held.append(_hold(row, "missing_disease"))
                continue
            year = _year(row)
            if year == "":
                held.append(_hold(row, "year_span"))
                continue
            if year is None:
                undated.append(row)
                continue
            kind = _bucket_kind(row)
            buckets[(disease, str(row.get("grain_path") or ""), year, kind)].append(row)
            continue
        if etype in PERSON_TYPES:
            inners.append(row)
            if _has_real_name(row):
                identity_rows.append(_scrub_person(row))
            continue
        if etype in IDENTITY_TYPES:
            identity_rows.append(row)

    for (disease, path, year, kind), rows in buckets.items():
        event_id = _eid(kind, disease, path, year)
        start, end, span_open = _span(rows)
        level = str(rows[0].get("grain_level") or "")
        events.append(
            {
                "event_id": event_id,
                "kind": kind,
                "event_type": "cases" if kind == "series" else "outbreak",
                "title": _event_title(kind=kind, disease=disease, path=path, year=year),
                "disease": disease,
                "grain_level": level,
                "grain_path": path,
                "year": year,
                "span_start": start,
                "span_end": end,
                "span_open": span_open,
                "parent_event_id": "",
            }
        )
        for row in rows:
            members.append(_member(event_id, row, _member_role(row, bucket_kind=kind)))

    indexed = _index_waves(events)
    for row in undated:
        disease = disease_slug(row)
        path = str(row.get("grain_path") or "")
        parent = _single_place_event(disease=disease, path=path, indexed=indexed)
        if not parent:
            reason = "ambiguous_year" if len(_ids_at_path(indexed, disease, path)) > 1 else "missing_year"
            held.append(_hold(row, reason))
            continue
        members.append(_member(parent, row, "update"))

    for event in events:
        if event["kind"] not in ("wave", "series"):
            continue
        parent = _parent_for_wave(event, indexed)
        if not parent:
            continue
        event["parent_event_id"] = parent
        links.append({"child_event_id": event["event_id"], "parent_event_id": parent, "relation": "belongs_to"})

    named_ids = {str(row.get("mention_id") or "") for row in identity_rows if str(row.get("event_type") or "") in PERSON_TYPES}
    for row in inners:
        if str(row.get("mention_id") or "") in named_ids:
            continue
        event_id = _eid("inner", str(row.get("mention_id") or ""))
        year_value = _year(row)
        if year_value == "":
            held.append(_hold(row, "year_span"))
            continue
        year = year_value or ""
        disease = disease_slug(row)
        path = str(row.get("grain_path") or "")
        parent = ""
        if disease and path and str(row.get("place_scope") or "") != "unplaced":
            if year:
                parent = _parent_for_place(disease=disease, path=path, year=year, indexed=indexed)
            else:
                parent = _single_place_event(disease=disease, path=path, indexed=indexed)
        point = _point_date(row) or ""
        events.append(
            {
                "event_id": event_id,
                "kind": "inner",
                "event_type": str(row.get("event_type") or ""),
                "title": str(row.get("title") or ""),
                "disease": disease,
                "grain_level": str(row.get("grain_level") or ""),
                "grain_path": path,
                "year": year,
                "span_start": point,
                "span_end": point,
                "span_open": False,
                "parent_event_id": parent,
            }
        )
        members.append(_member(event_id, row, "inner"))
        if parent:
            links.append({"child_event_id": event_id, "parent_event_id": parent, "relation": "belongs_to"})

    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in identity_rows:
        kind, value = classify(row)
        if kind != "key":
            if str(row.get("event_type") or "") in PERSON_TYPES:
                continue
            held.append(_hold(row, str(value)))
            continue
        assert isinstance(value, tuple)
        grouped[value].append(row)

    for key, rows in grouped.items():
        event_id = _eid("identity", *key)
        etype = str(rows[0].get("event_type") or "")
        title = str(rows[0].get("title") or "") or " · ".join(key)
        disease = disease_slug(rows[0])
        path = str(rows[0].get("grain_path") or "")
        year = _year(rows[0]) or ""
        parent = ""
        if etype in PERSON_TYPES:
            parent = _identity_parent(rows, indexed)
        point_dates = [point for row in rows if (point := _point_date(row))]
        events.append(
            {
                "event_id": event_id,
                "kind": "identity",
                "event_type": etype,
                "title": title,
                "disease": disease,
                "grain_level": str(rows[0].get("grain_level") or ""),
                "grain_path": path,
                "year": year if year != "" else "",
                "span_start": min(point_dates) if point_dates else "",
                "span_end": max(point_dates) if point_dates else "",
                "span_open": False,
                "parent_event_id": parent,
                "identity_key": list(key),
            }
        )
        for row in rows:
            members.append(_member(event_id, row, "identity"))
        if parent:
            links.append({"child_event_id": event_id, "parent_event_id": parent, "relation": "belongs_to"})

    events.sort(key=lambda event: (str(event.get("grain_path") or ""), str(event.get("year") or ""), str(event.get("title") or "")))
    return {"events": events, "members": members, "links": links, "held": held}


def _identity_parent(
    rows: list[dict[str, Any]],
    indexed: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> str:
    parents: list[str] = []
    for row in rows:
        disease = disease_slug(row)
        path = str(row.get("grain_path") or "")
        if not disease or not path or str(row.get("place_scope") or "") == "unplaced":
            continue
        year_value = _year(row)
        if year_value == "":
            continue
        if year_value:
            parent = _parent_for_place(disease=disease, path=path, year=year_value, indexed=indexed)
        else:
            parent = _single_place_event(disease=disease, path=path, indexed=indexed)
        if parent:
            parents.append(parent)
    unique = list(dict.fromkeys(parents))
    if len(unique) == 1:
        return unique[0]
    return ""


def _browse_html(result: dict[str, Any]) -> str:
    payload = json.dumps(result, ensure_ascii=False)
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Organized events</title>
<style>
  :root { --bg:#f6f5f2; --card:#fff; --ink:#1c1b19; --muted:#5e5a54; --line:#ddd9d2; --accent:#1f4e79; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--ink); background: var(--bg); }
  header { padding: 16px 20px; border-bottom: 1px solid var(--line); background: var(--card); }
  h1 { font-size: 18px; margin: 0 0 6px; }
  main { padding: 16px 20px 48px; }
  .node { background: var(--card); border: 1px solid var(--line); padding: 10px 12px; margin: 0 0 10px; }
  .child { margin: 8px 0 0 16px; }
  h2 { font-size: 15px; margin: 0 0 6px; }
  .pill { display: inline-block; border: 1px solid var(--line); color: var(--muted); font-size: 11px; padding: 1px 6px; margin-right: 4px; }
  .member { border-top: 1px solid var(--line); padding: 4px 0; }
  .meta { color: var(--muted); }
</style>
</head>
<body>
<header>
  <h1>Organized events</h1>
  <p class="meta" id="meta"></p>
</header>
<main id="main"></main>
<script>
const DATA = """ + payload + """;
const byId = Object.fromEntries(DATA.events.map((e) => [e.event_id, e]));
const members = {};
DATA.members.forEach((m) => { (members[m.event_id] ||= []).push(m); });
const children = {};
DATA.links.forEach((link) => { (children[link.parent_event_id] ||= []).push(link.child_event_id); });
function esc(s) { return String(s || "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])); }
function pills(event) {
  const span = event.span_start
    ? (event.span_open ? event.span_start + " – open" : event.span_start + " – " + (event.span_end || "closed"))
    : (event.year || "undated");
  return ["<span class='pill'>" + esc(event.kind) + "</span>",
    event.grain_path && "<span class='pill'>" + esc(event.grain_path) + "</span>",
    "<span class='pill'>" + esc(span) + "</span>"].filter(Boolean).join("");
}
function memberHtml(rows) {
  return (rows || []).map((m) => "<div class='member'>" + esc(m.title) +
    " <span class='pill'>" + esc(m.role) + "</span>" +
    (m.stated_start ? "<span class='pill'>" + esc(m.stated_start) + "</span>" : "") +
    "</div>").join("");
}
function render(id, depth) {
  const event = byId[id];
  if (!event) return "";
  const kids = (children[id] || []).map((cid) => render(cid, depth + 1)).join("");
  return "<article class='node" + (depth ? " child" : "") + "'><h2>" + esc(event.title) + "</h2><div>" +
    pills(event) + "</div>" + memberHtml(members[id]) + kids + "</article>";
}
const roots = DATA.events.filter((e) => !e.parent_event_id);
document.getElementById("meta").textContent =
  DATA.events.length + " events · " + DATA.links.length + " belongs-to links · " + DATA.held.length + " held";
const main = document.getElementById("main");
main.innerHTML = roots.map((e) => render(e.event_id, 0)).join("") +
  (DATA.held.length ? "<h2>Held</h2>" + DATA.held.map((h) =>
    "<div class='member'>" + esc(h.title) + " <span class='pill'>" + esc(h.reason) + "</span></div>").join("") : "");
</script>
</body>
</html>
"""


def _metrics(result: dict[str, Any]) -> dict[str, Any]:
    reasons: dict[str, int] = defaultdict(int)
    for row in result["held"]:
        reasons[str(row.get("reason") or "")] += 1
    kinds: dict[str, int] = defaultdict(int)
    for event in result["events"]:
        kinds[str(event.get("kind") or "")] += 1
    return {
        "n_events": len(result["events"]),
        "n_waves": kinds.get("wave", 0),
        "n_series": kinds.get("series", 0),
        "n_inner": kinds.get("inner", 0),
        "n_identity": kinds.get("identity", 0),
        "n_links": len(result["links"]),
        "n_members": len(result["members"]),
        "n_held": len(result["held"]),
        "held_reasons": dict(reasons),
    }


def next_organize_dir(merges_dir: Path | None = None) -> Path:
    root = Path(merges_dir) if merges_dir is not None else MERGES_DIR
    root.mkdir(parents=True, exist_ok=True)
    n = 1
    while True:
        candidate = root / f"organize_{n}"
        if not candidate.exists():
            return candidate
        n += 1


def write_organize(*, mentions: list[dict[str, Any]], out_dir: Path) -> dict[str, Any]:
    result = organize_mentions(mentions)
    out_dir.mkdir(parents=True, exist_ok=True)
    claims_io.write_jsonl(out_dir / "events.jsonl", result["events"])
    claims_io.write_jsonl(out_dir / "members.jsonl", result["members"])
    claims_io.write_jsonl(out_dir / "links.jsonl", result["links"])
    claims_io.write_jsonl(out_dir / "held.jsonl", result["held"])
    metrics = _metrics(result)
    claims_io.write_json(out_dir / "metrics.json", metrics)
    (out_dir / "browse.html").write_text(_browse_html(result), encoding="utf-8")
    log_stderr(
        f"organize: {out_dir}  events={metrics['n_events']} links={metrics['n_links']} held={metrics['n_held']}"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m apps.claims.events.merge.organize")
    parser.add_argument("--mentions", required=True, help="Path to mentions.jsonl")
    parser.add_argument("--out-dir", default=None, help="Write here instead of merges/organize_N")
    parser.add_argument("--merges-dir", default=None, help="Override apps/claims/events/merges")
    args = parser.parse_args(argv)
    mentions_path = Path(args.mentions)
    if not mentions_path.is_file():
        raise SystemExit(f"Missing mentions file: {mentions_path}")
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        root = Path(args.merges_dir) if args.merges_dir else MERGES_DIR
        out_dir = next_organize_dir(root)
    mentions = claims_io.read_jsonl(mentions_path)
    log_stderr(f"organize: loaded {len(mentions)} mentions")
    write_organize(mentions=mentions, out_dir=out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
