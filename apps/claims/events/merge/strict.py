"""Strict, type-specific joins. Incomplete mentions stay ambiguous.

Writes a browse page of the merges that meet the rule, plus the mentions
that were held out and why. This is the surface to iterate the rules on.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.events.extract import log_stderr
from apps.claims.events.merge.clean import clean_place, generic_national_actors, grain, iso_date_only
from apps.claims.events.merge.group import disease_slug, title_slug
from events.schema import slugify

WAVE_TYPES = frozenset({"outbreak", "cases"})
PERSON_TYPES = frozenset({"notable_case", "vaccine_injury"})
ROLE_ACTORS = frozenset(
    {
        "child",
        "baby",
        "person",
        "toddler",
        "infant",
        "boy",
        "girl",
        "man",
        "woman",
        "adult",
        "student",
        "refugee",
        "patient",
        "someone",
        "officials",
        "health-officials",
        "unvaccinated-adult",
        "the-child",
        "the-infant",
        "resident",
    }
)
_PERSON_TITLE_RE = re.compile(
    r"^(?:dr|doctor|prof|professor|mr|mrs|ms|sir)\.?\s+",
    re.I,
)
STUDY_ACTIONS = frozenset({"publication", "retraction"})
LOOKALIKE_RES = (
    ("Wakefield", re.compile(r"wakefield", re.I)),
    ("ACIP", re.compile(r"\bacip\b", re.I)),
    ("Elimination", re.compile(r"eliminat|measles[-\s]?free", re.I)),
    ("Dilley / ICE", re.compile(r"dilley|ice detention|ice facility", re.I)),
    ("MMRV split", re.compile(r"\bmmrv\b|split(?:ting)? the mmr|separate mmr", re.I)),
)

TYPE_ORDER = (
    "outbreak",
    "cases",
    "notable_case",
    "vaccine_injury",
    "institutional_action",
    "study_publication",
    "other",
)

# Current key, then three separate proposals. fields: ask the extractor for
# different data. rule: same data, a different strict key. judge: what the
# model is asked to attach after the strict pass. "None" means no change there.
TYPE_GUIDE = [
    {
        "event_type": "outbreak",
        "label": "Outbreak",
        "current": "Type + disease + country + finest place + one stated year. A county does not join its state.",
        "held_out": "Missing disease, place, or year; a span of more than one year; unplaced; worldwide.",
        "fields": "Add a region field. A named region (Europe, North America) goes there, not in the country slot and not only in the display label. A count of separate outbreaks is still not one outbreak. A worldwide case tally is cases. The place is where that wave is, not a second place the sentence says it spread to.",
        "rule": "A region is its own grain: type + disease + region + one stated year. It does not join a country, state, or city inside it. Missing place is one category. An explicit unplaced scope and empty place slots are the same hold.",
        "judge": "A worldwide mention is compared only with other worldwide events of the same disease whose years do not conflict. Same attaches. A missing year stays unattached. A missing-place outbreak is not compared with a placed or regional one.",
    },
    {
        "event_type": "cases",
        "label": "Cases",
        "current": "Same key as outbreak, and kept as its own type: type + disease + country + finest place + one stated year.",
        "held_out": "Missing disease, place, or year; a span of more than one year; unplaced; worldwide.",
        "fields": "No new field. A local count keeps the finest place the text gives. It does not take the country as its place only because the country is named.",
        "rule": "No change.",
        "judge": "A missing year is compared with same-type, same-place events whose years do not conflict, and stays unattached when the text does not pick one. A worldwide tally is compared only with other worldwide case events of the same disease.",
    },
    {
        "event_type": "notable_case",
        "label": "Notable case",
        "current": "Type + country + a person string from actor or people. Disease, place, and year are not part of the key.",
        "held_out": "No person string, or only a role word or a generic agency.",
        "fields": "No new field. Actor is the patient's name, or empty. A role, age, kinship word, agency, official, or place is not a name.",
        "rule": "A description is not a person key. Different exposure sites that share a visitor do not join. Disease is already collected; a different disease does not join.",
        "judge": "A mention with no real name is compared with events of the same type and country that do not conflict on disease. Different sites are not candidates for each other.",
    },
    {
        "event_type": "vaccine_injury",
        "label": "Vaccine injury",
        "current": "Same key as notable case: type + country + a person string from actor or people.",
        "held_out": "No person string, or only a role word or a generic agency.",
        "fields": "No new field. Same actor request as notable case. \"Nurses\" is not a name.",
        "rule": "A role is not a person key.",
        "judge": "Paraphrases of the same injury, same type and country, with no disease conflict. No real name means the judge decides.",
    },
    {
        "event_type": "institutional_action",
        "label": "Institutional action",
        "current": "Exact title + country + scope + stated year, or a blank year. Different wording does not join.",
        "held_out": "A missing title only. Different wording stays unmerged.",
        "fields": "None.",
        "rule": "No change. The exact title remains the only deterministic join.",
        "judge": "Different wording of the same act, limited to the same actor and country. Years must not conflict.",
    },
    {
        "event_type": "study_publication",
        "label": "Study",
        "current": "study_action (publication or retraction) + an anchor. The anchor is Wakefield if that name appears, otherwise the actor.",
        "held_out": "No study_action, or no named anchor.",
        "fields": "No new field. The actor is the paper's author, not the journal. The title names the paper.",
        "rule": "A journal name is not an anchor, so those mentions do not get a strict key.",
        "judge": "A paper with no author anchor is compared with studies of the same study_action. Publication and retraction stay different.",
    },
    {
        "event_type": "other",
        "label": "Other",
        "current": "Exact title + country + scope + stated year, or a blank year.",
        "held_out": "A missing title only.",
        "fields": "None.",
        "rule": "No change.",
        "judge": "Different wording of the same happening, same country. Years must not conflict.",
    },
]

RULES = [
    {
        "types": "outbreak, cases",
        "joins_on": "type + disease + country + place grain + stated year",
        "place": "finest filled level; a county does not join its state",
        "date": "a single stated year, from the text only",
        "held_out": "missing disease, missing place, no stated year, a year span, unplaced, or global",
    },
    {
        "types": "notable_case, vaccine_injury",
        "joins_on": "type + country + a normalized person name",
        "place": "country only",
        "date": "not required",
        "held_out": "no named person, or only a role word or a generic agency",
    },
    {
        "types": "study_publication",
        "joins_on": "study_action + a named anchor (Wakefield, or a specific actor)",
        "place": "not required",
        "date": "not required",
        "held_out": "no study_action, or no named anchor",
    },
    {
        "types": "institutional_action, other",
        "joins_on": "exact title + country + scope + stated year or blank",
        "place": "country and scope",
        "date": "joins only the same stated year, or other undated copies",
        "held_out": "a different wording of the same act stays unmerged",
    },
]


def _prepare(mention: dict[str, Any]) -> dict[str, Any]:
    row = dict(mention)
    place = clean_place(row)
    level, path = grain(place)
    row["clean_country"] = place["country"]
    row["clean_admin1"] = place["admin1"]
    row["clean_admin2"] = place["admin2"]
    row["clean_locality"] = place["locality"]
    row["clean_site"] = place["site"]
    row["grain_level"] = level
    row["grain_path"] = path
    return row


def _person_slug(name: str) -> str:
    text = _PERSON_TITLE_RE.sub("", str(name or "").strip())
    return slugify(text)


def _named_person(mention: dict[str, Any]) -> tuple[str, str]:
    """Return (slug, '') or ('', reason). A role word or a generic agency is not a person."""
    names: list[str] = []
    actor = str(mention.get("actor") or "").strip()
    if actor:
        names.append(actor)
    people = mention.get("people") or []
    if isinstance(people, list):
        names.extend(str(item).strip() for item in people if str(item).strip())
    saw_rejected = False
    blocked = generic_national_actors() | ROLE_ACTORS
    for name in names:
        slug = _person_slug(name)
        if not slug:
            continue
        if slug in blocked:
            saw_rejected = True
            continue
        return slug, ""
    if saw_rejected:
        return "", "generic_actor"
    return "", "missing_actor"


def _study_anchor(mention: dict[str, Any]) -> str:
    blob = f"{mention.get('title') or ''} {mention.get('actor') or ''}"
    if re.search(r"wakefield", blob, re.I):
        return "wakefield"
    slug, _reason = _named_person(mention)
    return slug


def _stated_year(mention: dict[str, Any]) -> str | None:
    """The one year the text states. None when it states none, or more than one."""
    if str(mention.get("date_precision") or "unknown") == "unknown":
        return None
    years: list[str] = []
    for field in ("stated_start", "stated_end"):
        text = iso_date_only(mention.get(field)) or ""
        if len(text) >= 4 and text[:4].isdigit():
            years.append(text[:4])
    unique = list(dict.fromkeys(years))
    if len(unique) == 1:
        return unique[0]
    if len(unique) > 1:
        return ""
    return None


def _wave_place(mention: dict[str, Any]) -> tuple[str, str]:
    """Return (path, '') or ('', held reason)."""
    scope = str(mention.get("place_scope") or "").strip()
    if scope == "global":
        return "", "global"
    if scope == "unplaced":
        return "", "unplaced"
    if scope == "countrywide":
        country = str(mention.get("clean_country") or "")
        if not country:
            return "", "missing_place"
        return country, ""
    path = str(mention.get("grain_path") or "")
    if not path:
        return "", "missing_place"
    return path, ""


def classify(mention: dict[str, Any]) -> tuple[str, tuple[str, ...] | str]:
    """Return ('key', parts) or ('ambiguous', reason)."""
    etype = str(mention.get("event_type") or "").strip() or "other"
    if etype in WAVE_TYPES:
        disease = disease_slug(mention)
        if not disease:
            return "ambiguous", "missing_disease"
        place, held = _wave_place(mention)
        if held:
            return "ambiguous", held
        year = _stated_year(mention)
        if not year:
            return "ambiguous", "year_span" if year == "" else "missing_year"
        country = str(mention.get("clean_country") or "")
        return "key", (etype, disease, country, place, year)
    if etype in PERSON_TYPES:
        person, reason = _named_person(mention)
        if not person:
            return "ambiguous", reason
        country = str(mention.get("clean_country") or "")
        return "key", (etype, country, person)
    if etype == "study_publication":
        act = str(mention.get("study_action") or "").strip()
        if act not in STUDY_ACTIONS:
            return "ambiguous", "missing_study_action"
        anchor = _study_anchor(mention)
        if not anchor:
            return "ambiguous", "missing_anchor"
        return "key", (etype, act, anchor)
    slug = title_slug(str(mention.get("title") or ""))
    if not slug or slug == "untitled":
        return "ambiguous", "missing_title"
    country = str(mention.get("clean_country") or "")
    scope = str(mention.get("place_scope") or "").strip()
    year = _stated_year(mention) or ""
    return "key", (etype, country, scope, year, slug)


def _card(mention: dict[str, Any]) -> dict[str, Any]:
    quotes = mention.get("quotes") or []
    quote = str(quotes[0])[:240] if quotes else ""
    return {
        "title": str(mention.get("title") or ""),
        "actor": str(mention.get("actor") or ""),
        "place": str(mention.get("grain_path") or ""),
        "country": str(mention.get("clean_country") or ""),
        "year": _stated_year(mention) or "unknown",
        "diseases": mention.get("diseases") or [],
        "quote": quote,
        "post_id": mention.get("post_id"),
    }


def _label(key: tuple[str, ...]) -> str:
    return " · ".join(part for part in key if part)


def build_payload(mentions: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    ambiguous: dict[str, list[dict[str, Any]]] = defaultdict(list)
    type_counts: dict[str, int] = defaultdict(int)
    for raw in mentions:
        row = _prepare(raw)
        kind, value = classify(row)
        card = _card(row)
        card["event_type"] = str(row.get("event_type") or "other")
        type_counts[card["event_type"]] += 1
        if kind == "ambiguous":
            card["reason"] = value
            ambiguous[str(value)].append(card)
            continue
        assert isinstance(value, tuple)
        groups[value].append(card)

    merged = []
    alone = []
    for key, cards in groups.items():
        entry = {
            "label": _label(key),
            "event_type": key[0],
            "n": len(cards),
            "members": cards[:12],
            "n_hidden": max(0, len(cards) - 12),
        }
        if len(cards) >= 2:
            merged.append(entry)
        else:
            alone.append(entry)
    merged.sort(key=lambda g: (-g["n"], g["label"]))
    alone.sort(key=lambda g: g["label"])
    alone_shown: list[dict[str, Any]] = []
    alone_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for entry in alone:
        alone_by_type[str(entry.get("event_type") or "other")].append(entry)
    for etype in list(TYPE_ORDER) + sorted(set(alone_by_type) - set(TYPE_ORDER)):
        alone_shown.extend(alone_by_type.get(etype, [])[:80])

    amb_rows = []
    by_reason_type: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for reason, cards in ambiguous.items():
        for card in cards:
            by_reason_type[(reason, str(card.get("event_type") or "other"))].append(card)
    for (reason, etype), cards in sorted(by_reason_type.items()):
        amb_rows.append(
            {
                "reason": reason,
                "event_type": etype,
                "n": len(cards),
                "members": cards[:40],
                "n_hidden": max(0, len(cards) - 40),
            }
        )

    lookalikes = []
    titled = merged + alone
    for name, pattern in LOOKALIKE_RES:
        hits = []
        for entry in titled:
            blob = " ".join(m["title"] for m in entry["members"])
            if pattern.search(blob) or pattern.search(entry["label"]):
                hits.append({"label": entry["label"], "n": entry["n"], "event_type": entry["event_type"]})
        hits.sort(key=lambda h: (-h["n"], h["label"]))
        if len(hits) >= 2:
            lookalikes.append({"name": name, "n_groups": len(hits), "groups": hits[:30]})

    n_merged_mentions = sum(g["n"] for g in merged)
    return {
        "rules": RULES,
        "type_guide": TYPE_GUIDE,
        "type_order": list(TYPE_ORDER),
        "type_counts": dict(type_counts),
        "n_mentions": len(mentions),
        "n_merged_groups": len(merged),
        "n_merged_mentions": n_merged_mentions,
        "n_alone": len(alone),
        "n_ambiguous": sum(b["n"] for b in amb_rows),
        "merged": merged,
        "alone": alone_shown,
        "n_alone_hidden": max(0, len(alone) - len(alone_shown)),
        "ambiguous": amb_rows,
        "lookalikes": lookalikes,
    }


def _html(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>Strict joins</title>
<style>
  :root { --bg:#f6f5f2; --card:#fff; --ink:#1c1b19; --muted:#5e5a54; --line:#ddd9d2; --accent:#1f4e79; --warn:#8a4b08; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--ink); background: var(--bg); }
  header { padding: 16px 20px 10px; border-bottom: 1px solid var(--line); background: var(--card); position: sticky; top: 0; z-index: 1; }
  h1 { font-size: 18px; margin: 0 0 8px; }
  .types { display: flex; gap: 6px; flex-wrap: wrap; margin: 0 0 10px; }
  .types button { padding: 6px 10px; }
  .types button small { display: block; font-size: 11px; font-weight: 400; opacity: 0.75; }
  .guide { border: 1px solid var(--line); background: var(--bg); padding: 8px 10px; margin: 0 0 10px; }
  .guide p { margin: 2px 0; }
  .guide .label { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }
  .guide .cols { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-top: 8px; }
  .guide .col { background: var(--card); border: 1px solid var(--line); padding: 6px 8px; }
  .controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  button, select, input { font: inherit; }
  button[aria-selected="true"] { background: var(--ink); color: var(--bg); }
  button { border: 1px solid var(--line); background: transparent; padding: 6px 10px; cursor: pointer; }
  input, select { border: 1px solid var(--line); padding: 6px 8px; background: var(--bg); }
  .meta { color: var(--muted); }
  main { padding: 16px 20px 48px; }
  .node { background: var(--card); border: 1px solid var(--line); padding: 10px 12px; margin: 0 0 10px; }
  h2 { font-size: 15px; margin: 0 0 6px; }
  .pill { display: inline-block; border: 1px solid var(--line); color: var(--muted); font-size: 11px; padding: 1px 6px; margin-right: 4px; }
  .member { border-top: 1px solid var(--line); padding: 6px 0; }
  .q { color: var(--muted); margin: 2px 0 0; padding-left: 8px; border-left: 2px solid var(--accent); }
</style>
</head>
<body>
<header>
  <h1>Deterministic joins</h1>
  <div class="types" id="types"></div>
  <div class="guide" id="guide" hidden></div>
  <div class="controls">
    <button type="button" id="tab-merged" aria-selected="true">Merges</button>
    <button type="button" id="tab-amb" aria-selected="false">Ambiguous</button>
    <button type="button" id="tab-like" aria-selected="false">Lookalikes not merged</button>
    <button type="button" id="tab-alone" aria-selected="false">Complete but alone</button>
    <input id="search" type="search" placeholder="Filter titles and labels" />
    <span class="meta" id="meta"></span>
  </div>
</header>
<main id="main"></main>
<script>
const DATA = """ + data + """;
const typeRow = document.getElementById("types");
const counts = DATA.type_counts || {};
function typeButton(id, label) {
  const b = document.createElement("button");
  b.type = "button";
  b.dataset.type = id;
  const n = id ? (counts[id] || 0) : DATA.n_mentions;
  b.innerHTML = label + "<small>" + n.toLocaleString() + "</small>";
  b.onclick = () => { currentType = id; paintTypes(); renderGuide(); show(); };
  typeRow.appendChild(b);
}
typeButton("", "All");
(DATA.type_order || []).forEach((id) => {
  const guide = (DATA.type_guide || []).find((g) => g.event_type === id);
  typeButton(id, (guide && guide.label) || id);
});
function paintTypes() {
  typeRow.querySelectorAll("button").forEach((b) => {
    b.setAttribute("aria-selected", String(b.dataset.type === currentType));
  });
}
function renderGuide() {
  const box = document.getElementById("guide");
  const guide = (DATA.type_guide || []).find((g) => g.event_type === currentType);
  if (!guide) { box.hidden = true; box.innerHTML = ""; return; }
  box.hidden = false;
  function col(title, text) {
    return "<div class='col'><span class='label'>" + title + "</span><p>" + esc(text) + "</p></div>";
  }
  box.innerHTML =
    "<p><span class='label'>Current rule</span><br>" + esc(guide.current) + "</p>" +
    "<p><span class='label'>Held out</span><br>" + esc(guide.held_out) + "</p>" +
    "<div class='cols'>" +
      col("Fields to collect", guide.fields) +
      col("How the strict key uses them", guide.rule) +
      col("Judge", guide.judge) +
    "</div>";
}
document.getElementById("meta").textContent =
  DATA.n_mentions + " mentions · " + DATA.n_merged_groups + " merges (" + DATA.n_merged_mentions +
  " mentions) · " + DATA.n_ambiguous + " ambiguous · " + DATA.n_alone + " complete but alone";
let tab = "merged";
let currentType = "";
function esc(s) { return String(s || "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c])); }
function memberHtml(m) {
  return "<div class='member'><div>" + esc(m.title) + "</div><div>" +
    ["<span class='pill'>" + esc(m.event_type) + "</span>",
     m.country && "<span class='pill'>" + esc(m.country) + "</span>",
     m.place && "<span class='pill'>" + esc(m.place) + "</span>",
     m.year && m.year !== "unknown" && "<span class='pill'>" + esc(m.year) + "</span>",
     m.actor && "<span class='pill'>" + esc(m.actor) + "</span>"].filter(Boolean).join("") +
    "</div>" + (m.quote ? "<p class='q'>" + esc(m.quote) + "</p>" : "") + "</div>";
}
function show() {
  const q = document.getElementById("search").value.toLowerCase();
  const ty = currentType;
  const main = document.getElementById("main");
  main.innerHTML = "";
  const ok = (text, eventType) => (!ty || eventType === ty) && (!q || text.toLowerCase().includes(q));
  if (tab === "merged" || tab === "alone") {
    const rows = tab === "merged" ? DATA.merged : DATA.alone;
    rows.forEach((g) => {
      const blob = g.label + " " + g.members.map((m) => m.title).join(" ");
      if (!ok(blob, g.event_type)) return;
      const node = document.createElement("article");
      node.className = "node";
      node.innerHTML = "<h2>" + esc(g.label) + " <span class='pill'>" + g.n + "</span></h2>" +
        g.members.map(memberHtml).join("") +
        (g.n_hidden ? "<p class='meta'>" + g.n_hidden + " more not shown</p>" : "");
      main.appendChild(node);
    });
    return;
  }
  if (tab === "amb") {
    DATA.ambiguous.forEach((b) => {
      const members = b.members.filter((m) => ok(m.title + " " + b.reason, m.event_type));
      if (!members.length) return;
      const node = document.createElement("article");
      node.className = "node";
      node.innerHTML = "<h2>" + esc(b.reason) + " <span class='pill'>" + b.n + "</span></h2>" +
        members.map(memberHtml).join("") +
        (b.n_hidden ? "<p class='meta'>list capped</p>" : "");
      main.appendChild(node);
    });
    return;
  }
  DATA.lookalikes.forEach((fam) => {
    const groups = fam.groups.filter((g) => ok(g.label + " " + fam.name, g.event_type));
    if (!groups.length) return;
    const node = document.createElement("article");
    node.className = "node";
    node.innerHTML = "<h2>" + esc(fam.name) + " <span class='pill'>" + fam.n_groups + " groups, not merged</span></h2>" +
      groups.map((g) => "<div class='member'>" + esc(g.label) + " <span class='pill'>" + g.n + "</span> <span class='pill'>" + esc(g.event_type) + "</span></div>").join("");
    main.appendChild(node);
  });
}
document.getElementById("tab-merged").onclick = () => { tab = "merged"; setTab(); show(); };
document.getElementById("tab-amb").onclick = () => { tab = "amb"; setTab(); show(); };
document.getElementById("tab-like").onclick = () => { tab = "like"; setTab(); show(); };
document.getElementById("tab-alone").onclick = () => { tab = "alone"; setTab(); show(); };
function setTab() {
  ["merged", "amb", "like", "alone"].forEach((name) => {
    document.getElementById("tab-" + name).setAttribute("aria-selected", String(tab === name || (tab === "amb" && name === "amb")));
  });
  document.getElementById("tab-merged").setAttribute("aria-selected", String(tab === "merged"));
  document.getElementById("tab-amb").setAttribute("aria-selected", String(tab === "amb"));
  document.getElementById("tab-like").setAttribute("aria-selected", String(tab === "like"));
  document.getElementById("tab-alone").setAttribute("aria-selected", String(tab === "alone"));
}
document.getElementById("search").oninput = show;
paintTypes();
renderGuide();
show();
</script>
</body>
</html>
"""


def write_strict_browse(*, mentions_path: Path, out_dir: Path) -> dict[str, Any]:
    mentions = claims_io.read_jsonl(mentions_path)
    payload = build_payload(mentions)
    out_dir.mkdir(parents=True, exist_ok=True)
    claims_io.write_json(out_dir / "summary.json", {k: payload[k] for k in (
        "n_mentions", "n_merged_groups", "n_merged_mentions", "n_alone", "n_ambiguous", "rules"
    )} | {
        "ambiguous_reasons": {
            reason: sum(row["n"] for row in payload["ambiguous"] if row["reason"] == reason)
            for reason in {row["reason"] for row in payload["ambiguous"]}
        },
    })
    html_path = out_dir / "browse.html"
    html_path.write_text(_html(payload), encoding="utf-8")
    log_stderr(f"strict: {html_path}")
    log_stderr(
        f"strict: mentions={payload['n_mentions']} merges={payload['n_merged_groups']} "
        f"merged_mentions={payload['n_merged_mentions']} ambiguous={payload['n_ambiguous']} "
        f"alone={payload['n_alone']}"
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m apps.claims.events.merge.strict")
    parser.add_argument("--mentions", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)
    write_strict_browse(mentions_path=Path(args.mentions), out_dir=Path(args.out_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
