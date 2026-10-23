"""Compare two extract runs on the same chunks.

The reference run is the model already accepted (gpt-5.6-luna). The candidate
is the model under test (gpt-6-luna). A mention matches only when the two
quotes share a real span. Title wording is ignored.

Replacement is acceptable when the candidate keeps the reference mentions
(quote recall and precision), and the matched pairs agree on type, stated
year, and place.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from apps.claims.events.mention import normalize_ws

PLACE_FIELDS = ("country", "admin1", "admin2", "locality", "site")
QUOTE_JACCARD = 0.5
MIN_QUOTE_CHARS = 12
MIN_QUOTE_TOKENS = 4

# Bars are fixed before looking at a run. "replace" means the candidate can
# stand in for the reference on this corpus. "caution" means the misses are
# large enough to read before switching. Anything else stays on the reference.
REPLACE_RECALL = 0.90
REPLACE_PRECISION = 0.90
REPLACE_TYPE = 0.95
REPLACE_YEAR = 0.90
REPLACE_PLACE_CONFLICT = 0.05
CAUTION_RECALL = 0.80
CAUTION_PRECISION = 0.80
CAUTION_TYPE = 0.90


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _doc_id(mention: dict[str, Any]) -> str:
    if mention.get("task_id"):
        return str(mention["task_id"])
    return f"{mention.get('post_id')}:{mention.get('chunk_index')}"


def _norm(val: Any) -> str:
    return " ".join(str(val or "").casefold().split())


def _quotes(mention: dict[str, Any]) -> list[str]:
    out = []
    for raw in mention.get("quotes") or []:
        quote = normalize_ws(str(raw)).casefold()
        if quote:
            out.append(quote)
    return out


def _quote_score(left: str, right: str) -> float:
    if left == right and left:
        return 1.0
    # A long single-token span (a URL, a slug) still counts when one contains the other.
    if len(left) >= 24 and len(right) >= 24 and (left in right or right in left):
        return 1.0
    if len(left) < MIN_QUOTE_CHARS or len(right) < MIN_QUOTE_CHARS:
        return 0.0
    left_tokens = left.split()
    right_tokens = right.split()
    if len(left_tokens) < MIN_QUOTE_TOKENS or len(right_tokens) < MIN_QUOTE_TOKENS:
        return 0.0
    if left in right or right in left:
        return 1.0
    left_set = set(left_tokens)
    right_set = set(right_tokens)
    return len(left_set & right_set) / len(left_set | right_set)


def quote_overlap(left: dict[str, Any], right: dict[str, Any]) -> float:
    best = 0.0
    for a in _quotes(left):
        for b in _quotes(right):
            best = max(best, _quote_score(a, b))
    return best


def _year(mention: dict[str, Any]) -> str:
    if str(mention.get("date_precision") or "unknown") == "unknown":
        return ""
    start = str(mention.get("stated_start") or "")
    return start[:4] if len(start) >= 4 else ""


def _place(mention: dict[str, Any]) -> dict[str, str]:
    loc = mention.get("location") or {}
    return {field: _norm(loc.get(field)) for field in PLACE_FIELDS}


def _card(mention: dict[str, Any]) -> dict[str, Any]:
    quotes = _quotes(mention)
    return {
        "event_type": mention.get("event_type") or "",
        "title": str(mention.get("title") or "")[:160],
        "year": _year(mention),
        "place": ", ".join(v for v in _place(mention).values() if v),
        "actor": str(mention.get("actor") or "")[:80],
        "quote": quotes[0][:180] if quotes else "",
    }


def _match(ref: list[dict[str, Any]], cand: list[dict[str, Any]]) -> list[tuple[int, int, float]]:
    pairs: list[tuple[float, int, int]] = []
    for ri, r in enumerate(ref):
        for ci, c in enumerate(cand):
            score = quote_overlap(r, c)
            if score >= QUOTE_JACCARD:
                pairs.append((score, ri, ci))
    pairs.sort(reverse=True)
    used_r: set[int] = set()
    used_c: set[int] = set()
    matched: list[tuple[int, int, float]] = []
    for score, ri, ci in pairs:
        if ri in used_r or ci in used_c:
            continue
        used_r.add(ri)
        used_c.add(ci)
        matched.append((ri, ci, score))
    return matched


def _place_conflict(ref: dict[str, Any], cand: dict[str, Any]) -> bool:
    left = _place(ref)
    right = _place(cand)
    for field in PLACE_FIELDS:
        if left[field] and right[field] and left[field] != right[field]:
            return True
    return False


def _index(mentions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_doc: dict[str, list[dict[str, Any]]] = {}
    for mention in mentions:
        by_doc.setdefault(_doc_id(mention), []).append(mention)
    return by_doc


def _logged_ids(path: Path) -> set[str]:
    ids: set[str] = set()
    for row in _load_jsonl(path):
        doc_id = row.get("doc_id")
        if doc_id and not row.get("skip"):
            ids.add(str(doc_id))
    return ids


def decide(metrics: dict[str, Any]) -> str:
    recall = metrics["quote_recall"]
    precision = metrics["quote_precision"]
    type_agree = metrics["type_agree"]
    year_agree = metrics["year_agree_when_ref_dated"]
    place_conflict = metrics["place_conflict_rate"]
    if (
        recall >= REPLACE_RECALL
        and precision >= REPLACE_PRECISION
        and type_agree >= REPLACE_TYPE
        and year_agree >= REPLACE_YEAR
        and place_conflict <= REPLACE_PLACE_CONFLICT
    ):
        return "replace"
    if recall >= CAUTION_RECALL and precision >= CAUTION_PRECISION and type_agree >= CAUTION_TYPE:
        return "caution"
    return "do_not_replace"


def compare(
    *,
    reference: list[dict[str, Any]],
    candidate: list[dict[str, Any]],
    doc_ids: set[str],
) -> dict[str, Any]:
    """Score candidate mentions against reference mentions on doc_ids.

    A doc in doc_ids with no mentions is an empty extract. Docs outside
    doc_ids are ignored, so the candidate sample can be a subset.
    """
    ref_by = _index(reference)
    cand_by = _index(candidate)
    both_empty = 0
    both_nonempty = 0
    ref_only_docs = 0
    cand_only_docs = 0
    ref_n = 0
    cand_n = 0
    matched_n = 0
    type_hits = 0
    year_both = 0
    year_hits = 0
    year_ref_dated = 0
    year_ref_hits = 0
    place_conflicts = 0
    ref_types: Counter[str] = Counter()
    cand_types: Counter[str] = Counter()
    lost: list[dict[str, Any]] = []
    gained: list[dict[str, Any]] = []
    lost_on_empty = 0
    lost_on_other = 0
    gained_on_empty = 0
    gained_on_other = 0
    type_mismatch: list[dict[str, Any]] = []
    year_mismatch: list[dict[str, Any]] = []

    for doc_id in sorted(doc_ids):
        ref = ref_by.get(doc_id, [])
        cand = cand_by.get(doc_id, [])
        for mention in ref:
            ref_types[str(mention.get("event_type") or "")] += 1
        for mention in cand:
            cand_types[str(mention.get("event_type") or "")] += 1
        ref_n += len(ref)
        cand_n += len(cand)
        if not ref and not cand:
            both_empty += 1
            continue
        if ref and cand:
            both_nonempty += 1
        elif ref:
            ref_only_docs += 1
        else:
            cand_only_docs += 1
        pairs = _match(ref, cand)
        matched_n += len(pairs)
        used_r = {ri for ri, _ci, _score in pairs}
        used_c = {ci for _ri, ci, _score in pairs}
        for ri, ci, _score in pairs:
            same_type = _norm(ref[ri].get("event_type")) == _norm(cand[ci].get("event_type"))
            type_hits += int(same_type)
            ref_year = _year(ref[ri])
            cand_year = _year(cand[ci])
            if ref_year or cand_year:
                year_both += 1
                year_hits += int(ref_year == cand_year)
            if ref_year:
                year_ref_dated += 1
                year_ref_hits += int(ref_year == cand_year)
            conflict = _place_conflict(ref[ri], cand[ci])
            place_conflicts += int(conflict)
            if not same_type and len(type_mismatch) < 30:
                type_mismatch.append(
                    {"doc_id": doc_id, "reference": _card(ref[ri]), "candidate": _card(cand[ci])}
                )
            if ref_year and ref_year != cand_year and len(year_mismatch) < 30:
                year_mismatch.append(
                    {"doc_id": doc_id, "reference": _card(ref[ri]), "candidate": _card(cand[ci])}
                )
        for ri, mention in enumerate(ref):
            if ri in used_r:
                continue
            if cand:
                lost_on_other += 1
            else:
                lost_on_empty += 1
            if len(lost) < 40:
                lost.append({"doc_id": doc_id, "reference": _card(mention)})
        for ci, mention in enumerate(cand):
            if ci in used_c:
                continue
            if ref:
                gained_on_other += 1
            else:
                gained_on_empty += 1
            if len(gained) < 40:
                gained.append({"doc_id": doc_id, "candidate": _card(mention)})

    precision = matched_n / cand_n if cand_n else 1.0
    recall = matched_n / ref_n if ref_n else 1.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    n_docs = len(doc_ids)
    informative = both_nonempty + ref_only_docs + cand_only_docs
    metrics = {
        "n_docs": n_docs,
        "both_empty": both_empty,
        "both_nonempty": both_nonempty,
        "ref_only_docs": ref_only_docs,
        "cand_only_docs": cand_only_docs,
        "emptiness_agreement": round((both_empty + both_nonempty) / n_docs, 4) if n_docs else 0.0,
        "informative_agreement": round(both_nonempty / informative, 4) if informative else 1.0,
        "ref_mentions": ref_n,
        "cand_mentions": cand_n,
        "quote_matched": matched_n,
        "quote_precision": round(precision, 4),
        "quote_recall": round(recall, 4),
        "quote_f1": round(f1, 4),
        "type_agree": round(type_hits / matched_n, 4) if matched_n else 1.0,
        "year_agree_when_either_dated": round(year_hits / year_both, 4) if year_both else 1.0,
        "year_agree_when_ref_dated": round(year_ref_hits / year_ref_dated, 4) if year_ref_dated else 1.0,
        "place_conflict_rate": round(place_conflicts / matched_n, 4) if matched_n else 0.0,
        "lost_on_empty_chunk": lost_on_empty,
        "lost_beside_other_mention": lost_on_other,
        "gained_on_empty_chunk": gained_on_empty,
        "gained_beside_other_mention": gained_on_other,
        "ref_types": dict(ref_types),
        "cand_types": dict(cand_types),
    }
    metrics["verdict"] = decide(metrics)
    return {
        **metrics,
        "bars": {
            "replace": (
                f"quote recall>={REPLACE_RECALL}, precision>={REPLACE_PRECISION}, "
                f"type agree>={REPLACE_TYPE}, dated-year agree>={REPLACE_YEAR}, "
                f"place conflict<={REPLACE_PLACE_CONFLICT}"
            ),
            "caution": (
                f"quote recall>={CAUTION_RECALL}, precision>={CAUTION_PRECISION}, "
                f"type agree>={CAUTION_TYPE}"
            ),
        },
        "lost": lost,
        "gained": gained,
        "type_mismatch": type_mismatch,
        "year_mismatch": year_mismatch,
    }


def compare_runs(
    *,
    reference_mentions: Path,
    candidate_mentions: Path,
    reference_log: Path,
    candidate_log: Path,
) -> dict[str, Any]:
    ref_docs = _logged_ids(reference_log)
    cand_docs = _logged_ids(candidate_log)
    paired = cand_docs & ref_docs
    missing = cand_docs - ref_docs
    result = compare(
        reference=_load_jsonl(reference_mentions),
        candidate=_load_jsonl(candidate_mentions),
        doc_ids=paired,
    )
    result["candidate_docs"] = len(cand_docs)
    result["missing_from_reference"] = len(missing)
    return result


def _html(result: dict[str, Any]) -> str:
    data = json.dumps(
        {
            "verdict": result["verdict"],
            "bars": result["bars"],
            "summary": {k: result[k] for k in result if k not in {"lost", "gained", "type_mismatch", "year_mismatch", "bars"}},
            "lost": result["lost"],
            "gained": result["gained"],
            "type_mismatch": result["type_mismatch"],
            "year_mismatch": result["year_mismatch"],
        },
        ensure_ascii=False,
    )
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>gpt-6-luna vs gpt-5.6-luna</title>
<style>
  :root { --bg:#f6f5f2; --card:#fff; --ink:#1c1b19; --muted:#5e5a54; --line:#ddd9d2; --accent:#1f4e79; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; color: var(--ink); background: var(--bg); }
  header { padding: 16px 20px 8px; border-bottom: 1px solid var(--line); background: var(--card); position: sticky; top: 0; }
  h1 { font-size: 18px; margin: 0 0 6px; }
  button { font: inherit; border: 1px solid var(--line); background: transparent; padding: 6px 10px; cursor: pointer; }
  button[aria-selected="true"] { background: var(--ink); color: var(--bg); }
  .meta { color: var(--muted); }
  main { padding: 16px 20px 48px; }
  .node { background: var(--card); border: 1px solid var(--line); padding: 10px 12px; margin: 0 0 10px; }
  .pill { display: inline-block; border: 1px solid var(--line); color: var(--muted); font-size: 11px; padding: 1px 6px; margin-right: 4px; }
  .q { color: var(--muted); margin: 4px 0 0; padding-left: 8px; border-left: 2px solid var(--accent); }
</style>
</head>
<body>
<header>
  <h1 id="title"></h1>
  <p class="meta" id="bars"></p>
  <p class="meta" id="counts"></p>
  <button type="button" id="tab-lost" aria-selected="true">Lost</button>
  <button type="button" id="tab-gained" aria-selected="false">Gained</button>
  <button type="button" id="tab-type" aria-selected="false">Type mismatch</button>
  <button type="button" id="tab-year" aria-selected="false">Year mismatch</button>
</header>
<main id="main"></main>
<script>
const DATA = """ + data + """;
document.getElementById("title").textContent = "gpt-6-luna vs gpt-5.6-luna · " + DATA.verdict;
document.getElementById("bars").textContent = "Replace when " + DATA.bars.replace + ". Caution when " + DATA.bars.caution + ".";
const s = DATA.summary;
document.getElementById("counts").textContent =
  s.n_docs + " paired chunks · emptiness " + s.emptiness_agreement +
  " · informative " + s.informative_agreement +
  " · quote recall " + s.quote_recall + " · precision " + s.quote_precision +
  " · type " + s.type_agree + " · dated year " + s.year_agree_when_ref_dated +
  " · place conflict " + s.place_conflict_rate;
function esc(v) { return String(v || "").replace(/[&<>]/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;"}[c])); }
function card(label, m) {
  if (!m) return "";
  return "<div><b>" + esc(label) + "</b> " + esc(m.title) +
    " <span class='pill'>" + esc(m.event_type) + "</span>" +
    (m.year ? "<span class='pill'>" + esc(m.year) + "</span>" : "") +
    (m.place ? "<span class='pill'>" + esc(m.place) + "</span>" : "") +
    (m.quote ? "<p class='q'>" + esc(m.quote) + "</p>" : "") + "</div>";
}
let tab = "lost";
function show() {
  const rows = tab === "lost" ? DATA.lost : tab === "gained" ? DATA.gained : tab === "type" ? DATA.type_mismatch : DATA.year_mismatch;
  const main = document.getElementById("main");
  main.innerHTML = "";
  rows.forEach((row) => {
    const node = document.createElement("article");
    node.className = "node";
    node.innerHTML = "<div class='meta'>" + esc(row.doc_id) + "</div>" +
      card("5.6", row.reference) + card("6", row.candidate);
    main.appendChild(node);
  });
}
["lost", "gained", "type", "year"].forEach((name) => {
  document.getElementById("tab-" + name).onclick = () => {
    tab = name;
    ["lost", "gained", "type", "year"].forEach((n) => {
      document.getElementById("tab-" + n).setAttribute("aria-selected", String(n === tab));
    });
    show();
  };
});
show();
</script>
</body>
</html>
"""


def write_compare(
    *,
    reference_mentions: Path,
    candidate_mentions: Path,
    reference_log: Path,
    candidate_log: Path,
    out_dir: Path,
) -> dict[str, Any]:
    result = compare_runs(
        reference_mentions=reference_mentions,
        candidate_mentions=candidate_mentions,
        reference_log=reference_log,
        candidate_log=candidate_log,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "compare_run_12.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (out_dir / "compare_run_12.html").write_text(_html(result), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m apps.claims.events.model_compare")
    parser.add_argument("--reference-mentions", required=True)
    parser.add_argument("--candidate-mentions", required=True)
    parser.add_argument("--reference-log", required=True)
    parser.add_argument("--candidate-log", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)
    result = write_compare(
        reference_mentions=Path(args.reference_mentions),
        candidate_mentions=Path(args.candidate_mentions),
        reference_log=Path(args.reference_log),
        candidate_log=Path(args.candidate_log),
        out_dir=Path(args.out_dir),
    )
    summary = {k: result[k] for k in (
        "verdict", "n_docs", "missing_from_reference", "quote_recall", "quote_precision",
        "quote_f1", "type_agree", "year_agree_when_ref_dated", "place_conflict_rate",
        "emptiness_agreement", "informative_agreement", "ref_mentions", "cand_mentions",
        "quote_matched", "lost_on_empty_chunk", "lost_beside_other_mention",
        "gained_on_empty_chunk", "gained_beside_other_mention",
    )}
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
