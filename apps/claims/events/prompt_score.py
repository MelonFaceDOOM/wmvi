"""Score an extract try against prompt_sample/labels.jsonl.

Alignment is within a chunk. A predicted mention matches a gold event when
the event types are the same. Place, date, place_scope, and study_action are
scored only on matches. study_action and stated_start break ties so a
publication is not paired with the retraction of the same paper. Diseases,
vaccines, and words from the gold identity break a remaining tie so two
events of the same type are not swapped. The model does not emit identity.

An event with `"optional": true` may be absent. Omitting it is not a miss
and does not lower recall. A prediction that matches it is not an extra.
Required events still have to match. A required event wins a tie against an
optional one.

`"also"` lists other values that pass for a place field. The primary value
still has to be one the text supports. `"site": "Englewood Kroger"` with
`"also": {"site": ["Englewood Kroger store"]}` accepts either string.

composite = 0.40*emptiness + 0.30*event F1 + 0.10*place + 0.08*date
            + 0.07*place_scope + 0.05*study_action
composite_core is the previous weighting, without the two new fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PLACE_FIELDS = ("country", "admin1", "admin2", "locality", "site")


def _norm(val: Any) -> str:
    return " ".join(str(val or "").casefold().split())


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text().splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _pred_place(mention: dict[str, Any]) -> dict[str, str]:
    loc = mention.get("location") or {}
    return {field: str(loc.get(field) or "") for field in PLACE_FIELDS}


def _doc_id(mention: dict[str, Any]) -> str:
    if mention.get("task_id"):
        return str(mention["task_id"])
    return f"{mention.get('post_id')}:{mention.get('chunk_index')}"


def _optional(event: dict[str, Any]) -> bool:
    return bool(event.get("optional"))


def _accepted(event: dict[str, Any], field: str) -> set[str]:
    vals = {_norm(event.get(field))}
    extra = (event.get("also") or {}).get(field) or []
    if isinstance(extra, str):
        extra = [extra]
    vals.update(_norm(item) for item in extra)
    vals.discard("")
    return vals


def _tag_set(event: dict[str, Any], key: str) -> set[str]:
    return {_norm(item) for item in (event.get(key) or []) if _norm(item)}


def _pred_text(pred: dict[str, Any]) -> str:
    parts = [str(pred.get("title") or "")]
    parts.extend(str(quote) for quote in (pred.get("quotes") or []))
    return _norm(" ".join(parts))


def _content_tie_break(gold: dict[str, Any], pred: dict[str, Any]) -> int:
    """Prefer the prediction that is about the same disease, vaccine, or identity.

    A shared quote can mention every disease in the sentence, so a tag on the
    prediction's own list outranks the same word merely appearing in the quote.
    """
    text = _pred_text(pred)
    score = 0
    for key in ("diseases", "vaccines"):
        for tag in _tag_set(gold, key):
            if tag in _tag_set(pred, key):
                score += 3
            elif tag in text:
                score += 1
    for token in _norm(str(gold.get("identity") or "").replace("_", " ")).split():
        if len(token) >= 4 and token in text:
            score += 1
    return score


def _match_events(
    gold: list[dict[str, Any]], pred: list[dict[str, Any]]
) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int, int]] = []
    for gi, g in enumerate(gold):
        for pi, p in enumerate(pred):
            if _norm(g.get("event_type")) != _norm(p.get("event_type")):
                continue
            pplace = {field: _norm(v) for field, v in _pred_place(p).items()}
            score = 10
            for field in PLACE_FIELDS:
                if pplace[field] and pplace[field] in _accepted(g, field):
                    score += 2
            if _norm(g.get("date_precision")) == _norm(p.get("date_precision")):
                score += 1
            gstart = _norm(g.get("stated_start"))
            if gstart and gstart == _norm(p.get("stated_start")):
                score += 3
            gaction = _norm(g.get("study_action"))
            if gaction and gaction == _norm(p.get("study_action")):
                score += 5
            if not _optional(g):
                score += 1
            score += _content_tie_break(g, p)
            pairs.append((score, gi, pi))
    pairs.sort(reverse=True)
    used_g: set[int] = set()
    used_p: set[int] = set()
    matched: list[tuple[int, int]] = []
    for _score, gi, pi in pairs:
        if gi in used_g or pi in used_p:
            continue
        used_g.add(gi)
        used_p.add(pi)
        matched.append((gi, pi))
    return matched


def _place_date(gold: dict[str, Any], pred: dict[str, Any]) -> dict[str, Any]:
    pplace = _pred_place(pred)
    filled = 0
    hits = 0
    diffs = []
    for field in PLACE_FIELDS:
        accepted = _accepted(gold, field)
        if not accepted:
            continue
        filled += 1
        if _norm(pplace[field]) in accepted:
            hits += 1
        else:
            diffs.append(f"{field}: {gold.get(field)!r} vs {pplace[field]!r}")
    date_ok = _norm(gold.get("date_precision")) == _norm(pred.get("date_precision"))
    if not date_ok:
        diffs.append(
            f"date_precision: {gold.get('date_precision')!r} vs {pred.get('date_precision')!r}"
        )
    start_ok = _norm(gold.get("stated_start")) == _norm(pred.get("stated_start"))
    if gold.get("stated_start") and not start_ok:
        diffs.append(
            f"stated_start: {gold.get('stated_start')!r} vs {pred.get('stated_start')!r}"
        )
    scope_ok = _norm(gold.get("place_scope")) == _norm(pred.get("place_scope"))
    if gold.get("place_scope") and not scope_ok:
        diffs.append(
            f"place_scope: {gold.get('place_scope')!r} vs {pred.get('place_scope')!r}"
        )
    study_ok = _norm(gold.get("study_action")) == _norm(pred.get("study_action"))
    if gold.get("study_action") or pred.get("study_action"):
        if not study_ok:
            diffs.append(
                f"study_action: {gold.get('study_action')!r} vs {pred.get('study_action')!r}"
            )
    return {
        "place_filled": filled,
        "place_hits": hits,
        "date_ok": date_ok,
        "start_ok": start_ok,
        "scope_ok": scope_ok,
        "study_ok": study_ok,
        "diffs": diffs,
    }


def score_try(*, labels_path: Path, mentions_path: Path) -> dict[str, Any]:
    labels = _load_jsonl(labels_path)
    by_doc: dict[str, list[dict[str, Any]]] = {}
    for mention in _load_jsonl(mentions_path):
        by_doc.setdefault(_doc_id(mention), []).append(mention)

    docs = []
    emptiness_hits = 0
    fp_docs = 0
    fn_docs = 0
    gold_n = 0
    pred_n = 0
    matched_n = 0
    place_filled = 0
    place_hits = 0
    date_hits = 0
    start_filled = 0
    start_hits = 0
    scope_n = 0
    scope_hits = 0
    study_n = 0
    study_hits = 0
    optional_matched_n = 0
    optional_omitted_n = 0

    for row in labels:
        doc_id = str(row["doc_id"])
        gold = list(row.get("events") or [])
        pred = by_doc.get(doc_id, [])
        required = [i for i, event in enumerate(gold) if not _optional(event)]
        pairs = _match_events(gold, pred)
        matched_g = {gi for gi, _pi in pairs}
        matched_p = {pi for _gi, pi in pairs}
        required_pairs = [(gi, pi) for gi, pi in pairs if gi in set(required)]
        optional_pairs = [(gi, pi) for gi, pi in pairs if gi not in set(required)]
        optional_omitted = [
            i for i, event in enumerate(gold) if _optional(event) and i not in matched_g
        ]
        unmatched_pred = [i for i in range(len(pred)) if i not in matched_p]
        # No required event: empty output and an output that is only the
        # optional events are the same result.
        gold_empty = not required
        pred_empty = not pred if required else not unmatched_pred
        if gold_empty == pred_empty:
            emptiness_hits += 1
        elif pred_empty:
            fn_docs += 1
        else:
            fp_docs += 1
        matched_n += len(required_pairs)
        optional_matched_n += len(optional_pairs)
        optional_omitted_n += len(optional_omitted)
        gold_n += len(required)
        pred_n += len(pred)
        pair_rows = []
        for gi, pi in pairs:
            detail = _place_date(gold[gi], pred[pi])
            place_filled += detail["place_filled"]
            place_hits += detail["place_hits"]
            date_hits += int(detail["date_ok"])
            if gold[gi].get("stated_start"):
                start_filled += 1
                start_hits += int(detail["start_ok"])
            if gold[gi].get("place_scope"):
                scope_n += 1
                scope_hits += int(detail["scope_ok"])
            study_n += 1
            study_hits += int(detail["study_ok"])
            pair_rows.append(
                {
                    "identity": gold[gi].get("identity"),
                    "event_type": gold[gi].get("event_type"),
                    "pred_title": pred[pi].get("title"),
                    "diffs": detail["diffs"],
                }
            )
        missed = [
            {"identity": gold[i].get("identity"), "event_type": gold[i].get("event_type")}
            for i in required
            if i not in matched_g
        ]
        extra = [
            {"event_type": pred[i].get("event_type"), "title": pred[i].get("title")}
            for i in unmatched_pred
        ]
        if missed or extra or any(pair["diffs"] for pair in pair_rows) or gold_empty != pred_empty:
            docs.append(
                {
                    "doc_id": doc_id,
                    "gold_n": len(required),
                    "pred_n": len(pred),
                    "missed": missed,
                    "extra": extra,
                    "matched": pair_rows,
                    "optional_omitted": [
                        {"identity": gold[i].get("identity"), "event_type": gold[i].get("event_type")}
                        for i in optional_omitted
                    ],
                }
            )

    accounted = matched_n + optional_matched_n
    precision = accounted / pred_n if pred_n else 1.0
    recall = matched_n / gold_n if gold_n else 1.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    emptiness = emptiness_hits / len(labels) if labels else 0.0
    place = place_hits / place_filled if place_filled else 1.0
    date = date_hits / accounted if accounted else 1.0
    start = start_hits / start_filled if start_filled else 1.0
    scope = scope_hits / scope_n if scope_n else 1.0
    study = study_hits / study_n if study_n else 1.0
    composite_core = 0.45 * emptiness + 0.35 * f1 + 0.10 * place + 0.10 * date
    composite = (
        0.40 * emptiness + 0.30 * f1 + 0.10 * place + 0.08 * date + 0.07 * scope + 0.05 * study
    )
    return {
        "n_docs": len(labels),
        "emptiness_hits": emptiness_hits,
        "fp_docs": fp_docs,
        "fn_docs": fn_docs,
        "gold_events": gold_n,
        "pred_events": pred_n,
        "matched": matched_n,
        "optional_matched": optional_matched_n,
        "optional_omitted": optional_omitted_n,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "place_acc": round(place, 4),
        "date_acc": round(date, 4),
        "start_acc": round(start, 4),
        "scope_acc": round(scope, 4),
        "study_acc": round(study, 4),
        "composite_core": round(composite_core, 4),
        "composite": round(composite, 4),
        "docs": docs,
    }
