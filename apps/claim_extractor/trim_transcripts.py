"""
Build per-post ``contexts``: merged, sentence-bounded text windows for LLM input.

Reads JSON like ``scripts.get_posts_for_term`` (``posts`` with ``hits``). Uses
``text_coreference_resolved`` when present (offsets are remapped from ``text``).

For each post, **syntok** segments the body once. Each hit maps to a sentence index;
sentence windows ``[idx - SENTENCES_BEFORE, idx + SENTENCES_AFTER]`` (clamped) are
merged when overlapping or adjacent, then split if longer than ``MAX_SENTENCES`` using
sliding windows with ``CONTEXT_SENTENCE_OVERLAP``. If syntok yields at most one
sentence, falls back to merged character windows around hits.

Each context includes ``text``, ``start_sentence_idx``, ``end_sentence_idx``,
``term_ids``, and ``hit_spans`` for traceability. Per-hit ``trimmed_text`` is not used.

Usage:
  python apps/claim_extractor/trim_transcripts.py
  python apps/claim_extractor/coreference_resolution.py --input sample.json --output sample_coref.json
  python apps/claim_extractor/trim_transcripts.py --input sample_coref.json --output out_trimmed.json

Full sample test (from repo root; coref needs ``pip install -r apps/claim_extractor/requirements-coref.txt``):

  cd /path/to/wmvi
  pip install -r requirements.txt
  pip install -r apps/claim_extractor/requirements-coref.txt
  python apps/claim_extractor/coreference_resolution.py --input apps/claim_extractor/sample.json --output /tmp/sample_coref.json
  python apps/claim_extractor/trim_transcripts.py --input /tmp/sample_coref.json --output /tmp/sample_contexts.json
  python3 -c "import json; d=json.load(open('/tmp/sample_contexts.json')); print(sum(len(p.get('contexts', [])) for p in d['posts']))"
"""

from __future__ import annotations

import argparse
import copy
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from syntok.segmenter import analyze

SENTENCES_BEFORE = 2
SENTENCES_AFTER = 2
MAX_CHARS_BEFORE = 400
MAX_CHARS_AFTER = 400

# After merging hit windows, cap context length in sentences; split with overlap if larger.
MAX_SENTENCES = 5
CONTEXT_SENTENCE_OVERLAP = 2

# Fallback when syntok yields ≤1 sentence: cap merged char slice; split with overlap if larger.
MAX_CONTEXT_CHARS = MAX_SENTENCES * 500
CONTEXT_CHAR_OVERLAP = 300

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "sample.json"
DEFAULT_OUTPUT = SCRIPT_DIR / "out_trimmed.json"


def syntok_sentence_spans(text: str) -> list[tuple[int, int]]:
    """
    Character [start, end) spans per syntok sentence, document order.

    ``analyze`` preserves original offsets on tokens; spans slice the input exactly.
    """
    if not text:
        return []
    spans: list[tuple[int, int]] = []
    for paragraph in analyze(text):
        for sentence in paragraph:
            toks = list(sentence)
            if not toks:
                continue
            first, last = toks[0], toks[-1]
            s = first.offset - len(first.spacing)
            e = last.offset + len(last.value)
            s = max(0, min(s, len(text)))
            e = max(s, min(e, len(text)))
            spans.append((s, e))
    return spans


def _anchor_sentence_index(spans: list[tuple[int, int]], match_start: int, match_end: int) -> int:
    """Sentence index overlapping the hit, or nearest by gap."""
    ms, me = match_start, match_end
    best_i = 0
    best_d: Optional[int] = None
    for i, (s, e) in enumerate(spans):
        if s < me and e > ms:
            return i
        if me <= s:
            d = s - me
        elif e <= ms:
            d = ms - e
        else:
            d = 0
        if best_d is None or d < best_d or (d == best_d and i < best_i):
            best_d = d
            best_i = i
    return best_i


def _hit_span_in_trim_body(
    original: str,
    body: str,
    match_start: int,
    match_end: int,
    term_name: Optional[str],
) -> tuple[int, int]:
    """
    Map [match_start, match_end) from ``original`` into ``body`` (e.g. coref output).

    Tries exact substring, then case-insensitive ``term_name``, then proportional
    scaling when lengths differ.
    """
    no = len(original)
    nb = len(body)
    ms = max(0, min(int(match_start), no))
    me = max(ms, min(int(match_end), no))
    if body == original:
        return max(0, min(ms, nb)), max(0, min(me, nb))

    needle = original[ms:me]
    if needle:
        anchor = int(ms * nb / no) if no else 0
        window = max(len(needle) * 6, 320)
        lo = max(0, anchor - window)
        hi = min(nb, anchor + window + len(needle))
        local = body.find(needle, lo, hi)
        if local < 0:
            local = body.find(needle)
        if local >= 0:
            return local, local + len(needle)

    if term_name and term_name.strip():
        tn = term_name.strip()
        anchor = int(ms * nb / no) if no else 0
        window = max(len(tn) * 8, 400)
        lo = max(0, anchor - window)
        hi = min(nb, anchor + window + len(tn))
        blob = body[lo:hi]
        blob_lower = blob.lower()
        pos = blob_lower.find(tn.lower())
        if pos >= 0:
            s = lo + pos
            return s, s + len(tn)
        pos2 = body.lower().find(tn.lower())
        if pos2 >= 0:
            return pos2, pos2 + len(tn)

    if no > 0 and nb > 0:
        rs = int(ms * nb / no)
        re_ = int(me * nb / no)
        rs = max(0, min(rs, nb))
        re_ = max(rs, min(re_, nb))
        return rs, re_
    return 0, min(nb, max(0, me - ms))


def _merge_inclusive_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or touching [start, end] inclusive sentence index ranges."""
    if not ranges:
        return []
    ranges = sorted(ranges, key=lambda x: (x[0], x[1]))
    out: list[tuple[int, int]] = []
    cur_s, cur_e = ranges[0]
    for s, e in ranges[1:]:
        if s <= cur_e + 1:
            cur_e = max(cur_e, e)
        else:
            out.append((cur_s, cur_e))
            cur_s, cur_e = s, e
    out.append((cur_s, cur_e))
    return out


def _split_long_sentence_range(s: int, e: int) -> list[tuple[int, int]]:
    """
    If inclusive range [s, e] has more than MAX_SENTENCES sentences, split into
    sliding windows of length MAX_SENTENCES with CONTEXT_SENTENCE_OVERLAP overlap.
    """
    length = e - s + 1
    if length <= MAX_SENTENCES:
        return [(s, e)]
    step = MAX_SENTENCES - CONTEXT_SENTENCE_OVERLAP
    if step <= 0:
        step = 1
    windows: list[tuple[int, int]] = []
    cur = s
    while cur <= e:
        end_win = min(cur + MAX_SENTENCES - 1, e)
        windows.append((cur, end_win))
        if end_win >= e:
            break
        cur += step
    return windows


def _merge_char_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge half-open or inclusive char ranges — use [a,b) half-open internally."""
    if not ranges:
        return []
    # normalize to [a, b) half-open
    norm: list[tuple[int, int]] = []
    for a, b in ranges:
        if b <= a:
            continue
        norm.append((a, b))
    norm.sort(key=lambda x: (x[0], x[1]))
    out: list[tuple[int, int]] = []
    ca, cb = norm[0]
    for a, b in norm[1:]:
        if a <= cb:
            cb = max(cb, b)
        else:
            out.append((ca, cb))
            ca, cb = a, b
    out.append((ca, cb))
    return out


def _split_long_char_range(a: int, b: int, body_len: int) -> list[tuple[int, int]]:
    """Half-open [a, b); split if longer than MAX_CONTEXT_CHARS with CONTEXT_CHAR_OVERLAP."""
    a = max(0, min(a, body_len))
    b = max(a, min(b, body_len))
    if b - a <= MAX_CONTEXT_CHARS:
        return [(a, b)]
    step = MAX_CONTEXT_CHARS - CONTEXT_CHAR_OVERLAP
    if step <= 0:
        step = MAX_CONTEXT_CHARS // 2 or 1
    windows: list[tuple[int, int]] = []
    cur = a
    while cur < b:
        end_win = min(cur + MAX_CONTEXT_CHARS, b)
        windows.append((cur, end_win))
        if end_win >= b:
            break
        cur += step
    return windows


@dataclass
class _HitMeta:
    term_id: int
    match_start: int
    match_end: int
    sentence_idx: int
    body_ms: int
    body_me: int


def _context_dict_from_sentence_range(
    body: str,
    sent_spans: list[tuple[int, int]],
    cs: int,
    ce: int,
    metas: list[_HitMeta],
) -> dict[str, Any]:
    """Inclusive sentence indices cs..ce."""
    n = len(body)
    start_char = sent_spans[cs][0]
    end_char = sent_spans[ce][1]
    start_char = max(0, min(start_char, n))
    end_char = max(start_char, min(end_char, n))
    text = body[start_char:end_char].strip()
    term_ids_set: set[int] = set()
    hit_spans: list[dict[str, Any]] = []
    for h in metas:
        if cs <= h.sentence_idx <= ce:
            term_ids_set.add(h.term_id)
            hit_spans.append(
                {
                    "term_id": h.term_id,
                    "match_start": h.match_start,
                    "match_end": h.match_end,
                    "sentence_index": h.sentence_idx,
                }
            )
    return {
        "text": text,
        "start_sentence_idx": cs,
        "end_sentence_idx": ce,
        "term_ids": sorted(term_ids_set),
        "hit_spans": hit_spans,
    }


def _build_contexts_sentence_mode(
    body: str,
    sent_spans: list[tuple[int, int]],
    metas: list[_HitMeta],
) -> list[dict[str, Any]]:
    n_sent = len(sent_spans)
    ranges: list[tuple[int, int]] = []
    for h in metas:
        idx = h.sentence_idx
        lo = max(0, idx - SENTENCES_BEFORE)
        hi = min(n_sent - 1, idx + SENTENCES_AFTER)
        ranges.append((lo, hi))
    merged = _merge_inclusive_ranges(ranges)
    final_ranges: list[tuple[int, int]] = []
    for s, e in merged:
        final_ranges.extend(_split_long_sentence_range(s, e))
    contexts = [
        _context_dict_from_sentence_range(body, sent_spans, cs, ce, metas)
        for cs, ce in final_ranges
    ]
    return contexts


def _build_contexts_fallback_chars(body: str, metas: list[_HitMeta]) -> list[dict[str, Any]]:
    """≤1 syntok sentence: merge char windows around hits, split long runs by char overlap."""
    n = len(body)
    char_ranges: list[tuple[int, int]] = []
    for h in metas:
        a = max(0, h.body_ms - MAX_CHARS_BEFORE)
        b = min(n, h.body_me + MAX_CHARS_AFTER)
        char_ranges.append((a, b))
    merged = _merge_char_ranges(char_ranges)
    contexts: list[dict[str, Any]] = []
    for a, b in merged:
        for ca, cb in _split_long_char_range(a, b, n):
            # Map char span to pseudo sentence indices 0 for schema consistency
            text = body[ca:cb].strip()
            term_ids_set: set[int] = set()
            hit_spans: list[dict[str, Any]] = []
            for h in metas:
                if not (h.body_me <= ca or h.body_ms >= cb):
                    term_ids_set.add(h.term_id)
                    hit_spans.append(
                        {
                            "term_id": h.term_id,
                            "match_start": h.match_start,
                            "match_end": h.match_end,
                            "sentence_index": h.sentence_idx,
                        }
                    )
            contexts.append(
                {
                    "text": text,
                    "start_sentence_idx": 0,
                    "end_sentence_idx": 0,
                    "term_ids": sorted(term_ids_set),
                    "hit_spans": hit_spans,
                }
            )
    return contexts


def _strip_trimmed_text_from_hits(hits: list[Any]) -> None:
    for h in hits:
        if isinstance(h, dict) and "trimmed_text" in h:
            del h["trimmed_text"]


def build_contexts_for_post(
    body: str,
    original: str,
    hits: list[Any],
) -> list[dict[str, Any]]:
    """Compute ``contexts`` for one post; ``hits`` are mutated to remove ``trimmed_text``."""
    if not isinstance(hits, list):
        return []
    _strip_trimmed_text_from_hits(hits)

    sent_spans = syntok_sentence_spans(body) if body else []
    metas: list[_HitMeta] = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        tid = hit.get("term_id")
        if tid is None:
            continue
        try:
            term_id = int(tid)
        except (TypeError, ValueError):
            continue
        ms = int(hit["match_start"]) if hit.get("match_start") is not None else 0
        me = int(hit["match_end"]) if hit.get("match_end") is not None else 0
        tn = hit.get("term_name")
        tname = tn if isinstance(tn, str) else None
        bms, bme = _hit_span_in_trim_body(original, body, ms, me, tname)
        idx = _anchor_sentence_index(sent_spans, bms, bme) if sent_spans else 0
        metas.append(
            _HitMeta(
                term_id=term_id,
                match_start=ms,
                match_end=me,
                sentence_idx=idx,
                body_ms=bms,
                body_me=bme,
            )
        )

    if not metas:
        return []

    if len(sent_spans) <= 1:
        return _build_contexts_fallback_chars(body, metas)
    return _build_contexts_sentence_mode(body, sent_spans, metas)


def process_payload(data: dict[str, Any], *, progress: bool = False) -> dict[str, Any]:
    out = copy.deepcopy(data)
    posts = out.get("posts")
    if not isinstance(posts, list):
        return out
    post_iter = posts
    if progress and posts:
        from tqdm import tqdm

        post_iter = tqdm(posts, desc="contexts", unit="post", leave=True)
    for post in post_iter:
        if not isinstance(post, dict):
            continue
        original = post.get("text")
        if not isinstance(original, str):
            original = ""
        resolved = post.get("text_coreference_resolved")
        if isinstance(resolved, str) and resolved.strip():
            body = resolved
        else:
            body = original
        hits = post.get("hits")
        if not isinstance(hits, list):
            post["contexts"] = []
            continue
        post["contexts"] = build_contexts_for_post(body, original, hits)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Add merged sentence contexts per post.")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args()

    raw = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit("input JSON must be an object")
    processed = process_payload(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(processed, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    n_posts = len(processed.get("posts", []))
    n_ctx = sum(len(p.get("contexts", [])) for p in processed.get("posts", []) if isinstance(p, dict))
    print(f"[ok] wrote {n_posts} posts, {n_ctx} contexts total, to {args.output.resolve()}")


if __name__ == "__main__":
    main()
