"""
Sentence-boundary trimming helpers for search-term hit context extraction.

``trim_sentence_boundary(text, hits)`` steps:

1. Segment ``text`` into sentence char spans (syntok). Abort if empty.
2. If ≤1 sentence: build a char window around each hit
   (± MAX_CHARS_BEFORE/AFTER); else find each hit's anchor sentence and expand
   by SENTENCES_BEFORE/AFTER.
3. Cluster hit windows: new group when hit starts are ≥ FAR_HIT_GAP_CHARS apart,
   or (sentence path) anchor indices differ by more than MAX_SENTENCES.
4. Within each cluster, merge overlapping/adjacent windows into eligible spans.
5. For each merged span, if longer than CHUNK_CHAR_LIMIT, split into ~equal pieces
   (prefer sentence boundaries; else whitespace; else hard char cuts). Emit chunks.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from syntok.segmenter import analyze

SENTENCES_BEFORE = 5
SENTENCES_AFTER = 5
MAX_CHARS_BEFORE = 1000
MAX_CHARS_AFTER = 1000

# Far-hit clustering only (not a length cap): anchors farther than this many
# sentences apart are not merged into one window.
MAX_SENTENCES = 20

# Single size budget for emitted chunks (even-split when a merged span exceeds it).
CHUNK_CHAR_LIMIT = 4000

# Do not merge hit windows when anchors are this far apart (chars).
FAR_HIT_GAP_CHARS = 2000


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


def _merge_char_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge half-open [a, b) char ranges."""
    if not ranges:
        return []
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


def _snap_cut_point(
    ideal: int,
    lo: int,
    hi: int,
    text: str,
    sent_spans: list[tuple[int, int]] | None,
) -> int:
    """Choose a cut in (lo, hi) near ``ideal``, preferring sentence then whitespace."""
    if hi - lo <= 1:
        return lo
    ideal = max(lo + 1, min(ideal, hi - 1))

    cands: list[int] = []
    if sent_spans:
        for s, e in sent_spans:
            if lo < e < hi:
                cands.append(e)
            if lo < s < hi:
                cands.append(s)
    if cands:
        return min(cands, key=lambda x: (abs(x - ideal), x))

    # Whitespace near ideal (search outward).
    for radius in range(0, min(120, hi - lo)):
        for pos in (ideal - radius, ideal + radius):
            if lo < pos < hi and text[pos].isspace():
                # cut after the whitespace run start → prefer boundary after space
                return pos + 1 if pos + 1 < hi else pos
    return ideal


def _split_char_span_evenly(
    text: str,
    start: int,
    end: int,
    *,
    limit: int,
    sent_spans: list[tuple[int, int]] | None = None,
) -> list[tuple[int, int]]:
    """
    Cover half-open [start, end) with ~equal pieces each ideally ≤ ``limit``.

    Prefer sentence-boundary cuts; fall back to whitespace, then hard cuts.
    Pieces that remain over ``limit`` (e.g. one huge sentence) are hard-split evenly.
    """
    n = len(text)
    start = max(0, min(start, n))
    end = max(start, min(end, n))
    length = end - start
    limit = max(1, int(limit))
    if length <= 0:
        return []
    if length <= limit:
        return [(start, end)]

    n_pieces = max(2, int(math.ceil(length / limit)))
    piece_len = length / n_pieces
    cuts = [start]
    for i in range(1, n_pieces):
        ideal = start + int(round(i * piece_len))
        cut = _snap_cut_point(ideal, cuts[-1], end, text, sent_spans)
        if cut <= cuts[-1]:
            cut = min(cuts[-1] + max(1, limit), end)
        if cut >= end:
            break
        cuts.append(cut)
    cuts.append(end)

    raw: list[tuple[int, int]] = []
    for a, b in zip(cuts, cuts[1:]):
        if b > a:
            raw.append((a, b))

    out: list[tuple[int, int]] = []
    for a, b in raw:
        if b - a <= limit:
            out.append((a, b))
            continue
        # Hard even split (no further snap) so a single oversized sentence still fits.
        sub_n = max(2, int(math.ceil((b - a) / limit)))
        sub_len = (b - a) / sub_n
        prev = a
        for i in range(1, sub_n):
            mid = a + int(round(i * sub_len))
            mid = max(prev + 1, min(mid, b - 1))
            out.append((prev, mid))
            prev = mid
        out.append((prev, b))
    return out


def _emit_chunks_for_span(
    text: str,
    start: int,
    end: int,
    *,
    limit: int,
    sent_spans: list[tuple[int, int]] | None,
    chunks_out: list[str],
) -> None:
    for a, b in _split_char_span_evenly(text, start, end, limit=limit, sent_spans=sent_spans):
        piece = text[a:b].strip()
        if piece:
            chunks_out.append(piece)


@dataclass
class _HitMeta:
    term_id: int
    match_start: int
    match_end: int
    sentence_idx: int
    body_ms: int
    body_me: int


def _context_dict_from_char_span(
    body: str,
    sent_spans: list[tuple[int, int]],
    ca: int,
    cb: int,
    metas: list[_HitMeta],
) -> dict[str, Any]:
    """Build a context dict for half-open body[ca:cb)."""
    text = body[ca:cb].strip()
    overlapping = [i for i, (s, e) in enumerate(sent_spans) if s < cb and e > ca]
    if overlapping:
        cs, ce = overlapping[0], overlapping[-1]
    else:
        cs = ce = 0
    term_ids_set: set[int] = set()
    hit_spans: list[dict[str, Any]] = []
    for h in metas:
        if h.body_me <= ca or h.body_ms >= cb:
            continue
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
    *,
    chunk_char_limit: int = CHUNK_CHAR_LIMIT,
) -> list[dict[str, Any]]:
    n_sent = len(sent_spans)
    ranges: list[tuple[int, int]] = []
    for h in metas:
        idx = h.sentence_idx
        lo = max(0, idx - SENTENCES_BEFORE)
        hi = min(n_sent - 1, idx + SENTENCES_AFTER)
        ranges.append((lo, hi))
    merged = _merge_inclusive_ranges(ranges)
    contexts: list[dict[str, Any]] = []
    for s, e in merged:
        ca = sent_spans[s][0]
        cb = sent_spans[e][1]
        for a, b in _split_char_span_evenly(
            body, ca, cb, limit=chunk_char_limit, sent_spans=sent_spans
        ):
            ctx = _context_dict_from_char_span(body, sent_spans, a, b, metas)
            if ctx["text"]:
                contexts.append(ctx)
    return contexts


def _build_contexts_fallback_chars(
    body: str,
    metas: list[_HitMeta],
    *,
    chunk_char_limit: int = CHUNK_CHAR_LIMIT,
) -> list[dict[str, Any]]:
    """≤1 syntok sentence: merge char windows around hits, even-split if over limit."""
    n = len(body)
    char_ranges: list[tuple[int, int]] = []
    for h in metas:
        a = max(0, h.body_ms - MAX_CHARS_BEFORE)
        b = min(n, h.body_me + MAX_CHARS_AFTER)
        char_ranges.append((a, b))
    merged = _merge_char_ranges(char_ranges)
    contexts: list[dict[str, Any]] = []
    empty_spans: list[tuple[int, int]] = []
    for a, b in merged:
        for ca, cb in _split_char_span_evenly(
            body, a, b, limit=chunk_char_limit, sent_spans=None
        ):
            ctx = _context_dict_from_char_span(body, empty_spans, ca, cb, metas)
            if ctx["text"]:
                contexts.append(ctx)
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


def _cluster_hit_windows(
    windows: list[tuple[int, int, int, int, int]],
    *,
    far_hit_gap_chars: int,
    max_sentence_gap: int,
) -> list[list[tuple[int, int]]]:
    """
    Cluster (match_start, match_end, anchor_idx, sent_lo, sent_hi) into merge groups.

    Starts a new cluster when char gap from previous hit start >= far_hit_gap_chars
    or anchor sentence indices differ by more than max_sentence_gap.
    """
    if not windows:
        return []
    ordered = sorted(windows, key=lambda w: (w[0], w[1]))
    clusters: list[list[tuple[int, int]]] = []
    cur: list[tuple[int, int]] = [(ordered[0][3], ordered[0][4])]
    prev_ms = ordered[0][0]
    prev_anchor = ordered[0][2]
    for ms, _me, anchor, lo, hi in ordered[1:]:
        far_chars = (ms - prev_ms) >= far_hit_gap_chars
        far_sents = (anchor - prev_anchor) > max_sentence_gap
        if far_chars or far_sents:
            clusters.append(cur)
            cur = [(lo, hi)]
        else:
            cur.append((lo, hi))
        prev_ms = ms
        prev_anchor = anchor
    clusters.append(cur)
    return clusters


def _cluster_char_hit_windows(
    windows: list[tuple[int, int, int, int]],
    *,
    far_hit_gap_chars: int,
) -> list[list[tuple[int, int]]]:
    """Cluster (ms, me, char_lo, char_hi) for the ≤1-sentence fallback path."""
    if not windows:
        return []
    ordered = sorted(windows, key=lambda w: (w[0], w[1]))
    clusters: list[list[tuple[int, int]]] = []
    cur: list[tuple[int, int]] = [(ordered[0][2], ordered[0][3])]
    prev_ms = ordered[0][0]
    for ms, _me, lo, hi in ordered[1:]:
        if (ms - prev_ms) >= far_hit_gap_chars:
            clusters.append(cur)
            cur = [(lo, hi)]
        else:
            cur.append((lo, hi))
        prev_ms = ms
    clusters.append(cur)
    return clusters


def trim_sentence_boundary(
    text: str,
    hits: list[Any],
    *,
    sentences_before: int = SENTENCES_BEFORE,
    sentences_after: int = SENTENCES_AFTER,
    max_sentences: int = MAX_SENTENCES,
    max_chars_before: int = MAX_CHARS_BEFORE,
    max_chars_after: int = MAX_CHARS_AFTER,
    chunk_char_limit: int = CHUNK_CHAR_LIMIT,
    far_hit_gap_chars: int = FAR_HIT_GAP_CHARS,
) -> list[str]:
    """
    Return merged sentence-boundary chunks around hit spans for one text body.

    Hit windows that are far apart (char gap or sentence gap) are not merged.
    Merged spans longer than ``chunk_char_limit`` are split into ~equal pieces.
    """
    if not isinstance(text, str) or not text.strip() or not isinstance(hits, list):
        return []

    sent_spans = syntok_sentence_spans(text)
    if not sent_spans:
        return []

    chunks: list[str] = []
    local_max_sentences = max(1, int(max_sentences))
    gap = max(0, int(far_hit_gap_chars))
    limit = max(1, int(chunk_char_limit))

    if len(sent_spans) <= 1:
        hit_windows: list[tuple[int, int, int, int]] = []
        for hit in hits:
            if not isinstance(hit, dict):
                continue
            ms = int(hit.get("match_start", 0) or 0)
            me = int(hit.get("match_end", 0) or 0)
            ms = max(0, min(ms, len(text)))
            me = max(ms, min(me, len(text)))
            hit_windows.append(
                (
                    ms,
                    me,
                    max(0, ms - max_chars_before),
                    min(len(text), me + max_chars_after),
                )
            )
        if not hit_windows:
            return []
        for cluster in _cluster_char_hit_windows(hit_windows, far_hit_gap_chars=gap):
            for a, b in _merge_char_ranges(cluster):
                _emit_chunks_for_span(
                    text, a, b, limit=limit, sent_spans=None, chunks_out=chunks
                )
        return chunks

    hit_windows_s: list[tuple[int, int, int, int, int]] = []
    for hit in hits:
        if not isinstance(hit, dict):
            continue
        ms = int(hit.get("match_start", 0) or 0)
        me = int(hit.get("match_end", 0) or 0)
        ms = max(0, min(ms, len(text)))
        me = max(ms, min(me, len(text)))
        idx = _anchor_sentence_index(sent_spans, ms, me)
        lo = max(0, idx - max(0, int(sentences_before)))
        hi = min(len(sent_spans) - 1, idx + max(0, int(sentences_after)))
        hit_windows_s.append((ms, me, idx, lo, hi))

    if not hit_windows_s:
        return []

    for cluster_ranges in _cluster_hit_windows(
        hit_windows_s,
        far_hit_gap_chars=gap,
        max_sentence_gap=local_max_sentences,
    ):
        for cs, ce in _merge_inclusive_ranges(cluster_ranges):
            a = sent_spans[cs][0]
            b = sent_spans[ce][1]
            _emit_chunks_for_span(
                text, a, b, limit=limit, sent_spans=sent_spans, chunks_out=chunks
            )
    return chunks
