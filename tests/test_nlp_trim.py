"""Unit tests for nlp.trim (no model download)."""

from __future__ import annotations

from nlp.trim import syntok_sentence_spans, trim_sentence_boundary


def test_syntok_sentence_spans_basic():
    text = "One. Two! Three?"
    spans = syntok_sentence_spans(text)
    assert len(spans) >= 2
    assert all(0 <= a < b <= len(text) for a, b in spans)


def test_trim_sentence_boundary_around_hit():
    text = (
        "First sentence about measles. "
        "Second sentence continues the thought. "
        "Third sentence mentions MMR vaccine explicitly. "
        "Fourth sentence wraps up."
    )
    start = text.index("MMR")
    end = start + len("MMR vaccine")
    chunks = trim_sentence_boundary(
        text,
        [{"term_id": 1, "match_start": start, "match_end": end}],
    )
    assert isinstance(chunks, list)
    assert len(chunks) >= 1
    assert "MMR" in chunks[0]


def test_trim_far_apart_hits_force_split():
    """Hits farther than MAX_SENTENCES anchors must not merge into one mega-chunk."""
    sents = [f"Sentence number {i} with filler words here." for i in range(40)]
    text = " ".join(sents)
    # Anchor near start vs near end → large sentence gap
    a0 = text.index("Sentence number 2 ")
    a1 = text.index("Sentence number 35 ")
    chunks = trim_sentence_boundary(
        text,
        [
            {"match_start": a0, "match_end": a0 + len("Sentence number 2")},
            {"match_start": a1, "match_end": a1 + len("Sentence number 35")},
        ],
    )
    assert len(chunks) >= 2
    assert any("Sentence number 2" in c for c in chunks)
    assert any("Sentence number 35" in c for c in chunks)


def test_trim_even_split_under_chunk_char_limit():
    """Oversized merged span splits into ~equal pieces, not a long head + short tail."""
    from nlp.trim import CHUNK_CHAR_LIMIT

    # One hit with a huge surrounding window: many short sentences so merge is large.
    sents = [f"Word{i} keeps this sentence short enough." for i in range(200)]
    text = " ".join(sents)
    mid = text.index("Word100 ")
    chunks = trim_sentence_boundary(
        text,
        [{"match_start": mid, "match_end": mid + len("Word100")}],
        sentences_before=100,
        sentences_after=100,
        chunk_char_limit=1000,
    )
    assert len(chunks) >= 2
    assert all(len(c) <= 1000 for c in chunks)
    # Even-ish: no leftover stub that is a tiny fraction of the largest piece.
    lengths = sorted(len(c) for c in chunks)
    assert lengths[-1] / max(lengths[0], 1) < 3.0
    # Default limit constant still exists for callers.
    assert CHUNK_CHAR_LIMIT == 4000


def test_nlp_coref_importable_without_loading_model():
    from nlp import coref as coref_mod

    assert callable(coref_mod.iter_coref_resolved_posts)
    assert callable(coref_mod.process_payload)
    assert coref_mod._NLP is None
