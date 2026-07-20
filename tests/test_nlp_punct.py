"""Unit tests for nlp.punct gate (no model download)."""

from __future__ import annotations

from nlp.punct import needs_punctuation, punctuation_ratio


def test_punctuation_ratio_basic():
    assert punctuation_ratio("") == 0.0
    assert punctuation_ratio("aaaa") == 0.0
    assert punctuation_ratio("a.b") > 0.0


def test_needs_punctuation_base_gate():
    # ~0 punct marks, long enough
    low = ("word " * 40).strip()  # len ~200, ratio 0
    assert needs_punctuation(low)
    # Normal prose
    prose = ("Hello world. This is fine. " * 10).strip()
    assert not needs_punctuation(prose)
    # Too short
    assert not needs_punctuation("no punct here at all")


def test_needs_punctuation_length_aware_bump():
    # ratio between 0.004 and 0.008, length >= 2000 → should restore
    # Use commas so ratio is ~0.006 without many sentence terminators.
    # "abcdefghij, " = 12 chars, 1 comma → ratio ≈ 1/12 ≈ 0.083 too high
    # Need ~0.006: 1 punct per ~167 chars.
    unit = "x" * 160 + ", "
    text = unit * 15  # 15 commas, len = 15*162 = 2430, ratio = 15/2430 ≈ 0.0062
    assert len(text) >= 2000
    r = punctuation_ratio(text)
    assert 0.004 <= r < 0.008
    assert needs_punctuation(text)
    # Same ratio but short → no
    short = unit * 2
    assert len(short) < 2000
    assert 0.004 <= punctuation_ratio(short) < 0.008
    assert not needs_punctuation(short)
