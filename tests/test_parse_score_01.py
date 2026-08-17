from __future__ import annotations

import math

import pytest

from nlp.claim_extraction.scores import clamp_score_01, parse_score_01


@pytest.mark.parametrize(
    "raw,expected,invalid",
    [
        (0.3, 0.3, False),
        (1, 1.0, False),
        ("0.75", 0.75, False),
        (" 0 ", 0.0, False),
        (None, None, True),
        ("", None, True),
        (1.01, None, True),
        (-0.1, None, True),
        (True, None, True),
        ("x", None, True),
    ],
)
def test_parse_score_01(raw: object, expected: float | None, invalid: bool) -> None:
    v, bad = parse_score_01(raw)
    assert v == expected
    assert bad is invalid


def test_clamp_score_01() -> None:
    assert clamp_score_01(2.0) == 1.0
    assert clamp_score_01(-1.0) == 0.0
    assert math.isfinite(clamp_score_01(float("nan")))
