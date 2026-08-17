"""Score field names and parsers used by extraction."""

from __future__ import annotations

import math
from typing import Any

SCORE_FIELD_NAMES: tuple[str, ...] = (
    "claim_vaccine_alignment_score",
    "author_claim_agreement_score",
    "attribution_anecdote_score",
    "attribution_authority_score",
    "attribution_common_knowledge_score",
)


def clamp_score_01(x: float) -> float:
    if math.isnan(x) or math.isinf(x):
        return 0.5
    return max(0.0, min(1.0, float(x)))


def parse_score_01(raw: Any) -> tuple[float | None, bool]:
    """Parse a finite float in [0, 1]. Returns (value, invalid)."""
    if raw is None:
        return None, True
    if isinstance(raw, bool):
        return None, True
    if isinstance(raw, (int, float)):
        v = float(raw)
        if math.isnan(v) or math.isinf(v):
            return None, True
        if 0.0 <= v <= 1.0:
            return v, False
        return None, True
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None, True
        try:
            v = float(s)
        except ValueError:
            return None, True
        if math.isnan(v) or math.isinf(v):
            return None, True
        if 0.0 <= v <= 1.0:
            return v, False
        return None, True
    return None, True
