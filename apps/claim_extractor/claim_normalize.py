"""Normalize claim text for deduplication (labeler lab + future pipeline)."""

from __future__ import annotations

import re

_TRAILING_PUNCT_RE = re.compile(r"[.;!?]+$")


def normalize_claim_text(raw: str | None) -> str:
    """
    Stable dedup key for claim strings.

    - Strip, collapse whitespace, casefold
    - Strip trailing ``.`` ``;`` ``!`` ``?``
    """
    if not raw or not isinstance(raw, str):
        return ""
    s = " ".join(raw.strip().split())
    if not s:
        return ""
    s = s.casefold()
    s = _TRAILING_PUNCT_RE.sub("", s).strip()
    return s
