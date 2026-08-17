"""Stable claim / occurrence keys for annotations and selections.

``group_id`` is enumerate() order and changes when claims change — do not use it
as a join key. Prefer:

- ``claim_key(text)`` — group scope (default). Stable across re-extraction /
  re-grouping as long as the normalized claim text is unchanged.
- ``row_id(task_id, claim_index)`` — per-occurrence scope.
"""

from __future__ import annotations

import hashlib


def normalize_claim_key(text: str) -> str:
    """Whitespace-collapse + casefold; used for grouping and claim_key."""
    return " ".join(text.split()).casefold()


def claim_key(text: str) -> str:
    """Stable 16-hex key for a claim text (group scope)."""
    return hashlib.sha1(normalize_claim_key(text).encode("utf-8")).hexdigest()[:16]


def make_row_id(task_id: str, claim_index: int) -> str:
    """Per-occurrence key: ``{task_id}:{claim_index}``."""
    return f"{task_id}:{int(claim_index)}"
