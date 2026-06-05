"""Create one Ridge head per standard SCORE_FIELD_NAMES entry."""

from __future__ import annotations

import sqlite3

from apps.claim_extractor.labeler_lab import db
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES


def create_standard_heads(conn: sqlite3.Connection) -> tuple[list[tuple[str, int]], list[str]]:
    """
    Create Ridge heads bound to canonical per-field input templates.

    Returns (created, skipped) where each created item is (field_name, head_id).
    Skipped heads already exist by name.
    """
    existing = {h.name for h in db.list_heads(conn)}
    created: list[tuple[str, int]] = []
    skipped: list[str] = []
    for field_name in SCORE_FIELD_NAMES:
        if field_name in existing:
            skipped.append(field_name)
            continue
        hid = db.create_head(conn, field_name, input_var_keys=[], score_field_name=field_name)
        created.append((field_name, hid))
    return created, skipped
