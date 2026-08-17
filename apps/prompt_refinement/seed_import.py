"""One-time import of problem posts from labeler_lab SQLite (no shared schema)."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from apps.prompt_refinement import db, posts_data


def _load_labeler_problem_claims(labeler_db: Path) -> list[dict[str, Any]]:
    if not labeler_db.is_file():
        raise FileNotFoundError(f"Labeler lab DB not found: {labeler_db}")
    conn = sqlite3.connect(str(labeler_db))
    conn.row_factory = sqlite3.Row
    try:
        info = conn.execute("PRAGMA table_info(problem_claims)").fetchall()
        if not info:
            return []
        rows = conn.execute(
            """
            SELECT task_id, claim_index, post_json, claim_json, note
            FROM problem_claims
            ORDER BY task_id, claim_index
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]

def import_from_labeler_lab(
    conn: sqlite3.Connection,
    *,
    labeler_db_path: Path,
) -> tuple[int, int]:
    """
    Copy labeler problem_claims into refinement problem_posts (grouped by task_id).

    Returns (inserted_count, skipped_existing_count).
    """
    rows = _load_labeler_problem_claims(labeler_db_path)
    if not rows:
        return 0, 0

    by_task: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        tid = str(row["task_id"])
        by_task.setdefault(tid, []).append(row)

    inserted = 0
    skipped = 0
    for task_id, group in by_task.items():
        if db.is_problem_post(conn, task_id):
            skipped += 1
            continue
        post_row = json.loads(str(group[0]["post_json"]))
        notes = [str(r.get("note") or "").strip() for r in group if str(r.get("note") or "").strip()]
        comment = "\n---\n".join(notes) if notes else ""
        baseline = posts_data.baseline_claims_from_post(post_row)
        status = posts_data.extraction_status(post_row)
        if db.insert_problem_post_ignore(
            conn,
            task_id=task_id,
            post_row=post_row,
            baseline_claims=baseline,
            baseline_status=status,
            comment=comment,
            source="seed",
        ):
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped
