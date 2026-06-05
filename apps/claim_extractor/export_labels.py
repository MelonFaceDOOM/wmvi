"""
Export manual labels from the labeler lab SQLite to JSONL (label_source=manual only).

  python -m apps.claim_extractor.export_labels \\
    --posts data/posts_with_claims_full.json \\
    --lab-db apps/claim_extractor/labeler_lab/data/lab.sqlite \\
    --out data/labeled_claim_scores.jsonl
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claim_extractor.labeler_lab import claims_data, db
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES, load_posts_from_claims_json
from apps.claim_extractor.scoring_inputs import context_text_for_post_row

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POSTS = REPO_ROOT / "data" / "posts_with_claims_full.json"
DEFAULT_LAB_DB = Path(__file__).resolve().parent / "labeler_lab" / "data" / "lab.sqlite"
DEFAULT_OUT = REPO_ROOT / "data" / "labeled_claim_scores.jsonl"


def _merge_row(
    *,
    task_id: str,
    claim_index: int,
    post_row: dict[str, Any],
    claim_dict: dict[str, Any],
    labels_by_field: dict[str, float],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "task_id": task_id,
        "claim_index": claim_index,
        "claim": str(claim_dict.get("claim") or ""),
        "text_coreference_resolved": context_text_for_post_row(post_row),
        "label_source": "manual",
        "labeled_at": datetime.now(timezone.utc).isoformat(),
    }
    for field in SCORE_FIELD_NAMES:
        if field in labels_by_field:
            row[field] = labels_by_field[field]
    if post_row.get("post_id") is not None:
        row["post_id"] = post_row["post_id"]
    if post_row.get("platform") is not None:
        row["platform"] = str(post_row["platform"])
    return row


def export_labels(
    posts: list[dict[str, Any]],
    conn: sqlite3.Connection,
) -> list[dict[str, Any]]:
    """One JSONL row per (task_id, claim_index) that has at least one manual label."""
    idx = claims_data.index_claims_by_key(posts)
    merged: dict[tuple[str, int], dict[str, Any]] = {}

    for head in db.list_heads(conn):
        if not head.score_field_name or head.score_field_name not in SCORE_FIELD_NAMES:
            continue
        for tid, cidx, y in db.fetch_labels_xy(conn, head.id, split=None):
            key = (tid, cidx)
            if key not in idx:
                continue
            post_row, claim_dict = idx[key]
            if key not in merged:
                merged[key] = _merge_row(
                    task_id=tid,
                    claim_index=cidx,
                    post_row=post_row,
                    claim_dict=claim_dict,
                    labels_by_field={head.score_field_name: y},
                )
            else:
                merged[key][head.score_field_name] = y
    return list(merged.values())


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.export_labels")
    ap.add_argument("--posts", type=Path, default=DEFAULT_POSTS)
    ap.add_argument("--lab-db", type=Path, default=DEFAULT_LAB_DB)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    if not args.posts.is_file():
        raise SystemExit(f"Posts file not found: {args.posts}")
    if not args.lab_db.is_file():
        raise SystemExit(f"Lab DB not found: {args.lab_db}")

    _payload, posts = load_posts_from_claims_json(args.posts)
    conn = db.connect(args.lab_db)
    db.init_schema(conn)
    rows = export_labels(posts, conn)
    conn.close()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"[ok] exported {len(rows)} row(s) -> {args.out.resolve()}")


if __name__ == "__main__":
    main()
