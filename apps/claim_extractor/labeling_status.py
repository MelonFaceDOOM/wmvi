"""
Report manual labeling progress per standard score-field head.

  python -m apps.claim_extractor.labeling_status \\
    --lab-db apps/claim_extractor/labeler_lab/data/lab.sqlite
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from apps.claim_extractor.labeler_lab import db, standard_heads
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES

DEFAULT_LAB_DB = Path(__file__).resolve().parent / "labeler_lab" / "data" / "lab.sqlite"
TARGET_EVAL_LABELS = 30


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.labeling_status")
    ap.add_argument("--lab-db", type=Path, default=DEFAULT_LAB_DB)
    ap.add_argument("--create-missing-heads", action="store_true")
    ap.add_argument("--target-eval", type=int, default=TARGET_EVAL_LABELS)
    args = ap.parse_args()

    conn = db.connect(args.lab_db)
    db.init_schema(conn)

    if args.create_missing_heads:
        created, skipped = standard_heads.create_standard_heads(conn)
        if created:
            print(f"Created heads: {[f for f, _ in created]}")
        if skipped:
            print(f"Already existed: {skipped}")

    heads_by_name = {h.name: h for h in db.list_heads(conn)}
    report: dict[str, dict] = {}
    for field_name in SCORE_FIELD_NAMES:
        head = heads_by_name.get(field_name)
        if head is None:
            report[field_name] = {"status": "missing_head", "train": 0, "eval": 0}
            continue
        n_train = db.count_labels(conn, head.id, "train")
        n_eval = db.count_labels(conn, head.id, "eval")
        trained = bool(head.artifact_dir)
        ready = n_eval >= args.target_eval and trained
        report[field_name] = {
            "head_id": head.id,
            "train": n_train,
            "eval": n_eval,
            "trained": trained,
            "target_eval": args.target_eval,
            "eval_target_met": n_eval >= args.target_eval,
            "ready_for_benchmark": ready,
        }

    conn.close()
    print(json.dumps(report, indent=2))
    print()
    print("Suggested order: label claim_vaccine_alignment_score first (claim-only input).")
    print(f"Target >={args.target_eval} eval labels per field before slimming LLM extraction.")


if __name__ == "__main__":
    main()
