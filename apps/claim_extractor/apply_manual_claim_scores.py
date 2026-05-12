"""
Apply manual score JSONL (from ``manual_label_claims``) onto a posts-with-claims JSON copy.

Each JSONL line must include ``task_id``, ``claim_index``, and the five score field names.
Matching claims in ``--input`` are updated in-place in the written copy (original file unchanged).

Example:

  python -m apps.claim_extractor.apply_manual_claim_scores \\
    --input data/posts_with_claims_full.json \\
    --labels data/manual_claim_scores.jsonl \\
    --output data/posts_with_claims_manual_gold.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from apps.claim_extractor.model_common import SCORE_FIELD_NAMES, parse_score_01, stable_task_id


def _load_labels(path: Path) -> dict[tuple[str, int], dict[str, Any]]:
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            continue
        tid = obj.get("task_id")
        idx = obj.get("claim_index")
        if tid is None or idx is None:
            continue
        key = (str(tid), int(idx))
        by_key[key] = obj
    return by_key


def run(*, posts_path: Path, labels_path: Path, out_path: Path) -> None:
    payload = json.loads(posts_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("posts file must be a JSON object")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("posts file must have posts[]")

    labels = _load_labels(labels_path)
    applied = 0
    skipped_invalid = 0

    for row in posts:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("task_id") or stable_task_id(row))
        out = row.get("claim_extraction_output")
        if not isinstance(out, dict):
            continue
        claims = out.get("claims")
        if not isinstance(claims, list):
            continue
        for i, claim in enumerate(claims):
            if not isinstance(claim, dict):
                continue
            lab = labels.get((tid, i))
            if lab is None:
                continue
            ok = True
            for k in SCORE_FIELD_NAMES:
                v, bad = parse_score_01(lab.get(k))
                if v is None or bad:
                    ok = False
                    break
                claim[k] = v
            if ok:
                applied += 1
            else:
                skipped_invalid += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(out_path)
    print(
        f"[ok] wrote {out_path}  score rows applied to claims: {applied}  skipped_invalid: {skipped_invalid}",
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Merge manual score JSONL onto posts JSON.")
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--labels", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    run(posts_path=args.input, labels_path=args.labels, out_path=args.output)


if __name__ == "__main__":
    main()
