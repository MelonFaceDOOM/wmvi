from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "posts_with_claims.json"
TOP_ERROR_LIMIT = 15


def _load_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        posts = payload.get("posts")
        if not isinstance(posts, list):
            raise ValueError("Object input JSON must contain top-level 'posts' list.")
        payload = posts
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be a top-level list or object with 'posts'.")
    rows: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_rows = len(rows)
    failed_rows = 0
    success_rows = 0
    malformed_rows = 0
    total_claims = 0

    claim_count_hist = Counter()
    error_counter = Counter()

    for row in rows:
        # New format from get_claims.py
        status = row.get("claim_extraction_status")
        if status == "failed":
            failed_rows += 1
            err = row.get("claim_extraction_error")
            err_text = str(err).strip() if err is not None else "unknown error"
            error_counter[err_text] += 1
            continue
        if status == "success":
            out = row.get("claim_extraction_output")
            claims = out.get("claims") if isinstance(out, dict) else None
            if not isinstance(claims, list):
                malformed_rows += 1
                error_counter["malformed: success row missing claim_extraction_output.claims"] += 1
                continue
            success_rows += 1
            n_claims = len(claims)
            total_claims += n_claims
            claim_count_hist[n_claims] += 1
            continue

        # Legacy format support
        output = row.get("output")
        if not isinstance(output, dict):
            malformed_rows += 1
            error_counter["malformed: missing extraction output"] += 1
            continue
        if output.get("failed") is True:
            failed_rows += 1
            err = output.get("error")
            err_text = str(err).strip() if err is not None else "unknown error"
            error_counter[err_text] += 1
            continue
        claims = output.get("claims")
        if not isinstance(claims, list):
            malformed_rows += 1
            error_counter["malformed: legacy success output missing claims list"] += 1
            continue
        success_rows += 1
        n_claims = len(claims)
        total_claims += n_claims
        claim_count_hist[n_claims] += 1

    return {
        "total_rows": total_rows,
        "success_rows": success_rows,
        "failed_rows": failed_rows,
        "malformed_rows": malformed_rows,
        "total_claims": total_claims,
        "claim_count_hist": claim_count_hist,
        "error_counter": error_counter,
    }


def _print_summary(summary: dict[str, Any], *, top_errors: int) -> None:
    total_rows = int(summary["total_rows"])
    success_rows = int(summary["success_rows"])
    failed_rows = int(summary["failed_rows"])
    malformed_rows = int(summary["malformed_rows"])
    total_claims = int(summary["total_claims"])
    claim_count_hist: Counter = summary["claim_count_hist"]
    error_counter: Counter = summary["error_counter"]

    print(f"Total rows: {total_rows}")
    print(f"Successful rows: {success_rows}")
    print(f"Failed rows: {failed_rows}")
    print(f"Malformed rows: {malformed_rows}")
    print(f"Total claims: {total_claims}")
    print()

    print("Posts by claim count:")
    print(f"  0 claims: {claim_count_hist.get(0, 0)}")
    print(f"  1 claim : {claim_count_hist.get(1, 0)}")
    print(f"  2 claims: {claim_count_hist.get(2, 0)}")
    print(f"  3 claims: {claim_count_hist.get(3, 0)}")
    over_3 = sum(v for k, v in claim_count_hist.items() if isinstance(k, int) and k > 3)
    print(f"  >3 claims: {over_3}")
    print()

    if error_counter:
        print(f"Most common errors (top {top_errors}):")
        for err, count in error_counter.most_common(max(1, int(top_errors))):
            print(f"  {count:>6}  {err}")
    else:
        print("Most common errors: none")


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.validate_claims_output")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--top-errors", type=int, default=TOP_ERROR_LIMIT, metavar="N")
    args = ap.parse_args()

    rows = _load_rows(args.input)
    summary = _summarize(rows)
    _print_summary(summary, top_errors=max(1, int(args.top_errors)))


if __name__ == "__main__":
    main()
