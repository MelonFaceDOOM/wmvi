"""Validate posts-with-claims JSON (extraction QA)."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any


def load_rows(path: Path) -> list[dict[str, Any]]:
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        posts = payload.get("posts")
        if not isinstance(posts, list):
            raise ValueError("Object input JSON must contain top-level 'posts' list.")
        payload = posts
    if not isinstance(payload, list):
        raise ValueError("Input JSON must be a top-level list or object with 'posts'.")
    return [item for item in payload if isinstance(item, dict)]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_rows = len(rows)
    failed_rows = 0
    success_rows = 0
    malformed_rows = 0
    total_claims = 0
    claim_count_hist: Counter[int] = Counter()
    error_counter: Counter[str] = Counter()

    for row in rows:
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

    over_3 = sum(v for k, v in claim_count_hist.items() if k > 3)
    return {
        "total_rows": total_rows,
        "success_rows": success_rows,
        "failed_rows": failed_rows,
        "malformed_rows": malformed_rows,
        "total_claims": total_claims,
        "claim_count_hist": {
            "0": claim_count_hist.get(0, 0),
            "1": claim_count_hist.get(1, 0),
            "2": claim_count_hist.get(2, 0),
            "3": claim_count_hist.get(3, 0),
            ">3": over_3,
        },
        "top_errors": [
            {"error": err, "count": count}
            for err, count in error_counter.most_common(15)
        ],
    }


def run(claims_path: Path) -> dict[str, Any]:
    return summarize(load_rows(claims_path))
