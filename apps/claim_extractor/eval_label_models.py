"""
Evaluate ``pred_*`` continuous scores vs gold columns on claims JSON.

Reports MAE and RMSE per field (gold vs prediction). Gold = same field name without ``pred_`` prefix.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from apps.claim_extractor.model_common import (
    PRED_JSON_KEYS,
    iter_success_claim_records,
    load_posts_from_claims_json,
    parse_score_01,
)


def _evaluate_score_field(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    pred_key = PRED_JSON_KEYS[field]
    missing_pred = 0
    missing_gold = 0
    invalid_gold = 0
    invalid_pred = 0
    total = 0
    evaluated = 0
    abs_errs: list[float] = []
    sq_errs: list[float] = []

    for row in rows:
        if row.get("claim_extraction_status") != "success":
            continue
        out = row.get("claim_extraction_output")
        if not isinstance(out, dict):
            continue
        claims = out.get("claims")
        if not isinstance(claims, list):
            continue
        for c in claims:
            if not isinstance(c, dict):
                continue
            total += 1
            g_raw = c.get(field)
            gv, bad_g = parse_score_01(g_raw)
            if gv is None:
                missing_gold += 1
                continue
            if bad_g:
                invalid_gold += 1
                continue

            if pred_key not in c:
                missing_pred += 1
                continue
            pv, bad_p = parse_score_01(c.get(pred_key))
            if pv is None or bad_p:
                invalid_pred += 1
                continue

            evaluated += 1
            d = pv - gv
            abs_errs.append(abs(d))
            sq_errs.append(d * d)

    mae = sum(abs_errs) / len(abs_errs) if abs_errs else 0.0
    rmse = math.sqrt(sum(sq_errs) / len(sq_errs)) if sq_errs else 0.0

    return {
        "field": field,
        "total_claim_rows": total,
        "evaluated_pairs": evaluated,
        "mae": mae,
        "rmse": rmse,
        "missing_prediction": missing_pred,
        "missing_gold": missing_gold,
        "invalid_gold": invalid_gold,
        "invalid_prediction": invalid_pred,
    }


def _render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("=== Score model evaluation (continuous) ===")
    for field, block in report["fields"].items():
        lines.append("")
        lines.append(f"-- {field} --")
        lines.append(f"MAE:  {block['mae']:.4f}")
        lines.append(f"RMSE: {block['rmse']:.4f}")
        lines.append(f"pairs: {block['evaluated_pairs']} / claim rows seen: {block['total_claim_rows']}")
        lines.append(
            f"missing_pred: {block['missing_prediction']}  missing_gold: {block['missing_gold']}  "
            f"invalid_gold: {block['invalid_gold']}  invalid_pred: {block['invalid_prediction']}"
        )
    lines.append("")
    return "\n".join(lines)


def run_eval(*, input_path: Path, fields: list[str] | None, metrics_out: Path | None) -> dict[str, Any]:
    _, posts = load_posts_from_claims_json(input_path)
    want = fields or list(PRED_JSON_KEYS.keys())
    report_fields: dict[str, Any] = {}
    for f in want:
        if f not in PRED_JSON_KEYS:
            raise ValueError(f"Unknown field {f!r}")
        report_fields[f] = _evaluate_score_field(posts, f)

    n_succ_posts = sum(
        1
        for row in posts
        if isinstance(row, dict)
        and row.get("claim_extraction_status") == "success"
        and isinstance(row.get("claim_extraction_output"), dict)
    )
    n_claims_iter = sum(1 for _ in iter_success_claim_records(posts))

    report = {
        "input": str(input_path),
        "successful_posts approximate": n_succ_posts,
        "success_claim_records": n_claims_iter,
        "fields": report_fields,
    }

    print(_render_text(report), flush=True)

    if metrics_out is not None:
        metrics_out.parent.mkdir(parents=True, exist_ok=True)
        metrics_out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[eval_label_models] Wrote metrics JSON to {metrics_out}", flush=True)

    return report


def main() -> None:
    p = argparse.ArgumentParser(description="Evaluate pred_* vs gold scores (MAE/RMSE).")
    p.add_argument("--input", type=Path, required=True)
    p.add_argument(
        "--field",
        action="append",
        default=[],
        help="Repeatable; default all score fields",
    )
    p.add_argument("--metrics-out", type=Path, default=None)
    args = p.parse_args()
    fields = args.field if args.field else None
    run_eval(input_path=args.input, fields=fields, metrics_out=args.metrics_out)


if __name__ == "__main__":
    main()
