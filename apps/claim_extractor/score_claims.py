"""
Batch-score claims with trained Ridge heads and optionally benchmark vs LLM + manual labels.

  python -m apps.claim_extractor.score_claims \\
    --input data/posts_with_claims_full.json \\
    --out data/posts_with_claims_scored.json \\
    --lab-db apps/claim_extractor/labeler_lab/data/lab.sqlite

  python -m apps.claim_extractor.score_claims \\
    --input data/posts_with_claims_full.json \\
    --benchmark \\
    --lab-db apps/claim_extractor/labeler_lab/data/lab.sqlite
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

from apps.claim_extractor.labeler_lab import claims_data, db, eval_metrics, field_inputs
from apps.claim_extractor.learned.predict import FieldPredictor
from apps.claim_extractor.learned.train import resolve_out_dir
from apps.claim_extractor.model_common import (
    PRED_JSON_KEYS,
    SCORE_FIELD_NAMES,
    load_posts_from_claims_json,
    parse_score_01,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = REPO_ROOT / "data" / "posts_with_claims_full.json"
DEFAULT_OUT = REPO_ROOT / "data" / "posts_with_claims_scored.json"
DEFAULT_LAB_DB = Path(__file__).resolve().parent / "labeler_lab" / "data" / "lab.sqlite"


def _resolve_artifact_map(
    conn: sqlite3.Connection | None,
    *,
    artifact_dirs: dict[str, Path] | None,
) -> dict[str, Path]:
    """Map score_field_name -> artifact directory."""
    out: dict[str, Path] = {}
    if artifact_dirs:
        out.update(artifact_dirs)
    if conn is not None:
        for head in db.list_heads(conn):
            if head.score_field_name and head.artifact_dir:
                out.setdefault(head.score_field_name, resolve_out_dir(Path(head.artifact_dir)))
    return out


def _load_predictors(artifact_map: dict[str, Path], *, batch_size: int) -> dict[str, FieldPredictor]:
    predictors: dict[str, FieldPredictor] = {}
    for field_name, art_dir in artifact_map.items():
        if field_name not in SCORE_FIELD_NAMES:
            continue
        if not art_dir.is_dir():
            raise FileNotFoundError(f"Artifact dir for {field_name!r} not found: {art_dir}")
        predictors[field_name] = FieldPredictor.load(art_dir, batch_size=batch_size)
    return predictors


def score_posts(
    posts: list[dict[str, Any]],
    predictors: dict[str, FieldPredictor],
) -> int:
    """Write pred_{field} on each claim. Returns number of claims scored."""
    n_scored = 0
    for row in posts:
        if row.get("claim_extraction_status") != "success":
            continue
        outd = row.get("claim_extraction_output")
        if not isinstance(outd, dict):
            continue
        claims = outd.get("claims")
        if not isinstance(claims, list):
            continue
        tid = str(row.get("task_id") or "")
        for i, claim_dict in enumerate(claims):
            if not isinstance(claim_dict, dict):
                continue
            for field_name, predictor in predictors.items():
                pred_key = PRED_JSON_KEYS[field_name]
                txt = field_inputs.build_input_for_head(
                    score_field_name=field_name,
                    input_var_keys=[],
                    post_row=row,
                    claim_dict=claim_dict,
                    claim_index=i,
                    task_id=tid or None,
                )
                claim_dict[pred_key] = predictor.predict_scores([txt])[0]
            n_scored += 1
    return n_scored


def run_benchmark(
    posts: list[dict[str, Any]],
    conn: sqlite3.Connection,
    predictors: dict[str, FieldPredictor],
    *,
    batch_size: int,
) -> dict[str, Any]:
    """Aggregate Ridge vs LLM vs manual gold per standard score field."""
    results: dict[str, Any] = {}
    for field_name in SCORE_FIELD_NAMES:
        head = db.get_head_by_name(conn, field_name)
        if head is None:
            results[field_name] = {"error": "no standard head in lab DB"}
            continue
        eval_rows = db.fetch_labels_xy(conn, head.id, "eval")
        if not eval_rows:
            results[field_name] = {"error": "no eval labels"}
            continue
        if field_name not in predictors:
            if head.artifact_dir:
                art = resolve_out_dir(Path(head.artifact_dir))
                predictors[field_name] = FieldPredictor.load(art, batch_size=batch_size)
            else:
                results[field_name] = {"error": "head not trained (no artifact_dir)"}
                continue

        texts, ys = claims_data.build_xy_for_labels(
            posts,
            eval_rows,
            input_var_keys=head.input_var_keys,
            score_field_name=head.score_field_name,
        )
        if not texts:
            results[field_name] = {"error": "could not join eval labels to posts"}
            continue

        y_hat = predictors[field_name].predict_scores(texts)
        idx = claims_data.index_claims_by_key(posts)
        y_llm: list[float | None] = []
        for tid, cidx, _y in eval_rows:
            llm_v: float | None = None
            hit = idx.get((tid, cidx))
            if hit is not None:
                _post, claim_dict = hit
                parsed, bad = parse_score_01(claim_dict.get(field_name))
                if parsed is not None and not bad:
                    llm_v = parsed
            y_llm.append(llm_v)

        results[field_name] = eval_metrics.compare_to_llm_baseline(ys, y_hat, y_llm)
    return results


def _parse_artifact_arg(raw: str) -> tuple[str, Path]:
    if "=" not in raw:
        raise ValueError(f"Expected field=path, got {raw!r}")
    field, path = raw.split("=", 1)
    field = field.strip()
    if field not in SCORE_FIELD_NAMES:
        raise ValueError(f"Unknown score field {field!r}")
    return field, Path(path.strip())


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.score_claims")
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSON (omit with --benchmark-only)")
    ap.add_argument("--lab-db", type=Path, default=DEFAULT_LAB_DB, help="Labeler SQLite for head artifacts")
    ap.add_argument(
        "--artifact",
        action="append",
        default=[],
        metavar="FIELD=DIR",
        help="Explicit artifact dir for a score field (repeatable)",
    )
    ap.add_argument("--benchmark", action="store_true", help="Print Ridge vs LLM vs manual eval metrics")
    ap.add_argument(
        "--benchmark-only",
        action="store_true",
        help="Run --benchmark without writing scored output",
    )
    ap.add_argument("--batch-size", type=int, default=32)
    args = ap.parse_args()

    if not args.input.is_file():
        raise SystemExit(f"Input not found: {args.input}")

    payload, posts = load_posts_from_claims_json(args.input)

    explicit: dict[str, Path] = {}
    for item in args.artifact:
        field, path = _parse_artifact_arg(item)
        explicit[field] = resolve_out_dir(path)

    conn = None
    if args.lab_db.is_file() or args.benchmark or args.benchmark_only:
        conn = db.connect(args.lab_db)
        db.init_schema(conn)

    artifact_map = _resolve_artifact_map(conn, artifact_dirs=explicit or None)
    if not artifact_map and not args.benchmark and not args.benchmark_only:
        raise SystemExit("No artifact dirs found. Train heads in the lab or pass --artifact FIELD=DIR.")

    predictors = _load_predictors(artifact_map, batch_size=max(1, args.batch_size)) if artifact_map else {}

    if args.benchmark or args.benchmark_only:
        if conn is None:
            raise SystemExit("--benchmark requires --lab-db")
        bench = run_benchmark(posts, conn, predictors, batch_size=max(1, args.batch_size))
        print(json.dumps(bench, indent=2, ensure_ascii=False))
        for field_name, res in bench.items():
            if "error" in res:
                print(f"[{field_name}] {res['error']}")
                continue
            ridge = res.get("ridge_vs_manual", {})
            llm = res.get("llm_vs_manual", {})
            beats = res.get("beats_llm")
            print(
                f"[{field_name}] ridge_mae={ridge.get('mae')} llm_mae={llm.get('mae')} "
                f"beats_llm={beats} n_eval={ridge.get('n')}"
            )

    if not args.benchmark_only:
        if not predictors:
            raise SystemExit("No predictors loaded; cannot write scored output.")
        n = score_posts(posts, predictors)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        out_payload = {k: v for k, v in payload.items() if k != "posts"}
        out_payload["posts"] = posts
        out_payload["post_count"] = len(posts)
        out_payload["score_claims_fields"] = sorted(predictors.keys())
        args.out.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[ok] scored {n} claims across {len(predictors)} field(s) -> {args.out.resolve()}")

    if conn is not None:
        conn.close()


if __name__ == "__main__":
    main()
