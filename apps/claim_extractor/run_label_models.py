"""
Run per-label score baselines on extracted claims JSON.

Iterative loop (copy-paste):
  python -m apps.claim_extractor.run_label_models \\
    --input data/posts_with_claims_full.json \\
    --output data/tmp_label_preds.json \\
    --profile baseline --n-posts 200 --n-claims 500

  python -m apps.claim_extractor.eval_label_models \\
    --input data/tmp_label_preds.json

Gold columns on each claim (from extraction or manual labeling) are compared to ``pred_*`` fields.

Fields: ``claim_vaccine_alignment_score``, ``author_claim_agreement_score``,
``attribution_anecdote_score``, ``attribution_authority_score``,
``attribution_common_knowledge_score``.
"""

from __future__ import annotations

import argparse
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apps.claim_extractor import model_attribution
from apps.claim_extractor import model_author_stance
from apps.claim_extractor import model_claim_stance
from apps.claim_extractor.model_common import (
    ATTRIBUTION_SCORE_FIELDS,
    PRED_JSON_KEYS,
    SCORE_FIELD_NAMES,
    ClaimRecord,
    SinglePrediction,
    iter_success_claim_records,
    load_posts_from_claims_json,
    stable_task_id,
)

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "model_configs.json"

FIELD_ORDER: tuple[str, ...] = SCORE_FIELD_NAMES

FIELD_REGISTRY: dict[str, Any] = {
    "claim_vaccine_alignment_score": model_claim_stance,
    "author_claim_agreement_score": model_author_stance,
}


def _load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config must be a JSON object.")
    return raw


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def resolve_variant_params(
    cfg: dict[str, Any],
    *,
    profile: str,
    field: str,
    overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    profiles = cfg.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("model_configs.json missing 'profiles' object.")
    prof = profiles.get(profile)
    if not isinstance(prof, dict):
        raise KeyError(f"Unknown profile {profile!r}. Known: {list(profiles.keys())}")

    variant_key = prof.get(field)
    if not isinstance(variant_key, str):
        raise KeyError(f"Profile {profile!r} has no variant for field {field!r}")

    variants_root = cfg.get("variants")
    if not isinstance(variants_root, dict):
        raise ValueError("model_configs.json missing 'variants' object.")
    field_block = variants_root.get(field)
    if not isinstance(field_block, dict):
        raise KeyError(f"No variants block for field {field!r}")
    params = field_block.get(variant_key)
    if not isinstance(params, dict):
        raise KeyError(f"Unknown variant {variant_key!r} for field {field!r}")

    merged: dict[str, Any] = {"variant_name": variant_key, **params}
    if overrides:
        field_ov = overrides.get(field)
        if isinstance(field_ov, dict):
            merged = _deep_merge(merged, field_ov)
    return merged


def _collect_records(
    posts: list[dict[str, Any]],
    *,
    n_posts: int,
    n_claims: int,
) -> list[ClaimRecord]:
    mp = n_posts if n_posts > 0 else None
    mc = n_claims if n_claims > 0 else None
    return list(iter_success_claim_records(posts, max_posts=mp, max_claims=mc))


def _apply_prediction(
    claim: dict[str, Any],
    field_key: str,
    pred: SinglePrediction,
) -> None:
    pred_key = PRED_JSON_KEYS[field_key]
    claim[pred_key] = pred.value
    meta = claim.get("prediction_meta")
    if not isinstance(meta, dict):
        meta = {}
        claim["prediction_meta"] = meta
    meta[field_key] = {
        "pred_model_name": pred.pred_model_name,
        "pred_confidence": pred.confidence,
        "pred_reason": pred.reason,
        "coerced_from_invalid": pred.coerced_from_invalid,
    }


def run(
    *,
    input_path: Path,
    output_path: Path,
    config_path: Path,
    profile: str,
    fields: list[str] | None,
    n_posts: int,
    n_claims: int,
    overrides: dict[str, Any] | None,
) -> None:
    payload, posts = load_posts_from_claims_json(input_path)
    records = _collect_records(posts, n_posts=n_posts, n_claims=n_claims)
    if not records:
        print("[run_label_models] No successful extraction rows / claims to score.", flush=True)
        return

    cfg = _load_json(config_path)
    want_fields = list(fields) if fields else list(FIELD_ORDER)
    for f in want_fields:
        if f not in PRED_JSON_KEYS:
            raise KeyError(f"Unknown field {f!r}")

    coercion_counts: dict[str, int] = {k: 0 for k in want_fields}

    new_posts = copy.deepcopy(posts)

    by_task: dict[str, dict[str, Any]] = {}
    for row in new_posts:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("task_id") or "")
        if not tid:
            tid = stable_task_id(row)
            row["task_id"] = tid
        by_task[tid] = row

    attr_needed = [f for f in want_fields if f in ATTRIBUTION_SCORE_FIELDS]
    bundle: dict[str, list[SinglePrediction]] | None = None
    if attr_needed:
        first_attr = attr_needed[0]
        params = resolve_variant_params(cfg, profile=profile, field=first_attr, overrides=overrides)
        bundle = model_attribution.predict_bundle(records, params)

    for field_key in want_fields:
        if field_key in ATTRIBUTION_SCORE_FIELDS:
            assert bundle is not None
            preds = bundle[field_key]
        else:
            mod = FIELD_REGISTRY[field_key]
            params = resolve_variant_params(cfg, profile=profile, field=field_key, overrides=overrides)
            preds = mod.predict(records, params)

        if len(preds) != len(records):
            raise RuntimeError(f"Predictor for {field_key} returned {len(preds)} preds for {len(records)} records")

        for rec, pred in zip(records, preds):
            row = by_task.get(rec.task_id)
            if row is None:
                continue
            out = row.get("claim_extraction_output")
            if not isinstance(out, dict):
                continue
            clist = out.get("claims")
            if not isinstance(clist, list) or rec.claim_index >= len(clist):
                continue
            claim = clist[rec.claim_index]
            if not isinstance(claim, dict):
                continue
            _apply_prediction(claim, field_key, pred)
            if pred.coerced_from_invalid:
                coercion_counts[field_key] = coercion_counts.get(field_key, 0) + 1

    run_meta = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "config_path": str(config_path),
        "input_path": str(input_path),
        "fields": want_fields,
        "records_scored": len(records),
        "n_posts_limit": n_posts,
        "n_claims_limit": n_claims,
        "coercion_counts": coercion_counts,
    }

    out_payload = {k: v for k, v in payload.items() if k != "posts"}
    out_payload["posts"] = new_posts
    out_payload["label_model_run"] = run_meta

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(output_path)
    print(
        f"[run_label_models] Wrote {len(records)} claim-level predictions to {output_path}",
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Apply per-label baseline score models to extracted claims.")
    p.add_argument("--input", type=Path, required=True, help="posts_with_claims_full-style JSON")
    p.add_argument("--output", type=Path, required=True, help="Output JSON path (atomic write)")
    p.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="model_configs.json")
    p.add_argument("--profile", type=str, default="baseline", help="Profile name inside config")
    p.add_argument(
        "--fields",
        type=str,
        default="",
        help="Comma-separated subset of score field names (default: all five)",
    )
    p.add_argument("--n-posts", type=int, default=0, help="Max successful posts (0 = no limit)")
    p.add_argument("--n-claims", type=int, default=0, help="Max claims total (0 = no limit)")
    p.add_argument(
        "--overrides-json",
        type=str,
        default="",
        help="JSON object merged into variant params per field key",
    )
    args = p.parse_args()

    overrides: dict[str, Any] | None = None
    if args.overrides_json.strip():
        parsed = json.loads(args.overrides_json)
        if not isinstance(parsed, dict):
            raise SystemExit("--overrides-json must be a JSON object")
        overrides = parsed

    fields_list: list[str] | None = None
    if args.fields.strip():
        fields_list = [x.strip() for x in args.fields.split(",") if x.strip()]

    run(
        input_path=args.input,
        output_path=args.output,
        config_path=args.config,
        profile=args.profile,
        fields=fields_list,
        n_posts=args.n_posts,
        n_claims=args.n_claims,
        overrides=overrides,
    )


if __name__ == "__main__":
    main()
