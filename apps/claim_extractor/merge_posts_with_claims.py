from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POSTS_INPUT = REPO_ROOT / "data" / "posts_for_term.json"
DEFAULT_CLAIMS_INPUT = REPO_ROOT / "data" / "posts_with_claims.json"
DEFAULT_OUT = REPO_ROOT / "data" / "posts_with_claims_full.json"


def _task_id_from_post_row(row: dict[str, Any], idx: int) -> str:
    src = row.get("source_post_id")
    chunk_idx = row.get("sentence_boundary_chunk_index")
    if src is not None and chunk_idx is not None:
        return f"{src}:{chunk_idx}"
    post_id = row.get("post_id", "unknown")
    return f"{post_id}:{idx}"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def run(posts_input: Path, claims_input: Path, out_path: Path) -> None:
    posts_payload = _load_json(posts_input)
    if not isinstance(posts_payload, dict):
        raise ValueError("posts_input must be a JSON object with top-level posts[].")
    posts = posts_payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("posts_input missing top-level posts[] list.")

    claims_payload = _load_json(claims_input)
    if not isinstance(claims_payload, list):
        raise ValueError("claims_input must be a task-level JSON list.")

    claims_by_task_id: dict[str, dict[str, Any]] = {}
    duplicate_claim_rows = 0
    for row in claims_payload:
        if not isinstance(row, dict):
            continue
        tid = str(row.get("task_id", "")).strip()
        if not tid:
            continue
        if tid in claims_by_task_id:
            duplicate_claim_rows += 1
        claims_by_task_id[tid] = row

    matched = 0
    unmatched_posts = 0
    merged_posts: list[dict[str, Any]] = []
    for idx, post in enumerate(posts):
        if not isinstance(post, dict):
            continue
        out_post = dict(post)
        task_id = _task_id_from_post_row(out_post, idx)
        out_post["task_id"] = task_id
        claim_row = claims_by_task_id.get(task_id)
        if claim_row is None:
            unmatched_posts += 1
            merged_posts.append(out_post)
            continue
        matched += 1
        output = claim_row.get("output")
        if isinstance(output, dict) and output.get("failed") is True:
            out_post["claim_extraction_status"] = "failed"
            out_post["claim_extraction_error"] = str(output.get("error") or "unknown error")
            out_post["claim_extraction_output"] = None
        elif isinstance(output, dict) and isinstance(output.get("claims"), list):
            out_post["claim_extraction_status"] = "success"
            out_post["claim_extraction_error"] = None
            out_post["claim_extraction_output"] = {"claims": output["claims"]}
        else:
            out_post["claim_extraction_status"] = "failed"
            out_post["claim_extraction_error"] = "RuntimeError: malformed legacy claim row output"
            out_post["claim_extraction_output"] = None
        merged_posts.append(out_post)

    unmatched_claim_rows = max(0, len(claims_by_task_id) - matched)
    out_payload = {k: v for k, v in posts_payload.items() if k != "posts"}
    out_payload["posts"] = merged_posts
    out_payload["post_count"] = len(merged_posts)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(out_path)

    print(f"[ok] wrote merged file -> {out_path.resolve()}")
    print(
        "[summary] "
        f"posts={len(merged_posts)} matched={matched} unmatched_posts={unmatched_posts} "
        f"unmatched_claim_rows={unmatched_claim_rows} duplicate_claim_rows={duplicate_claim_rows}"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.merge_posts_with_claims")
    ap.add_argument("--posts-input", type=Path, default=DEFAULT_POSTS_INPUT)
    ap.add_argument("--claims-input", type=Path, default=DEFAULT_CLAIMS_INPUT)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    run(args.posts_input, args.claims_input, args.out)
