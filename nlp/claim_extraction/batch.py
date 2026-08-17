from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterator, Optional

from openai._exceptions import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError

from nlp.claim_extraction.api_requester import (
    AzureClaimsClient,
    ConcurrentApiRequester,
    RequestResult,
    RequestStatus,
    RequestTask,
    RetryPolicy,
    ThrottlePolicy,
    classify_error_text,
    default_is_retryable_exception,
)
from nlp.claim_extraction.clients import build_azure_claims_client, load_azure_config
from nlp.claim_extraction.defaults import (
    DEFAULT_429_COOLDOWN_S,
    DEFAULT_BATCH_COUNT,
    DEFAULT_MAX_CLAIMS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_WORKERS,
    DEFAULT_TARGET_RPM,
    MODEL_NAME,
)
from nlp.claim_extraction.prompts import load_system_template, load_user_template, render_system, render_user
from nlp.claim_extraction.schema import (
    CLAIMS_ALIGNMENT_JSON_SCHEMA,
    CLAIMS_JSON_SCHEMA,
    CLAIMS_ONLY_JSON_SCHEMA,
    parse_claims_alignment_output,
    parse_claims_only_output,
    parse_claims_with_scores_output,
)
from nlp.claim_extraction.text import format_input_text

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_FILE = REPO_ROOT / "data" / "posts_for_term.json"
DEFAULT_OUT_FILE = REPO_ROOT / "data" / "posts_with_claims_full.json"
DEFAULT_MAX_TASKS = 0
DEFAULT_N_POSTS = 0

# Legacy scored prompts (with-scores path); claims-only uses render_system/render_user.
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_PROMPT = (PROMPTS_DIR / "extract_system_scored.txt").read_text(encoding="utf-8-sig")
USER_PROMPT = (PROMPTS_DIR / "extract_user_scored.txt").read_text(encoding="utf-8-sig")
# Eager-load canonical claims-only templates (validates required placeholders).
load_system_template()
load_user_template()


class PostsJsonStreamWriter:
    def __init__(self, final_path: Path, *, meta: dict[str, Any]) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self.final_path = final_path
        self.tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        self._f = self.tmp_path.open("w", encoding="utf-8")
        self._first_post = True
        self.written_posts = 0

        header = {k: v for k, v in meta.items() if k != "posts"}
        self._f.write("{\n")
        for k, v in header.items():
            self._f.write(f'  "{k}": {json.dumps(v, ensure_ascii=False)},\n')
        self._f.write('  "posts": [\n')

    def write_post(self, post: dict[str, Any]) -> None:
        if not self._first_post:
            self._f.write(",\n")
        self._first_post = False
        blob = json.dumps(post, ensure_ascii=False, indent=2)
        self._f.write("\n".join("    " + line for line in blob.splitlines()))
        self.written_posts += 1
        self._f.flush()

    def finalize(self) -> None:
        self._f.write("\n  ],\n")
        self._f.write(f'  "post_count": {self.written_posts}\n')
        self._f.write("}\n")
        self._f.flush()
        self._f.close()
        self.tmp_path.replace(self.final_path)


def _build_client(*, claims_only: bool = False, alignment: bool = False) -> AzureClaimsClient:
    cfg = load_azure_config()
    if alignment:
        schema = CLAIMS_ALIGNMENT_JSON_SCHEMA
        parser = parse_claims_alignment_output
    elif claims_only:
        schema = CLAIMS_ONLY_JSON_SCHEMA
        parser = parse_claims_only_output
    else:
        schema = CLAIMS_JSON_SCHEMA
        parser = parse_claims_with_scores_output
    return build_azure_claims_client(
        model=MODEL_NAME,
        api_key=cfg.key,
        azure_endpoint=cfg.endpoint,
        api_version=cfg.api_version,
        claims_only=claims_only and not alignment,
        response_schema=schema,
        output_parser=parser,
        system_prompt_builder=lambda payload: _build_system_prompt(
            max_claims=int(payload["max_claims"]),
            claims_only=bool(payload.get("claims_only")),
            alignment=bool(payload.get("alignment")),
        ),
        user_prompt_builder=lambda payload: _build_user_prompt(
            str(payload["input_text"]),
            max_claims=int(payload["max_claims"]),
            claims_only=bool(payload.get("claims_only")),
            alignment=bool(payload.get("alignment")),
        ),
    )


def _build_system_prompt(
    *,
    max_claims: int,
    claims_only: bool = False,
    alignment: bool = False,
) -> str:
    if claims_only or alignment:
        return render_system(max_claims=max_claims)
    return SYSTEM_PROMPT.replace("{{max_claims}}", str(max_claims)).replace(
        "[[max_claims]]", str(max_claims)
    )


def _build_user_prompt(
    input_text: str,
    *,
    max_claims: int,
    claims_only: bool = False,
    alignment: bool = False,
) -> str:
    if claims_only or alignment:
        return render_user(input_text, max_claims=max_claims)
    return (
        USER_PROMPT.replace("{{max_claims}}", str(max_claims))
        .replace("[[max_claims]]", str(max_claims))
        .replace("{{text_input}}", input_text)
    )


def _stable_task_id(row: dict[str, Any]) -> str:
    src = row.get("source_post_id")
    idx = row.get("sentence_boundary_chunk_index")
    if src is not None and idx is not None:
        return f"{src}:{idx}"
    post_id = row.get("post_id", "unknown")
    text = str(row.get("text_coreference_resolved") or row.get("text") or "")
    digest = hashlib.sha256(f"{post_id}|{text}".encode("utf-8")).hexdigest()[:16]
    return f"{post_id}:{digest}"


def _normalize_row_state(row: dict[str, Any]) -> tuple[str, Optional[str]]:
    disposition = row.get("claim_extraction_disposition")
    if disposition == "success":
        return "completed", None
    if disposition == "terminal_failure":
        err = str(row.get("claim_extraction_error") or "")
        return "terminal_failed", err
    if disposition == "retryable_failure":
        err = str(row.get("claim_extraction_error") or "")
        return "retryable_failed", err

    status = row.get("claim_extraction_status")
    if status == "success":
        out = row.get("claim_extraction_output")
        if isinstance(out, dict) and isinstance(out.get("claims"), list):
            row["claim_extraction_disposition"] = "success"
            return "completed", None
    if status == "failed":
        err = str(row.get("claim_extraction_error") or "")
        if classify_error_text(err) == RequestStatus.FAILED_TERMINAL:
            row["claim_extraction_disposition"] = "terminal_failure"
            return "terminal_failed", err
        row["claim_extraction_disposition"] = "retryable_failure"
        return "retryable_failed", err

    row["claim_extraction_disposition"] = "unprocessed"
    return "unprocessed", None


def _is_retryable_exception(exc: BaseException) -> bool:
    return default_is_retryable_exception(exc)


def _apply_request_result(row: dict[str, Any], result: RequestResult) -> dict[str, Any]:
    row["task_id"] = result.task_id
    if result.status == RequestStatus.SUCCESS:
        row["claim_extraction_status"] = "success"
        row["claim_extraction_error"] = None
        row["claim_extraction_output"] = result.output
        row["claim_extraction_disposition"] = "success"
        return row

    row["claim_extraction_status"] = "failed"
    row["claim_extraction_error"] = result.error
    row["claim_extraction_output"] = None
    if result.status == RequestStatus.FAILED_RETRYABLE:
        row["claim_extraction_disposition"] = "retryable_failure"
    else:
        row["claim_extraction_disposition"] = "terminal_failure"
    return row


def _build_tasks(posts: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    skipped: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for row in posts:
        if not isinstance(row, dict):
            continue
        task_id = _stable_task_id(row)
        row["task_id"] = task_id
        text = row.get("text_coreference_resolved")
        if not isinstance(text, str) or not text.strip():
            text = row.get("text")
        if not isinstance(text, str) or not text.strip():
            row["claim_extraction_status"] = "failed"
            row["claim_extraction_error"] = "RuntimeError: missing text input"
            row["claim_extraction_output"] = None
            row["claim_extraction_disposition"] = "terminal_failure"
            skipped.append(row)
            continue
        state, _ = _normalize_row_state(row)
        if state in ("completed", "terminal_failed"):
            skipped.append(row)
            continue
        pending.append(
            {
                "task_id": task_id,
                "input_text": format_input_text(row, text),
                "row": row,
            }
        )
    return skipped, pending


def _load_existing_output_rows(path: Path) -> tuple[set[str], list[dict[str, Any]]]:
    if not path.exists():
        return set(), []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return set(), []
    posts = payload.get("posts")
    if not isinstance(posts, list):
        return set(), []
    rows = [p for p in posts if isinstance(p, dict)]
    task_ids: set[str] = set()
    for row in rows:
        task_id = str(row.get("task_id") or _stable_task_id(row))
        row["task_id"] = task_id
        task_ids.add(task_id)
    return task_ids, rows


def _load_payload(input_file: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = json.loads(input_file.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object with top-level 'posts'.")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Input JSON must have top-level 'posts' list.")
    rows = [p for p in posts if isinstance(p, dict)]
    return payload, rows


def batched(seq: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    step = max(1, int(size))
    for i in range(0, len(seq), step):
        yield seq[i : i + step]


def run(
    *,
    input_file: Path,
    out_file: Path,
    batch_count: int,
    max_workers: int,
    max_claims: int,
    max_retries: int,
    max_tasks: int,
    n_posts: int,
    claims_only: bool = False,
    alignment: bool = False,
) -> None:
    if claims_only and alignment:
        raise ValueError("Use only one of claims_only or alignment.")
    payload, rows = _load_payload(input_file)
    existing_ids, existing_rows = _load_existing_output_rows(out_file)
    print(
        f"[resume] {len(existing_ids)} existing rows found in output; removing from input pool",
        flush=True,
    )

    total_input_rows = len(rows)
    filtered_input: list[dict[str, Any]] = []
    removed_by_existing = 0
    for row in rows:
        task_id = str(row.get("task_id") or _stable_task_id(row))
        row["task_id"] = task_id
        if task_id in existing_ids:
            removed_by_existing += 1
            continue
        filtered_input.append(row)

    skipped_rows, pending_tasks = _build_tasks(filtered_input)
    pending_before_limit = len(pending_tasks)
    limit = max(0, int(n_posts))
    if limit == 0:
        limit = max(0, int(max_tasks))
    if limit > 0:
        pending_tasks = pending_tasks[:limit]

    print(
        "[startup] "
        f"total_input_rows={total_input_rows} "
        f"removed_due_to_existing={removed_by_existing} "
        f"existing_rows_kept={len(existing_rows)} "
        f"skipped_pre_dump={len(skipped_rows)} "
        f"pending_before_limit={pending_before_limit} "
        f"pending_after_limit={len(pending_tasks)}",
        flush=True,
    )
    meta = {k: v for k, v in payload.items() if k != "posts"}
    if alignment:
        meta["claims_extraction_mode"] = "alignment"
    elif claims_only:
        meta["claims_extraction_mode"] = "claims_only"
    writer = PostsJsonStreamWriter(out_file, meta=meta)

    # Requirement: filtered rows are dumped first before prompting starts.
    for row in existing_rows:
        writer.write_post(row)
    for row in skipped_rows:
        writer.write_post(row)
    print(
        f"[filter] pre-dumped {len(existing_rows) + len(skipped_rows)} rows to {out_file.resolve()}",
        flush=True,
    )

    client = _build_client(claims_only=claims_only, alignment=alignment)
    throttle = ThrottlePolicy(
        target_requests_per_minute=max(1, int(os.getenv("CLAIMS_TARGET_RPM", str(DEFAULT_TARGET_RPM)))),
        global_429_cooldown_s=max(0.0, float(os.getenv("CLAIMS_429_COOLDOWN_S", str(DEFAULT_429_COOLDOWN_S)))),
    )
    retry_policy = RetryPolicy(max_retries=max_retries)
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=max_workers,
        retry_policy=retry_policy,
        throttle_policy=throttle,
        is_retryable=_is_retryable_exception,
        on_log=lambda msg: print(msg, flush=True),
    )

    completed = 0
    for batch in batched(pending_tasks, batch_count):
        request_tasks = [
            RequestTask(
                task_id=t["task_id"],
                payload={
                    "input_text": t["input_text"],
                    "max_claims": max_claims,
                    "claims_only": claims_only,
                    "alignment": alignment,
                },
            )
            for t in batch
        ]
        row_by_task_id = {str(t["task_id"]): t["row"] for t in batch}
        for result in requester.run(request_tasks):
            row = row_by_task_id.get(result.task_id)
            if row is None:
                continue
            writer.write_post(_apply_request_result(row, result))
            completed += 1
        print(f"[progress] extracted {completed}/{len(pending_tasks)} pending rows", flush=True)
    writer.finalize()
    print(f"[ok] wrote {writer.written_posts} total rows -> {out_file.resolve()}", flush=True)



def run_on_posts(
    *,
    posts_path: Path,
    out_path: Path,
    n_posts: int = 0,
    claims_only: bool = False,
    alignment: bool = False,
    batch_count: int = DEFAULT_BATCH_COUNT,
    max_workers: int = DEFAULT_MAX_WORKERS,
    max_claims: int = DEFAULT_MAX_CLAIMS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> None:
    """Convenience wrapper used by scripts.get_posts_extract_upload."""
    run(
        input_file=posts_path,
        out_file=out_path,
        batch_count=batch_count,
        max_workers=max_workers,
        max_claims=max_claims,
        max_retries=max_retries,
        max_tasks=0,
        n_posts=n_posts,
        claims_only=claims_only,
        alignment=alignment,
    )



if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="python -m nlp.claim_extraction.batch")
    ap.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_FILE)
    ap.add_argument("--out-file", type=Path, default=DEFAULT_OUT_FILE)
    ap.add_argument("--batch-count", type=int, default=DEFAULT_BATCH_COUNT)
    ap.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    ap.add_argument("--max-claims", type=int, default=DEFAULT_MAX_CLAIMS)
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    ap.add_argument(
        "--max-tasks",
        type=int,
        default=DEFAULT_MAX_TASKS,
        help="Deprecated alias for --n-posts (0 means unlimited).",
    )
    ap.add_argument("--n-posts", type=int, default=DEFAULT_N_POSTS, help="Process at most N pending rows.")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument(
        "--claims-only",
        action="store_true",
        help="Extract claim text only (no LLM scores). Run score_claims post-pass for Ridge pred_* fields.",
    )
    mode.add_argument(
        "--alignment",
        action="store_true",
        help="Extract claims plus discrete claim_vaccine_alignment_score (canonical extract_*.txt).",
    )
    mode.add_argument(
        "--with-scores",
        action="store_true",
        help="Extract claims plus all five score fields (default).",
    )
    args = ap.parse_args()
    run(
        input_file=args.input_file,
        out_file=args.out_file,
        batch_count=max(1, int(args.batch_count)),
        max_workers=max(1, int(args.max_workers)),
        max_claims=max(1, int(args.max_claims)),
        max_retries=max(1, int(args.max_retries)),
        max_tasks=max(0, int(args.max_tasks)),
        n_posts=max(0, int(args.n_posts)),
        claims_only=bool(args.claims_only),
        alignment=bool(args.alignment),
    )
