from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterator, Optional

from dotenv import load_dotenv
from openai._exceptions import APIConnectionError, APIStatusError, APITimeoutError, RateLimitError

from apps.claim_extractor.api_requester import (
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
from apps.claim_extractor.model_common import SCORE_FIELD_NAMES, parse_score_01

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_FILE = REPO_ROOT / "data" / "posts_for_term.json"
DEFAULT_OUT_FILE = REPO_ROOT / "data" / "posts_with_claims_full.json"
DEFAULT_BATCH_COUNT = 100
DEFAULT_MAX_WORKERS = 6
DEFAULT_MAX_CLAIMS = 8
DEFAULT_MAX_RETRIES = 3
DEFAULT_MAX_TASKS = 0
DEFAULT_N_POSTS = 0
DEFAULT_TARGET_RPM = 90
DEFAULT_429_COOLDOWN_S = 20.0

load_dotenv()

MODEL_NAME = "gpt-5.4-mini"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_PROMPT = (PROMPTS_DIR / "extract_system.txt").read_text(encoding="utf-8-sig")
USER_PROMPT = (PROMPTS_DIR / "extract_user.txt").read_text(encoding="utf-8-sig")

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

_SCORE_PROPS: dict[str, Any] = {
    name: {"type": "number", "minimum": 0.0, "maximum": 1.0} for name in SCORE_FIELD_NAMES
}

CLAIMS_JSON_SCHEMA: dict[str, Any] = {
    "name": "vaccine_claim_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "claim": {"type": "string"},
                        **_SCORE_PROPS,
                    },
                    "required": ["claim", *SCORE_FIELD_NAMES],
                },
            }
        },
        "required": ["claims"],
    },
}

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


def _build_client() -> AzureClaimsClient:
    if not AZURE_OPENAI_KEY:
        raise RuntimeError("Missing AZURE_OPENAI_KEY in environment.")
    if not AZURE_OPENAI_ENDPOINT:
        raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT in environment.")
    return AzureClaimsClient(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
        model=MODEL_NAME,
        system_prompt_builder=lambda payload: _build_system_prompt(max_claims=int(payload["max_claims"])),
        user_prompt_builder=lambda payload: _build_user_prompt(
            str(payload["input_text"]), max_claims=int(payload["max_claims"])
        ),
        response_schema=CLAIMS_JSON_SCHEMA,
        output_parser=_parse_and_validate_output,
    )


def _build_system_prompt(*, max_claims: int) -> str:
    return SYSTEM_PROMPT.replace("{{max_claims}}", str(max_claims)).replace("[[max_claims]]", str(max_claims))


def _build_user_prompt(input_text: str, *, max_claims: int) -> str:
    return (
        USER_PROMPT.replace("{{max_claims}}", str(max_claims))
        .replace("[[max_claims]]", str(max_claims))
        .replace("{{text_input}}", input_text)
    )


def _parse_and_validate_output(content: str) -> dict[str, Any]:
    parsed = json.loads(content.strip())
    if not isinstance(parsed, dict):
        raise ValueError("model output JSON top-level is not an object")
    claims = parsed.get("claims")
    if not isinstance(claims, list):
        raise ValueError("model output missing list field 'claims'")
    for i, c in enumerate(claims):
        if not isinstance(c, dict):
            raise ValueError(f"claims[{i}] is not an object")
        if not isinstance(c.get("claim"), str):
            raise ValueError(f"claims[{i}].claim must be a string")
        for key in SCORE_FIELD_NAMES:
            v, bad = parse_score_01(c.get(key))
            if v is None or bad:
                raise ValueError(f"claims[{i}].{key} must be a number in [0, 1]")
    return parsed


def _stable_task_id(row: dict[str, Any]) -> str:
    src = row.get("source_post_id")
    idx = row.get("sentence_boundary_chunk_index")
    if src is not None and idx is not None:
        return f"{src}:{idx}"
    post_id = row.get("post_id", "unknown")
    text = str(row.get("text_coreference_resolved") or row.get("text") or "")
    digest = hashlib.sha256(f"{post_id}|{text}".encode("utf-8")).hexdigest()[:16]
    return f"{post_id}:{digest}"


def _format_input_text(row: dict[str, Any], text: str) -> str:
    platform = str(row.get("platform", "unknown"))
    if platform == "reddit_submission":
        return f"Submission title: {row.get('reddit_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "reddit_comment":
        return f"Reddit comment context title: {row.get('reddit_comment_submission_title') or 'Unknown'}\n\n{text}"
    if platform == "youtube_video":
        return f"YouTube video title: {row.get('youtube_video_title') or 'Unknown'}\n\n{text}"
    if platform == "podcast_episode":
        return f"Podcast name: {row.get('podcast_name') or 'Unknown'}\n\n{text}"
    return text


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
                "input_text": _format_input_text(row, text),
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
) -> None:
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

    client = _build_client()
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
                payload={"input_text": t["input_text"], "max_claims": max_claims},
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


if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.get_claims")
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
    )
