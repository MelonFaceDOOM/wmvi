from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Callable, Iterator

from apps.claim_extractor.vaccine_claim_extractor import extract_vaccine_claims

"""
Batch claim extraction over precomputed post contexts.

Examples:
  python -m apps.claim_extractor.get_claims
  python -m apps.claim_extractor.get_claims --input-file data/mmr.json --out-file data/mmr_claims.jsonl
  python -m apps.claim_extractor.get_claims --batch-count 50 --max-workers 6 --max-claims 8
"""

DEFAULT_INPUT_FILE = Path("data/mmr.json")
DEFAULT_OUT_FILE = Path("data/mmr_claims.jsonl")
DEFAULT_BATCH_COUNT = 50
DEFAULT_MAX_WORKERS = 6
DEFAULT_MAX_CLAIMS = 4
DEFAULT_MAX_TASKS = 0

LOG = logging.getLogger(__name__)


def batched(seq: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    size = max(1, int(size))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def stable_task_id(post_id: Any, text: str) -> str:
    base = f"{post_id}|{text}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]
    return f"{post_id}:{digest}"


def format_input_text(post: dict[str, Any], text: str) -> str:
    platform = str(post.get("platform", "unknown"))

    if platform == "reddit_submission":
        title = str(post.get("reddit_submission_title") or post.get("title") or "Unknown title")
        return (
            f"This is a segment from a Reddit Submission with the title {title}. "
            f"Here is the Reddit Submission segment:\n{text}"
        )

    if platform == "reddit_comment":
        submission_title = str(
            post.get("reddit_comment_submission_title")
            or post.get("submission_title")
            or "Unknown submission title"
        )
        return (
            "This is a segment from a Reddit Comment that was made in a Submission "
            f"titled {submission_title}. Here is the Reddit Comment segment:\n{text}"
        )

    if platform == "telegram_post":
        telegram_channel = str(post.get("telegram_channel") or "Unknown channel")
        return (
            "This is a segment from a Telegram message that was made in a channel "
            f"called {telegram_channel}. Here is the Telegram message segment:\n{text}"
        )

    if platform == "youtube_video":
        title = str(post.get("youtube_video_title") or post.get("title") or "Unknown title")
        return (
            "This is a segment from a transcript of a YouTube video with the title "
            f"{title}. Here is the YouTube Video transcript segment:\n{text}"
        )

    if platform == "podcast_episode":
        podcast_name = str(post.get("podcast_name") or "Unknown podcast")
        return (
            "This is a segment from a transcript of a podcast called "
            f"{podcast_name}. Here is the Podcast transcript segment:\n{text}"
        )

    return f"text from {platform}: {text}"


def build_context_tasks(
    batch_posts: list[dict[str, Any]],
) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]]]:
    tasks: list[dict[str, str]] = []
    ctx_by_task_id: dict[str, dict[str, Any]] = {}
    for post in batch_posts:
        if not isinstance(post, dict):
            LOG.warning("Skipping non-dict post in batch.")
            continue
        post_id = post.get("post_id", "unknown")
        contexts = post.get("contexts", [])
        if not isinstance(contexts, list):
            raise ValueError(f"post_id={post_id} has non-list contexts.")
        for ctx in contexts:
            if not isinstance(ctx, dict):
                LOG.warning("post_id=%s has non-dict context; skipping.", post_id)
                continue
            if "text" not in ctx:
                raise ValueError(f"post_id={post_id} context missing required key 'text'.")
            text = str(ctx["text"])
            task_id = stable_task_id(post_id, text)
            tasks.append(
                {
                    "task_id": task_id,
                    "input_text": format_input_text(post, text),
                }
            )
            if task_id in ctx_by_task_id:
                LOG.warning("Duplicate task_id detected for post_id=%s task_id=%s", post_id, task_id)
            ctx_by_task_id[task_id] = ctx
    return tasks, ctx_by_task_id


def merge_outputs_into_posts(
    ctx_by_task_id: dict[str, dict[str, Any]],
    claim_outputs: list[dict[str, Any]],
) -> None:
    unmatched_outputs = 0
    for row in claim_outputs:
        if not isinstance(row, dict):
            LOG.warning("Skipping non-dict extractor row.")
            continue
        task_id = str(row.get("task_id", "")).strip()
        if not task_id:
            LOG.warning("Extractor row missing task_id: %s", row)
            continue
        ctx = ctx_by_task_id.get(task_id)
        if ctx is None:
            unmatched_outputs += 1
            LOG.warning("No matching context found for task_id=%s", task_id)
            continue
        if "output" not in row:
            LOG.warning("Extractor row missing output for task_id=%s", task_id)
            continue
        if isinstance(row["output"], dict):
            ctx["output"] = row["output"]
        else:
            LOG.warning("Extractor row has non-dict output for task_id=%s", task_id)
            ctx["output"] = {
                "parse_error": "non-dict output",
                "raw_output": row["output"],
            }
    if unmatched_outputs:
        LOG.warning("Unmatched extractor outputs in batch: %d", unmatched_outputs)

    # Traceability warning for contexts that did not receive output.
    missing_outputs = [tid for tid, ctx in ctx_by_task_id.items() if "output" not in ctx]

    if missing_outputs:
        if len(missing_outputs) == len(ctx_by_task_id):
            LOG.warning("No outputs produced for entire batch.")
        else:
            LOG.debug("Contexts missing outputs in batch: %d", len(missing_outputs))


def append_posts_jsonl(path: Path, posts: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post, ensure_ascii=False) + "\n")


def load_completed_task_ids(path: Path) -> set[str]:
    out: set[str] = set()
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                post = json.loads(s)
            except json.JSONDecodeError:
                LOG.warning("Skipping invalid JSONL line %d in %s", line_no, path)
                continue
            if not isinstance(post, dict):
                continue
            post_id = post.get("post_id", "unknown")
            contexts = post.get("contexts", [])
            if not isinstance(contexts, list):
                continue
            for ctx in contexts:
                if not isinstance(ctx, dict):
                    continue
                if "output" not in ctx or "text" not in ctx:
                    continue
                task_id = stable_task_id(post_id, str(ctx["text"]))
                out.add(task_id)
    return out


def select_posts_with_task_ids(
    batch_posts: list[dict[str, Any]],
    selected_task_ids: set[str],
) -> list[dict[str, Any]]:
    selected_posts: list[dict[str, Any]] = []
    for post in batch_posts:
        if not isinstance(post, dict):
            continue
        post_id = post.get("post_id", "unknown")
        contexts = post.get("contexts", [])
        if not isinstance(contexts, list):
            continue
        keep = False
        for ctx in contexts:
            if not isinstance(ctx, dict) or "text" not in ctx:
                continue
            task_id = stable_task_id(post_id, str(ctx["text"]))
            if task_id in selected_task_ids:
                keep = True
                break
        if keep:
            selected_posts.append(post)
    return selected_posts


def run(
    *,
    input_file: Path,
    out_file: Path,
    batch_count: int,
    max_workers: int,
    max_claims: int,
    max_tasks: int,
    extractor_fn: Callable[..., Iterator[dict[str, Any]]] = extract_vaccine_claims,
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    payload = json.loads(input_file.read_text(encoding="utf-8"))
    posts = payload.get("posts", [])
    if not isinstance(posts, list):
        raise ValueError("Input JSON must have top-level 'posts' list.")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    completed_task_ids = load_completed_task_ids(out_file)
    if not out_file.exists():
        out_file.write_text("", encoding="utf-8")

    total_posts = len(posts)
    done_posts = 0
    completed_this_run = 0
    remaining_tasks = max(0, int(max_tasks))

    for batch in batched(posts, batch_count):
        tasks, ctx_by_task_id = build_context_tasks(batch)
        pending_tasks = [t for t in tasks if t["task_id"] not in completed_task_ids]
        if remaining_tasks > 0:
            pending_tasks = pending_tasks[:remaining_tasks]
        if not pending_tasks:
            done_posts += len(batch)
            print(f"[batch] skipped {done_posts}/{total_posts} posts (no pending tasks)")
            continue

        claim_outputs = list(
            extractor_fn(
                pending_tasks,
                max_workers=max_workers,
                max_claims=max_claims,
            )
        )
        merge_outputs_into_posts(ctx_by_task_id, claim_outputs)
        claimed_task_ids = {t["task_id"] for t in pending_tasks}
        posts_to_append = select_posts_with_task_ids(batch, claimed_task_ids)
        append_posts_jsonl(out_file, posts_to_append)
        successful_ids = {
            row["task_id"]
            for row in claim_outputs
            if isinstance(row, dict) and "output" in row
        }

        completed_task_ids.update(successful_ids)
        completed_this_run += len(claimed_task_ids)
        if remaining_tasks > 0:
            remaining_tasks -= len(claimed_task_ids)

        done_posts += len(batch)
        print(
            f"[batch] wrote {done_posts}/{total_posts} posts to {out_file} "
            f"(tasks completed this run: {completed_this_run})"
        )
        if max_tasks > 0 and remaining_tasks <= 0:
            break

    print(f"[ok] finished. output: {out_file.resolve()}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.get_claims")
    ap.add_argument(
        "--input-file",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help=f"Input JSON file (default: {DEFAULT_INPUT_FILE}).",
    )
    ap.add_argument(
        "--out-file",
        type=Path,
        default=DEFAULT_OUT_FILE,
        help=f"Output JSONL file (default: {DEFAULT_OUT_FILE}).",
    )
    ap.add_argument(
        "--batch-count",
        type=int,
        default=DEFAULT_BATCH_COUNT,
        help=f"Posts per batch (default: {DEFAULT_BATCH_COUNT}).",
    )
    ap.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Thread workers for extraction (default: {DEFAULT_MAX_WORKERS}).",
    )
    ap.add_argument(
        "--max-claims",
        type=int,
        default=DEFAULT_MAX_CLAIMS,
        help=f"Max claims requested per context (default: {DEFAULT_MAX_CLAIMS}).",
    )
    ap.add_argument(
        "--max-tasks",
        type=int,
        default=DEFAULT_MAX_TASKS,
        help=(
            "Max number of context tasks to process this run "
            f"(default: {DEFAULT_MAX_TASKS}; 0 means unlimited)."
        ),
    )
    args = ap.parse_args()

    run(
        input_file=args.input_file,
        out_file=args.out_file,
        batch_count=max(1, int(args.batch_count)),
        max_workers=max(1, int(args.max_workers)),
        max_claims=max(1, int(args.max_claims)),
        max_tasks=max(0, int(args.max_tasks)),
    )
