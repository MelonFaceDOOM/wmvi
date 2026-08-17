"""Sample posts that failed with BadRequestError and re-submit to Azure."""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

from nlp.claim_extraction.api_requester import (
    ConcurrentApiRequester,
    RequestStatus,
    RequestTask,
    RetryPolicy,
)
from nlp.claim_extraction.text import format_input_text
from nlp.claim_extraction.batch import (
    DEFAULT_MAX_CLAIMS,
    _build_client,
    _stable_task_id,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_FILE = REPO_ROOT / "data" / "posts_with_claims_full.json"
DEFAULT_SAMPLE_SIZE = 50


def _is_badrequest_failure(post: dict[str, Any]) -> bool:
    err = post.get("claim_extraction_error")
    return isinstance(err, str) and "badrequesterror" in err.lower()


def _input_text_for_post(post: dict[str, Any]) -> str | None:
    text = post.get("text_coreference_resolved")
    if not isinstance(text, str) or not text.strip():
        text = post.get("text")
    if not isinstance(text, str) or not text.strip():
        return None
    return format_input_text(post, text)


def _load_posts(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected top-level JSON object")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"{path}: expected top-level 'posts' list")
    return [p for p in posts if isinstance(p, dict)]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Re-submit a sample of posts that previously failed with BadRequestError."
    )
    ap.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_FILE)
    ap.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for sampling failed posts.")
    ap.add_argument("--max-workers", type=int, default=2)
    ap.add_argument("--max-claims", type=int, default=DEFAULT_MAX_CLAIMS)
    args = ap.parse_args()

    posts = _load_posts(args.input_file)
    failures = [p for p in posts if _is_badrequest_failure(p)]
    successes = [p for p in posts if p.get("claim_extraction_status") == "success"]

    print(f"{len(failures)} failures and {len(successes)} successes found in file")

    sample_size = min(max(0, int(args.sample_size)), len(failures))
    if sample_size == 0:
        print("trying to re-submit 0 posts")
        print("0% succeeded")
        return

    rng = random.Random(int(args.seed))
    sample = rng.sample(failures, sample_size)
    print(f"trying to re-submit {sample_size} posts")

    client = _build_client(claims_only=False)
    requester = ConcurrentApiRequester(
        client=client,
        max_workers=max(1, int(args.max_workers)),
        retry_policy=RetryPolicy(max_retries=1),
    )

    tasks: list[RequestTask] = []
    for post in sample:
        input_text = _input_text_for_post(post)
        if input_text is None:
            continue
        tasks.append(
            RequestTask(
                task_id=_stable_task_id(post),
                payload={
                    "input_text": input_text,
                    "max_claims": max(1, int(args.max_claims)),
                    "claims_only": False,
                },
            )
        )

    if not tasks:
        print("0% succeeded")
        return

    ok = 0
    for result in requester.run(tasks):
        if result.status == RequestStatus.SUCCESS:
            ok += 1
            claim_count = 0
            if isinstance(result.output, dict) and isinstance(result.output.get("claims"), list):
                claim_count = len(result.output["claims"])
            print(f"[success] task_id={result.task_id} claims={claim_count}", flush=True)
        else:
            print(f"[failure] task_id={result.task_id} {result.error or 'unknown error'}", flush=True)

    pct = round(100.0 * ok / len(tasks))
    print(f"{pct}% succeeded")


if __name__ == "__main__":
    main()
