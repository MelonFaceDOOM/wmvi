from __future__ import annotations

import argparse
import json
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, local
from typing import Any, Iterable, Iterator, Optional

from dotenv import load_dotenv
from openai import AzureOpenAI
from openai._exceptions import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    RateLimitError,
)

load_dotenv()

MODEL_NAME = "gpt-5-mini"
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_PROMPT = (PROMPTS_DIR / "extract_system.txt").read_text(encoding="utf-8-sig")
USER_PROMPT = (PROMPTS_DIR / "extract_user.txt").read_text(encoding="utf-8-sig")

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
CLAIM_STANCE_VALUES = ("pro", "anti", "neutral", "unclear")
AUTHOR_STANCE_VALUES = ("support", "reject", "neutral", "unclear")
ATTRIBUTION_VALUES = ("self", "personal relation", "authority", "common knowledge", "unknown")

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
                        "claim_stance_to_vaccines": {
                            "type": "string",
                            "enum": list(CLAIM_STANCE_VALUES),
                        },
                        "author_stance_to_claim": {
                            "type": "string",
                            "enum": list(AUTHOR_STANCE_VALUES),
                        },
                        "attribution": {
                            "type": "string",
                            "enum": list(ATTRIBUTION_VALUES),
                        },
                    },
                    "required": [
                        "claim",
                        "claim_stance_to_vaccines",
                        "author_stance_to_claim",
                        "attribution",
                    ],
                },
            }
        },
        "required": ["claims"],
    },
}


def _build_client() -> AzureOpenAI:
    if not AZURE_OPENAI_KEY:
        raise RuntimeError("Missing AZURE_OPENAI_KEY in environment.")
    if not AZURE_OPENAI_ENDPOINT:
        raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT in environment.")
    return AzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )


_thread_local = local()


def _get_client() -> AzureOpenAI:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = _build_client()
        _thread_local.client = client
    return client




def _build_user_prompt(input_text: str, *, max_claims: int) -> str:
    return (
        USER_PROMPT.replace("{{max_claims}}", str(max_claims))
        .replace("[[max_claims]]", str(max_claims))
        .replace("{{text_input}}", input_text)
    )


def _build_system_prompt(*, max_claims: int) -> str:
    return (
        SYSTEM_PROMPT.replace("{{max_claims}}", str(max_claims))
        .replace("[[max_claims]]", str(max_claims))
    )


def _call_extract_once(
    client: AzureOpenAI,
    *,
    input_text: str,
    max_claims: int,
) -> str:
    system_prompt = _build_system_prompt(max_claims=max_claims)
    user_prompt = _build_user_prompt(input_text, max_claims=max_claims)
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0,
        response_format={
            "type": "json_schema",
            "json_schema": CLAIMS_JSON_SCHEMA,
        },
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    if not getattr(resp, "choices", None):
        raise RuntimeError("Model response has no choices.")
    choice0 = resp.choices[0]
    message = getattr(choice0, "message", None)
    if message is None:
        raise RuntimeError("Model response missing message.")
    content = getattr(message, "content", None)
    if content is None:
        raise RuntimeError("Model response content is None.")
    if not isinstance(content, str):
        raise RuntimeError(f"Unexpected model content type: {type(content)}")
    if not content.strip():
        raise RuntimeError("Model response content is empty.")
    return content


def _parse_and_validate_output(content: str) -> dict[str, Any]:
    s = content.strip()
    if s.startswith("```"):
        if s.startswith("```json"):
            s = s[len("```json") :].strip()
        else:
            s = s[len("```") :].strip()
        if s.endswith("```"):
            s = s[:-3].strip()

    parsed = json.loads(s)
    if not isinstance(parsed, dict):
        raise ValueError("model output JSON top-level is not an object")
    claims = parsed.get("claims")
    if not isinstance(claims, list):
        raise ValueError("model output missing list field 'claims'")
    required = (
        "claim",
        "claim_stance_to_vaccines",
        "author_stance_to_claim",
        "attribution",
    )
    for i, row in enumerate(claims):
        if not isinstance(row, dict):
            raise ValueError(f"claim row {i} is not an object")
        missing = [k for k in required if k not in row]
        if missing:
            raise ValueError(f"claim row {i} missing keys: {missing}")
        claim_stance = row.get("claim_stance_to_vaccines")
        if claim_stance not in CLAIM_STANCE_VALUES:
            raise ValueError(
                f"claim row {i} has invalid claim_stance_to_vaccines={claim_stance!r}; "
                f"expected one of {CLAIM_STANCE_VALUES}"
            )
        author_stance = row.get("author_stance_to_claim")
        if author_stance not in AUTHOR_STANCE_VALUES:
            raise ValueError(
                f"claim row {i} has invalid author_stance_to_claim={author_stance!r}; "
                f"expected one of {AUTHOR_STANCE_VALUES}"
            )
        attribution = row.get("attribution")
        if attribution not in ATTRIBUTION_VALUES:
            raise ValueError(
                f"claim row {i} has invalid attribution={attribution!r}; "
                f"expected one of {ATTRIBUTION_VALUES}"
            )
    return parsed


def _call_extract_with_retries(
    client: AzureOpenAI,
    *,
    input_text: str,
    max_claims: int,
    max_retries: int,
) -> dict[str, Any]:
    last_err: Optional[BaseException] = None
    for attempt in range(max_retries):
        try:
            raw = _call_extract_once(client, input_text=input_text, max_claims=max_claims)
            return _parse_and_validate_output(raw)
        except (RateLimitError, APITimeoutError, APIConnectionError) as e:
            last_err = e
            sleep_s = min(30.0, (2**attempt) * 0.75) + random.random() * 0.5
            time.sleep(sleep_s)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            last_err = e
            # Only retry once for schema/parse issues
            if attempt >= 1:
                raise
            sleep_s = min(5.0, (2**attempt) * 0.5) + random.random() * 0.25
            time.sleep(sleep_s)
        except APIStatusError as e:
            last_err = e
            status = getattr(e, "status_code", None)
            if status is not None and 500 <= int(status) <= 599:
                sleep_s = min(30.0, (2**attempt) * 0.75) + random.random() * 0.5
                time.sleep(sleep_s)
                continue
            raise
    if last_err is None:
        raise RuntimeError("Unknown error after retries")
    raise RuntimeError(f"{type(last_err).__name__}: {last_err}")


def _validate_tasks(tasks: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError(f"Task at index {idx} is not a dict.")
        if "task_id" not in task:
            raise ValueError(f"Task at index {idx} missing required key 'task_id'.")
        if "input_text" not in task:
            raise ValueError(f"Task at index {idx} missing required key 'input_text'.")
        out.append(task)
    return out


def _worker(
    task: dict[str, Any],
    *,
    max_claims: int,
    max_retries: int,
) -> dict[str, Any]:
    task_id = task["task_id"]
    input_text = task["input_text"]
    if not isinstance(input_text, str):
        input_text = str(input_text)
    client = _get_client()
    try:
        output = _call_extract_with_retries(
            client,
            input_text=input_text,
            max_claims=max_claims,
            max_retries=max_retries,
        )
    except Exception as e:
        output = {
            "failed": True,
            "error": f"{type(e).__name__}: {e}",
        }
    return {
        "task_id": task_id,
        "input_text": input_text,
        "output": output,
    }


def _load_tasks_from_posts_json(input_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Input JSON top-level must be an object.")
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Input JSON missing top-level 'posts' array.")
    tasks: list[dict[str, Any]] = []
    for idx, post in enumerate(posts):
        if not isinstance(post, dict):
            continue
        source_post_id = post.get("source_post_id", post.get("post_id"))
        chunk_index = post.get("sentence_boundary_chunk_index")
        if chunk_index is None:
            task_id = str(source_post_id if source_post_id is not None else idx)
        else:
            task_id = f"{source_post_id}:{chunk_index}"

        text = post.get("text_coreference_resolved")
        if not isinstance(text, str) or not text.strip():
            text = post.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        tasks.append({"task_id": task_id, "input_text": text})
    return tasks


def preview_prompts(
    *,
    input_path: Path,
    preview_count: int,
    max_claims: int,
) -> int:
    tasks = _load_tasks_from_posts_json(input_path)
    if not tasks:
        print(f"[preview] no valid tasks from {input_path.resolve()}", flush=True)
        return 0

    n = min(max(0, int(preview_count)), len(tasks))
    print(
        f"[preview] loaded {len(tasks)} tasks from {input_path.resolve()}; printing {n} prompts",
        flush=True,
    )
    for i, task in enumerate(tasks[:n], start=1):
        prompt = _build_user_prompt(task["input_text"], max_claims=max_claims)
        print(f"\n===== PROMPT {i}/{n} task_id={task['task_id']} =====", flush=True)
        print(prompt, flush=True)
    return len(tasks)


def run_extraction_to_json(
    *,
    input_path: Path,
    output_path: Path,
    max_workers: int,
    max_claims: int,
    max_retries: int,
) -> int:
    tasks = _load_tasks_from_posts_json(input_path)
    if not tasks:
        print(f"[extract] no valid tasks from {input_path.resolve()}", flush=True)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("[]\n", encoding="utf-8")
        return 0

    print(
        f"[extract] loaded {len(tasks)} tasks from {input_path.resolve()}",
        flush=True,
    )
    results: list[dict[str, Any]] = []
    completed = 0
    for row in extract_vaccine_claims(
        tasks,
        max_workers=max_workers,
        max_claims=max_claims,
        max_retries=max_retries,
    ):
        results.append(row)
        completed += 1
        if completed % 100 == 0 or completed == len(tasks):
            print(f"[progress] extract {completed}/{len(tasks)}", flush=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[ok] wrote {len(results)} extraction results -> {output_path.resolve()}", flush=True)
    return len(results)


def extract_vaccine_claims(
    tasks: Iterable[dict[str, Any]],
    *,
    max_workers: int = 6,
    max_claims: int = 8,
    max_retries: int = 3,
) -> Iterator[dict[str, Any]]:
    """
    Run vaccine claim extraction for tasks and yield results as they complete.

    Input task shape:
      {"task_id": <any>, "input_text": <str-like>}

    Yields:
      {"task_id": ..., "input_text": ..., "output": {...}}
    """
    task_list = _validate_tasks(tasks)
    if not task_list:
        return

    futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
    executor = ThreadPoolExecutor(max_workers=max(1, int(max_workers)))
    try:
        for task in task_list:
            fut = executor.submit(
                _worker,
                task,
                max_claims=max_claims,
                max_retries=max_retries,
            )
            futures[fut] = task

        for fut in as_completed(futures):
            try:
                yield fut.result()
            except Exception as e:
                task = futures[fut]
                yield {
                    "task_id": task.get("task_id"),
                    "input_text": task.get("input_text"),
                    "output": {
                        "failed": True,
                        "error": f"{type(e).__name__}: {e}",
                    },
                }

    except KeyboardInterrupt:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=False)


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.vaccine_claim_extractor")
    ap.add_argument("--input", type=Path, required=True, help="Path to JSON with top-level posts[]")
    ap.add_argument("--preview-prompts", type=int, default=0, metavar="N")
    ap.add_argument("--output", type=Path, default=None, help="Output JSON path (required for extraction mode)")
    ap.add_argument("--max-claims", type=int, default=8, metavar="N")
    ap.add_argument("--max-workers", type=int, default=6, metavar="N")
    ap.add_argument("--max-retries", type=int, default=3, metavar="N")
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.preview_prompts > 0:
        preview_prompts(
            input_path=args.input,
            preview_count=args.preview_prompts,
            max_claims=max(1, int(args.max_claims)),
        )
        return

    if args.output is None:
        raise SystemExit(
            "Extraction mode requires --output. "
            "Use --preview-prompts N for no-API preview mode."
        )
    run_extraction_to_json(
        input_path=args.input,
        output_path=args.output,
        max_workers=max(1, int(args.max_workers)),
        max_claims=max(1, int(args.max_claims)),
        max_retries=max(1, int(args.max_retries)),
    )


if __name__ == "__main__":
    main()
