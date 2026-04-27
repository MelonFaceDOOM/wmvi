from __future__ import annotations

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

if not AZURE_OPENAI_KEY:
    raise RuntimeError("Missing AZURE_OPENAI_KEY in environment.")
if not AZURE_OPENAI_ENDPOINT:
    raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT in environment.")

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
                        "claim_stance_to_vaccines": {"type": "string"},
                        "author_stance_to_claim": {"type": "string"},
                        "attribution": {"type": "string"},
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


def _call_extract_once(
    client: AzureOpenAI,
    *,
    input_text: str,
    max_claims: int,
) -> str:
    user_prompt = _build_user_prompt(input_text, max_claims=max_claims)
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        temperature=0,
        response_format={
            "type": "json_schema",
            "json_schema": CLAIMS_JSON_SCHEMA,
        },
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
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
