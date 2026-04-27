from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from openai import AsyncAzureOpenAI
from openai._exceptions import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    RateLimitError,
)

from db.db import init_pool, close_pool, getcursor

load_dotenv()

MODEL_NAME = "gpt-5-mini"

AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2025-03-01-preview")

if not AZURE_OPENAI_KEY:
    raise RuntimeError("Missing AZURE_OPENAI_KEY in environment.")
if not AZURE_OPENAI_ENDPOINT:
    raise RuntimeError("Missing AZURE_OPENAI_ENDPOINT in environment.")

# Keep this aligned with the prompt, which is written for social-media posts.
SOCIAL_PLATFORMS = (
    "tweet",
    "reddit_submission",
    "reddit_comment",
    "telegram_post",
    "youtube_comment",
)


@dataclass(frozen=True)
class PostRow:
    post_id: int
    source_post_id: int
    platform: str
    created_at: Optional[str]
    key1: str
    key2: Optional[str]
    prompt_text: str


def setup_logging(level: str = "INFO") -> None:
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def load_prompt_template() -> str:
    here = Path(__file__).resolve()
    prompt_file = here.parent / "extraction.txt"
    if prompt_file.is_file():
        txt = prompt_file.read_text(encoding="utf-8-sig")
        if "__PROMPT_CONTEXT__" not in txt:
            logging.warning("Prompt template does not contain '__PROMPT_CONTEXT__' placeholder.")
        return txt
    raise FileNotFoundError("Could not find extraction.txt")


def _to_iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def fetch_random_english_posts(n: int) -> list[PostRow]:
    """
    Faster than ORDER BY random() over the whole view:
      - sample random post_registry IDs
      - join to sm.posts_all
      - filter is_en + text + selected platforms
      - fallback to ORDER BY random() if too sparse
    """
    log = logging.getLogger(__name__)

    if n <= 0:
        return []

    oversample = max(2000, n * 50)

    with getcursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM sm.post_registry;")
        (max_id,) = cur.fetchone()
        max_id = int(max_id or 0)

        if max_id <= 0:
            log.warning("sm.post_registry is empty (max_id=0); cannot sample posts.")
            return []

        t0 = time.perf_counter()
        log.info(
            "Sampling %d english posts (max_id=%d, oversample=%d, platforms=%s)...",
            n,
            max_id,
            oversample,
            ",".join(SOCIAL_PLATFORMS),
        )

        cur.execute(
            """
            WITH params AS (
                SELECT %s::bigint AS max_id,
                       %s::int    AS want,
                       %s::int    AS k
            ),
            candidate_ids AS (
                SELECT DISTINCT (1 + floor(random() * p.max_id))::bigint AS post_id
                FROM params p, generate_series(1, (SELECT k FROM params))
            )
            SELECT pa.post_id,
                   pa.platform,
                   pa.key1,
                   pa.key2,
                   pa.created_at_ts,
                   pa.text
            FROM candidate_ids c
            JOIN sm.posts_all pa
              ON pa.post_id = c.post_id
            WHERE pa.is_en IS TRUE
              AND pa.text IS NOT NULL
              AND pa.platform = ANY(%s)
            LIMIT (SELECT want FROM params);
            """,
            (max_id, int(n), int(oversample), list(SOCIAL_PLATFORMS)),
        )
        rows = cur.fetchall()

        dt = time.perf_counter() - t0
        log.info("Sample query returned %d/%d rows in %.2fs.", len(rows), n, dt)

    if len(rows) < n:
        log.warning(
            "Sample query returned only %d/%d; falling back to ORDER BY random() (may be slow).",
            len(rows),
            n,
        )
        with getcursor() as cur:
            t0 = time.perf_counter()
            cur.execute(
                """
                SELECT post_id,
                       platform,
                       key1,
                       key2,
                       created_at_ts,
                       text
                FROM sm.posts_all
                WHERE is_en IS TRUE
                  AND text IS NOT NULL
                  AND platform = ANY(%s)
                ORDER BY random()
                LIMIT %s;
                """,
                (list(SOCIAL_PLATFORMS), int(n)),
            )
            rows = cur.fetchall()
            log.info("Fallback query returned %d rows in %.2fs.", len(rows), time.perf_counter() - t0)

    return [
        PostRow(
            post_id=int(post_id),
            source_post_id=int(post_id),
            platform=str(platform),
            created_at=_to_iso_or_none(created_at_ts),
            key1=str(key1),
            key2=str(key2) if key2 not in (None, "") else None,
            prompt_text=str(text),
        )
        for (post_id, platform, key1, key2, created_at_ts, text) in rows
    ]


def build_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )


def parse_json_response(content: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    raw = (content or "").strip()
    if not raw:
        return None, "empty_response"

    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return None, "top_level_not_object"
        return parsed, None
    except json.JSONDecodeError:
        pass

    if raw.startswith("```"):
        stripped = raw.strip()
        if stripped.startswith("```json"):
            stripped = stripped[len("```json"):].strip()
        elif stripped.startswith("```"):
            stripped = stripped[len("```"):].strip()

        if stripped.endswith("```"):
            stripped = stripped[:-3].strip()

        try:
            parsed = json.loads(stripped)
            if not isinstance(parsed, dict):
                return None, "top_level_not_object"
            return parsed, None
        except json.JSONDecodeError as e:
            return None, f"json_decode_error: {e}"

    return None, "json_decode_error"


async def call_extract(
    client: AsyncAzureOpenAI,
    prompt_template: str,
    post: PostRow,
    *,
    max_retries: int = 6,
) -> tuple[bool, str, str, float]:
    """
    Returns: (ok, response_or_error, error_type, latency_seconds)
    """
    prompt = prompt_template.replace("__PROMPT_CONTEXT__", post.prompt_text)

    last_err: Optional[BaseException] = None
    start = time.perf_counter()

    for attempt in range(max_retries):
        try:
            resp = await client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}],
            )
            content = resp.choices[0].message.content or ""
            return True, content, "", time.perf_counter() - start

        except (RateLimitError, APITimeoutError, APIConnectionError) as e:
            last_err = e
            sleep_s = min(30.0, (2**attempt) * 0.75) + random.random() * 0.5
            await asyncio.sleep(sleep_s)

        except APIStatusError as e:
            last_err = e
            status = getattr(e, "status_code", None)
            if status is not None and 500 <= int(status) <= 599:
                sleep_s = min(30.0, (2**attempt) * 0.75) + random.random() * 0.5
                await asyncio.sleep(sleep_s)
                continue
            return False, f"{type(e).__name__}: {e}", type(e).__name__, time.perf_counter() - start

        except Exception as e:
            return False, f"{type(e).__name__}: {e}", type(e).__name__, time.perf_counter() - start

    if last_err is None:
        return False, "Unknown error after retries", "UnknownError", time.perf_counter() - start

    return False, f"{type(last_err).__name__}: {last_err}", type(last_err).__name__, time.perf_counter() - start


async def run(n: int, out_jsonl: str, concurrency: int) -> None:
    log = logging.getLogger(__name__)

    prompt_template = load_prompt_template()
    posts = fetch_random_english_posts(n)

    if not posts:
        log.warning("No posts fetched; nothing to do.")
        return

    client = build_client()
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    write_lock = asyncio.Lock()

    out_path = Path(out_jsonl)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = len(posts)
    start_all = time.perf_counter()

    done = 0
    ok_count = 0
    err_count = 0
    parse_err_count = 0
    log_every = max(1, min(25, total // 10))

    log.info(
        "Starting extraction: requested_n=%d fetched=%d concurrency=%d out=%s model=%s",
        n,
        total,
        concurrency,
        str(out_path),
        MODEL_NAME,
    )

    try:
        with out_path.open("w", encoding="utf-8") as f:

            async def write_jsonl_row(obj: dict[str, Any]) -> None:
                async with write_lock:
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    f.flush()

            async def worker(post: PostRow) -> None:
                nonlocal done, ok_count, err_count, parse_err_count

                t0 = time.perf_counter()

                try:
                    async with sem:
                        ok, resp_or_err, err_type, latency_s = await call_extract(
                            client,
                            prompt_template,
                            post,
                        )

                    parsed_response: Optional[dict[str, Any]] = None
                    parse_error: Optional[str] = None

                    if ok:
                        parsed_response, parse_error = parse_json_response(resp_or_err)
                        if parse_error is not None:
                            parse_err_count += 1

                    row = {
                        "claim_id": None,
                        "source_post_id": post.source_post_id,
                        "post_id": post.post_id,
                        "platform": post.platform,
                        "created_at": post.created_at,
                        "prompt_text": post.prompt_text,
                        "key1": post.key1,
                        "key2": post.key2,
                        "ok": ok,
                        "error_type": err_type or None,
                        "latency_s": round(latency_s, 3),
                        "model": MODEL_NAME,
                        "raw_response": resp_or_err,
                        "parsed_response": parsed_response,
                        "parse_error": parse_error,
                    }

                except Exception as e:
                    ok = False
                    row = {
                        "claim_id": None,
                        "source_post_id": post.source_post_id,
                        "post_id": post.post_id,
                        "platform": post.platform,
                        "created_at": post.created_at,
                        "prompt_text": post.prompt_text,
                        "key1": post.key1,
                        "key2": post.key2,
                        "ok": False,
                        "error_type": type(e).__name__,
                        "latency_s": round(time.perf_counter() - t0, 3),
                        "model": MODEL_NAME,
                        "raw_response": f"{type(e).__name__}: {e}",
                        "parsed_response": None,
                        "parse_error": None,
                    }

                await write_jsonl_row(row)

                async with write_lock:
                    done += 1
                    if ok:
                        ok_count += 1
                    else:
                        err_count += 1

                    if done == 1 or done % log_every == 0 or done == total:
                        elapsed = time.perf_counter() - start_all
                        rate = done / elapsed if elapsed > 0 else 0.0
                        log.info(
                            "Progress %d/%d (ok=%d err=%d parse_err=%d) elapsed=%.1fs rate=%.2f posts/s",
                            done,
                            total,
                            ok_count,
                            err_count,
                            parse_err_count,
                            elapsed,
                            rate,
                        )

            await asyncio.gather(*(worker(p) for p in posts))

    finally:
        try:
            await client.close()
        except Exception:
            pass

        elapsed = time.perf_counter() - start_all
        log.info(
            "Finished: done=%d ok=%d err=%d parse_err=%d elapsed=%.1fs out=%s",
            done,
            ok_count,
            err_count,
            parse_err_count,
            elapsed,
            str(out_path),
        )


def main(
    *,
    n: int,
    out_jsonl: str,
    concurrency: int = 6,
    prod: bool = False,
) -> None:
    setup_logging(os.getenv("WMVI_LOG_LEVEL", "INFO"))

    if n <= 0:
        raise ValueError("n must be > 0")

    prefix = "PROD" if prod else "DEV"
    logging.info("Initializing DB pool (prefix=%s)...", prefix)
    init_pool(prefix=prefix)

    try:
        asyncio.run(run(n=n, out_jsonl=out_jsonl, concurrency=concurrency))
        logging.info("Done. Output: %s", out_jsonl)
    finally:
        logging.info("Closing DB pool...")
        close_pool()


if __name__ == "__main__":
    main(
        n=300,
        out_jsonl="services/claim_extractor/extractions_sample.jsonl",
        concurrency=6,
        prod=False,
    )