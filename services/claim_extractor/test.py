from __future__ import annotations

import asyncio
import csv
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

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


@dataclass(frozen=True)
class PostRow:
    post_id: int
    platform: str
    key1: str
    key2: Optional[str]
    filtered_text: str


def setup_logging(level: str = "INFO") -> None:
    lvl = getattr(logging, (level or "INFO").upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def load_prompt_template() -> str:
    # Prefer file beside this script; fallback to repo-relative.
    here = Path(__file__).resolve()
    prompt_file = here.parent / "extraction.txt"
    if prompt_file.is_file():
        txt = prompt_file.read_text(encoding="utf-8-sig")
        if "__PROMPT_CONTEXT__" not in txt:
            logging.warning("Prompt template does not contain '__PROMPT_CONTEXT__' placeholder.")
        return txt
    raise FileNotFoundError("Could not find extraction.txt")


def fetch_random_english_posts(n: int) -> List[PostRow]:
    """
    Faster than ORDER BY random() over the whole view:
      - sample random post_registry IDs
      - join to sm.post_summary
      - filter is_en + filtered_text
      - fallback to ORDER BY random() if too sparse
    """
    log = logging.getLogger(__name__)

    if n <= 0:
        return []

    # Oversample candidate IDs to account for:
    # - gaps in post_registry.id
    # - non-English rows
    # - NULL filtered_text
    oversample = max(2000, n * 50)

    with getcursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM sm.post_registry;")
        (max_id,) = cur.fetchone()
        max_id = int(max_id or 0)
        if max_id <= 0:
            log.warning("sm.post_registry is empty (max_id=0); cannot sample posts.")
            return []

        t0 = time.perf_counter()
        log.info("Sampling %d english posts (max_id=%d, oversample=%d)...", n, max_id, oversample)

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
            SELECT ps.post_id, ps.platform, ps.key1, ps.key2, ps.filtered_text
            FROM candidate_ids c
            JOIN sm.post_summary ps
              ON ps.post_id = c.post_id
            WHERE ps.is_en IS TRUE
              AND ps.filtered_text IS NOT NULL
            LIMIT (SELECT want FROM params);
            """,
            (max_id, int(n), int(oversample)),
        )
        rows = cur.fetchall()

        dt = time.perf_counter() - t0
        log.info("Sample query returned %d/%d rows in %.2fs.", len(rows), n, dt)

    # If we didn’t get enough (dataset very sparse), fall back to expensive method as a last resort.
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
                SELECT post_id, platform, key1, key2, filtered_text
                FROM sm.post_summary
                WHERE is_en IS TRUE
                  AND filtered_text IS NOT NULL
                ORDER BY random()
                LIMIT %s;
                """,
                (int(n),),
            )
            rows = cur.fetchall()
            log.info("Fallback query returned %d rows in %.2fs.", len(rows), time.perf_counter() - t0)

    return [
        PostRow(
            post_id=int(post_id),
            platform=str(platform),
            key1=str(key1),
            key2=str(key2) if key2 is not None else None,
            filtered_text=str(filtered_text),
        )
        for (post_id, platform, key1, key2, filtered_text) in rows
    ]


def build_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )


async def call_extract(
    client: AsyncAzureOpenAI,
    prompt_template: str,
    post: PostRow,
    *,
    max_retries: int = 6,
) -> Tuple[bool, str, str, float]:
    """
    Returns: (ok, response_or_error, error_type, latency_seconds)
    """
    prompt = prompt_template.replace("__PROMPT_CONTEXT__", post.filtered_text)

    last_err: Optional[BaseException] = None
    start = time.perf_counter()

    for attempt in range(max_retries):
        try:
            resp = await client.chat.completions.create(
                model=MODEL_NAME,
                messages=[{"role": "user", "content": prompt}]
            )
            content = resp.choices[0].message.content or ""
            return True, content, "", time.perf_counter() - start

        except (RateLimitError, APITimeoutError, APIConnectionError) as e:
            last_err = e
            # exponential backoff with jitter
            sleep_s = min(30.0, (2**attempt) * 0.75) + random.random() * 0.5
            await asyncio.sleep(sleep_s)

        except APIStatusError as e:
            last_err = e
            # retry 5xx; do not retry most 4xx (except rate limit which is handled above)
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


async def run(n: int, out_csv: str, concurrency: int) -> None:
    """Concurrency is on prompting (each request can be slow)."""
    log = logging.getLogger(__name__)

    prompt_template = load_prompt_template()
    posts = fetch_random_english_posts(n)

    if not posts:
        log.warning("No posts fetched; nothing to do.")
        return

    client = build_client()
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    write_lock = asyncio.Lock()

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "post_id",
        "platform",
        "key1",
        "key2",
        "filtered_text",
        "ok",
        "response",
        "error_type",
        "latency_s",
        "model",
    ]

    total = len(posts)
    start_all = time.perf_counter()

    done = 0
    ok_count = 0
    err_count = 0

    # progress logging cadence
    log_every = max(1, min(25, total // 10))

    log.info(
        "Starting extraction: n=%d (fetched=%d), concurrency=%d, out=%s, model=%s",
        n,
        total,
        sem._value,  # fine for logging
        str(out_path),
        MODEL_NAME,
    )

    try:
        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()

            async def write_row(row: dict) -> None:
                async with write_lock:
                    try:
                        w.writerow(row)
                        f.flush()
                    except Exception as e:
                        log.error("Failed to write csv row: %s: %s", type(e).__name__, e)

            async def worker(post: PostRow) -> None:
                nonlocal done, ok_count, err_count

                t0 = time.perf_counter()
                try:
                    async with sem:
                        ok, resp_or_err, err_type, latency_s = await call_extract(
                            client,
                            prompt_template,
                            post,
                        )

                    row = {
                        "post_id": post.post_id,
                        "platform": post.platform,
                        "key1": post.key1,
                        "key2": post.key2 or "",
                        "filtered_text": post.filtered_text,
                        "ok": ok,
                        "response": resp_or_err,
                        "error_type": err_type,
                        "latency_s": f"{latency_s:.3f}",
                        "model": MODEL_NAME,
                    }
                except Exception as e:
                    ok = False
                    row = {
                        "post_id": post.post_id,
                        "platform": post.platform,
                        "key1": post.key1,
                        "key2": post.key2 or "",
                        "filtered_text": post.filtered_text,
                        "ok": False,
                        "response": f"{type(e).__name__}: {e}",
                        "error_type": type(e).__name__,
                        "latency_s": f"{(time.perf_counter() - t0):.3f}",
                        "model": MODEL_NAME,
                    }

                await write_row(row)

                # update progress after write
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
                            "Progress %d/%d (ok=%d err=%d) elapsed=%.1fs rate=%.2f posts/s",
                            done,
                            total,
                            ok_count,
                            err_count,
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
            "Finished: done=%d ok=%d err=%d elapsed=%.1fs out=%s",
            done,
            ok_count,
            err_count,
            elapsed,
            str(out_path),
        )


def main() -> None:
    setup_logging(os.getenv("WMVI_LOG_LEVEL", "INFO"))

    out = "services/claim_extractor/extractions_sample.csv"
    n = 300
    concurrency = 6

    logging.info("Initializing DB pool (prefix=DEV)...")
    init_pool(prefix="DEV")

    try:
        asyncio.run(run(n=n, out_csv=out, concurrency=concurrency))
        logging.info("Done. Output: %s", out)
    finally:
        logging.info("Closing DB pool...")
        close_pool()


if __name__ == "__main__":
    main()
