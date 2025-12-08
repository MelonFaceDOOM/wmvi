import json
import time
from pathlib import Path
from datetime import datetime

import pytest

from ingestion.ingestion import ensure_scrape_job
from ingestion.telegram_post import flush_telegram_batch
from filtering.anonymization import redact_pii


"""
run with:
pytest -m benchmark tests/test_telegram_import_benchmarks.py::test_telegram_import_benchmark_100 -s

batch size 50:
tests\test_telegram_import_benchmarks.py
[telegram import benchmark]
  rows requested : 1000
  rows seen      : 1000
  inserted       : 1000
  skipped        : 0
  parse_time_s   : 9.1519
  db_time_s      : 20.4054
  total_time_s   : 29.5752
  
batch size 1000:
tests\test_telegram_import_benchmarks.py
[telegram import benchmark]
  rows requested : 1000
  rows seen      : 1000
  inserted       : 1000
  skipped        : 0
  parse_time_s   : 8.2428
  db_time_s      : 19.1765
  total_time_s   : 27.4248
  
after merging post_registry-to-scrape_job to 1 step:
tests\test_telegram_import_benchmarks.py
[telegram import benchmark]
  rows requested : 1000
  rows seen      : 1000
  inserted       : 1000
  skipped        : 0
  parse_time_s   : 8.7633
  db_time_s      : 9.8823
  total_time_s   : 18.6518
"""


def _benchmark_import_telegram_n(
    n: int,
    batch_commit: int = 2000,
    path: str = "data/telegram.jsonl",
) -> dict:
    """
    Minimal reimplementation of import_telegram_jsonl that:

    - Only processes the first `n` posts.
    - Times parse/PII vs DB (flush_telegram_batch).
    - Uses TEST_ pool & schema via ensure_scrape_job + flush_telegram_batch.

    Returns a dict with counts and timing metrics.
    """
    p = Path(path)
    if not p.exists():
        pytest.skip(f"[benchmark] telegram JSONL not found at {path}")

    job_id = ensure_scrape_job(
        name="benchmark telegram import",
        description="Benchmark import of first N telegram posts from disk",
        platforms=["telegram_post"],
    )

    total_start = time.perf_counter()
    parse_time = 0.0
    db_time = 0.0

    inserted = 0
    skipped = 0
    seen = 0
    pending: list[dict] = []

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if seen >= n:
                break

            line = line.strip()
            if not line:
                continue

            t0 = time.perf_counter()
            rec = json.loads(line)

            dt = datetime.fromisoformat(rec["date"])
            text = rec.get("text") or ""
            d: dict = {
                "channel_id": rec["channel_id"],
                "message_id": rec["message_id"],
                "link": rec["link"],
                "created_at_ts": dt,
                "text": text,
                "filtered_text": redact_pii(text),
                "views": rec.get("views"),
                "forwards": rec.get("forwards"),
                "replies": rec.get("replies"),
                "reactions_total": rec.get("reactions_total"),
                "is_pinned": rec.get("is_pinned", False),
                "has_media": rec.get("has_media", False),
                "raw_type": rec.get("raw_type"),
                "is_en": None,
            }
            parse_time += time.perf_counter() - t0

            pending.append(d)
            seen += 1

            if len(pending) >= batch_commit:
                t_db0 = time.perf_counter()
                batch_ins, batch_skip = flush_telegram_batch(pending, job_id)
                db_time += time.perf_counter() - t_db0
                inserted += batch_ins
                skipped += batch_skip
                pending.clear()

    if pending:
        t_db0 = time.perf_counter()
        batch_ins, batch_skip = flush_telegram_batch(pending, job_id)
        db_time += time.perf_counter() - t_db0
        inserted += batch_ins
        skipped += batch_skip
        pending.clear()

    total_elapsed = time.perf_counter() - total_start

    return {
        "rows_requested": n,
        "rows_seen": seen,
        "inserted": inserted,
        "skipped": skipped,
        "parse_time_s": parse_time,
        "db_time_s": db_time,
        "total_time_s": total_elapsed,
    }

@pytest.mark.benchmark
def test_telegram_import_benchmark_1000(prepared_fresh_db):
    """
    Benchmark the first 1000 telegram posts end-to-end.

    This is not a correctness test; it's a profiling harness that prints
    timings so you can see where the cost is (parsing/PII vs DB).
    """
    results = _benchmark_import_telegram_n(
        n=1000,
        batch_commit=1000,
        path="data/telegram.jsonl",
    )

    # Basic sanity checks
    assert results["rows_seen"] <= results["rows_requested"]
    assert results["inserted"] + results["skipped"] <= results["rows_seen"]

    # Print metrics so you can inspect with `pytest -s`
    print("\n[telegram import benchmark]")
    print(f"  rows requested : {results['rows_requested']}")
    print(f"  rows seen      : {results['rows_seen']}")
    print(f"  inserted       : {results['inserted']}")
    print(f"  skipped        : {results['skipped']}")
    print(f"  parse_time_s   : {results['parse_time_s']:.4f}")
    print(f"  db_time_s      : {results['db_time_s']:.4f}")
    print(f"  total_time_s   : {results['total_time_s']:.4f}")
