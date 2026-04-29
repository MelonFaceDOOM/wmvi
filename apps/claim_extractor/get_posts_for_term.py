"""
Export posts that have taxonomy term hits (matches.post_term_hit) to JSON.

Each post appears once with a ``hits`` list: term_id, term_name, match_start, match_end.
After the DB fetch, runs **coreference resolution** then **context trimming** (syntok
sentence windows → ``contexts``).

Large exports **stream** posts to the output file (``*.tmp`` then rename) so a crash
still leaves a recoverable partial file and peak RAM stays bounded.

Requires claim-extractor NLP deps (see repo ``requirements.txt``) plus coref extras:

  pip install -r apps/claim_extractor/requirements-coref.txt

Usage:
  python -m apps.claim_extractor.get_posts_for_term
  python -m apps.claim_extractor.get_posts_for_term --prod --out data/mmr.json
  python -m apps.claim_extractor.get_posts_for_term --prod --stage fetch --raw-out data/mmr_raw.json
  python -m apps.claim_extractor.get_posts_for_term --prod --stage enrich --raw-out data/mmr_raw.json --out data/mmr.json
  python -m apps.claim_extractor.get_posts_for_term --terms measles mmr --terms-file more_terms.txt
  python -m apps.claim_extractor.get_posts_for_term --progress-every 200


If memory issues, try:
  PGOPTIONS='-c work_mem=256MB' python -m apps.claim_extractor.get_posts_for_term --prod --out data/mmr.json --terms mmr
"""

from __future__ import annotations

import argparse
import gc
import importlib
import json
import multiprocessing as mp
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from db.db import close_pool, getcursor, init_pool

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = PROJECT_ROOT / "data" / "posts_for_term.json"
DEFAULT_RAW_OUT = PROJECT_ROOT / "data" / "posts_for_term_raw.json"
DEFAULT_ENRICH_WINDOW = 100
DEFAULT_ENRICH_TIMEOUT_SEC = 1200

DEFAULT_TERMS: tuple[str, ...] = (
    "priorix",
    "measles jab",
    "measles injection",
    "measles",
    "mmr vaccine",
    "mmr autism",
    "mumps vaccine",
    "rubella vaccine",
)

DEFAULT_PROGRESS_EVERY = 100


def _silence_third_party_progress() -> None:
    """Stop HF/datasets/transformers from spawning third-party progress bars."""
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("HF_DATASETS_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    # Disable library-owned tqdm bars.
    os.environ.setdefault("TQDM_DISABLE", "1")
    try:
        import datasets

        datasets.disable_progress_bars()
    except Exception:
        pass
    try:
        import transformers

        transformers.logging.set_verbosity_error()
    except Exception:
        pass
    import logging

    for name in ("transformers", "datasets", "huggingface_hub", "httpx", "urllib3"):
        logging.getLogger(name).setLevel(logging.ERROR)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _json_val(v: Any) -> Any:
    if isinstance(v, datetime):
        return _ensure_utc(v).isoformat()
    if v is None:
        return None
    return v


def _load_terms_file(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            lines.append(s)
    return lines


def _collect_terms(cli_terms: list[str], terms_file: Optional[Path]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for t in cli_terms:
        key = t.strip()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    if terms_file is not None:
        for t in _load_terms_file(terms_file):
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out

def _sql_fetch_post_id_page() -> str:
    return """
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT DISTINCT ph.post_id
        FROM matches.post_term_hit ph
        JOIN term_ids t
          ON t.id = ph.term_id
        WHERE ph.post_id > %s
        ORDER BY ph.post_id
        LIMIT %s
    """


def _sql_fetch_posts_for_ids() -> str:
    return """
        SELECT
            p.post_id,
            p.platform,
            p.key1,
            p.key2,
            p.date_entered,
            p.created_at_ts,
            p.text,
            p.tsv_en,
            p.is_en,
            p.primary_metric,
            p.url,
            rs_meta.title AS reddit_submission_title,
            rc_sub.title AS reddit_comment_submission_title,
            tp_meta.channel_id::text AS telegram_channel,
            yv_meta.title AS youtube_video_title,
            ps_meta.title AS podcast_name
        FROM sm.posts_all p
        LEFT JOIN sm.reddit_submission rs_meta
          ON p.platform = 'reddit_submission'
         AND p.key1 = rs_meta.id
         AND p.key2 = ''
        LEFT JOIN sm.reddit_comment rc_meta
          ON p.platform = 'reddit_comment'
         AND p.key1 = rc_meta.id
         AND p.key2 = ''
        LEFT JOIN sm.reddit_submission rc_sub
          ON p.platform = 'reddit_comment'
         AND rc_sub.id = regexp_replace(rc_meta.link_id, '^t3_', '')
        LEFT JOIN sm.telegram_post tp_meta
          ON p.platform = 'telegram_post'
         AND p.key1 = tp_meta.channel_id::text
         AND p.key2 = tp_meta.message_id::text
        LEFT JOIN youtube.video yv_meta
          ON p.platform = 'youtube_video'
         AND p.key1 = yv_meta.video_id
         AND p.key2 = ''
        LEFT JOIN podcasts.episodes pe_meta
          ON p.platform = 'podcast_episode'
         AND p.key1 = pe_meta.id
         AND p.key2 = ''
        LEFT JOIN podcasts.shows ps_meta
          ON p.platform = 'podcast_episode'
         AND pe_meta.podcast_id = ps_meta.id
        WHERE p.post_id = ANY(%s)
        ORDER BY p.post_id
    """


def _sql_fetch_hits_for_ids() -> str:
    return """
        WITH term_ids AS (
            SELECT id, name
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT
            ph.post_id,
            ph.term_id,
            t.name AS term_name,
            ph.match_start,
            ph.match_end
        FROM matches.post_term_hit ph
        JOIN term_ids t
          ON t.id = ph.term_id
        WHERE ph.post_id = ANY(%s)
        ORDER BY ph.post_id, ph.match_start, ph.match_end, ph.term_id
    """


def count_posts_with_hits(terms: list[str]) -> int:
    if not terms:
        return 0
    sql = """
        WITH term_ids AS (
            SELECT id
            FROM taxonomy.vaccine_term
            WHERE name = ANY(%s)
        )
        SELECT count(DISTINCT ph.post_id)
        FROM matches.post_term_hit ph
        WHERE ph.term_id IN (SELECT id FROM term_ids)
    """
    with getcursor() as cur:
        cur.execute(sql, (terms,))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def iter_post_chunks(
    terms: list[str],
    *,
    posts_per_chunk: int = 50,
    row_fetch_size: int = 2000,
) -> Iterator[list[dict[str, Any]]]:
    if not terms:
        return
    sql_post_id_page = _sql_fetch_post_id_page()
    sql_posts_for_ids = _sql_fetch_posts_for_ids()
    sql_hits_for_ids = _sql_fetch_hits_for_ids()
    chunk: list[dict[str, Any]] = []
    last_post_id = 0

    with getcursor() as cur_ids, getcursor() as cur_posts, getcursor() as cur_hits:
        while True:
            cur_ids.execute(sql_post_id_page, (terms, last_post_id, row_fetch_size))
            id_rows = cur_ids.fetchall()
            if not id_rows:
                break
            post_ids = [int(r[0]) for r in id_rows if r and r[0] is not None]
            if not post_ids:
                break
            last_post_id = post_ids[-1]

            cur_posts.execute(sql_posts_for_ids, (post_ids,))
            posts_by_id: dict[int, dict[str, Any]] = {}
            for row in cur_posts.fetchall():
                try:
                    (
                        post_id,
                        platform,
                        key1,
                        key2,
                        date_entered,
                        created_at_ts,
                        text,
                        tsv_en,
                        is_en,
                        primary_metric,
                        url,
                        reddit_submission_title,
                        reddit_comment_submission_title,
                        telegram_channel,
                        youtube_video_title,
                        podcast_name,
                    ) = row
                except Exception as e:
                    print(f"[warn] skipping malformed DB post row: {e}", file=sys.stderr, flush=True)
                    continue
                posts_by_id[int(post_id)] = {
                    "post_id": post_id,
                    "platform": platform,
                    "key1": key1,
                    "key2": key2,
                    "date_entered": _json_val(_ensure_utc(date_entered)),
                    "created_at_ts": _json_val(_ensure_utc(created_at_ts)),
                    "text": text,
                    "tsv_en": str(tsv_en) if tsv_en is not None else None,
                    "is_en": is_en,
                    "primary_metric": primary_metric,
                    "url": url,
                    "reddit_submission_title": reddit_submission_title,
                    "reddit_comment_submission_title": reddit_comment_submission_title,
                    "telegram_channel": telegram_channel,
                    "youtube_video_title": youtube_video_title,
                    "podcast_name": podcast_name,
                    "hits": [],
                }

            cur_hits.execute(sql_hits_for_ids, (terms, post_ids))
            for row in cur_hits.fetchall():
                try:
                    post_id, term_id, term_name, match_start, match_end = row
                except Exception as e:
                    print(f"[warn] skipping malformed DB hit row: {e}", file=sys.stderr, flush=True)
                    continue
                post = posts_by_id.get(int(post_id))
                if post is None:
                    print(
                        f"[warn] skipping orphaned hit row for post_id={post_id}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                post["hits"].append(
                    {
                        "term_id": term_id,
                        "term_name": term_name,
                        "match_start": match_start,
                        "match_end": match_end,
                    }
                )

            for post_id in post_ids:
                post = posts_by_id.get(int(post_id))
                if post is None:
                    continue
                if not post["hits"]:
                    continue
                chunk.append(post)
                if len(chunk) >= posts_per_chunk:
                    yield chunk
                    chunk = []

    if chunk:
        yield chunk


def fetch_posts_with_hits(terms: list[str]) -> list[dict[str, Any]]:
    # Retained for compatibility with older callers/tests.
    posts: list[dict[str, Any]] = []
    for chunk in iter_post_chunks(terms):
        posts.extend(chunk)
    return posts


class PostsJsonStreamWriter:
    """
    Stream posts into a JSON object. Writes ``*.tmp`` then caller renames on success.

    Layout::

      { "generated_at_utc", "terms", "matched_post_count", "posts": [ ... ],
        "post_count", "skipped_post_count", "context_count" }
    """

    def __init__(
        self,
        final_path: Path,
        *,
        generated_at_utc: str,
        terms: list[str],
        matched_post_count: int,
    ) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        self.final_path = final_path
        self.tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        self._f = self.tmp_path.open("w", encoding="utf-8")
        self._first_post = True
        self._json_closed = False
        self.written_posts = 0
        self.context_count = 0
        lines = [
            "{",
            f'  "generated_at_utc": {json.dumps(generated_at_utc, ensure_ascii=False)},',
            f'  "terms": {json.dumps(terms, ensure_ascii=False)},',
            f'  "matched_post_count": {matched_post_count},',
            '  "posts": [',
        ]
        self._f.write("\n".join(lines) + "\n")

    def write_post(self, post: dict[str, Any]) -> None:
        if not self._first_post:
            self._f.write(",\n")
        self._first_post = False
        blob = json.dumps(post, ensure_ascii=False, indent=2)
        indented = "\n".join("    " + line for line in blob.splitlines())
        self._f.write(indented)
        self.written_posts += 1
        ctx = post.get("contexts")
        if isinstance(ctx, list):
            self.context_count += len(ctx)
        self._f.flush()

    def finalize(self, *, skipped_post_count: int) -> None:
        if self._json_closed:
            return
        tail = [
            "",
            "  ],",
            f'  "post_count": {self.written_posts},',
            f'  "skipped_post_count": {skipped_post_count},',
            f'  "context_count": {self.context_count}',
            "}\n",
        ]
        self._f.write("\n".join(tail))
        self._f.flush()
        self._f.close()
        self._json_closed = True
        self.tmp_path.replace(self.final_path)

    def abort_keep_partial(self) -> None:
        try:
            if not self._json_closed:
                # Keep partial data recoverable as valid JSON.
                self._f.write("\n  ]\n}\n")
                self._json_closed = True
            self._f.flush()
            self._f.close()
        except Exception:
            pass
        print(
            f"[warn] partial output (may be invalid JSON): {self.tmp_path.resolve()}",
            flush=True,
        )


def _reset_coref_runtime(coref_module: Any) -> None:
    try:
        if hasattr(coref_module, "_NLP"):
            coref_module._NLP = None
    except Exception:
        pass
    _maybe_cuda_gc()
    gc.collect()


def _process_microbatch_with_fallback(
    posts: list[dict[str, Any]],
    *,
    coref_payload,
    trim_payload,
    coref_module: Any,
) -> tuple[list[dict[str, Any]], int]:
    if not posts:
        return [], 0
    payload = {"posts": posts}
    try:
        payload = coref_payload(payload, progress=False)
        payload = trim_payload(payload, progress=False)
        out = payload.get("posts")
        return (out if isinstance(out, list) else []), 0
    except Exception as e:
        print(f"[warn] microbatch failed ({len(posts)} posts), resetting coref + retrying per-post: {e}", file=sys.stderr, flush=True)
        _reset_coref_runtime(coref_module)
    out_posts: list[dict[str, Any]] = []
    skipped = 0
    for post in posts:
        try:
            pld = {"posts": [post]}
            pld = coref_payload(pld, progress=False)
            pld = trim_payload(pld, progress=False)
            rows = pld.get("posts")
            if isinstance(rows, list) and rows:
                out_posts.append(rows[0])
            else:
                skipped += 1
        except Exception as e:
            # One more chance after forcing a full coref reload.
            try:
                _reset_coref_runtime(coref_module)
                pld = {"posts": [post]}
                pld = coref_payload(pld, progress=False)
                pld = trim_payload(pld, progress=False)
                rows = pld.get("posts")
                if isinstance(rows, list) and rows:
                    out_posts.append(rows[0])
                    continue
            except Exception:
                pass
            skipped += 1
            print(
                f"[warn] skipping post_id={post.get('post_id', '<unknown>')} after retry failure: {e}",
                file=sys.stderr,
                flush=True,
            )
    return out_posts, skipped


def _maybe_cuda_gc() -> None:
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        pass


def _load_pipeline_processors() -> tuple[Any, Any, Any]:
    try:
        coref_module = importlib.import_module("apps.claim_extractor.coreference_resolution")
        trim_module = importlib.import_module("apps.claim_extractor.trim_transcripts")
        return coref_module, coref_module.process_payload, trim_module.process_payload
    except ImportError as e:
        print(
            "Import failed (claim_extractor pipeline). From repo root, install deps:\n"
            "  pip install -r requirements.txt\n"
            "  pip install -r apps/claim_extractor/requirements-coref.txt",
            file=sys.stderr,
        )
        raise e


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _default_enrich_state_path(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".enrich_state.json")


def _default_enrich_jsonl_path(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".enrich.jsonl")


def _append_posts_jsonl(path: Path, posts: list[dict[str, Any]]) -> int:
    if not posts:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post, ensure_ascii=False) + "\n")
    return len(posts)


def _finalize_enrich_json_from_jsonl(
    *,
    jsonl_path: Path,
    out_path: Path,
    terms: list[str],
    matched_post_count: int,
    skipped_post_count: int,
) -> int:
    writer = PostsJsonStreamWriter(
        out_path,
        generated_at_utc=_utc_now().isoformat(),
        terms=terms,
        matched_post_count=matched_post_count,
    )
    written = 0
    try:
        if jsonl_path.exists():
            with jsonl_path.open("r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, start=1):
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        post = json.loads(s)
                    except Exception as e:
                        print(
                            f"[warn] skipping invalid enrich jsonl line {line_no}: {e}",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue
                    if isinstance(post, dict):
                        writer.write_post(post)
                        written += 1
        writer.finalize(skipped_post_count=skipped_post_count)
    except BaseException:
        writer.abort_keep_partial()
        raise
    return written


def _enrich_worker_entry(
    posts: list[dict[str, Any]],
    micro_batch: int,
    result_q: mp.Queue,
) -> None:
    try:
        _silence_third_party_progress()
        coref_module, coref_payload, trim_payload = _load_pipeline_processors()
        processed_all: list[dict[str, Any]] = []
        skipped_total = 0
        for i in range(0, len(posts), micro_batch):
            micro = posts[i : i + micro_batch]
            processed, skipped = _process_microbatch_with_fallback(
                micro,
                coref_payload=coref_payload,
                trim_payload=trim_payload,
                coref_module=coref_module,
            )
            processed_all.extend(processed)
            skipped_total += skipped
            del micro, processed
            _maybe_cuda_gc()
        result_q.put({"ok": True, "posts": processed_all, "skipped": skipped_total})
    except BaseException as e:
        result_q.put({"ok": False, "error": f"{type(e).__name__}: {e}"})


def _run_enrich_worker_with_timeout(
    posts: list[dict[str, Any]],
    *,
    micro_batch: int,
    timeout_sec: int,
) -> tuple[bool, list[dict[str, Any]], int, str]:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue(maxsize=1)
    proc = ctx.Process(target=_enrich_worker_entry, args=(posts, micro_batch, q), daemon=True)
    proc.start()
    proc.join(timeout=max(1, int(timeout_sec)))
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        return False, [], 0, "worker timeout"
    if proc.exitcode not in (0, None):
        return False, [], 0, f"worker exit code {proc.exitcode}"
    try:
        msg = q.get_nowait()
    except Exception:
        return False, [], 0, "worker returned no result"
    if not isinstance(msg, dict):
        return False, [], 0, "worker returned malformed result"
    if not msg.get("ok"):
        return False, [], 0, str(msg.get("error", "worker failed"))
    posts_out = msg.get("posts")
    skipped = msg.get("skipped", 0)
    if not isinstance(posts_out, list):
        return False, [], 0, "worker result missing posts list"
    return True, posts_out, int(skipped), ""


def stream_fetch_only_and_write_raw(
    terms: list[str],
    raw_out_path: Path,
    *,
    db_posts_per_chunk: int,
    row_fetch_size: int,
    progress_every: int,
) -> int:
    matched_total = count_posts_with_hits(terms)
    print(f"[fetch] {matched_total} posts (matched in DB); writing raw file", flush=True)
    if matched_total == 0:
        writer = PostsJsonStreamWriter(
            raw_out_path,
            generated_at_utc=_utc_now().isoformat(),
            terms=terms,
            matched_post_count=0,
        )
        writer.finalize(skipped_post_count=0)
        return 0

    writer = PostsJsonStreamWriter(
        raw_out_path,
        generated_at_utc=_utc_now().isoformat(),
        terms=terms,
        matched_post_count=matched_total,
    )
    started = time.monotonic()
    fetched_total = 0
    next_progress_mark = max(1, int(progress_every))

    def _print_progress() -> None:
        elapsed = max(0.001, time.monotonic() - started)
        rate = fetched_total / elapsed
        remaining = max(0, matched_total - fetched_total)
        eta_s = int(remaining / rate) if rate > 0 else -1
        eta_txt = f"{eta_s}s" if eta_s >= 0 else "unknown"
        print(
            f"[progress] fetch {fetched_total}/{matched_total} posts; eta={eta_txt}",
            flush=True,
        )

    try:
        for chunk in iter_post_chunks(
            terms,
            posts_per_chunk=db_posts_per_chunk,
            row_fetch_size=row_fetch_size,
        ):
            for post in chunk:
                writer.write_post(post)
            fetched_total += len(chunk)
            if fetched_total >= next_progress_mark:
                _print_progress()
                next_progress_mark += max(1, int(progress_every))
            del chunk
        writer.finalize(skipped_post_count=0)
    except BaseException:
        writer.abort_keep_partial()
        raise
    _print_progress()
    return writer.written_posts


def enrich_from_raw_and_write(
    raw_in_path: Path,
    out_path: Path,
    *,
    enrich_window: int,
    micro_batch: int,
    progress_every: int,
    state_path: Path,
    jsonl_path: Path,
    restart_enrich: bool,
    worker_timeout_sec: int,
) -> tuple[int, int]:
    raw_payload = json.loads(raw_in_path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        raise ValueError(f"Raw input must be a JSON object: {raw_in_path}")
    posts = raw_payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"Raw input missing top-level posts list: {raw_in_path}")
    terms = raw_payload.get("terms")
    if not isinstance(terms, list):
        terms = []
    matched_total = len(posts)
    terms_out = [str(t) for t in terms]
    print(f"[enrich] {matched_total} raw posts loaded from {raw_in_path.resolve()}", flush=True)

    if restart_enrich:
        for p in (state_path, jsonl_path, out_path):
            if p.exists():
                p.unlink()

    start_index = 0
    skipped_total = 0
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                start_index = int(state.get("next_index", 0))
                skipped_total = int(state.get("skipped_total", 0))
                print(f"[enrich] resuming at post index {start_index}", flush=True)
        except Exception as e:
            print(f"[warn] ignoring unreadable enrich state file: {e}", file=sys.stderr, flush=True)

    if start_index > matched_total:
        start_index = matched_total

    started = time.monotonic()
    processed_total = start_index
    next_progress_mark = max(1, int(progress_every))
    while next_progress_mark <= processed_total:
        next_progress_mark += max(1, int(progress_every))

    def _print_progress() -> None:
        elapsed = max(0.001, time.monotonic() - started)
        rate = processed_total / elapsed
        remaining = max(0, matched_total - processed_total)
        eta_s = int(remaining / rate) if rate > 0 else -1
        eta_txt = f"{eta_s}s" if eta_s >= 0 else "unknown"
        print(
            f"[progress] enrich {processed_total}/{matched_total} posts; eta={eta_txt}",
            flush=True,
        )

    window_size = max(1, int(enrich_window))
    i = start_index
    while i < matched_total:
        remaining = matched_total - i
        size = min(window_size, remaining)
        success = False
        while size >= 1:
            window_posts = posts[i : i + size]
            ok, processed_posts, skipped, err = _run_enrich_worker_with_timeout(
                window_posts,
                micro_batch=micro_batch,
                timeout_sec=worker_timeout_sec,
            )
            if ok:
                _append_posts_jsonl(jsonl_path, processed_posts)
                skipped_total += skipped
                i += size
                processed_total = i
                _atomic_write_json(
                    state_path,
                    {
                        "next_index": i,
                        "skipped_total": skipped_total,
                        "matched_total": matched_total,
                        "raw_in_path": str(raw_in_path),
                        "jsonl_path": str(jsonl_path),
                        "out_path": str(out_path),
                    },
                )
                if processed_total >= next_progress_mark:
                    _print_progress()
                    next_progress_mark += max(1, int(progress_every))
                success = True
                break
            print(
                f"[warn] enrich worker failed at index={i} size={size}: {err}",
                file=sys.stderr,
                flush=True,
            )
            if size == 1:
                skipped_total += 1
                i += 1
                processed_total = i
                _atomic_write_json(
                    state_path,
                    {
                        "next_index": i,
                        "skipped_total": skipped_total,
                        "matched_total": matched_total,
                        "raw_in_path": str(raw_in_path),
                        "jsonl_path": str(jsonl_path),
                        "out_path": str(out_path),
                    },
                )
                success = True
                break
            size = max(1, size // 2)
        if not success:
            raise RuntimeError("enrich loop made no progress")

    written = _finalize_enrich_json_from_jsonl(
        jsonl_path=jsonl_path,
        out_path=out_path,
        terms=terms_out,
        matched_post_count=matched_total,
        skipped_post_count=skipped_total,
    )
    if state_path.exists():
        state_path.unlink()
    _print_progress()
    return written, skipped_total


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(prog="python -m scripts.get_posts_for_term")
    ap.add_argument("--prod", action="store_true", help="Use prod DB credentials (DEV_* vs PROD_*).")
    ap.add_argument(
        "--stage",
        choices=("all", "fetch", "enrich"),
        default="all",
        help="Pipeline stage to run: all (default), fetch (DB-only), or enrich (coref+trim from raw).",
    )
    ap.add_argument(
        "--raw-out",
        type=Path,
        default=DEFAULT_RAW_OUT,
        help=f"Raw DB fetch JSON path (default: {DEFAULT_RAW_OUT}).",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Final enriched JSON path (default: {DEFAULT_OUT}).",
    )
    ap.add_argument(
        "--recheck-fetch",
        action="store_true",
        help="Force re-running DB fetch and overwrite --raw-out even if it exists.",
    )
    ap.add_argument(
        "--terms",
        nargs="*",
        default=[],
        metavar="TERM",
        help="Search terms (vaccine_term.name). Repeatable; merged with --terms-file.",
    )
    ap.add_argument(
        "--terms-file",
        type=Path,
        default=None,
        help="Optional file: one term per line (# comments allowed).",
    )
    ap.add_argument(
        "--no-default-terms",
        action="store_true",
        help="Do not use the built-in default term list when --terms / --terms-file are empty.",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        metavar="N",
        help=f"Print progress + ETA every N processed posts (default: {DEFAULT_PROGRESS_EVERY}).",
    )
    ap.add_argument(
        "--db-chunk-posts",
        type=int,
        default=50,
        metavar="N",
        help="Max posts to accumulate from the DB cursor before processing (default: 50).",
    )
    ap.add_argument(
        "--db-fetch-rows",
        type=int,
        default=2000,
        metavar="N",
        help="Row fetchmany size for the DB cursor (default: 2000).",
    )
    ap.add_argument(
        "--micro-batch",
        type=int,
        default=2,
        metavar="N",
        help="Posts per coref+trim GPU/CPU batch (default: 2; lower uses less RAM).",
    )
    ap.add_argument(
        "--enrich-window",
        type=int,
        default=DEFAULT_ENRICH_WINDOW,
        metavar="N",
        help=f"Posts per enrich worker process before restart (default: {DEFAULT_ENRICH_WINDOW}).",
    )
    ap.add_argument(
        "--worker-timeout-sec",
        type=int,
        default=DEFAULT_ENRICH_TIMEOUT_SEC,
        metavar="SEC",
        help=f"Per enrich worker timeout seconds (default: {DEFAULT_ENRICH_TIMEOUT_SEC}).",
    )
    ap.add_argument(
        "--enrich-state",
        type=Path,
        default=None,
        help="Optional enrich state file path for resume (default: <out>.enrich_state.json).",
    )
    ap.add_argument(
        "--restart-enrich",
        action="store_true",
        help="Discard existing enrich state/jsonl/output and restart enrich from index 0.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    terms = _collect_terms(list(args.terms), args.terms_file)
    if not terms and not args.no_default_terms:
        terms = list(DEFAULT_TERMS)

    _silence_third_party_progress()
    db_posts_per_chunk = max(1, int(args.db_chunk_posts))
    row_fetch_size = max(1, int(args.db_fetch_rows))
    micro_batch = max(1, int(args.micro_batch))
    progress_every = max(1, int(args.progress_every))
    enrich_window = max(1, int(args.enrich_window))
    worker_timeout_sec = max(30, int(args.worker_timeout_sec))
    enrich_state_path = args.enrich_state if args.enrich_state is not None else _default_enrich_state_path(args.out)
    enrich_jsonl_path = _default_enrich_jsonl_path(args.out)

    need_fetch = args.stage in ("all", "fetch") and (args.recheck_fetch or not args.raw_out.exists())

    if args.stage == "enrich" and not args.raw_out.exists():
        raise SystemExit(f"--stage enrich requires raw input file: {args.raw_out}")

    if args.stage in ("all", "fetch"):
        if need_fetch:
            init_pool(prefix="prod" if args.prod else "dev")
            try:
                fetched = stream_fetch_only_and_write_raw(
                    terms,
                    args.raw_out,
                    db_posts_per_chunk=db_posts_per_chunk,
                    row_fetch_size=row_fetch_size,
                    progress_every=progress_every,
                )
            finally:
                close_pool()
            print(f"[ok] fetched {fetched} raw posts → {args.raw_out.resolve()}", flush=True)
        else:
            print(f"[fetch] reusing existing raw file (skip DB fetch): {args.raw_out.resolve()}", flush=True)

    if args.stage in ("all", "enrich"):
        written, skipped = enrich_from_raw_and_write(
            args.raw_out,
            args.out,
            enrich_window=enrich_window,
            micro_batch=micro_batch,
            progress_every=progress_every,
            state_path=enrich_state_path,
            jsonl_path=enrich_jsonl_path,
            restart_enrich=bool(args.restart_enrich),
            worker_timeout_sec=worker_timeout_sec,
        )
        if skipped:
            print(f"[warn] skipped {skipped} posts after retries", flush=True)
        print(
            f"[ok] wrote {written} enriched posts, see context_count in JSON → {args.out.resolve()}",
            flush=True,
        )


if __name__ == "__main__":
    main()
