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
  python -m apps.claim_extractor.get_posts_for_term --prod --out data/mm.json
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
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from db.db import close_pool, getcursor, init_pool

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = PROJECT_ROOT / "data" / "posts_for_term.json"

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


def stream_fetch_enrich_and_write(
    terms: list[str],
    out_path: Path,
    *,
    db_posts_per_chunk: int,
    row_fetch_size: int,
    micro_batch: int,
    progress_every: int,
) -> tuple[int, int]:
    """
    Stream DB → micro-batch coref+trim → append JSON. Returns (written_posts, skipped_posts).
    """
    try:
        coref_module = importlib.import_module("apps.claim_extractor.coreference_resolution")
        trim_module = importlib.import_module("apps.claim_extractor.trim_transcripts")
        coref_payload = coref_module.process_payload
        trim_payload = trim_module.process_payload
    except ImportError as e:
        print(
            "Import failed (claim_extractor pipeline). From repo root, install deps:\n"
            "  pip install -r requirements.txt\n"
            "  pip install -r apps/claim_extractor/requirements-coref.txt",
            file=sys.stderr,
        )
        raise e

    matched_total = count_posts_with_hits(terms)
    print(f"[fetch] {matched_total} posts (matched in DB)", flush=True)
    if matched_total == 0:
        writer = PostsJsonStreamWriter(
            out_path,
            generated_at_utc=_utc_now().isoformat(),
            terms=terms,
            matched_post_count=0,
        )
        writer.finalize(skipped_post_count=0)
        return 0, 0

    writer = PostsJsonStreamWriter(
        out_path,
        generated_at_utc=_utc_now().isoformat(),
        terms=terms,
        matched_post_count=matched_total,
    )
    skipped_total = 0
    started = time.monotonic()
    processed_total = 0
    next_progress_mark = max(1, int(progress_every))

    def _print_progress() -> None:
        elapsed = max(0.001, time.monotonic() - started)
        rate = processed_total / elapsed
        remaining = max(0, matched_total - processed_total)
        eta_s = int(remaining / rate) if rate > 0 else -1
        eta_txt = f"{eta_s}s" if eta_s >= 0 else "unknown"
        print(
            f"[progress] {processed_total}/{matched_total} posts complete; eta={eta_txt}",
            flush=True,
        )

    try:
        for chunk in iter_post_chunks(
            terms,
            posts_per_chunk=db_posts_per_chunk,
            row_fetch_size=row_fetch_size,
        ):
            for i in range(0, len(chunk), micro_batch):
                micro = chunk[i : i + micro_batch]
                processed, skipped = _process_microbatch_with_fallback(
                    micro,
                    coref_payload=coref_payload,
                    trim_payload=trim_payload,
                    coref_module=coref_module,
                )
                skipped_total += skipped
                for post in processed:
                    writer.write_post(post)
                processed_total += len(micro)
                if processed_total >= next_progress_mark:
                    _print_progress()
                    next_progress_mark += max(1, int(progress_every))
                del micro, processed
                _maybe_cuda_gc()
            del chunk
        writer.finalize(skipped_post_count=skipped_total)
    except BaseException:
        writer.abort_keep_partial()
        raise
    _print_progress()
    return writer.written_posts, skipped_total


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(prog="python -m scripts.get_posts_for_term")
    ap.add_argument("--prod", action="store_true", help="Use prod DB credentials (DEV_* vs PROD_*).")
    ap.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output JSON path (default: {DEFAULT_OUT}).",
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
    args = ap.parse_args(list(argv) if argv is not None else None)

    terms = _collect_terms(list(args.terms), args.terms_file)
    if not terms and not args.no_default_terms:
        terms = list(DEFAULT_TERMS)

    _silence_third_party_progress()
    init_pool(prefix="prod" if args.prod else "dev")
    try:
        written, skipped = stream_fetch_enrich_and_write(
            terms,
            args.out,
            db_posts_per_chunk=max(1, int(args.db_chunk_posts)),
            row_fetch_size=max(1, int(args.db_fetch_rows)),
            micro_batch=max(1, int(args.micro_batch)),
            progress_every=max(1, int(args.progress_every)),
        )
        if skipped:
            print(f"[warn] skipped {skipped} posts after retries", flush=True)
        print(
            f"[ok] wrote {written} posts, see context_count in JSON → {args.out.resolve()}",
            flush=True,
        )
    finally:
        close_pool()


if __name__ == "__main__":
    main()
