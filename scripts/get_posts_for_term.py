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
  python -m scripts.get_posts_for_term
  python -m scripts.get_posts_for_term --prod --out data/mm.json
  python -m scripts.get_posts_for_term --terms measles mmr --terms-file more_terms.txt
  python -m scripts.get_posts_for_term --no-progress
"""

from __future__ import annotations

import argparse
import json
import os
import sys
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


def _silence_third_party_progress() -> None:
    """Stop HF/datasets/transformers from spawning tqdm bars that fight our single bar."""
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("HF_DATASETS_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TRANSFORMERS_NO_ADVISORY_WARNINGS", "1")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
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


def _sql_fetch_rows() -> str:
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
            vt.id AS term_id,
            vt.name AS term_name,
            ph.match_start,
            ph.match_end
        FROM sm.posts_all p
        JOIN matches.post_term_hit ph
          ON ph.post_id = p.post_id
        JOIN taxonomy.vaccine_term vt
          ON vt.id = ph.term_id
        WHERE vt.name = ANY(%s)
        ORDER BY p.date_entered DESC NULLS LAST, p.post_id, ph.match_start, ph.match_end, vt.id
    """


def count_posts_with_hits(terms: list[str]) -> int:
    if not terms:
        return 0
    sql = """
        SELECT count(DISTINCT p.post_id)
        FROM sm.posts_all p
        JOIN matches.post_term_hit ph
          ON ph.post_id = p.post_id
        JOIN taxonomy.vaccine_term vt
          ON vt.id = ph.term_id
        WHERE vt.name = ANY(%s)
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
    sql = _sql_fetch_rows()
    current_post_id: Optional[int] = None
    current_post: Optional[dict[str, Any]] = None
    chunk: list[dict[str, Any]] = []

    with getcursor() as cur:
        cur.execute(sql, (terms,))
        while True:
            rows = cur.fetchmany(row_fetch_size)
            if not rows:
                break
            for row in rows:
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
                        term_id,
                        term_name,
                        match_start,
                        match_end,
                    ) = row
                except Exception as e:
                    print(f"[warn] skipping malformed DB row: {e}", file=sys.stderr, flush=True)
                    continue

                hit = {
                    "term_id": term_id,
                    "term_name": term_name,
                    "match_start": match_start,
                    "match_end": match_end,
                }

                if current_post_id != post_id:
                    if current_post is not None:
                        chunk.append(current_post)
                        if len(chunk) >= posts_per_chunk:
                            yield chunk
                            chunk = []
                    current_post_id = post_id
                    current_post = {
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
                        "hits": [hit],
                    }
                else:
                    if current_post is None:
                        print(
                            f"[warn] skipping orphaned hit row for post_id={post_id}",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue
                    current_post["hits"].append(hit)

    if current_post is not None:
        chunk.append(current_post)
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
        self.tmp_path.replace(self.final_path)

    def abort_keep_partial(self) -> None:
        try:
            self._f.flush()
            self._f.close()
        except Exception:
            pass
        print(
            f"[warn] partial output (may be invalid JSON): {self.tmp_path.resolve()}",
            flush=True,
        )


def _process_microbatch_with_fallback(
    posts: list[dict[str, Any]],
    *,
    coref_payload,
    trim_payload,
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
        print(f"[warn] microbatch failed ({len(posts)} posts), retrying per-post: {e}", file=sys.stderr, flush=True)
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
    show_progress: bool,
    db_posts_per_chunk: int,
    row_fetch_size: int,
    micro_batch: int,
) -> tuple[int, int]:
    """
    Stream DB → micro-batch coref+trim → append JSON. Returns (written_posts, skipped_posts).
    """
    try:
        from apps.claim_extractor.coreference_resolution import process_payload as coref_payload
        from apps.claim_extractor.trim_transcripts import process_payload as trim_payload
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
    pbar = None
    if show_progress:
        from tqdm import tqdm

        pbar = tqdm(
            total=matched_total,
            desc="pipeline",
            unit="post",
            leave=True,
            mininterval=0.5,
            dynamic_ncols=True,
            file=sys.stderr,
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
                    micro, coref_payload=coref_payload, trim_payload=trim_payload
                )
                skipped_total += skipped
                for post in processed:
                    writer.write_post(post)
                if pbar is not None:
                    pbar.update(len(micro))
                del micro, processed
                _maybe_cuda_gc()
            del chunk
        writer.finalize(skipped_post_count=skipped_total)
    except BaseException:
        writer.abort_keep_partial()
        raise
    finally:
        if pbar is not None:
            pbar.close()
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
        "--no-progress",
        action="store_true",
        help="Disable the single global progress bar.",
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
    show_progress = not args.no_progress
    init_pool(prefix="prod" if args.prod else "dev")
    try:
        written, skipped = stream_fetch_enrich_and_write(
            terms,
            args.out,
            show_progress=show_progress,
            db_posts_per_chunk=max(1, int(args.db_chunk_posts)),
            row_fetch_size=max(1, int(args.db_fetch_rows)),
            micro_batch=max(1, int(args.micro_batch)),
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
