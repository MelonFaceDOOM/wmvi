"""
Search-term post pipeline: DB fetch -> sentence-boundary trim -> coreference resolution.

Run from the repository root (``wmvi``). Default paths are under ``data/`` at the repo root:
``posts_for_term_raw.json``, ``posts_for_term_trimmed.json``, ``posts_for_term.json``.
Resume coref uses ``<out>.coref.jsonl`` and ``<out>.coref_state.json`` next to ``--out``.

Standard use cases:

  # Full run (dev DB): fetch raw posts, write trimmed intermediates, run coref -> final JSON
  python -m apps.claim_extractor.run_term_pipeline

  # Same against prod DB credentials
  python -m apps.claim_extractor.run_term_pipeline --prod

  # Custom search terms (otherwise built-in defaults apply unless --no-default-terms)
  python -m apps.claim_extractor.run_term_pipeline --terms measles "mmr vaccine"

  # Terms from a file (one term per line; ``#`` starts a comment line)
  python -m apps.claim_extractor.run_term_pipeline --terms-file path/to/terms.txt

  # Run only one stage (expects prior artifacts on disk as noted)
  python -m apps.claim_extractor.run_term_pipeline --stage fetch
  python -m apps.claim_extractor.run_term_pipeline --stage trim      # needs --raw-out
  python -m apps.claim_extractor.run_term_pipeline --stage coref     # needs --trimmed-out

  # Custom output locations (trim/coref follow these names)
  python -m apps.claim_extractor.run_term_pipeline \\
    --raw-out data/my_raw.json \\
    --trimmed-out data/my_trimmed.json \\
    --out data/my_coref.json

Coref tuning (optional environment variables; see ``coreference_resolution.py``):

  COREF_MAX_CHARS=12000 COREF_PIPE_BATCH_SIZE=8 \\
    python -m apps.claim_extractor.run_term_pipeline --prod --stage coref
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from apps.claim_extractor.coreference_resolution import iter_coref_resolved_posts
from apps.claim_extractor.trim_transcripts import trim_sentence_boundary
from db.db import close_pool, init_pool
from scripts.get_posts_for_search_term import count_posts_with_hits, iter_posts_for_terms

# This file: <repo>/apps/claim_extractor/run_term_pipeline.py  ->  parents[2] is repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "data" / "posts_for_term.json"
DEFAULT_RAW_OUT = REPO_ROOT / "data" / "posts_for_term_raw.json"
DEFAULT_TRIMMED_OUT = REPO_ROOT / "data" / "posts_for_term_trimmed.json"
DEFAULT_COREF_BATCH_SIZE = 8
DEFAULT_PROGRESS_EVERY = 100

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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_terms_file(path: Path) -> list[str]:
    terms: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            terms.append(s)
    return terms


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


class PostsJsonStreamWriter:
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
        self._f.write(
            "\n".join(
                [
                    "{",
                    f'  "generated_at_utc": {json.dumps(generated_at_utc, ensure_ascii=False)},',
                    f'  "terms": {json.dumps(terms, ensure_ascii=False)},',
                    f'  "matched_post_count": {matched_post_count},',
                    '  "posts": [',
                ]
            )
            + "\n"
        )

    def write_post(self, post: dict[str, Any]) -> None:
        if not self._first_post:
            self._f.write(",\n")
        self._first_post = False
        blob = json.dumps(post, ensure_ascii=False, indent=2)
        self._f.write("\n".join("    " + line for line in blob.splitlines()))
        self.written_posts += 1
        self._f.flush()

    def finalize(self, *, skipped_post_count: int) -> None:
        if self._json_closed:
            return
        self._f.write(
            "\n".join(
                [
                    "",
                    "  ],",
                    f'  "post_count": {self.written_posts},',
                    f'  "skipped_post_count": {skipped_post_count}',
                    "}\n",
                ]
            )
        )
        self._f.flush()
        self._f.close()
        self._json_closed = True
        self.tmp_path.replace(self.final_path)

    def abort_keep_partial(self) -> None:
        try:
            if not self._json_closed:
                self._f.write("\n  ]\n}\n")
            self._f.flush()
            self._f.close()
        except Exception:
            pass
        print(f"[warn] partial output written: {self.tmp_path.resolve()}", flush=True)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _default_coref_state_path(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".coref_state.json")


def _default_coref_jsonl_path(out_path: Path) -> Path:
    return out_path.with_suffix(out_path.suffix + ".coref.jsonl")


def _append_posts_jsonl(path: Path, posts: list[dict[str, Any]]) -> int:
    if not posts:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for post in posts:
            f.write(json.dumps(post, ensure_ascii=False) + "\n")
    return len(posts)


def _finalize_json_from_jsonl(
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
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    post = json.loads(s)
                    if isinstance(post, dict):
                        writer.write_post(post)
                        written += 1
        writer.finalize(skipped_post_count=skipped_post_count)
    except BaseException:
        writer.abort_keep_partial()
        raise
    return written


def stream_fetch_and_write_raw(
    terms: list[str],
    raw_out_path: Path,
    *,
    row_fetch_size: int,
    progress_every: int,
    use_prod: bool,
) -> int:
    matched_total = count_posts_with_hits(terms)
    print(f"[fetch] {matched_total} posts matched in DB; writing raw file", flush=True)
    writer = PostsJsonStreamWriter(
        raw_out_path,
        generated_at_utc=_utc_now().isoformat(),
        terms=terms,
        matched_post_count=matched_total,
    )
    if matched_total == 0:
        writer.finalize(skipped_post_count=0)
        return 0

    started = time.monotonic()
    fetched_total = 0
    next_progress_mark = max(1, int(progress_every))

    def _print_progress() -> None:
        elapsed = max(0.001, time.monotonic() - started)
        rate = fetched_total / elapsed
        remaining = max(0, matched_total - fetched_total)
        eta_s = int(remaining / rate) if rate > 0 else -1
        eta_txt = f"{eta_s}s" if eta_s >= 0 else "unknown"
        print(f"[progress] fetch {fetched_total}/{matched_total} posts; eta={eta_txt}", flush=True)

    try:
        for post in iter_posts_for_terms(terms, use_prod=use_prod, row_fetch_size=row_fetch_size):
            writer.write_post(post)
            fetched_total += 1
            if fetched_total >= next_progress_mark:
                _print_progress()
                next_progress_mark += max(1, int(progress_every))
        writer.finalize(skipped_post_count=0)
    except BaseException:
        writer.abort_keep_partial()
        raise
    _print_progress()
    return writer.written_posts


def stream_trim_from_raw(
    raw_in_path: Path,
    trimmed_out_path: Path,
    *,
    progress_every: int,
) -> int:
    raw_payload = json.loads(raw_in_path.read_text(encoding="utf-8"))
    posts = raw_payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"Raw input missing top-level posts list: {raw_in_path}")
    terms = raw_payload.get("terms")
    terms_out = [str(t) for t in terms] if isinstance(terms, list) else []
    matched_total = len(posts)
    sample_keys = sorted(posts[0].keys()) if posts and isinstance(posts[0], dict) else []
    print(
        f"[trim] read {raw_in_path.resolve()} with {matched_total} posts; sample_keys={sample_keys}",
        flush=True,
    )

    writer = PostsJsonStreamWriter(
        trimmed_out_path,
        generated_at_utc=_utc_now().isoformat(),
        terms=terms_out,
        matched_post_count=matched_total,
    )
    started = time.monotonic()
    processed_total = 0
    next_progress_mark = max(1, int(progress_every))

    def _print_progress() -> None:
        elapsed = max(0.001, time.monotonic() - started)
        rate = processed_total / elapsed
        remaining = max(0, matched_total - processed_total)
        eta_s = int(remaining / rate) if rate > 0 else -1
        eta_txt = f"{eta_s}s" if eta_s >= 0 else "unknown"
        print(f"[progress] trim {processed_total}/{matched_total} posts; eta={eta_txt}", flush=True)

    try:
        for post in posts:
            if not isinstance(post, dict):
                continue
            body = post.get("text")
            hits = post.get("hits")
            chunks = trim_sentence_boundary(body if isinstance(body, str) else "", hits if isinstance(hits, list) else [])
            out_post = dict(post)
            out_post["sentence_boundary_chunks"] = chunks
            out_post["sentence_boundary_chunk_count"] = len(chunks)
            writer.write_post(out_post)
            processed_total += 1
            if processed_total >= next_progress_mark:
                _print_progress()
                next_progress_mark += max(1, int(progress_every))
        writer.finalize(skipped_post_count=max(0, matched_total - processed_total))
    except BaseException:
        writer.abort_keep_partial()
        raise

    _print_progress()
    print(f"[ok] wrote trimmed posts -> {trimmed_out_path.resolve()}", flush=True)
    return writer.written_posts


def _iter_chunk_posts(posts: list[Any], *, start_index: int = 0) -> Iterable[dict[str, Any]]:
    seen = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("sentence_boundary_chunks")
        if not isinstance(chunks, list):
            continue
        chunk_count = len(chunks)
        for idx, chunk in enumerate(chunks):
            if not isinstance(chunk, str) or not chunk.strip():
                continue
            if seen < start_index:
                seen += 1
                continue
            chunk_post = dict(post)
            # Do not duplicate all chunks inside each yielded chunk record.
            chunk_post.pop("sentence_boundary_chunks", None)
            chunk_post["source_post_id"] = post.get("post_id")
            chunk_post["sentence_boundary_chunk_index"] = idx
            chunk_post["sentence_boundary_chunk_count"] = chunk_count
            chunk_post["text"] = chunk
            seen += 1
            yield chunk_post


def _count_chunk_posts(posts: list[Any]) -> int:
    total = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        chunks = post.get("sentence_boundary_chunks")
        if not isinstance(chunks, list):
            continue
        for chunk in chunks:
            if isinstance(chunk, str) and chunk.strip():
                total += 1
    return total


def coref_from_trimmed_and_write(
    trimmed_in_path: Path,
    out_path: Path,
    *,
    coref_batch_size: int,
    progress_every: int,
    state_path: Path,
    jsonl_path: Path,
) -> tuple[int, int]:
    trimmed_payload = json.loads(trimmed_in_path.read_text(encoding="utf-8"))
    posts = trimmed_payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError(f"Trimmed input missing top-level posts list: {trimmed_in_path}")
    terms = trimmed_payload.get("terms")
    terms_out = [str(t) for t in terms] if isinstance(terms, list) else []
    sample_keys = sorted(posts[0].keys()) if posts and isinstance(posts[0], dict) else []
    print(
        f"[coref] read {trimmed_in_path.resolve()} with {len(posts)} posts; sample_keys={sample_keys}",
        flush=True,
    )

    matched_total = _count_chunk_posts(posts)
    print(f"[coref] prepared {matched_total} sentence-boundary chunks for coref", flush=True)

    start_index = 0
    skipped_total = 0
    if state_path.exists() or jsonl_path.exists():
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                start_index = int(state.get("next_index", 0))
                skipped_total = int(state.get("skipped_total", 0))
        print(f"[coref] resume detected: {start_index}/{matched_total} already processed", flush=True)
    else:
        print(f"[coref] no existing coref progress found -- starting from 0/{matched_total}", flush=True)

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
        print(f"[progress] coref {processed_total}/{matched_total} posts; eta={eta_txt}", flush=True)

    if start_index < matched_total:
        chunk_posts = _iter_chunk_posts(posts, start_index=start_index)
        for processed in iter_coref_resolved_posts(chunk_posts, batch_size=coref_batch_size):
            _append_posts_jsonl(jsonl_path, [processed])
            processed_total += 1
            _atomic_write_json(
                state_path,
                {
                    "next_index": processed_total,
                    "skipped_total": skipped_total,
                    "matched_total": matched_total,
                    "trimmed_in_path": str(trimmed_in_path),
                    "jsonl_path": str(jsonl_path),
                    "out_path": str(out_path),
                },
            )
            if processed_total >= next_progress_mark:
                _print_progress()
                next_progress_mark += max(1, int(progress_every))

    written = _finalize_json_from_jsonl(
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
    ap = argparse.ArgumentParser(prog="python -m apps.claim_extractor.run_term_pipeline")
    ap.add_argument("--prod", action="store_true", help="Use prod DB credentials.")
    ap.add_argument("--stage", choices=("all", "fetch", "trim", "coref"), default="all")
    ap.add_argument("--raw-out", type=Path, default=DEFAULT_RAW_OUT)
    ap.add_argument("--trimmed-out", type=Path, default=DEFAULT_TRIMMED_OUT)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--terms", nargs="*", default=[], metavar="TERM")
    ap.add_argument("--terms-file", type=Path, default=None)
    ap.add_argument("--no-default-terms", action="store_true")
    ap.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY, metavar="N")
    ap.add_argument("--db-fetch-rows", type=int, default=2000, metavar="N")
    ap.add_argument("--coref-batch-size", type=int, default=DEFAULT_COREF_BATCH_SIZE, metavar="N")
    args = ap.parse_args(list(argv) if argv is not None else None)

    terms = _collect_terms(list(args.terms), args.terms_file)
    if not terms and not args.no_default_terms:
        terms = list(DEFAULT_TERMS)

    row_fetch_size = max(1, int(args.db_fetch_rows))
    coref_batch_size = max(1, int(args.coref_batch_size))
    progress_every = max(1, int(args.progress_every))

    coref_state_path = _default_coref_state_path(args.out)
    coref_jsonl_path = _default_coref_jsonl_path(args.out)

    if args.stage == "trim" and not args.raw_out.exists():
        raise SystemExit(f"--stage trim requires raw input file: {args.raw_out}")
    if args.stage == "coref" and not args.trimmed_out.exists():
        raise SystemExit(f"--stage coref requires trimmed input file: {args.trimmed_out}")

    # TEMP: remove this block after coref debugging; forces a clean coref output file.
    if args.stage in ("all", "coref") and args.out.exists():
        args.out.unlink()
        print(f"[temp] deleted existing coref output: {args.out.resolve()}", flush=True)

    if args.stage in ("all", "fetch"):
        pool_prefix = "prod" if args.prod else "dev"
        init_pool(prefix=pool_prefix)
        try:
            fetched = stream_fetch_and_write_raw(
                terms,
                args.raw_out,
                row_fetch_size=row_fetch_size,
                progress_every=progress_every,
                use_prod=bool(args.prod),
            )
            print(f"[ok] fetched {fetched} raw posts -> {args.raw_out.resolve()}", flush=True)
        finally:
            close_pool()

    if args.stage in ("all", "trim"):
        trimmed = stream_trim_from_raw(
            args.raw_out,
            args.trimmed_out,
            progress_every=progress_every,
        )
        print(f"[ok] trimmed {trimmed} posts -> {args.trimmed_out.resolve()}", flush=True)

    if args.stage in ("all", "coref"):
        written, skipped = coref_from_trimmed_and_write(
            args.trimmed_out,
            args.out,
            coref_batch_size=coref_batch_size,
            progress_every=progress_every,
            state_path=coref_state_path,
            jsonl_path=coref_jsonl_path,
        )
        if skipped:
            print(f"[warn] skipped {skipped} posts after retries", flush=True)
        print(f"[ok] wrote {written} coref-resolved posts -> {args.out.resolve()}", flush=True)


if __name__ == "__main__":
    main()
