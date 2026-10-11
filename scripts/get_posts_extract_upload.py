"""Fetch posts for search terms, punct+trim, extract claims, optionally upload.

Pipeline::

    posts → nlp.punct (eligible) → nlp.trim chunks → concurrent claim extract
    → nested posts→chunks→claims JSON → optional nitwitch WebDAV PUT

CLI::

    # Count matching posts only (no fetch/write/extract):
    python -m scripts.get_posts_extract_upload \\
      --terms measles --since 2024-01-01 --until 2025-01-01 --prod --count-only

    # Live smoke: fetch 1 post, extract 1 chunk, print result (no --out):
    python -m scripts.get_posts_extract_upload \\
      --terms measles --since 2024-01-01 --until 2025-01-01 --prod --smoke

    # Full run (any date span; posts are fetched and extracted one page at a time):
    python -m scripts.get_posts_extract_upload \\
      --terms measles --since 2024-01-01 --until 2025-01-01 \\
      --prod --out measles_claims.json --upload
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from nlp.claim_extraction.batch import format_duration, format_eta, run_on_posts as extract_run
from nlp.claim_extraction.defaults import (
    DEFAULT_BATCH_COUNT,
    DEFAULT_MAX_CLAIMS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_MAX_WORKERS,
    MODEL_NAME,
)
from nlp.claim_extraction.nest import nest_posts_chunks_claims
from nlp.claim_extraction.prep import prepare_and_explode
from scripts.get_posts_for_search_term import (
    DEFAULT_ROW_FETCH_SIZE,
    JsonPostsStreamWriter,
    count_only as count_posts_only,
    fetch_and_write,
    iter_post_batches,
    parse_utc_datetime,
    write_posts_json,
)


def _load_terms_file(path: Path) -> list[str]:
    terms: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if s and not s.startswith("#"):
            terms.append(s)
    return terms


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _dedupe_terms(terms: list[str]) -> list[str]:
    seen: set[str] = set()
    uniq: list[str] = []
    for t in terms:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def run_count_only(
    *,
    terms: list[str],
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
) -> dict[str, Any]:
    """COUNT matching posts; no fetch, prep, extract, or files."""
    return count_posts_only(terms=terms, use_prod=use_prod, since=since, until=until)


def run_smoke(
    *,
    terms: list[str],
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    max_claims: int = DEFAULT_MAX_CLAIMS,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> dict[str, Any]:
    """Fetch 1 post, extract 1 chunk, return result without writing --out."""
    terms_list = [t.strip() for t in terms if str(t).strip()]
    if not terms_list:
        raise ValueError("At least one search term is required")

    with tempfile.TemporaryDirectory(prefix="get_posts_smoke_") as tmp:
        work_dir = Path(tmp)
        posts_path = work_dir / "posts.json"
        chunks_path = work_dir / "chunk_rows.json"
        extract_path = work_dir / "extract_rows.json"

        fetch_summary = fetch_and_write(
            terms=terms_list,
            out_path=posts_path,
            use_prod=use_prod,
            since=since,
            until=until,
            limit=1,
            count_first=False,
        )
        posts_payload = json.loads(posts_path.read_text(encoding="utf-8"))
        posts = [p for p in posts_payload.get("posts", []) if isinstance(p, dict)]
        if not posts:
            return {
                "mode": "smoke",
                "ok": False,
                "error": "no matching posts",
                "matched_post_count": fetch_summary.get("matched_post_count", 0),
                "terms": terms_list,
                "since": _iso(since),
                "until": _iso(until),
                "use_prod": use_prod,
                "model": MODEL_NAME,
            }

        prepared, chunk_rows = prepare_and_explode(posts)
        if not chunk_rows:
            return {
                "mode": "smoke",
                "ok": False,
                "error": "post fetched but trim produced no chunks",
                "post_id": posts[0].get("post_id"),
                "terms": terms_list,
                "since": _iso(since),
                "until": _iso(until),
                "use_prod": use_prod,
                "model": MODEL_NAME,
            }

        write_posts_json(
            chunks_path,
            chunk_rows,
            terms=terms_list,
            since=since,
            until=until,
        )
        extract_run(
            posts_path=chunks_path,
            out_path=extract_path,
            alignment=True,
            batch_count=1,
            max_workers=1,
            max_claims=max_claims,
            max_retries=max_retries,
            n_posts=1,
        )
        extract_payload = json.loads(extract_path.read_text(encoding="utf-8"))
        extract_rows = [r for r in extract_payload.get("posts", []) if isinstance(r, dict)]

        nested = nest_posts_chunks_claims(
            prepared,
            extract_rows,
            terms=terms_list,
            since=_iso(since),
            until=_iso(until),
            model=MODEL_NAME,
            extra_meta={"use_prod": use_prod, "mode": "smoke", "claims_extraction_mode": "alignment"},
        )
        # Prefer the single extracted chunk for a readable smoke printout.
        sample_chunk = None
        for post in nested.get("posts", []):
            for ch in post.get("chunks", []):
                if ch.get("claim_extraction_disposition") != "unprocessed":
                    sample_chunk = {
                        "post_id": post.get("post_id"),
                        "platform": post.get("platform"),
                        "punctuation_restored": post.get("punctuation_restored"),
                        "chunk": ch,
                    }
                    break
            if sample_chunk is not None:
                break

        disposition = (sample_chunk or {}).get("chunk", {}).get("claim_extraction_disposition")
        ok = disposition == "success"
        return {
            "mode": "smoke",
            "ok": ok,
            "model": MODEL_NAME,
            "terms": terms_list,
            "since": _iso(since),
            "until": _iso(until),
            "use_prod": use_prod,
            "posts_fetched": len(posts),
            "chunks_from_post": len(chunk_rows),
            "chunks_extracted": 1 if extract_rows else 0,
            "sample": sample_chunk,
        }


def _extract_chunk_batch(
    chunk_rows: list[dict[str, Any]],
    *,
    work_dir: Path,
    terms: list[str],
    since: datetime | None,
    until: datetime | None,
    batch_count: int,
    max_workers: int,
    max_claims: int,
    max_retries: int,
) -> list[dict[str, Any]]:
    """Extract one in-memory slice. The batch files are removed before return."""
    if not chunk_rows:
        return []
    batch_chunks = work_dir / "_batch_chunks.json"
    batch_extract = work_dir / "_batch_extract.json"
    batch_extract.unlink(missing_ok=True)
    try:
        write_posts_json(
            batch_chunks,
            chunk_rows,
            terms=terms,
            since=since,
            until=until,
        )
        extract_run(
            posts_path=batch_chunks,
            out_path=batch_extract,
            alignment=True,
            batch_count=batch_count,
            max_workers=max_workers,
            max_claims=max_claims,
            max_retries=max_retries,
        )
        extract_payload = json.loads(batch_extract.read_text(encoding="utf-8"))
        return [r for r in extract_payload.get("posts", []) if isinstance(r, dict)]
    finally:
        batch_chunks.unlink(missing_ok=True)
        batch_extract.unlink(missing_ok=True)


def run_pipeline(
    *,
    terms: list[str],
    out_path: Path,
    use_prod: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int | None = None,
    count_first: bool = False,
    max_claims: int = DEFAULT_MAX_CLAIMS,
    max_workers: int = DEFAULT_MAX_WORKERS,
    batch_count: int = DEFAULT_BATCH_COUNT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    upload: bool = False,
    upload_as: str | None = None,
    keep_work: bool = False,
    row_fetch_size: int = DEFAULT_ROW_FETCH_SIZE,
) -> dict[str, Any]:
    """Fetch → prep → extract → nest → optional upload, one DB slice at a time.

    Always counts matches first so the progress line can show an overall ETA.
    ``count_first`` is accepted so existing callers keep working.
    """
    terms_list = [t.strip() for t in terms if str(t).strip()]
    if not terms_list:
        raise ValueError("At least one search term is required")

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("[count] counting matching posts...", flush=True)
    matched = int(
        count_posts_only(
            terms=terms_list,
            use_prod=use_prod,
            since=since,
            until=until,
        )["matched_post_count"]
    )
    total_posts = matched if limit is None else min(matched, max(0, int(limit)))
    print(f"[count] {total_posts} posts to process", flush=True)

    work_dir_owned: tempfile.TemporaryDirectory[str] | None = None
    if keep_work:
        work_dir = out_path.parent / f".{out_path.stem}_work"
        work_dir.mkdir(parents=True, exist_ok=True)
    else:
        work_dir_owned = tempfile.TemporaryDirectory(prefix="get_posts_extract_")
        work_dir = Path(work_dir_owned.name)

    fetch_meta = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "terms": terms_list,
        "since": _iso(since),
        "until": _iso(until),
    }
    nested_writer = JsonPostsStreamWriter(
        out_path,
        {
            **fetch_meta,
            "model": MODEL_NAME,
            "use_prod": use_prod,
            "claims_extraction_mode": "alignment",
        },
    )
    posts_writer: JsonPostsStreamWriter | None = None
    chunks_writer: JsonPostsStreamWriter | None = None
    extract_writer: JsonPostsStreamWriter | None = None
    if keep_work:
        posts_writer = JsonPostsStreamWriter(work_dir / "posts.json", fetch_meta)
        chunks_writer = JsonPostsStreamWriter(work_dir / "chunk_rows.json", fetch_meta)
        extract_writer = JsonPostsStreamWriter(work_dir / "extract_rows.json", fetch_meta)
    writers = [w for w in (nested_writer, posts_writer, chunks_writer, extract_writer) if w is not None]

    finalized = False
    post_count = 0
    chunk_count = 0
    claim_count = 0
    try:
        page_n = 0
        fetched = 0
        started = time.monotonic()
        print(
            f"[fetch] keyset pages of up to {max(1, int(row_fetch_size))} posts; "
            "each slice is extracted before the next query",
            flush=True,
        )
        for batch in iter_post_batches(
            terms_list,
            use_prod=use_prod,
            since=since,
            until=until,
            limit=limit,
            row_fetch_size=row_fetch_size,
        ):
            page_n += 1
            fetched += len(batch)
            print(f"[fetch] page {page_n}: {len(batch)} posts ({fetched} total)", flush=True)
            if posts_writer is not None:
                for post in batch:
                    posts_writer.write_post(post)

            prepared, chunk_rows = prepare_and_explode(batch)
            if chunks_writer is not None:
                for row in chunk_rows:
                    chunks_writer.write_post(row)

            extract_rows = _extract_chunk_batch(
                chunk_rows,
                work_dir=work_dir,
                terms=terms_list,
                since=since,
                until=until,
                batch_count=batch_count,
                max_workers=max_workers,
                max_claims=max_claims,
                max_retries=max_retries,
            )
            if extract_writer is not None:
                for row in extract_rows:
                    extract_writer.write_post(row)

            nested = nest_posts_chunks_claims(
                prepared,
                extract_rows,
                terms=terms_list,
                since=_iso(since),
                until=_iso(until),
                model=MODEL_NAME,
            )
            for post in nested.get("posts", []):
                if isinstance(post, dict):
                    nested_writer.write_post(post)
            post_count += int(nested.get("post_count") or 0)
            chunk_count += int(nested.get("chunk_count") or 0)
            claim_count += int(nested.get("claim_count") or 0)
            elapsed = time.monotonic() - started
            eta = format_eta(done=fetched, total=total_posts, elapsed_s=elapsed)
            print(
                f"[progress] posts {fetched}/{total_posts}"
                f" | elapsed {format_duration(elapsed)} | eta {eta}"
                f" | chunks {chunk_count} | claims {claim_count}",
                flush=True,
            )

        nested_writer.finalize(extra={"chunk_count": chunk_count, "claim_count": claim_count})
        matched_out = matched if matched is not None else fetched
        for writer in (posts_writer, chunks_writer, extract_writer):
            if writer is not None:
                writer.finalize(extra={"matched_post_count": matched_out})
        finalized = True

        summary: dict[str, Any] = {
            "out": str(out_path),
            "post_count": post_count,
            "chunk_count": chunk_count,
            "claim_count": claim_count,
            "terms": terms_list,
            "since": _iso(since),
            "until": _iso(until),
            "use_prod": use_prod,
            "model": MODEL_NAME,
        }
        if matched is not None:
            summary["matched_post_count"] = matched
        if upload:
            from storage.nitwitch_upload import upload_file

            summary["uploaded_url"] = upload_file(out_path, remote_name=upload_as)
        return summary
    finally:
        if not finalized:
            for writer in writers:
                writer.abort()
        if work_dir_owned is not None:
            work_dir_owned.cleanup()


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(
        description="Fetch posts, punct+trim, extract claims, optionally upload to nitwitch"
    )
    ap.add_argument("--terms", nargs="*", default=[], help="Search term names")
    ap.add_argument("--terms-file", type=Path, default=None, help="One term per line")
    ap.add_argument("--since", type=str, default=None)
    ap.add_argument("--until", type=str, default=None)
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Nested claims JSON path (required unless --count-only or --smoke)",
    )
    ap.add_argument("--prod", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--count-first", action="store_true")
    ap.add_argument(
        "--count-only",
        action="store_true",
        help="Print matched post count and exit (no fetch/extract/files)",
    )
    ap.add_argument(
        "--smoke",
        action="store_true",
        help="Fetch 1 post, extract 1 chunk, print result to stdout (no --out)",
    )
    ap.add_argument("--max-claims", type=int, default=DEFAULT_MAX_CLAIMS)
    ap.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    ap.add_argument("--batch-count", type=int, default=DEFAULT_BATCH_COUNT)
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--upload-as", type=str, default=None)
    ap.add_argument(
        "--keep-work",
        action="store_true",
        help="Keep intermediate posts/chunk/extract JSON next to --out",
    )
    args = ap.parse_args(argv)

    terms = list(args.terms)
    if args.terms_file is not None:
        terms.extend(_load_terms_file(args.terms_file))
    uniq = _dedupe_terms(terms)

    if args.count_only and args.smoke:
        ap.error("use only one of --count-only / --smoke")
    if args.count_only or args.smoke:
        if args.upload:
            ap.error("--upload is incompatible with --count-only / --smoke")
        if args.out is not None:
            ap.error("--out is not used with --count-only / --smoke")

    since = parse_utc_datetime(args.since)
    until = parse_utc_datetime(args.until)

    if args.count_only:
        summary = run_count_only(
            terms=uniq,
            use_prod=bool(args.prod),
            since=since,
            until=until,
        )
        print(json.dumps(summary, ensure_ascii=False), flush=True)
        return 0

    if args.smoke:
        summary = run_smoke(
            terms=uniq,
            use_prod=bool(args.prod),
            since=since,
            until=until,
            max_claims=max(1, int(args.max_claims)),
            max_retries=max(1, int(args.max_retries)),
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
        return 0 if summary.get("ok") else 1

    if args.out is None:
        ap.error("--out is required unless --count-only or --smoke")

    summary = run_pipeline(
        terms=uniq,
        out_path=args.out,
        use_prod=bool(args.prod),
        since=since,
        until=until,
        limit=args.limit,
        count_first=bool(args.count_first),
        max_claims=max(1, int(args.max_claims)),
        max_workers=max(1, int(args.max_workers)),
        batch_count=max(1, int(args.batch_count)),
        max_retries=max(1, int(args.max_retries)),
        upload=bool(args.upload),
        upload_as=args.upload_as,
        keep_work=bool(args.keep_work),
    )
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
