"""
Export posts that have taxonomy term hits (matches.post_term_hit) to JSON.

Each post appears once with a ``hits`` list: term_id, term_name, match_start, match_end.
After the DB fetch, runs in-memory **coreference resolution** then **context trimming**
(syntok sentence windows → ``contexts``). One output file only.

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
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

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


def fetch_posts_with_hits(terms: list[str]) -> list[dict[str, Any]]:
    if not terms:
        return []

    sql = """
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
        ORDER BY p.date_entered DESC NULLS LAST, ph.match_start, ph.match_end, vt.id
    """

    posts: list[dict[str, Any]] = []
    by_id: dict[int, dict[str, Any]] = {}

    with getcursor() as cur:
        cur.execute(sql, (terms,))
        for row in cur.fetchall():
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

            hit = {
                "term_id": term_id,
                "term_name": term_name,
                "match_start": match_start,
                "match_end": match_end,
            }

            if post_id not in by_id:
                post = {
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
                by_id[post_id] = post
                posts.append(post)
            else:
                by_id[post_id]["hits"].append(hit)

    return posts


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _enrich_payload(
    payload: dict[str, Any],
    *,
    show_progress: bool,
) -> dict[str, Any]:
    """Coreference + context trimming in memory (single pass each)."""
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

    payload = coref_payload(payload, progress=show_progress)
    payload = trim_payload(payload, progress=show_progress)
    return payload


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
        help="Disable tqdm progress bars for coref and context phases.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    terms = _collect_terms(list(args.terms), args.terms_file)
    if not terms and not args.no_default_terms:
        terms = list(DEFAULT_TERMS)

    show_progress = not args.no_progress
    init_pool(prefix="prod" if args.prod else "dev")
    try:
        posts = fetch_posts_with_hits(terms)
        print(f"[fetch] {len(posts)} posts", flush=True)
        payload: dict[str, Any] = {
            "generated_at_utc": _utc_now().isoformat(),
            "terms": terms,
            "post_count": len(posts),
            "posts": posts,
        }
        if posts:
            payload = _enrich_payload(payload, show_progress=show_progress)
        n_ctx = sum(
            len(p.get("contexts", [])) for p in payload.get("posts", []) if isinstance(p, dict)
        )
        _write_json(args.out, payload)
        print(
            f"[ok] wrote {len(posts)} posts, {n_ctx} contexts → {args.out.resolve()}",
            flush=True,
        )
    finally:
        close_pool()


if __name__ == "__main__":
    main()
