#!/usr/bin/env python3
"""
One-command prep experiment runs under nlp/experiments/runs/run_N/.

  python -m nlp.experiments.do_experiment_run
  python -m nlp.experiments.do_experiment_run --continue
  python -m nlp.experiments.do_experiment_run --phases punct,chunk,report
  python -m nlp.experiments.do_experiment_run --run-dir runs/run_1 --continue

Default phases: punct,chunk,report (coref off). Products written by phase:
  prepare (punct/chunk/coref) → posts_prepared.json
  report → browse_coref_edits.html (if coref), summary_coref.md (if coref),
           summary_chunks.md
  assessment.md is written by hand after review
"""
from __future__ import annotations

import argparse
import html
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from nlp.coref import _resolve_with_fallback_single
from nlp.punct import needs_punctuation, remap_hits_to_text, restore_punctuation
from nlp.trim import (
    CHUNK_CHAR_LIMIT,
    MAX_CHARS_AFTER,
    MAX_CHARS_BEFORE,
    MAX_SENTENCES,
    SENTENCES_AFTER,
    SENTENCES_BEFORE,
    syntok_sentence_spans,
    trim_sentence_boundary,
)

ROOT = Path(__file__).resolve().parent
DEFAULT_INPUT = ROOT / "posts_for_term_raw.json"
RUNS_DIR = ROOT / "runs"

AUTOMATED_PRODUCTS = (
    "posts_prepared.json",
    "browse_coref_edits.html",
    "summary_coref.md",
    "summary_chunks.md",
)

VALID_PHASES = frozenset({"punct", "chunk", "coref", "report"})
DEFAULT_PHASES = ("punct", "chunk", "report")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_posts(path: Path) -> list[dict[str, Any]]:
    raw = _load_json(path)
    if isinstance(raw, list):
        return [p for p in raw if isinstance(p, dict)]
    if isinstance(raw, dict):
        posts = raw.get("posts")
        if isinstance(posts, list):
            return [p for p in posts if isinstance(p, dict)]
    raise SystemExit("Input must be a JSON array of posts or an object with a posts array")


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _next_run_dir(runs_dir: Path) -> Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    n = 1
    while True:
        candidate = runs_dir / f"run_{n}"
        if not candidate.exists():
            return candidate
        n += 1


def _product_status(run_dir: Path) -> dict[str, bool]:
    return {name: (run_dir / name).is_file() for name in AUTOMATED_PRODUCTS}


def _status_table(run_dir: Path) -> str:
    st = _product_status(run_dir)
    lines = [f"Run: {run_dir}", "Products:"]
    for name in AUTOMATED_PRODUCTS:
        mark = "yes" if st[name] else "missing"
        lines.append(f"  [{mark}] {name}")
    assess = run_dir / "assessment.md"
    lines.append(f"  [{'yes' if assess.is_file() else 'missing'}] assessment.md (manual)")
    return "\n".join(lines)


def _parse_phases(raw: str | None, *, skip_coref: bool) -> set[str]:
    if raw is None or not str(raw).strip():
        phases = set(DEFAULT_PHASES)
    else:
        phases = {p.strip().lower() for p in str(raw).split(",") if p.strip()}
    unknown = phases - VALID_PHASES
    if unknown:
        raise SystemExit(f"Unknown phase(s): {sorted(unknown)}. Valid: {sorted(VALID_PHASES)}")
    if skip_coref:
        phases.discard("coref")
    if not phases:
        raise SystemExit("No phases selected.")
    return phases


def _percentile(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    rank = (len(sorted_vals) - 1) * (p / 100.0)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return float(sorted_vals[lo])
    w = rank - lo
    return float(sorted_vals[lo] * (1.0 - w) + sorted_vals[hi] * w)


# ---------------------------------------------------------------------------
# Prepare
# ---------------------------------------------------------------------------


def _prepare_posts(
    posts: list[dict[str, Any]],
    *,
    do_punct: bool,
    do_chunk: bool,
    do_coref: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    punct_restored = 0
    punct_skipped = 0
    chunk_posts = 0
    coref_changed = 0
    coref_failed = 0

    for i, post in enumerate(posts):
        if not isinstance(post, dict):
            continue
        row = dict(post)
        text = str(row.get("text") or "")
        hits = row.get("hits") if isinstance(row.get("hits"), list) else []
        working = text
        working_hits = [dict(h) for h in hits if isinstance(h, dict)]

        if do_punct and text.strip():
            if needs_punctuation(text):
                restored, did = restore_punctuation(text, force=True)
                if did:
                    punct_restored += 1
                    row["text_punct"] = restored
                    row["punctuation_restored"] = True
                    working = restored
                    working_hits = remap_hits_to_text(text, restored, working_hits)
                else:
                    punct_skipped += 1
                    row["punctuation_restored"] = False
            else:
                punct_skipped += 1
                row["punctuation_restored"] = False

        if do_chunk:
            chunks = trim_sentence_boundary(working, working_hits)
            row["trimmed_chunks"] = chunks
            if working != text:
                row["trim_source_text"] = working
                row["hits_for_trim"] = working_hits
            chunk_posts += 1

        if do_coref:
            base_for_coref = working
            try:
                if base_for_coref.strip():
                    resolved, _n = _resolve_with_fallback_single(base_for_coref)
                else:
                    resolved = base_for_coref
            except Exception as exc:  # noqa: BLE001
                resolved = base_for_coref
                row["coref_error"] = f"{type(exc).__name__}: {exc}"
                coref_failed += 1
            row["text_coreference_resolved"] = resolved
            row["coref_changed"] = resolved != base_for_coref
            if row["coref_changed"]:
                coref_changed += 1
            if do_chunk and row["coref_changed"] and isinstance(row.get("trimmed_chunks"), list):
                remapped = remap_hits_to_text(base_for_coref, resolved, working_hits)
                row["trimmed_chunks_coref"] = trim_sentence_boundary(resolved, remapped)

        out.append(row)
        if (i + 1) % 500 == 0:
            print(f"  prepared {i + 1}/{len(posts)} …", flush=True)

    stats = {
        "posts": len(out),
        "punct_enabled": do_punct,
        "punct_restored": punct_restored,
        "punct_skipped_or_unchanged": punct_skipped,
        "chunk_enabled": do_chunk,
        "chunk_posts": chunk_posts,
        "coref_enabled": do_coref,
        "coref_changed": coref_changed,
        "coref_failed": coref_failed,
        "coref_changed_rate": (coref_changed / len(out)) if out else 0.0,
    }
    return out, stats


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


def _build_browse_html(posts: list[dict[str, Any]], *, max_items: int = 80) -> str:
    rows: list[dict[str, Any]] = []
    for p in posts:
        if not isinstance(p, dict):
            continue
        if not p.get("coref_changed"):
            continue
        before = str(p.get("text") or "")
        after = str(p.get("text_coreference_resolved") or p.get("text_coref") or "")
        if before == after:
            continue
        rows.append(
            {
                "post_id": p.get("post_id"),
                "before": before,
                "after": after,
                "delta_chars": len(after) - len(before),
            }
        )
        if len(rows) >= max_items:
            break

    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Coref browse</title>",
        "<style>",
        "body{font-family:system-ui,sans-serif;margin:1.5rem;line-height:1.4}",
        ".card{border:1px solid #ccc;padding:1rem;margin:1rem 0}",
        "pre{white-space:pre-wrap;background:#f6f6f6;padding:.75rem}",
        ".meta{color:#555;font-size:.9rem}",
        "</style></head><body>",
        f"<h1>Coreference edits ({len(rows)} shown)</h1>",
        "<p>Only posts where text_coreference_resolved ≠ text.</p>",
    ]
    for r in rows:
        parts.append("<div class='card'>")
        parts.append(
            f"<div class='meta'>post_id={html.escape(str(r['post_id']))} "
            f"delta_chars={r['delta_chars']}</div>"
        )
        parts.append("<h3>Before</h3><pre>" + html.escape(r["before"][:4000]) + "</pre>")
        parts.append("<h3>After</h3><pre>" + html.escape(r["after"][:4000]) + "</pre>")
        parts.append("</div>")
    parts.append("</body></html>")
    return "\n".join(parts)


def _write_coref_summary(posts: list[dict[str, Any]], path: Path) -> dict[str, Any]:
    n = len(posts)
    changed = sum(1 for p in posts if isinstance(p, dict) and p.get("coref_changed"))
    failed = sum(1 for p in posts if isinstance(p, dict) and p.get("coref_error"))
    deltas: list[int] = []
    for p in posts:
        if not isinstance(p, dict) or not p.get("coref_changed"):
            continue
        before = str(p.get("text") or "")
        after = str(p.get("text_coreference_resolved") or p.get("text_coref") or "")
        deltas.append(len(after) - len(before))
    deltas_sorted = sorted(float(x) for x in deltas)
    stats = {
        "posts": n,
        "coref_changed": changed,
        "coref_failed": failed,
        "coref_changed_rate": (changed / n) if n else 0.0,
        "delta_chars_p50": _percentile(deltas_sorted, 50),
        "delta_chars_p90": _percentile(deltas_sorted, 90),
        "delta_chars_mean": (sum(deltas) / len(deltas)) if deltas else None,
    }
    lines = [
        "# Coreference summary",
        "",
        f"- posts: {stats['posts']}",
        f"- coref_changed: {stats['coref_changed']} ({100.0 * stats['coref_changed_rate']:.1f}%)",
        f"- coref_failed: {stats['coref_failed']}",
        f"- delta_chars p50: {stats['delta_chars_p50']}",
        f"- delta_chars p90: {stats['delta_chars_p90']}",
        f"- delta_chars mean: {stats['delta_chars_mean']}",
        "",
        "See browse_coref_edits.html for side-by-side examples.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
    return stats


def _write_chunk_summary(posts: list[dict[str, Any]], path: Path) -> dict[str, Any]:
    chunk_lens: list[int] = []
    chunks_per_post: list[int] = []
    quirks: Counter[str] = Counter()
    punct_restored = 0

    for p in posts:
        if not isinstance(p, dict):
            continue
        if p.get("punctuation_restored"):
            punct_restored += 1
        text = str(p.get("trim_source_text") or p.get("text_punct") or p.get("text") or "")
        hits = (
            p.get("hits_for_trim")
            if isinstance(p.get("hits_for_trim"), list)
            else (p.get("hits") if isinstance(p.get("hits"), list) else [])
        )
        chunks = p.get("trimmed_chunks") if isinstance(p.get("trimmed_chunks"), list) else []
        chunks_per_post.append(len(chunks))
        for c in chunks:
            if isinstance(c, str):
                chunk_lens.append(len(c))

        if any(isinstance(c, str) and len(c) >= CHUNK_CHAR_LIMIT - 5 for c in chunks):
            quirks["near_chunk_char_limit"] += 1
        if len(chunks) >= 4:
            quirks["many_chunks"] += 1

        hit_starts = sorted(
            int(h.get("match_start", 0) or 0)
            for h in hits
            if isinstance(h, dict)
        )
        if len(hit_starts) >= 2 and (hit_starts[-1] - hit_starts[0]) > 2000:
            quirks["far_apart_hits"] += 1
            if len(chunks) <= 1:
                quirks["far_apart_still_one_chunk"] += 1

        spans = syntok_sentence_spans(text) if text else []
        if text and len(spans) <= 1 and hit_starts:
            quirks["single_sentence_fallback"] += 1

        for h in hits:
            if not isinstance(h, dict):
                continue
            ms = int(h.get("match_start", 0) or 0)
            me = int(h.get("match_end", 0) or 0)
            if (me - ms) < 8 and len(text) < 80:
                quirks["short_post_hit"] += 1
                break

        if text and hit_starts:
            probe = [
                {
                    "match_start": hit_starts[0],
                    "match_end": hit_starts[0] + 1,
                }
            ]
            tiny = trim_sentence_boundary(
                text,
                probe,
                sentences_before=SENTENCES_BEFORE,
                sentences_after=SENTENCES_AFTER,
                max_sentences=MAX_SENTENCES,
                max_chars_before=MAX_CHARS_BEFORE,
                max_chars_after=MAX_CHARS_AFTER,
                chunk_char_limit=CHUNK_CHAR_LIMIT,
            )
            if tiny and len(tiny[0]) < 40:
                quirks["tiny_context_window"] += 1

    lens_sorted = sorted(float(x) for x in chunk_lens)
    cpp_sorted = sorted(float(x) for x in chunks_per_post)
    stats = {
        "posts": len(posts),
        "punctuation_restored_posts": punct_restored,
        "total_chunks": len(chunk_lens),
        "chunk_len_p50": _percentile(lens_sorted, 50),
        "chunk_len_p90": _percentile(lens_sorted, 90),
        "chunk_len_p99": _percentile(lens_sorted, 99),
        "chunk_len_max": max(chunk_lens) if chunk_lens else None,
        "chunks_per_post_mean": (sum(chunks_per_post) / len(chunks_per_post)) if chunks_per_post else None,
        "chunks_per_post_p90": _percentile(cpp_sorted, 90),
        "quirks": dict(quirks),
        "knobs": {
            "SENTENCES_BEFORE": SENTENCES_BEFORE,
            "SENTENCES_AFTER": SENTENCES_AFTER,
            "MAX_SENTENCES": MAX_SENTENCES,
            "CHUNK_CHAR_LIMIT": CHUNK_CHAR_LIMIT,
        },
    }
    lines = [
        "# Chunk / trim summary",
        "",
        f"- posts: {stats['posts']}",
        f"- punctuation_restored_posts: {stats['punctuation_restored_posts']}",
        f"- total_chunks: {stats['total_chunks']}",
        f"- chunk_len p50/p90/p99/max: {stats['chunk_len_p50']} / {stats['chunk_len_p90']} / "
        f"{stats['chunk_len_p99']} / {stats['chunk_len_max']}",
        f"- chunks_per_post mean/p90: {stats['chunks_per_post_mean']} / {stats['chunks_per_post_p90']}",
        "",
        "## Knobs",
        "",
    ]
    for k, v in stats["knobs"].items():
        lines.append(f"- {k}: {v}")
    lines.extend(["", "## Quirk counts", ""])
    for k, v in sorted(quirks.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"- {k}: {v}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Create or continue a prep experiment run (run_N).",
    )
    ap.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input posts JSON (default: {DEFAULT_INPUT.name})",
    )
    ap.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Existing run dir (implies continue). Default: create runs/run_N",
    )
    ap.add_argument(
        "--continue",
        dest="continue_run",
        action="store_true",
        help="Resume latest run_N (or --run-dir); only build missing products",
    )
    ap.add_argument(
        "--phases",
        default=None,
        help=(
            "Comma-separated phases: punct,chunk,coref,report. "
            f"Default: {','.join(DEFAULT_PHASES)}"
        ),
    )
    ap.add_argument(
        "--skip-coref",
        action="store_true",
        help="Omit coref from phases (alias; default phases already skip coref)",
    )
    ap.add_argument("--limit", type=int, default=None, help="Optional post cap for smoke tests")
    ap.add_argument(
        "--force-prepare",
        action="store_true",
        help="Rebuild posts_prepared.json even if it exists",
    )
    ap.add_argument(
        "--status",
        action="store_true",
        help="Print product status for --run-dir or latest run and exit",
    )
    args = ap.parse_args(argv)

    phases = _parse_phases(args.phases, skip_coref=args.skip_coref)

    if args.status:
        target = args.run_dir
        if target is None:
            existing = sorted(RUNS_DIR.glob("run_*"), key=lambda p: p.name)
            if not existing:
                print("No runs yet.", file=sys.stderr)
                return 1
            target = existing[-1]
        print(_status_table(target.resolve()))
        return 0

    continuing = bool(args.continue_run or args.run_dir)
    if args.run_dir is not None:
        run_dir = args.run_dir.resolve()
        run_dir.mkdir(parents=True, exist_ok=True)
    elif args.continue_run:
        existing = sorted(RUNS_DIR.glob("run_*"), key=lambda p: p.name)
        if not existing:
            print("No existing run to continue; creating run_1", file=sys.stderr)
            run_dir = _next_run_dir(RUNS_DIR)
        else:
            run_dir = existing[-1].resolve()
    else:
        run_dir = _next_run_dir(RUNS_DIR)

    run_dir.mkdir(parents=True, exist_ok=True)
    print(_status_table(run_dir))
    print(f"Phases: {','.join(p for p in ('punct', 'chunk', 'coref', 'report') if p in phases)}")

    prepared_path = run_dir / "posts_prepared.json"
    need_prepare = args.force_prepare or not prepared_path.is_file()

    do_punct = "punct" in phases
    do_chunk = "chunk" in phases
    do_coref = "coref" in phases
    do_report = "report" in phases

    if need_prepare and (do_punct or do_chunk or do_coref):
        if not args.input.is_file():
            raise SystemExit(f"Input not found: {args.input}")
        print(f"Loading {args.input} …", flush=True)
        posts_in = _load_posts(args.input)
        if args.limit is not None:
            posts_in = posts_in[: max(0, int(args.limit))]
        print(
            f"Preparing {len(posts_in)} posts "
            f"(punct={do_punct}, chunk={do_chunk}, coref={do_coref}) …",
            flush=True,
        )
        prepared, prep_stats = _prepare_posts(
            posts_in,
            do_punct=do_punct,
            do_chunk=do_chunk,
            do_coref=do_coref,
        )
        _write_json(prepared_path, prepared)
        meta = {
            "created_at": _utc_now(),
            "input": str(args.input.resolve()),
            "phases": sorted(phases),
            "limit": args.limit,
            "prep_stats": prep_stats,
            "continuing": continuing,
        }
        _write_json(run_dir / "run_meta.json", meta)
        print(f"Wrote {prepared_path}", flush=True)
        print(json.dumps(prep_stats, indent=2), flush=True)
    elif not prepared_path.is_file() and do_report:
        raise SystemExit(f"Missing {prepared_path}; run prepare phases first")

    if do_report and prepared_path.is_file():
        posts = _load_json(prepared_path)
        if not isinstance(posts, list):
            raise SystemExit("posts_prepared.json must be a JSON array")

        had_coref = any(
            isinstance(p, dict)
            and ("text_coreference_resolved" in p or "text_coref" in p or p.get("coref_changed"))
            for p in posts
        )

        browse = run_dir / "browse_coref_edits.html"
        if had_coref and (args.force_prepare or not browse.is_file() or not continuing):
            browse.write_text(_build_browse_html(posts), encoding="utf-8")
            print(f"Wrote {browse}", flush=True)
        elif had_coref and continuing and not browse.is_file():
            browse.write_text(_build_browse_html(posts), encoding="utf-8")
            print(f"Wrote {browse}", flush=True)

        coref_md = run_dir / "summary_coref.md"
        if had_coref and (not continuing or not coref_md.is_file() or args.force_prepare):
            _write_coref_summary(posts, coref_md)
            print(f"Wrote {coref_md}", flush=True)
        elif had_coref and continuing and not coref_md.is_file():
            _write_coref_summary(posts, coref_md)
            print(f"Wrote {coref_md}", flush=True)

        chunk_md = run_dir / "summary_chunks.md"
        if not continuing or not chunk_md.is_file() or args.force_prepare:
            _write_chunk_summary(posts, chunk_md)
            print(f"Wrote {chunk_md}", flush=True)
        elif continuing and not chunk_md.is_file():
            _write_chunk_summary(posts, chunk_md)
            print(f"Wrote {chunk_md}", flush=True)

    print(_status_table(run_dir))
    print(
        "Done. Write assessment.md by hand when you have reviewed the summaries "
        f"({run_dir / 'assessment.md'})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
