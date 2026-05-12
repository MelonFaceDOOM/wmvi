"""
Interactive CLI to manually enter the five claim score fields (each in [0, 1]).

Append-only JSONL for progress/resume. Use ``--reset`` to wipe the output file before
starting (required when switching from the old categorical schema).

Examples:

  python -m apps.claim_extractor.manual_label_claims \\
    --input data/posts_with_claims_full.json \\
    --output data/manual_claim_scores.jsonl \\
    --reset

  python -m apps.claim_extractor.manual_label_claims \\
    --input data/posts_with_claims_full.json \\
    --output data/manual_claim_scores.jsonl

  python -m apps.claim_extractor.manual_label_claims \\
    --input data/posts_with_claims_full.json \\
    --output data/manual_claim_scores.jsonl \\
    --seed 42 --limit 50

For each score, type a decimal between 0 and 1 (e.g. ``0.65`` or ``1`` or ``0``).

Ctrl+C exits cleanly; each fully labeled claim is flushed to ``--output``.

Note: ``posts_with_claims_full.json`` is still fully loaded with ``json.loads`` (RAM).
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import statistics
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

from apps.claim_extractor.model_common import (
    MANUAL_SCORE_FIELDS,
    iter_success_claim_records,
    load_posts_from_claims_json,
    parse_score_01,
)


@dataclass(frozen=True)
class PendingClaim:
    task_id: str
    claim_index: int
    claim_text: str
    context_text: str


def _load_jsonl_labels(path: Path) -> tuple[set[tuple[str, int]], list[dict[str, Any]]]:
    if not path.exists():
        return set(), []
    keys: set[tuple[str, int]] = set()
    rows: list[dict[str, Any]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            print(f"[warn] skipping invalid JSONL line {lineno} in {path}", file=sys.stderr)
            continue
        if not isinstance(obj, dict):
            continue
        tid = obj.get("task_id")
        idx = obj.get("claim_index")
        if tid is None or idx is None:
            print(f"[warn] skipping line {lineno}: missing task_id or claim_index", file=sys.stderr)
            continue
        key = (str(tid), int(idx))
        keys.add(key)
        rows.append(obj)
    return keys, rows


def _format_stats(label_rows: list[dict[str, Any]], width: int = 100) -> str:
    lines: list[str] = []
    lines.append(f"Labeled claims: {len(label_rows)}")
    lines.append("-" * min(width, 100))
    if not label_rows:
        lines.append("(no labels yet)")
        return "\n".join(lines)

    for key, _desc in MANUAL_SCORE_FIELDS:
        vals: list[float] = []
        for row in label_rows:
            v, bad = parse_score_01(row.get(key))
            if v is not None and not bad:
                vals.append(v)
        lines.append(key)
        if not vals:
            lines.append("  (no valid values yet)")
        else:
            lines.append(
                f"  n={len(vals)}  mean={statistics.mean(vals):.3f}  "
                f"min={min(vals):.3f}  max={max(vals):.3f}"
            )
        lines.append("")
    return "\n".join(lines).rstrip()


def _prompt_score(field_key: str, hint: str) -> float:
    while True:
        print(f"\n{field_key}")
        print(f"  ({hint})")
        print("  Enter a number from 0 to 1 (e.g. 0.65)")
        raw = input("> ").strip()
        v, bad = parse_score_01(raw)
        if v is not None and not bad:
            return v
        print("  Invalid. Use a decimal in [0, 1] inclusive.", flush=True)


def _build_pending(
    posts: list[dict[str, Any]],
    completed: set[tuple[str, int]],
    *,
    warned_empty: list[bool],
) -> list[PendingClaim]:
    out: list[PendingClaim] = []
    for rec in iter_success_claim_records(posts):
        key = (rec.task_id, rec.claim_index)
        if key in completed:
            continue
        text = str(rec.claim.get("claim") or "").strip()
        if not text:
            if not warned_empty[0]:
                print("[warn] skipping claims with empty 'claim' text (at least once)", file=sys.stderr)
                warned_empty[0] = True
            continue
        ctx = rec.post_row.get("text_coreference_resolved")
        if not isinstance(ctx, str) or not ctx.strip():
            ctx = rec.post_row.get("text")
        context_text = (ctx if isinstance(ctx, str) else "") or ""
        out.append(
            PendingClaim(
                task_id=rec.task_id,
                claim_index=rec.claim_index,
                claim_text=text,
                context_text=context_text.strip(),
            )
        )
    return out


def _print_wrapped_section(title: str, body: str, *, width: int, max_chars: int) -> None:
    print("\n" + "=" * min(width, 100))
    print(title)
    print("-" * min(width, 100))
    display = body
    if len(display) > max_chars:
        display = (
            display[:max_chars]
            + "\n[... truncated for display; full text is in the input JSON file ...]"
        )
    wrapped = textwrap.fill(display, width=width, break_long_words=True, break_on_hyphens=False)
    print(wrapped, flush=True)


def run(
    *,
    input_path: Path,
    output_path: Path,
    seed: int | None,
    limit: int | None,
    wrap_width: int,
    reset: bool,
) -> None:
    if reset and output_path.exists():
        output_path.unlink()
        print(f"[reset] removed {output_path}", flush=True)

    print(f"[load] reading {input_path} ...", flush=True)
    _, posts = load_posts_from_claims_json(input_path)

    completed_keys, label_rows = _load_jsonl_labels(output_path)
    print(f"[resume] {len(completed_keys)} claims already in {output_path}", flush=True)

    warned_empty: list[bool] = [False]
    pending = _build_pending(posts, completed_keys, warned_empty=warned_empty)
    print(f"[queue] {len(pending)} claims pending (after resume filter)", flush=True)

    if seed is not None:
        random.seed(seed)
    random.shuffle(pending)

    if limit is not None and limit > 0:
        pending = pending[:limit]
        print(f"[limit] processing up to {len(pending)} claims this session", flush=True)

    if not pending:
        print("Nothing to label. Exiting.", flush=True)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_fp: TextIO = output_path.open("a", encoding="utf-8")

    def on_sigint(_signum: int, _frame: Any) -> None:
        print("\n[interrupt] flushing …", flush=True)
        out_fp.flush()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_sigint)

    try:
        for pc in pending:
            header = _format_stats(label_rows, width=wrap_width)
            print("\033[2J\033[H", end="")
            print(header)
            _print_wrapped_section(
                "TEXT (text_coreference_resolved)",
                pc.context_text or "(empty — check raw `text` on post)",
                width=wrap_width,
                max_chars=8000,
            )
            _print_wrapped_section("CLAIM", pc.claim_text, width=wrap_width, max_chars=8000)

            row: dict[str, Any] = {
                "task_id": pc.task_id,
                "claim_index": pc.claim_index,
                "labeled_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            for field_key, hint in MANUAL_SCORE_FIELDS:
                row[field_key] = _prompt_score(field_key, hint)

            out_fp.write(json.dumps(row, ensure_ascii=False) + "\n")
            out_fp.flush()
            label_rows.append(row)
            completed_keys.add((pc.task_id, pc.claim_index))
    except KeyboardInterrupt:
        print("\n[interrupt] exiting …", flush=True)
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        out_fp.flush()
        out_fp.close()
        print(f"[done] labels appended under {output_path}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Manually enter claim score fields [0,1] (JSONL output).")
    p.add_argument("--input", type=Path, required=True, help="posts_with_claims_full-style JSON")
    p.add_argument("--output", type=Path, required=True, help="Append-only JSONL path")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for shuffling pending claims")
    p.add_argument("--limit", type=int, default=0, help="Max claims to label this run (0 = no cap)")
    p.add_argument("--wrap-width", type=int, default=100, help="Terminal wrap width for text blocks")
    p.add_argument(
        "--reset",
        action="store_true",
        help="Delete output file before run (use when switching schema or starting fresh)",
    )
    args = p.parse_args()
    lim = args.limit if args.limit and args.limit > 0 else None
    run(
        input_path=args.input,
        output_path=args.output,
        seed=args.seed,
        limit=lim,
        wrap_width=max(40, int(args.wrap_width)),
        reset=bool(args.reset),
    )


if __name__ == "__main__":
    main()
