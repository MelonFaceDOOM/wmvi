"""Post-chunk event mention extraction.

Usage (repo root)::

    python -m apps.claims.events.run --corpus measles2 --n 1000 --sample-seed 17
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.events.browse import write_report
from apps.claims.events.documents import (
    DEFAULT_MAX_BODY_CHARS,
    DEFAULT_PLATFORM_FLOOR,
    DEFAULT_SAMPLE_SEED,
    load_corpus_documents,
    load_documents,
    stratified_sample,
    write_documents,
)
from apps.claims.events.extract import (
    DEFAULT_MAX_WORKERS,
    load_mentions,
    log_stderr,
    remaining_docs,
    run_extract,
)
from apps.claims.events.paths import (
    CONFIG_FILE,
    DOCUMENTS_FILE,
    MENTIONS_FILE,
    RUNS_DIR,
    latest_run_dir,
    next_run_dir,
)
from apps.claims.events.prompts import DEFAULT_MODEL, DEFAULT_PROVIDER, DEFAULT_TRIAGE_MODEL

VALID_PHASES = ("sample", "extract", "report")


def _parse_phases(raw: str | None) -> list[str]:
    if raw is None or not str(raw).strip():
        return list(VALID_PHASES)
    phases = [p.strip().lower() for p in str(raw).split(",") if p.strip()]
    unknown = [p for p in phases if p not in VALID_PHASES]
    if unknown:
        raise SystemExit(f"Unknown phase(s): {unknown}. Valid: {list(VALID_PHASES)}")
    return [p for p in VALID_PHASES if p in phases]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m apps.claims.events.run",
        description="Extract event mentions from post-chunks (no catalog matching).",
    )
    p.add_argument("--corpus", default="measles2", help="Corpus slug (default: measles2)")
    p.add_argument("--run-dir", default=None, help="Existing experiment run dir (with --continue)")
    p.add_argument("--continue", dest="continue_run", action="store_true", help="Resume latest (or --run-dir) run")
    p.add_argument("--phases", default=None, help="Comma list: sample,extract,report (default: all)")
    p.add_argument(
        "--n",
        type=int,
        default=None,
        help="Sample/extract this many unlogged post-chunks",
    )
    p.add_argument("--sample-seed", type=int, default=DEFAULT_SAMPLE_SEED)
    p.add_argument(
        "--platform-floor",
        type=int,
        default=DEFAULT_PLATFORM_FLOOR,
        help="Minimum chunks reserved per platform that has enough (default: 50)",
    )
    p.add_argument("--max-body-chars", type=int, default=DEFAULT_MAX_BODY_CHARS)
    p.add_argument(
        "--llm-provider",
        default=DEFAULT_PROVIDER,
        choices=("openai", "azure"),
        help="openai uses PERSONAL_OPENAI_API_KEY; azure uses AZURE_OPENAI_* (default: openai)",
    )
    p.add_argument("--model", default=DEFAULT_MODEL, help="Detail chat model name (default: gpt-6-luna)")
    p.add_argument(
        "--triage-model",
        default=DEFAULT_TRIAGE_MODEL,
        help=f"Triage chat model name (default: {DEFAULT_TRIAGE_MODEL})",
    )
    p.add_argument(
        "--service-tier",
        default=None,
        choices=("flex",),
        help="OpenAI processing tier. flex is half of standard price and slower.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Parallel LLM requests (default: {DEFAULT_MAX_WORKERS}; 0 RPM pre-pacing unless CLAIMS_TARGET_RPM is set)",
    )
    p.add_argument(
        "--estimate-only",
        action="store_true",
        help="Sample (or reuse documents) and print how many LLM calls --n would make",
    )
    p.add_argument("--runs-dir", default=None, help="Override apps/claims/events/runs")
    return p


def _resolve_run_dir(args: argparse.Namespace) -> Path:
    runs_root = Path(args.runs_dir) if args.runs_dir else RUNS_DIR
    if args.run_dir:
        path = Path(args.run_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path
    if args.continue_run:
        latest = latest_run_dir(runs_root)
        if latest is None:
            raise SystemExit("No existing run to continue; omit --continue to start run_1")
        return latest
    return next_run_dir(runs_root)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    phases = _parse_phases(args.phases)
    if args.estimate_only:
        phases = [p for p in phases if p in ("sample", "report")]
        if "sample" not in phases:
            phases.insert(0, "sample")

    run_dir = _resolve_run_dir(args)
    log_stderr(f"events: run dir {run_dir}")
    log_stderr(f"events: phases {','.join(phases)}")

    config: dict[str, Any] = {
        "corpus": args.corpus,
        "phases": phases,
        "n": args.n,
        "sample_seed": args.sample_seed,
        "platform_floor": args.platform_floor,
        "max_body_chars": args.max_body_chars,
        "llm_provider": args.llm_provider,
        "model": args.model,
        "triage_model": args.triage_model,
        "service_tier": args.service_tier,
        "max_workers": args.max_workers,
        "estimate_only": bool(args.estimate_only),
    }
    claims_io.write_json(run_dir / CONFIG_FILE, config)

    docs_path = run_dir / DOCUMENTS_FILE
    docs: list[dict[str, Any]] = []
    if "sample" in phases:
        if args.continue_run and docs_path.is_file():
            log_stderr(f"events: [sample] reuse {docs_path}")
            docs = load_documents(docs_path)
        else:
            log_stderr(f"events: [sample] loading chunks from corpus={args.corpus}")
            all_docs = load_corpus_documents(args.corpus, max_body_chars=args.max_body_chars)
            log_stderr(f"events: [sample] {len(all_docs)} non-empty chunks")
            docs = stratified_sample(
                all_docs,
                args.n,
                seed=args.sample_seed,
                floor=args.platform_floor,
            )
            write_documents(docs_path, docs)
            log_stderr(f"events: [sample] wrote {len(docs)} docs → {docs_path}")
    elif docs_path.is_file():
        docs = load_documents(docs_path)

    todo = remaining_docs(docs, run_dir, args.n if args.continue_run else None)
    # Fresh sample already sized to --n; continuing respects --n as remaining budget.
    if args.estimate_only:
        log_stderr(f"events: [estimate] {len(todo)} LLM prompts (n={args.n})")
        claims_io.emit_json(
            {
                "run_dir": str(run_dir),
                "n_prompts": len(todo),
                "n": args.n,
                "n_docs": len(docs),
            }
        )
        return 0

    mentions = load_mentions(run_dir / MENTIONS_FILE)
    if "extract" in phases:
        if not docs:
            raise SystemExit("extract needs documents.jsonl; run sample first")
        mentions = run_extract(
            docs=docs,
            run_dir=run_dir,
            n=args.n if args.continue_run else None,
            provider=args.llm_provider,
            model=args.model,
            triage_model=args.triage_model,
            max_workers=args.max_workers,
            service_tier=args.service_tier,
        )

    if "report" in phases:
        if not docs and docs_path.is_file():
            docs = load_documents(docs_path)
        if not mentions:
            mentions = load_mentions(run_dir / MENTIONS_FILE)
        metrics = write_report(run_dir=run_dir, mentions=mentions, docs=docs, config=config)
        claims_io.emit_json({"run_dir": str(run_dir), **metrics})
    else:
        claims_io.emit_json(
            {"run_dir": str(run_dir), "n_mentions": len(mentions), "n_docs": len(docs)}
        )
    log_stderr("events: finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
