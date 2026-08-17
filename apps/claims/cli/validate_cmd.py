"""CLI: validate nested claims.json."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.cli import paths as path_helpers


def cmd_validate(args: Namespace) -> int:
    from apps.claims import validate_nested as validate_mod

    try:
        if getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            claims_path = path_helpers.path_or_corpus(args.claims, corpus.claims)
        else:
            if args.claims is None:
                raise ValueError("Provide --claims or --corpus")
            claims_path = Path(args.claims)
        summary = validate_mod.run(claims_path)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1

    payload = {"ok": True, "claims": str(claims_path), **summary}
    claims_io.emit_json(payload)
    if getattr(args, "human", False):
        hist = summary.get("claim_count_hist") or {}
        print(f"Total posts: {summary['total_posts']}")
        print(f"Total chunks: {summary['total_chunks']}")
        print(f"Successful chunks: {summary['success_chunks']}")
        print(f"Failed chunks: {summary['failed_chunks']}")
        print(f"Unprocessed chunks: {summary['unprocessed_chunks']}")
        print(f"Empty chunks: {summary['empty_chunks']}")
        print(f"Malformed chunks: {summary['malformed_chunks']}")
        print(f"Total claims: {summary['total_claims']}")
        print("Chunks by claim count:")
        for key in ("0", "1", "2", "3", ">3"):
            print(f"  {key} claims: {hist.get(key, 0)}")
        top = summary.get("top_errors") or []
        if top:
            print("Most common errors:")
            for row in top:
                print(f"  {row['count']:>6}  {row['error']}")
    return 0
