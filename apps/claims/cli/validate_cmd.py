"""CLI: validate, prepare trim/coref."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.cli import paths as path_helpers


def cmd_validate(args: Namespace) -> int:
    from apps.claims.extraction import validate as validate_mod

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
        print(f"Total rows: {summary['total_rows']}")
        print(f"Successful rows: {summary['success_rows']}")
        print(f"Failed rows: {summary['failed_rows']}")
        print(f"Malformed rows: {summary['malformed_rows']}")
        print(f"Total claims: {summary['total_claims']}")
        print("Posts by claim count:")
        for key in ("0", "1", "2", "3", ">3"):
            print(f"  {key} claims: {hist.get(key, 0)}")
        top = summary.get("top_errors") or []
        if top:
            print("Most common errors:")
            for row in top:
                print(f"  {row['count']:>6}  {row['error']}")
    return 0


def cmd_prepare_trim(args: Namespace) -> int:
    from apps.claims.prepare import trim as trim_mod

    try:
        posts, out = _resolve_prepare_paths(
            args,
            default_suffix="posts_trimmed.json",
        )
        summary = trim_mod.run(posts_path=posts, out_path=out)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(summary)
    return 0


def cmd_prepare_coref(args: Namespace) -> int:
    from apps.claims.prepare import coref as coref_mod

    try:
        posts, out = _resolve_prepare_paths(
            args,
            default_suffix="posts_coref.json",
        )
        batch_size = getattr(args, "batch_size", None)
        summary = coref_mod.run(
            posts_path=posts,
            out_path=out,
            batch_size=batch_size,
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(summary)
    return 0


def _resolve_prepare_paths(
    args: Namespace,
    *,
    default_suffix: str,
) -> tuple[Path, Path]:
    if getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        posts = path_helpers.path_or_corpus(getattr(args, "posts", None), corpus.posts)
        if getattr(args, "force", False) and getattr(args, "out", None) is None:
            out = corpus.posts
        else:
            default_out = corpus.root / default_suffix
            out = path_helpers.path_or_corpus(getattr(args, "out", None), default_out)
        return posts, out
    if args.posts is None or args.out is None:
        raise ValueError("Provide --posts and --out, or --corpus")
    return Path(args.posts), Path(args.out)
