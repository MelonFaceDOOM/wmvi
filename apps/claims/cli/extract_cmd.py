from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims import notes as notes_mod
from apps.claims.cli import paths as path_helpers
from apps.claims.extraction import extract as extract_mod


def cmd_extract(args: Namespace) -> int:
    try:
        corpus = None
        if getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            posts = path_helpers.path_or_corpus(args.posts, corpus.posts)
            out = path_helpers.path_or_corpus(args.out, corpus.claims)
        else:
            if args.posts is None or args.out is None:
                raise ValueError("Provide --posts and --out, or --corpus")
            posts = Path(args.posts)
            out = Path(args.out)
        n_posts = int(args.n_posts)
        extract_mod.run(
            posts_path=posts,
            out_path=out,
            n_posts=n_posts,
            claims_only=bool(args.claims_only),
        )
        if corpus is not None:
            notes_mod.append_note(
                corpus.notes,
                "Extracted",
                notes_mod.fmt_kv(
                    {
                        "posts": str(posts),
                        "out": str(out),
                        "n_posts_limit": n_posts or "all",
                        "claims_only": bool(args.claims_only),
                    }
                ),
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, "out": str(out), "posts": str(posts)})
    return 0
