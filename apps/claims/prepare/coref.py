"""Claims posts-JSON adapters for coreference (impl in ``nlp.coref``)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Optional

from nlp.coref import (  # noqa: F401 — re-export public algorithm APIs
    COREF_DEBUG_EVERY,
    COREF_DEBUG_PERF,
    COREF_MAX_CHARS,
    COREF_METRICS_EVERY,
    COREF_RESET_EVERY_BATCHES,
    DEVICE,
    PIPE_BATCH_SIZE,
    SPACY_EXCLUDE,
    SPACY_MODEL,
    iter_coref_resolved_posts,
    process_payload,
)


def run(
    *,
    posts_path: Path,
    out_path: Path,
    batch_size: int | None = None,
) -> dict[str, Any]:
    """Resolve coreference for posts JSON and write result."""
    raw = json.loads(posts_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("input JSON must be an object")
    posts = raw.get("posts")
    if not isinstance(posts, list):
        raise ValueError("input JSON must have a top-level posts array")
    bs = max(1, int(batch_size if batch_size is not None else PIPE_BATCH_SIZE))
    raw["posts"] = list(iter_coref_resolved_posts(posts, batch_size=bs))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    n = len(raw.get("posts") or [])
    return {"ok": True, "out": str(out_path), "post_count": n}


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser(description="Resolve coreference in post JSON.")
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--batch-size", type=int, default=PIPE_BATCH_SIZE)
    args = ap.parse_args(list(argv) if argv is not None else None)

    summary = run(
        posts_path=args.input,
        out_path=args.output,
        batch_size=max(1, int(args.batch_size)),
    )
    print(
        f"[ok] wrote {summary['post_count']} posts with coreference fields "
        f"to {args.output.resolve()}"
    )


if __name__ == "__main__":
    main()
