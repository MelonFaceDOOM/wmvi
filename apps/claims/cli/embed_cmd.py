from __future__ import annotations

import sys
import time
from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims import models as models_mod
from apps.claims import notes as notes_mod
from apps.claims.cli import paths as path_helpers
from apps.claims.embedding import embed as embed_mod
from apps.claims.grouping import group as grouping
from apps.claims.types import EmbedConfig


def cmd_embed(args: Namespace) -> int:
    try:
        model_id = models_mod.resolve_model(str(args.model))
        corpus = None
        if getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            groups_path = path_helpers.path_or_corpus(args.groups, corpus.groups)
            model_tag = path_helpers.resolve_model_tag(args, model_id=str(args.model))
            run_name = str(args.run_name) if args.run_name else corpus.run_name(model_tag)
        else:
            if args.groups is None or args.run_name is None:
                raise ValueError("Provide --groups and --run-name, or --corpus")
            groups_path = Path(args.groups)
            run_name = str(args.run_name)

        bundle = grouping.load_groups_json(groups_path)
        groups = list(bundle.groups)
        limit = getattr(args, "limit", None)
        if limit is not None and int(limit) > 0:
            groups = groups[: int(limit)]

        run_dir = claims_io.runs_dir() / run_name
        if run_dir.exists() and any(run_dir.iterdir()) and not bool(getattr(args, "force", False)):
            raise FileExistsError(f"Run dir already exists: {run_dir} (pass --force to overwrite)")

        t0 = time.monotonic()

        def on_progress(done: int, total: int, msg: str) -> None:
            elapsed = time.monotonic() - t0
            rate = done / elapsed if elapsed > 0 else 0.0
            eta = (total - done) / rate if rate > 0 else None
            eta_s = f" eta={eta:.0f}s" if eta is not None else ""
            print(
                f"[embed] {done}/{total} ({100.0 * done / total if total else 0:.1f}%) "
                f"{rate:.1f}/s{eta_s} {msg}",
                file=sys.stderr,
                flush=True,
            )

        metrics = embed_mod.run(
            config=EmbedConfig(
                model_id=model_id,
                doc_instruction=str(args.doc_instruction or ""),
                query_instruction=str(args.query_instruction or ""),
                normalize=not bool(args.no_normalize),
            ),
            groups=groups,
            source_hash=bundle.source_hash,
            source_path=bundle.source_path,
            source_claim_count=bundle.source_claim_count,
            run_dir=run_dir,
            on_progress=on_progress,
        )
        if corpus is not None:
            notes_mod.append_note(
                corpus.notes,
                "Embedded",
                notes_mod.fmt_kv(
                    {
                        "run": run_name,
                        "model": model_id,
                        "claim_count": metrics.get("claim_count"),
                        "vector_dim": metrics.get("vector_dim"),
                        "source_hash": bundle.source_hash[:16] if bundle.source_hash else None,
                        "wall_seconds": metrics.get("wall_seconds"),
                    }
                ),
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, "run_dir": str(run_dir), "run_name": run_name, **metrics})
    return 0
