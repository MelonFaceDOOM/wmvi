from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims import notes as notes_mod
from apps.claims.cli import paths as path_helpers
from apps.claims.grouping import group as grouping


def cmd_group(args: Namespace) -> int:
    try:
        corpus = None
        if getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            claims_path = path_helpers.path_or_corpus(args.claims, corpus.claims)
            out_path = path_helpers.path_or_corpus(args.out, corpus.groups)
        else:
            if args.claims is None or args.out is None:
                raise ValueError("Provide --claims and --out, or --corpus")
            claims_path = Path(args.claims)
            out_path = Path(args.out)
        bundle = grouping.run(claims_path)
        claims_io.write_json(out_path, grouping.bundle_to_dict(bundle))
        if corpus is not None:
            notes_mod.append_note(
                corpus.notes,
                "Grouped",
                notes_mod.fmt_kv(
                    {
                        "claim_count": bundle.claim_count,
                        "source_claim_count": bundle.source_claim_count,
                        "source_hash": bundle.source_hash[:16] if bundle.source_hash else None,
                        "out": str(out_path),
                    }
                ),
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(
        {
            "ok": True,
            "out": str(out_path),
            "claim_count": bundle.claim_count,
            "source_claim_count": bundle.source_claim_count,
            "source_hash": bundle.source_hash,
        }
    )
    return 0
