"""CLI: browse claim nearest neighbors from an embed run."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Any

from apps.claims import claim_sample
from apps.claims import io as claims_io
from apps.claims import models as models_mod
from apps.claims import selections as sel_mod
from apps.claims.cli import paths as path_helpers
from apps.claims.embedding.triplet_neighbors import (
    format_neighbors_list,
    neighbors_for_claim_index,
    neighbors_for_query_text,
)


def _resolve_run_dir(args: Namespace) -> Path:
    if getattr(args, "run_dir", None) is not None:
        return Path(args.run_dir)
    if getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        tag = path_helpers.resolve_model_tag(args)
        return corpus.run_dir(tag)
    raise ValueError("Provide --run-dir, or --corpus with --model-tag/--model")


def _neighbor_rows(neighbors: list[tuple[int, float, str]]) -> list[dict[str, Any]]:
    return [{"idx": int(i), "score": float(s), "text": t} for i, s, t in neighbors]


def _print_human(results: list[dict[str, Any]]) -> None:
    for i, row in enumerate(results):
        if i:
            print()
        anchor = row.get("anchor") or {}
        idx = anchor.get("idx")
        key = anchor.get("claim_key")
        text = " ".join(str(anchor.get("text") or "").split())
        if idx is None:
            print(f"Query: {text}")
        elif key:
            print(f"Anchor [idx={idx} key={key}]: {text}")
        else:
            print(f"Anchor [idx={idx}]: {text}")
        neighbors = [
            (int(n["idx"]), float(n["score"]), str(n["text"]))
            for n in (row.get("neighbors") or [])
        ]
        print("Neighbors:")
        print(format_neighbors_list(neighbors))


def cmd_neighbors(args: Namespace) -> int:
    try:
        run_dir = _resolve_run_dir(args)
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        claim_keys = sel_mod.claim_keys_from_index(index)
        if len(claim_texts) != len(vectors):
            raise ValueError(
                f"claim_texts length ({len(claim_texts)}) != vectors rows ({len(vectors)})"
            )

        top_k = max(1, int(getattr(args, "top_k", 15) or 15))
        results: list[dict[str, Any]] = []
        mode: str
        exclude_path = getattr(args, "exclude", None)
        exclude_keys = claim_sample.load_exclude_claim_keys(
            Path(exclude_path) if exclude_path is not None else None
        )

        if getattr(args, "claim_index", None) is not None:
            mode = "claim_index"
            claim_index = int(args.claim_index)
            if claim_index < 0 or claim_index >= len(vectors):
                raise ValueError(
                    f"--claim-index {claim_index} out of range [0, {len(vectors) - 1}]"
                )
            neighbors = neighbors_for_claim_index(
                claim_index,
                vectors=vectors,
                claim_texts=claim_texts,
                top_k=top_k,
            )
            results.append(
                {
                    "anchor": {
                        "idx": claim_index,
                        "claim_key": claim_keys[claim_index] if claim_index < len(claim_keys) else "",
                        "text": claim_texts[claim_index] if claim_index < len(claim_texts) else "",
                    },
                    "neighbors": _neighbor_rows(neighbors),
                }
            )
        elif getattr(args, "text", None):
            mode = "text"
            query = str(args.text)
            model_raw = str(index.get("model_id") or "").strip()
            if not model_raw:
                raise ValueError("run index.json missing model_id (needed for --text)")
            model_id = models_mod.resolve_model(model_raw)
            neighbors = neighbors_for_query_text(
                query,
                vectors=vectors,
                claim_texts=claim_texts,
                model_id=model_id,
                doc_instruction=str(index.get("doc_instruction") or ""),
                query_instruction=str(index.get("query_instruction") or ""),
                top_k=top_k,
                dtype=str(index.get("dtype") or "auto"),
                max_seq_length=(
                    int(index["max_seq_length"])
                    if index.get("max_seq_length") is not None
                    else None
                ),
            )
            results.append(
                {
                    "anchor": {"idx": None, "claim_key": None, "text": query},
                    "neighbors": _neighbor_rows(neighbors),
                }
            )
        else:
            mode = "sample"
            n = int(args.sample)
            if n < 1:
                raise ValueError("--sample must be >= 1")
            seed = int(getattr(args, "seed", 0) or 0)
            for claim_index in claim_sample.sample_claim_indices(
                claim_texts,
                n=n,
                seed=seed,
                claim_keys=claim_keys,
                exclude_keys=exclude_keys,
            ):
                neighbors = neighbors_for_claim_index(
                    claim_index,
                    vectors=vectors,
                    claim_texts=claim_texts,
                    top_k=top_k,
                )
                results.append(
                    {
                        "anchor": {
                            "idx": claim_index,
                            "claim_key": claim_keys[claim_index] if claim_index < len(claim_keys) else "",
                            "text": claim_texts[claim_index],
                        },
                        "neighbors": _neighbor_rows(neighbors),
                    }
                )

        payload = {
            "ok": True,
            "mode": mode,
            "run_dir": str(run_dir),
            "top_k": top_k,
            "n_excluded": len(exclude_keys),
            "n_results": len(results),
            "exclude": str(exclude_path) if exclude_path is not None else None,
            "results": results,
        }
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1

    claims_io.emit_json(payload)
    if getattr(args, "human", False):
        _print_human(results)
    return 0
