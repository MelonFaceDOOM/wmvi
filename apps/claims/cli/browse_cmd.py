"""CLI: browse / sample claim texts for pointwise labeling (no neighbor search).

Default source is ``groups.json`` under a corpus (no embed run required).
Pass ``--run-dir`` or ``--corpus`` + ``--model-tag``/``--model`` to sample from
an embed run's ``index.json`` instead.
"""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Any

from apps.claims import claim_sample
from apps.claims import io as claims_io
from apps.claims import selections as sel_mod
from apps.claims.cli import paths as path_helpers


def _has_model_ref(args: Namespace) -> bool:
    return bool(getattr(args, "model_tag", None) or getattr(args, "model", None))


def _resolve_sample_index(args: Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (normalized index, source meta) for sampling.

    Precedence:
    1. ``--run-dir`` → embed run index
    2. ``--corpus`` + ``--model-tag``/``--model`` → that corpus run's index
    3. ``--corpus`` alone → ``corpora/<corpus>/groups.json``
    """
    if getattr(args, "run_dir", None) is not None:
        run_dir = Path(args.run_dir)
        return claims_io.load_run_index(run_dir), {
            "source": "run",
            "run_dir": str(run_dir),
            "groups_path": None,
        }

    if getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        if _has_model_ref(args):
            tag = path_helpers.resolve_model_tag(args)
            run_dir = corpus.run_dir(tag)
            return claims_io.load_run_index(run_dir), {
                "source": "run",
                "run_dir": str(run_dir),
                "groups_path": None,
            }
        groups_path = corpus.groups
        if not groups_path.is_file():
            raise FileNotFoundError(
                f"Missing groups.json at {groups_path} "
                "(run `group --corpus ...` first, or pass --model-tag / --run-dir)"
            )
        return claims_io.normalize_index(claims_io.read_json(groups_path)), {
            "source": "groups",
            "run_dir": None,
            "groups_path": str(groups_path),
        }

    raise ValueError(
        "Provide --corpus (samples groups.json), "
        "or --run-dir / --corpus with --model-tag/--model (samples a run index)"
    )


def _print_human(claims: list[dict[str, Any]]) -> None:
    for i, row in enumerate(claims, start=1):
        text = " ".join(str(row.get("text") or "").split())
        print(
            f"{i}. idx={row.get('idx')} key={row.get('claim_key')}: {text}"
        )


def _apply_key_subset(
    args: Namespace, index: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Subset ``index`` by ``--filter`` / ``--where-annotation`` and/or ``--selection``.

    Multiple annotation clauses are AND-ed; a named selection is also AND-ed.
    """
    from apps.claims import filtering as filt

    has_filter = bool(filt.clauses_from_args(args))
    sel_name = getattr(args, "selection", None)
    if not has_filter and not sel_name:
        return index, None
    if not getattr(args, "corpus", None):
        raise ValueError("--filter / --where-annotation / --selection require --corpus")

    corpus = path_helpers.require_corpus(args)
    groups_hash = str(index.get("source_hash") or None)
    wanted, filter_meta = filt.resolve_keys_for_args(
        args, corpus.root, groups_hash=groups_hash
    )
    if wanted is None:
        return index, None
    sub = filt.subset_index_by_keys(index, wanted, filter_meta=filter_meta)
    return sub, filter_meta


def cmd_browse(args: Namespace) -> int:
    try:
        index, source_meta = _resolve_sample_index(args)
        index, filter_meta = _apply_key_subset(args, index)
        claim_texts = claims_io.claim_texts_from_index(index)
        claim_keys = sel_mod.claim_keys_from_index(index)
        if len(claim_keys) != len(claim_texts):
            raise ValueError(
                f"claim_keys length ({len(claim_keys)}) != claim_texts ({len(claim_texts)})"
            )

        n = int(getattr(args, "sample", 0) or 0)
        if n < 1:
            raise ValueError("--sample must be >= 1")

        exclude_path = getattr(args, "exclude", None)
        exclude_keys = claim_sample.load_exclude_claim_keys(
            Path(exclude_path) if exclude_path is not None else None
        )
        seed = int(getattr(args, "seed", 0) or 0)
        indices = claim_sample.sample_claim_indices(
            claim_texts,
            n=n,
            seed=seed,
            claim_keys=claim_keys,
            exclude_keys=exclude_keys,
        )
        claims = claim_sample.claim_rows_from_index(
            index,
            indices,
            claim_texts=claim_texts,
            claim_keys=claim_keys,
        )
        payload = {
            "ok": True,
            "source": source_meta["source"],
            "run_dir": source_meta["run_dir"],
            "groups_path": source_meta["groups_path"],
            "sample": n,
            "seed": seed,
            "n_pool": len(claim_texts),
            "n_excluded": len(exclude_keys),
            "n_returned": len(claims),
            "exclude": str(exclude_path) if exclude_path is not None else None,
            "filter": filter_meta,
            "claims": claims,
        }
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1

    claims_io.emit_json(payload)
    if getattr(args, "human", False):
        _print_human(claims)
    return 0
