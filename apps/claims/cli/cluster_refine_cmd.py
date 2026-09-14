"""CLI: recursive 2-means split and greedy LLM merge of cluster labels."""

from __future__ import annotations

import sys
from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.clustering.browse import load_browse_bundle
from apps.claims.clustering.merge_prompts import (
    DEFAULT_MERGE_MODEL,
    DEFAULT_NEIGHBOR_K,
    DEFAULT_NN_THRESHOLD,
)
from apps.claims.clustering.refine import (
    DEFAULT_MIN_SPLIT_GAIN,
    estimate_merge_prompts,
    greedy_merge,
    n_logged_prompts,
    openai_merge_fn,
    parent_payload,
    read_jsonl,
    remap_labels,
    sibling_out_dir,
    split_labels,
    stamp_from_output,
    write_refine_experiment,
)


def _require_from(args: Namespace) -> Path:
    source = getattr(args, "from_path", None)
    if source is None:
        raise ValueError("Provide --from (experiment dir, result_*.json, or labels_*.npy)")
    return Path(source)


def _load_bundle(args: Namespace, source: Path):
    return load_browse_bundle(
        source,
        corpus=getattr(args, "corpus", None),
        model_tag=getattr(args, "model_tag", None),
        run_dir=getattr(args, "run_dir", None),
        labels=getattr(args, "labels", None),
        selection=getattr(args, "selection", None),
        filter_specs=list(getattr(args, "filter", None) or []),
        claims_path=getattr(args, "claims", None),
    )


def cmd_cluster_split(args: Namespace) -> int:
    try:
        source = _require_from(args)
        print(f"cluster-split: loading {source}", file=sys.stderr)
        bundle = _load_bundle(args, source)
        texts = claims_io.claim_texts_from_index(bundle.index)
        min_gain = float(getattr(args, "min_gain", DEFAULT_MIN_SPLIT_GAIN))
        min_child_n = int(getattr(args, "min_child_n", 2))
        seed = int(getattr(args, "seed", 0))

        def _progress(i: int, n: int) -> None:
            if i == 1 or i == n or i % 50 == 0:
                print(f"cluster-split: parent {i}/{n}", file=sys.stderr)

        labels, meta = split_labels(
            bundle.vectors,
            bundle.labels,
            texts,
            min_gain=min_gain,
            min_child_n=min_child_n,
            seed=seed,
            on_parent=_progress,
        )
        out_dir = Path(args.out_dir) if getattr(args, "out_dir", None) else sibling_out_dir(source, "_split")
        stamp = stamp_from_output(bundle.output.labels_path)
        payload = parent_payload(
            bundle.output.payload,
            n_clusters=int(meta["n_terminals"]),
            extra={"parent_run": str(source), "split": meta},
        )
        result_path, labels_path = write_refine_experiment(
            out_dir, stamp=stamp, labels=labels, payload=payload
        )
        payload["result_path"] = str(result_path)
        payload["labels_path"] = str(labels_path)
        payload["out_dir"] = str(out_dir)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0


def cmd_cluster_merge(args: Namespace) -> int:
    try:
        source = _require_from(args)
        print(f"cluster-merge: loading {source}", file=sys.stderr)
        bundle = _load_bundle(args, source)
        nn_threshold = float(getattr(args, "nn_threshold", DEFAULT_NN_THRESHOLD))
        neighbor_k = int(getattr(args, "neighbor_k", DEFAULT_NEIGHBOR_K))
        estimate = estimate_merge_prompts(
            bundle, nn_threshold=nn_threshold, neighbor_k=neighbor_k
        )
        print(
            "cluster-merge estimate: "
            f"n_terminals={estimate['n_terminals']} "
            f"n_seeds_max={estimate['n_seeds_max']} "
            f"(LLM never merges) "
            f"n_seeds_min={estimate['n_seeds_min']} "
            f"(absorb all neighbors above {nn_threshold})",
            file=sys.stderr,
        )
        if getattr(args, "estimate_only", False):
            claims_io.emit_json({"estimate": estimate, "nn_threshold": nn_threshold, "neighbor_k": neighbor_k})
            return 0

        from nlp.claim_extraction.clients import load_openai_config

        load_openai_config()

        out_dir = Path(args.out_dir) if getattr(args, "out_dir", None) else sibling_out_dir(source, "_merge")
        log_path = Path(args.log) if getattr(args, "log", None) else out_dir / "merge_log.jsonl"
        resume_log = read_jsonl(log_path)
        model = str(getattr(args, "model", None) or DEFAULT_MERGE_MODEL)
        print(
            f"cluster-merge: model={model} log={log_path} "
            f"resumed_rows={len(resume_log)}",
            file=sys.stderr,
        )
        id_to_root, _rows, n_prompts_run = greedy_merge(
            bundle,
            nn_threshold=nn_threshold,
            neighbor_k=neighbor_k,
            merge_fn=openai_merge_fn(model),
            log_path=log_path,
            resume_log=resume_log,
            on_prompt=lambda n, sid: print(
                f"cluster-merge: prompt {n} seed={sid}", file=sys.stderr
            ),
        )
        labels, _dense = remap_labels(bundle.labels, id_to_root)
        n_after = int(len({int(x) for x in labels.tolist() if int(x) != -1}))
        n_prompts = n_logged_prompts(resume_log) + int(n_prompts_run)
        stamp = stamp_from_output(bundle.output.labels_path)
        merge_meta = {
            "nn_threshold": nn_threshold,
            "neighbor_k": neighbor_k,
            "model": model,
            "n_before": int(estimate["n_terminals"]),
            "n_after": n_after,
            "n_prompts": n_prompts,
            "n_prompts_this_run": int(n_prompts_run),
        }
        payload = parent_payload(
            bundle.output.payload,
            n_clusters=n_after,
            extra={"parent_run": str(source), "merge": merge_meta, "estimate": estimate},
        )
        result_path, labels_path = write_refine_experiment(
            out_dir, stamp=stamp, labels=labels, payload=payload
        )
        payload["result_path"] = str(result_path)
        payload["labels_path"] = str(labels_path)
        payload["out_dir"] = str(out_dir)
        payload["log_path"] = str(log_path)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0
