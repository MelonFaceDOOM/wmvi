"""Thin step adapters used by pipeline recipes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims import selections as sel_mod
from apps.claims.clustering.browse import portable_run_dir_str
from apps.claims.grouping import group as grouping
from apps.claims.pipeline import Ctx, groups_source_hash
from apps.claims.types import EmbedConfig

# Re-export NLI step
from apps.claims.steps.nli_step import step_nli  # noqa: E402

__all__ = [
    "step_group",
    "step_embed",
    "step_label",
    "step_select_threshold",
    "step_cluster",
    "step_nli",
]


def step_group(ctx: Ctx) -> Path:
    """Write groups.json from claims.json."""
    bundle = grouping.run(ctx.corpus.claims)
    claims_io.write_json(ctx.corpus.groups, grouping.bundle_to_dict(bundle))
    return ctx.corpus.groups


def step_embed(ctx: Ctx, *, model: str, model_tag: str | None = None, force: bool = False) -> Path:
    """Embed groups into data/runs/<corpus>/<tag>/."""
    from apps.claims.embedding import embed as embed_mod
    from apps.claims import models as models_mod

    tag = model_tag or ctx.model_tag
    if not tag:
        from apps.claims import corpus as corpus_mod

        tag = corpus_mod.model_tag_from_path(model)
    run_dir = ctx.corpus.run_dir(tag)
    if run_dir.exists() and not (force or ctx.force):
        ctx.note(f"[skip] embed: {run_dir} exists")
        return run_dir
    if run_dir.exists() and (force or ctx.force):
        import shutil

        shutil.rmtree(run_dir)
    bundle = grouping.load_groups_json(ctx.corpus.groups)
    resolved = models_mod.resolve_model(model)
    config = EmbedConfig(model_id=str(resolved))
    embed_mod.run(
        config=config,
        groups=bundle.groups,
        source_hash=bundle.source_hash,
        source_path=bundle.source_path,
        source_claim_count=bundle.source_claim_count,
        run_dir=run_dir,
    )
    ctx.model_tag = tag
    return run_dir


def step_label(
    ctx: Ctx,
    *,
    model_ref: str,
    annotation_name: str,
    value_type: str | None = None,
    intent: str | None = None,
    batch_size: int = 32,
) -> ann_mod.Annotation:
    """Score grouped claims with a labeler model ref; write annotation.

    ``model_ref`` is ``intent@active``, ``intent/version``, or a filesystem path
    to a Ridge artifact directory (same resolution as ``labeler apply --model``).
    """
    from apps.claims.labeling import lifecycle as label_life

    return label_life.apply_labeler(
        corpus_root=ctx.root,
        groups_path=ctx.corpus.groups,
        model_ref=model_ref,
        annotation_name=annotation_name,
        batch_size=batch_size,
        force=ctx.force,
        source_hash=groups_source_hash(ctx),
        intent=intent,
        value_type=value_type,
    )


def step_select_threshold(
    ctx: Ctx,
    *,
    annotation: str,
    name: str,
    low: float | None = None,
    high: float | None = None,
) -> sel_mod.Selection:
    ann = ann_mod.read_annotation(ctx.root, annotation)
    sel = sel_mod.from_threshold(ann, name=name, low=low, high=high)
    sel_mod.write_selection(ctx.root, sel, force=ctx.force)
    return sel


def step_cluster(
    ctx: Ctx,
    *,
    algorithm: str,
    params: dict[str, Any] | None = None,
    selection: str | None = None,
    seed: int = 0,
    save_labels: bool = True,
    annotation_name: str | None = None,
    promote_annotation: bool = False,
) -> dict[str, Any]:
    """Cluster a run (optionally subset by selection).

    Cluster labels stay in the experiment directory by default. Pass
    ``promote_annotation=True`` (and optionally ``annotation_name``) to write a
    corpus annotation.
    """
    import json
    from argparse import Namespace

    from apps.claims.cli.cluster_cmd import cmd_cluster

    tag = ctx.model_tag
    if not tag:
        raise ValueError("ctx.model_tag is required for clustering")
    args = Namespace(
        algorithm=algorithm,
        params_json=json.dumps(params or {}),
        seed=seed,
        corpus=ctx.corpus.slug,
        model_tag=tag,
        model=None,
        run_dir=None,
        selection=selection,
        out_dir=None,
        save_labels=save_labels,
        queries=None,
    )
    # Capture via temporary monkeypatch of emit? Just call evaluate path directly.
    from apps.claims.cli import cluster_cmd as cc
    import hashlib
    import numpy as np
    from apps.claims.clustering import query_eval

    vectors, index, run_dir, selection_name = cc._load_run_with_selection(args)
    config = cc._config_from_index(index)
    stamp = hashlib.sha1(
        json.dumps({"a": algorithm, "p": params or {}, "s": seed, "sel": selection}, sort_keys=True).encode()
    ).hexdigest()[:10]
    default_exp = f"cluster_{selection}_{stamp}" if selection else f"cluster_{stamp}"
    out_dir = ctx.corpus.experiment_dir(tag, default_exp)
    out_dir.mkdir(parents=True, exist_ok=True)
    queries = query_eval.load_eval_queries(cc._DEFAULT_QUERIES) if cc._DEFAULT_QUERIES.is_file() else []
    query_vectors = (
        cc.load_or_build_query_cache(config=config, out_dir=out_dir, queries=queries, force=False)
        if queries
        else {}
    )
    if queries:
        labels, payload = cc.evaluate_clustering(
            vectors=vectors,
            config=config,
            algorithm=algorithm,
            params=params or {},
            seed=seed,
            queries=queries,
            query_vectors=query_vectors,
        )
    else:
        from apps.claims.clustering import cluster as clustering
        from apps.claims.clustering import metrics as cluster_metrics

        result = clustering.run_clustering(vectors, algorithm=algorithm, params=params or {}, seed=seed)
        labels = result.labels
        metrics = cluster_metrics.compute_cluster_metrics(vectors, labels)
        payload = {
            "algorithm": algorithm,
            "params": params or {},
            "seed": seed,
            "n_clusters": result.n_clusters,
            "n_noise": result.n_noise,
            "coverage_pct": metrics.get("coverage_pct"),
            "mean_intra_cosine": metrics.get("mean_intra_cosine"),
        }
    payload["run_dir"] = portable_run_dir_str(run_dir)
    if selection_name:
        payload["selection"] = selection_name
        payload["n_selected"] = int(vectors.shape[0])
    claims_io.write_json(out_dir / f"result_{stamp}.json", payload)
    labels_path = out_dir / f"labels_{stamp}.npy"
    if save_labels:
        np.save(labels_path, labels)
        payload["labels_path"] = str(labels_path)

    # Optionally promote cluster labels to a corpus annotation (off by default)
    if promote_annotation:
        ann_name = annotation_name or (
            f"cluster_labels_{selection}" if selection else "cluster_labels"
        )
        keys = sel_mod.claim_keys_from_index(index)
        values = {k: int(labels[i]) for i, k in enumerate(keys)}
        ann_mod.write_annotation(
            ctx.root,
            ann_name,
            values,
            scope="group",
            producer="apps.claims.steps.cluster",
            producer_kind="derived",
            params={"algorithm": algorithm, "params": params or {}, "selection": selection},
            source_hash=str(index.get("source_hash") or "") or groups_source_hash(ctx),
            force=True,
        )
        payload["annotation"] = ann_name
    payload["out_dir"] = str(out_dir)
    return payload
