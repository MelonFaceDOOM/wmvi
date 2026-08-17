"""Example recipe: measles corpus → group → embed → (optional) stance split → cluster.

Run from repo root after importing nested claims::

    python -m apps.claims corpus import-claims --name measles --create --force \\
      --from data/measles_1.json
    python -m apps.claims.pipelines.measles_v1 --model BAAI/bge-small-en-v1.5

Does not fetch or extract (use ``scripts/get_posts_extract_upload.py`` for that).
This recipe starts from an existing nested ``claims.json`` under ``data/corpora/measles/``.
"""

from __future__ import annotations

import argparse

from apps.claims.pipeline import Ctx, step
from apps.claims.steps import (
    step_cluster,
    step_embed,
    step_group,
    step_label,
    step_select_threshold,
)


def run(
    *,
    corpus: str = "measles",
    model: str,
    model_tag: str | None = None,
    force: bool = False,
    with_stance: bool = False,
    with_nli: bool = False,
    stance_model: str = "claim_vaccine_alignment_score@active",
    cluster_algorithm: str = "kmeans",
    cluster_params: dict | None = None,
) -> Ctx:
    ctx = Ctx.for_corpus(corpus, model_tag=model_tag, force=force)
    if not ctx.corpus.claims.is_file():
        raise FileNotFoundError(
            f"Missing {ctx.corpus.claims}; import nested claims first "
            f"(e.g. python -m apps.claims corpus import-claims --name measles "
            f"--from data/measles_1.json --create)"
        )

    step(ctx, name="group", run=lambda c: step_group(c))
    step(
        ctx,
        name="embed",
        run=lambda c: step_embed(c, model=model, model_tag=model_tag, force=force),
    )

    if with_stance:
        step(
            ctx,
            name="label_stance",
            output_annotation="stance",
            run=lambda c: step_label(
                c,
                model_ref=stance_model,
                annotation_name="stance",
                intent="claim_vaccine_alignment_score",
                value_type="float",
            ),
        )
        for sel_name, low, high in (
            ("anti", None, 0.33),
            ("neutral", 0.33, 0.66),
            ("pro", 0.66, None),
        ):
            step(
                ctx,
                name=f"select_{sel_name}",
                run=lambda c, n=sel_name, lo=low, hi=high: step_select_threshold(
                    c, annotation="stance", name=n, low=lo, high=hi
                ),
            )
            step(
                ctx,
                name=f"cluster_{sel_name}",
                run=lambda c, n=sel_name: step_cluster(
                    c,
                    algorithm=cluster_algorithm,
                    params=cluster_params or {"n_clusters": 25},
                    selection=n,
                    annotation_name=f"cluster_labels_{n}",
                ),
            )
    else:
        step(
            ctx,
            name="cluster",
            run=lambda c: step_cluster(
                c,
                algorithm=cluster_algorithm,
                params=cluster_params or {"n_clusters": 50},
            ),
        )

    if with_nli:
        from apps.claims.steps import step_nli

        cluster_ann = "cluster_labels_anti" if with_stance else "cluster_labels"
        step(
            ctx,
            name="nli",
            output_annotation="nli_subgroup",
            run=lambda c: step_nli(c, cluster_annotation=cluster_ann),
        )
    return ctx


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m apps.claims.pipelines.measles_v1")
    ap.add_argument("--corpus", default="measles")
    ap.add_argument("--model", required=True, help="Embedder id, path, or registered tag")
    ap.add_argument("--model-tag", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument(
        "--with-stance",
        action="store_true",
        help="Label stance, split anti/neutral/pro, cluster each selection (no re-embed)",
    )
    ap.add_argument(
        "--with-nli",
        action="store_true",
        help="After clustering, write nli_subgroup annotation (heuristic placeholder)",
    )
    ap.add_argument(
        "--stance-model",
        default="claim_vaccine_alignment_score@active",
        help="Labeler model ref for stance (intent@alias, intent/version, or path)",
    )
    args = ap.parse_args(argv)
    run(
        corpus=args.corpus,
        model=args.model,
        model_tag=args.model_tag,
        force=args.force,
        with_stance=args.with_stance,
        with_nli=args.with_nli,
        stance_model=args.stance_model,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
