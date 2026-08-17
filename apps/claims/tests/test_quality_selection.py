"""Quality filtering uses the same Phase 1 primitives as stance.

Workflow (no new architecture)::

    # 1. Train a 'quality' Ridge head via `python -m apps.claims labeler train ...`
    # 2. Score the corpus:
    python -m apps.claims labeler apply --corpus measles \\
        --model quality@active --name quality --force

    # 3. Keep non-junk (e.g. quality >= 0.4):
    python -m apps.claims select --corpus measles --annotation quality \\
        --name usable --low 0.4 --force

    # 4. Cluster only usable claims against the existing embed run (no re-embed):
    python -m apps.claims cluster --corpus measles --model-tag TAG \\
        --selection usable --algorithm kmeans --params-json '{"n_clusters":40}'

Or via steps::

    from apps.claims.pipeline import Ctx, step
    from apps.claims.steps import step_label, step_select_threshold, step_cluster

    ctx = Ctx.for_corpus("measles", model_tag="TAG")
    step(ctx, name="quality", output_annotation="quality",
         run=lambda c: step_label(c, model_ref="quality@active", annotation_name="quality"))
    step(ctx, name="usable",
         run=lambda c: step_select_threshold(c, annotation="quality", name="usable", low=0.4))
    step(ctx, name="cluster_usable",
         run=lambda c: step_cluster(c, algorithm="kmeans", params={"n_clusters": 40},
                                    selection="usable"))
"""

from __future__ import annotations

from pathlib import Path

from apps.claims import annotations as ann_mod
from apps.claims import selections as sel_mod
from apps.claims.keys import claim_key


def test_quality_filter_is_just_annotation_plus_selection(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    values = {
        claim_key("good"): 0.9,
        claim_key("ok"): 0.5,
        claim_key("junk"): 0.1,
    }
    ann = ann_mod.write_annotation(root, "quality", values, producer="test")
    usable = sel_mod.from_threshold(ann, name="usable", low=0.4)
    assert set(usable.keys) == {claim_key("good"), claim_key("ok")}
    sel_mod.write_selection(root, usable)
    loaded = sel_mod.read_selection(root, "usable")
    assert len(loaded.keys) == 2
