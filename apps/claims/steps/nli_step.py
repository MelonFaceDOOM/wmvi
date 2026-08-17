"""Wire NLI into the steps package."""

from __future__ import annotations

from apps.claims.nli import NliResult, run_nli_step
from apps.claims.pipeline import Ctx


def step_nli(
    ctx: Ctx,
    *,
    cluster_annotation: str = "cluster_labels",
    output_annotation: str = "nli_subgroup",
    threshold: float = 0.7,
) -> NliResult:
    return run_nli_step(
        ctx,
        cluster_annotation=cluster_annotation,
        output_annotation=output_annotation,
        threshold=threshold,
    )
