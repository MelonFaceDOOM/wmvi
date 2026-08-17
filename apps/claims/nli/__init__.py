"""NLI contradiction splitting within clusters (experimental).

Input: cluster_labels annotation + claim texts from groups/index.
Output: nli_subgroup annotation keyed by claim_key (integer subgroup id within
each parent cluster).

Default backend is a simple heuristic placeholder so the pipeline plumbing works
without a GPU model. Swap ``score_pair`` / ``assign_subgroups`` for a real NLI
model (e.g. cross-encoder) when ready.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims.keys import claim_key
from apps.claims.pipeline import Ctx, groups_source_hash


PairScorer = Callable[[str, str], float]
"""Return contradiction score in [0,1] for (premise, hypothesis)."""


def heuristic_contradiction(a: str, b: str) -> float:
    """Cheap placeholder: high score when texts share few tokens but both are long.

    Not a real NLI model — used so the step is runnable in CI without torch.
    """
    ta = set(a.casefold().split())
    tb = set(b.casefold().split())
    if not ta or not tb:
        return 0.0
    jaccard = len(ta & tb) / max(1, len(ta | tb))
    # Invert similarity; bias toward 0 for near-duplicates
    return float(max(0.0, min(1.0, 1.0 - jaccard)))


@dataclass
class NliResult:
    annotation: ann_mod.Annotation
    n_clusters: int
    n_subgroups: int


def assign_subgroups(
    *,
    cluster_of: dict[str, int],
    texts: dict[str, str],
    score_pair: PairScorer = heuristic_contradiction,
    threshold: float = 0.7,
) -> dict[str, int]:
    """Greedy: within each cluster, start subgroups; put claim in first subgroup
    whose medoid does not contradict it above ``threshold``; else new subgroup.

    Returns claim_key -> subgroup_id (global ints, unique across clusters).
    """
    by_cluster: dict[int, list[str]] = defaultdict(list)
    for ck, cid in cluster_of.items():
        if cid < 0:
            continue
        if ck in texts:
            by_cluster[cid].append(ck)

    out: dict[str, int] = {}
    next_gid = 0
    for cid in sorted(by_cluster):
        members = by_cluster[cid]
        # subgroups: list of (gid, medoid_key)
        subgroups: list[tuple[int, str]] = []
        for ck in members:
            text = texts[ck]
            placed = False
            for gid, medoid_key in subgroups:
                score = score_pair(texts[medoid_key], text)
                if score < threshold:
                    out[ck] = gid
                    placed = True
                    break
            if not placed:
                gid = next_gid
                next_gid += 1
                subgroups.append((gid, ck))
                out[ck] = gid
        # noise / unassigned stay absent
    return out


def run_nli_step(
    ctx: Ctx,
    *,
    cluster_annotation: str = "cluster_labels",
    output_annotation: str = "nli_subgroup",
    threshold: float = 0.7,
    score_pair: PairScorer | None = None,
) -> NliResult:
    """Read cluster labels + groups, write nli_subgroup annotation."""
    cluster_ann = ann_mod.read_annotation(ctx.root, cluster_annotation)
    groups_payload = claims_io.read_json(ctx.corpus.groups)
    texts: dict[str, str] = {}
    for g in groups_payload.get("groups") or []:
        text = str(g.get("claim_text") or "")
        ck = str(g.get("claim_key") or claim_key(text))
        texts[ck] = text

    cluster_of = {k: int(v) for k, v in cluster_ann.values.items()}
    scorer = score_pair or heuristic_contradiction
    subgroups = assign_subgroups(
        cluster_of=cluster_of, texts=texts, score_pair=scorer, threshold=threshold
    )
    ann = ann_mod.write_annotation(
        ctx.root,
        output_annotation,
        subgroups,
        scope="group",
        producer="apps.claims.nli",
        params={
            "cluster_annotation": cluster_annotation,
            "threshold": threshold,
            "scorer": getattr(scorer, "__name__", "custom"),
        },
        source_hash=groups_source_hash(ctx),
        force=ctx.force,
    )
    n_clusters = len({int(v) for v in cluster_of.values() if int(v) >= 0})
    n_subgroups = len(set(subgroups.values()))
    return NliResult(annotation=ann, n_clusters=n_clusters, n_subgroups=n_subgroups)
