"""Dump measles2 anti-vax merge-leaves (mean alignment ≤ 0.33).

Does not load the full browse catalog (no 2-means / neighbor matrix).
Run from repo root::

  python apps/claims/scripts/dump_anti_vax_leaves.py
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import numpy as np

from apps.claims.clustering.browse import (
    DEFAULT_ANTI_HIGH,
    group_alignment_from_sources,
    load_occurrence_index,
)
from apps.claims.clustering.metrics import _cluster_medoid

EXP = Path("apps/claims/data/experiments/clustering/measles2/qwen3-emb-8b")
MERGE = EXP / "cluster_5cb45e9814_split_merge"
RUN = Path("apps/claims/data/runs/measles2/qwen3-emb-8b")
CLAIMS = Path("apps/claims/data/corpora/measles2/claims.json")
OUT = EXP / "anti_vax_leaves.json"
TOP_MEMBERS = 3


def _top_members(idxs: list[int], groups: list[dict], texts: list[str], n: int) -> list[dict]:
    rows = []
    for i in idxs:
        g = groups[i] if i < len(groups) else {}
        count = int(g.get("count") or 1)
        text = str(g.get("claim_text") or (texts[i] if i < len(texts) else ""))
        rows.append({"idx": int(i), "count": count, "claim_text": text})
    rows.sort(key=lambda r: (-r["count"], r["claim_text"].casefold(), r["idx"]))
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        key = r["claim_text"].casefold()[:120]
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
        if len(out) >= n:
            break
    return out


def main() -> None:
    labels = np.asarray(np.load(MERGE / "labels_5cb45e9814.npy"), dtype=int)
    index = json.loads((RUN / "index.json").read_text(encoding="utf-8"))
    vectors = np.load(RUN / "vectors.npy")
    groups = index["groups"]
    texts = [str(t) for t in (index.get("claim_texts") or [g.get("claim_text") or "" for g in groups])]
    print(f"labels={labels.shape[0]} loading occurrence index…", flush=True)
    occ = load_occurrence_index(CLAIMS)
    print(f"chunks={len(occ)} scoring groups…", flush=True)
    group_means = [group_alignment_from_sources(g.get("sources") or [], occ) for g in groups]

    members: dict[int, list[int]] = defaultdict(list)
    for i, cid in enumerate(labels.tolist()):
        members[int(cid)].append(i)

    leaves: list[dict] = []
    for cid, idxs in sorted(members.items()):
        vals = [group_means[i] for i in idxs if group_means[i] is not None]
        if not vals:
            continue
        mean_align = round(sum(vals) / len(vals), 4)
        if mean_align > DEFAULT_ANTI_HIGH:
            continue
        arr = np.asarray(idxs, dtype=int)
        med_i = _cluster_medoid(vectors, arr)
        occ_count = sum(int(groups[i].get("count") or 1) for i in idxs)
        frac_anti = round(sum(1 for v in vals if v <= DEFAULT_ANTI_HIGH) / len(vals), 4)
        leaves.append(
            {
                "cluster_id": int(cid),
                "n": len(idxs),
                "occurrence_count": int(occ_count),
                "mean_alignment": mean_align,
                "frac_anti": frac_anti,
                "n_scored": len(vals),
                "medoid_idx": int(med_i),
                "medoid_text": texts[int(med_i)] if 0 <= int(med_i) < len(texts) else "",
                "members": _top_members(idxs, groups, texts, TOP_MEMBERS),
            }
        )

    leaves.sort(key=lambda r: (-r["n"], -r["occurrence_count"], r["cluster_id"]))
    payload = {
        "source": str(MERGE),
        "alignment_cutoff": DEFAULT_ANTI_HIGH,
        "n_leaves": len(leaves),
        "n_grouped_texts": sum(r["n"] for r in leaves),
        "n_occurrences": sum(r["occurrence_count"] for r in leaves),
        "n_ge3": sum(1 for r in leaves if r["n"] >= 3),
        "leaves": leaves,
    }
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {OUT} n_leaves={len(leaves)} n_ge3={payload['n_ge3']}", flush=True)


if __name__ == "__main__":
    main()
