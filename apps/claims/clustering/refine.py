"""Recursive 2-means split and greedy LLM merge of cluster labels."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import numpy as np

from apps.claims import io as claims_io
from apps.claims.clustering.browse import (
    BrowseBundle,
    ClusterRow,
    DEFAULT_MIN_SPLIT_GAIN,
    neighbors_for_cluster,
    recursive_twomeans,
    split_tree_stats,
)
from apps.claims.clustering.merge_prompts import (
    DEFAULT_MEMBER_SNIPPETS,
    DEFAULT_NEIGHBOR_K,
    DEFAULT_NN_THRESHOLD,
    MERGE_RESPONSE_SCHEMA,
    MERGE_SYSTEM_PROMPT,
    MERGE_USER_PROMPT,
)

MergeFn = Callable[[int, str, list[dict[str, Any]]], list[int]]


def sibling_out_dir(source: Path, suffix: str) -> Path:
    """``.../cluster_abc`` + ``_split`` → ``.../cluster_abc_split``."""
    p = Path(source)
    if p.is_file():
        p = p.parent
    return p.parent / f"{p.name}{suffix}"


def stamp_from_output(labels_path: Path) -> str:
    name = Path(labels_path).name
    if name.startswith("labels_") and name.endswith(".npy"):
        return name[len("labels_") : -len(".npy")]
    return "refine"


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def n_logged_prompts(log_rows: list[dict[str, Any]]) -> int:
    return sum(1 for r in log_rows if "merge_ids" in r and not r.get("skip"))


def split_labels(
    vectors: np.ndarray,
    labels: np.ndarray,
    claim_texts: list[str],
    *,
    min_gain: float = DEFAULT_MIN_SPLIT_GAIN,
    min_child_n: int = 2,
    seed: int = 0,
    on_parent: Callable[[int, int], None] | None = None,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Replace each cluster id with recursive-2-means terminal ids (dense 0..T-1)."""
    labels = np.asarray(labels, dtype=int)
    out = np.full(labels.shape, -1, dtype=int)
    parent_ids = sorted({int(x) for x in labels.tolist() if int(x) != -1})
    n_terminals = 0
    n_parents_split = 0
    n_parent = len(parent_ids)
    for i, cid in enumerate(parent_ids, start=1):
        if on_parent is not None:
            on_parent(i, n_parent)
        idx = np.where(labels == cid)[0]
        tree = recursive_twomeans(
            vectors,
            idx,
            claim_texts,
            min_gain=min_gain,
            min_child_n=min_child_n,
            seed=seed,
        )
        terminals = split_tree_stats(tree)["terminals"]
        if len(terminals) > 1:
            n_parents_split += 1
        for node in terminals:
            out[np.asarray(node.idx, dtype=int)] = n_terminals
            n_terminals += 1
    noise = labels == -1
    if np.any(noise):
        out[noise] = -1
    meta = {
        "min_gain": float(min_gain),
        "min_child_n": int(min_child_n),
        "n_parent": n_parent,
        "n_parents_split": n_parents_split,
        "n_terminals": int(n_terminals),
    }
    return out, meta


def remap_labels(labels: np.ndarray, id_to_root: dict[int, int]) -> tuple[np.ndarray, dict[int, int]]:
    """Map old cluster ids through union-find roots onto dense 0..M-1."""
    labels = np.asarray(labels, dtype=int)
    roots = sorted({id_to_root.get(int(x), int(x)) for x in labels.tolist() if int(x) != -1})
    dense = {r: i for i, r in enumerate(roots)}
    out = np.full(labels.shape, -1, dtype=int)
    for i, old in enumerate(labels.tolist()):
        old = int(old)
        if old == -1:
            continue
        out[i] = dense[id_to_root.get(old, old)]
    return out, dense


class UnionFind:
    def __init__(self, ids: list[int]) -> None:
        self.parent = {int(i): int(i) for i in ids}

    def find(self, x: int) -> int:
        x = int(x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def roots(self) -> dict[int, int]:
        return {i: self.find(i) for i in self.parent}


def _member_lines(bundle: BrowseBundle, cluster_id: int, *, n: int = DEFAULT_MEMBER_SNIPPETS) -> str:
    from apps.claims.clustering.browse import members_for_cluster

    rows = members_for_cluster(bundle, cluster_id)[: max(0, int(n))]
    if not rows:
        return "(no members)"
    lines = []
    for m in rows:
        lines.append(f"- ×{m.occurrence_count} {m.claim_text}")
    return "\n".join(lines)


def format_seed_block(bundle: BrowseBundle, row: ClusterRow) -> str:
    return (
        f"id={row.cluster_id} n={row.size} intra={row.mean_intra_cosine}\n"
        f"Medoid: {row.medoid_text}\n"
        f"Members:\n{_member_lines(bundle, row.cluster_id)}"
    )


def format_neighbor_blocks(
    ranked: list[tuple[ClusterRow, float]],
    bundle: BrowseBundle,
) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for other, cos in ranked:
        blocks.append(
            {
                "id": int(other.cluster_id),
                "cosine": round(float(cos), 4),
                "n": int(other.size),
                "medoid": other.medoid_text,
                "members": _member_lines(bundle, other.cluster_id),
            }
        )
    return blocks


def neighbor_prompt_text(blocks: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for b in blocks:
        parts.append(
            f"id={b['id']} cosine={b['cosine']} n={b['n']}\n"
            f"Medoid: {b['medoid']}\n"
            f"Members:\n{b['members']}"
        )
    return "\n\n".join(parts) if parts else "(no neighbors)"


def estimate_merge_prompts(
    bundle: BrowseBundle,
    *,
    nn_threshold: float = DEFAULT_NN_THRESHOLD,
    neighbor_k: int = DEFAULT_NEIGHBOR_K,
) -> dict[str, int]:
    """Upper bound = seeds with nn > τ. Lower bound = greedy absorb-all-above-τ."""
    clusters = list(bundle.clusters)
    n_terminals = len(clusters)
    thresh = float(nn_threshold)
    n_seeds_max = sum(
        1
        for c in clusters
        if c.nn_cosine is not None and float(c.nn_cosine) > thresh and c.nn_id is not None
    )
    consumed: set[int] = set()
    n_seeds_min = 0
    order = sorted(
        clusters,
        key=lambda c: (
            -(c.nn_cosine if c.nn_cosine is not None else -1.0),
            c.cluster_id,
        ),
    )
    for seed in order:
        if seed.cluster_id in consumed:
            continue
        if seed.nn_cosine is None or float(seed.nn_cosine) <= thresh or seed.nn_id is None:
            continue
        if int(seed.nn_id) in consumed:
            continue
        n_seeds_min += 1
        consumed.add(int(seed.cluster_id))
        for other, cos in neighbors_for_cluster(bundle, seed.cluster_id, k=int(neighbor_k)):
            if float(cos) > thresh:
                consumed.add(int(other.cluster_id))
    return {
        "n_terminals": n_terminals,
        "n_seeds_max": int(n_seeds_max),
        "n_seeds_min": int(n_seeds_min),
    }


def openai_merge_fn(model: str, *, retries: int = 6) -> MergeFn:
    import time

    from nlp.claim_extraction.clients import openai_structured_completion

    retryable = ("connection", "timeout", "rate limit", "temporarily", "503", "429")

    def _call(seed_id: int, user: str, blocks: list[dict[str, Any]]) -> list[int]:
        last_exc: BaseException | None = None
        for attempt in range(max(1, int(retries))):
            try:
                out = openai_structured_completion(
                    model=model,
                    system=MERGE_SYSTEM_PROMPT,
                    user=user,
                    schema=MERGE_RESPONSE_SCHEMA,
                )
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                msg = str(exc).lower()
                if attempt + 1 >= retries or not any(tok in msg for tok in retryable):
                    raise
                time.sleep(min(60.0, 2.0 ** attempt))
        else:
            raise last_exc or RuntimeError("openai merge failed")
        allowed = {int(b["id"]) for b in blocks}
        raw = out.get("merge_ids") or []
        ids: list[int] = []
        for item in raw:
            try:
                n = int(item)
            except (TypeError, ValueError):
                continue
            if n in allowed:
                ids.append(n)
        return ids

    return _call


def greedy_merge(
    bundle: BrowseBundle,
    *,
    nn_threshold: float = DEFAULT_NN_THRESHOLD,
    neighbor_k: int = DEFAULT_NEIGHBOR_K,
    merge_fn: MergeFn,
    log_path: Path | None = None,
    resume_log: list[dict[str, Any]] | None = None,
    on_prompt: Callable[[int, int], None] | None = None,
) -> tuple[dict[int, int], list[dict[str, Any]], int]:
    """Greedy LLM merge. Returns (id→root, new log rows, n_prompts this run)."""
    from datetime import datetime, timezone

    thresh = float(nn_threshold)
    k = int(neighbor_k)
    ids = [int(c.cluster_id) for c in bundle.clusters]
    uf = UnionFind(ids)
    consumed: set[int] = set()
    n_prompts = 0
    new_rows: list[dict[str, Any]] = []

    done_seeds: dict[int, dict[str, Any]] = {}
    for row in resume_log or []:
        sid = int(row.get("seed_id", -1))
        if sid >= 0:
            done_seeds[sid] = row

    order = sorted(
        bundle.clusters,
        key=lambda c: (
            -(c.nn_cosine if c.nn_cosine is not None else -1.0),
            c.cluster_id,
        ),
    )

    def _append_log(record: dict[str, Any]) -> None:
        new_rows.append(record)
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _apply_group(sid: int, merge_ids: list[int]) -> None:
        group = {int(sid), *[int(x) for x in merge_ids]}
        for oid in group:
            if oid in uf.parent:
                uf.union(sid, oid)
                consumed.add(int(oid))

    for seed in order:
        sid = int(seed.cluster_id)
        if sid in consumed:
            continue
        if seed.nn_cosine is None or float(seed.nn_cosine) <= thresh or seed.nn_id is None:
            continue

        if sid in done_seeds:
            prev = done_seeds[sid]
            if prev.get("skip") == "stale_nn":
                continue
            _apply_group(sid, [int(x) for x in (prev.get("merge_ids") or [])])
            continue

        if int(seed.nn_id) in consumed:
            _append_log(
                {
                    "seed_id": sid,
                    "skip": "stale_nn",
                    "nn_id": int(seed.nn_id),
                    "nn_cosine": seed.nn_cosine,
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue

        ranked = neighbors_for_cluster(bundle, sid, k=k)
        blocks = format_neighbor_blocks(ranked, bundle)
        live_blocks = [b for b in blocks if int(b["id"]) not in consumed]
        user = MERGE_USER_PROMPT.format(
            seed_id=sid,
            seed_medoid=seed.medoid_text,
            seed_members=_member_lines(bundle, sid),
            neighbors=neighbor_prompt_text(live_blocks),
        )
        n_prompts += 1
        if on_prompt is not None:
            on_prompt(n_prompts, sid)
        merge_ids = merge_fn(sid, user, live_blocks)
        _append_log(
            {
                "seed_id": sid,
                "nn_id": int(seed.nn_id),
                "nn_cosine": seed.nn_cosine,
                "neighbor_ids": [int(b["id"]) for b in live_blocks],
                "merge_ids": merge_ids,
                "n_prompts": n_prompts,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        _apply_group(sid, merge_ids)

    return uf.roots(), new_rows, n_prompts


def parent_payload(parent: dict[str, Any], *, n_clusters: int, extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(parent)
    out.pop("result_path", None)
    out.pop("labels_path", None)
    out["n_clusters"] = int(n_clusters)
    out.update(extra)
    return out


def write_refine_experiment(
    out_dir: Path,
    *,
    stamp: str,
    labels: np.ndarray,
    payload: dict[str, Any],
) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    result_path = out_dir / f"result_{stamp}.json"
    labels_path = out_dir / f"labels_{stamp}.npy"
    claims_io.write_json(result_path, payload)
    np.save(labels_path, labels)
    return result_path, labels_path
