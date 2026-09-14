"""Read-only loaders for browsing one flat ``cluster`` experiment.

Discovers ``result_*.json`` + ``labels_*.npy``, reapplies the same
``--selection`` / ``--filter`` used at cluster time, and joins member claims
to nested ``claims.json`` post-chunks.

At load, each leaf gets mean intra cosine, nearest-other-leaf medoid cosine,
and (n≥3) a 2-means split used for member highlighting and split_gain.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims import corpus as corpus_mod
from apps.claims import filtering as filt
from apps.claims import io as claims_io
from apps.claims import selections as sel_mod
from apps.claims.claims_data import context_row_for_chunk, load_posts_from_claims_json, stable_task_id
from apps.claims.clustering.metrics import _cluster_medoid, _per_cluster_mean_intra_cosine

_RUNS_TAIL = re.compile(r"(?:^|/)runs/([^/]+)/([^/]+)/?$")
_HIERARCHY_MSG = (
    "This path looks like a two-level hierarchy experiment "
    "(hierarchy_*.json / leaf_labels_*.npy). Re-run "
    "`python -m apps.claims cluster ... --save-labels` and point --from at that output."
)

POST_META_KEYS = (
    "platform",
    "post_id",
    "url",
    "created_at_ts",
    "primary_metric",
    "youtube_video_title",
    "reddit_submission_title",
    "reddit_comment_submission_title",
    "podcast_name",
    "telegram_channel",
)


@dataclass(frozen=True)
class SelectionHint:
    """Best-effort parse of the ``selection`` string stored on cluster JSON."""

    raw: str | None
    selection: str | None = None
    filter_annotations: tuple[str, ...] = ()


@dataclass
class ClusterOutput:
    directory: Path
    meta_path: Path | None
    payload: dict[str, Any]
    labels_path: Path
    run_dir: Path | None
    n_selected: int | None
    hint: SelectionHint


@dataclass
class ClusterRow:
    cluster_id: int
    size: int
    mean_intra_cosine: float | None = None
    medoid_text: str = ""
    medoid_idx: int | None = None
    nn_id: int | None = None
    nn_cosine: float | None = None
    split_gain: float | None = None
    child_sizes: tuple[int, int] | None = None
    child_intras: tuple[float, float] | None = None
    mean_alignment: float | None = None
    medoid_alignment: float | None = None
    frac_anti: float | None = None
    n_scored: int = 0


@dataclass
class TwomeansSplit:
    child_labels: np.ndarray
    sizes: tuple[int, int]
    intras: tuple[float, float]
    split_gain: float


@dataclass
class SplitNode:
    """One node in an in-memory recursive 2-means tree."""

    path: str
    idx: np.ndarray
    size: int
    intra: float
    medoid_idx: int | None
    medoid_text: str
    split_gain: float | None
    stop_reason: str
    children: tuple["SplitNode", "SplitNode"] | None = None


@dataclass
class MemberRow:
    idx: int
    claim_key: str
    claim_text: str
    occurrence_count: int
    sources: list[dict[str, Any]] = field(default_factory=list)
    cosine_to_medoid: float | None = None
    twomeans_child: int | None = None
    alignment: float | None = None


@dataclass
class BrowseBundle:
    output: ClusterOutput
    index: dict[str, Any]
    labels: np.ndarray
    vectors: np.ndarray
    corpus: str | None
    claims_path: Path | None
    applied_selection: str | None
    applied_filters: tuple[str, ...]
    clusters: list[ClusterRow]
    twomeans: dict[int, TwomeansSplit] = field(default_factory=dict)
    group_alignment: list[float | None] = field(default_factory=list)


LEAF_NEIGHBOR_K = 15
MIN_TWOMEANS_SIZE = 3
ALIGNMENT_SCORE_FIELD = "claim_vaccine_alignment_score"
DEFAULT_ANTI_HIGH = 0.33


def _alignment_from_claim(claim: dict[str, Any]) -> float | None:
    raw = claim.get(ALIGNMENT_SCORE_FIELD)
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v != v or v < 0.0 or v > 1.0:
        return None
    return float(v)


def _mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    return round(sum(vals) / len(vals), 4)


def group_alignment_from_sources(
    sources: list[dict[str, Any]],
    occ_index: dict[str, dict[str, Any]],
) -> float | None:
    """Mean extract alignment over a group's source occurrences."""
    vals: list[float] = []
    for src in sources or []:
        tid = str(src.get("task_id") or "")
        if not tid:
            continue
        try:
            cidx = int(src.get("claim_index") or 0)
        except (TypeError, ValueError):
            cidx = 0
        hit = occ_index.get(tid)
        if not hit:
            continue
        scores = hit.get("claim_scores") or []
        if 0 <= cidx < len(scores) and scores[cidx] is not None:
            vals.append(float(scores[cidx]))
    return _mean(vals)


def attach_leaf_alignment(bundle: BrowseBundle, occ_index: dict[str, dict[str, Any]]) -> None:
    """Fill per-group and per-leaf mean ``claim_vaccine_alignment_score`` (in place)."""
    groups = bundle.index.get("groups") or []
    n = int(bundle.labels.shape[0])
    means: list[float | None] = []
    for i in range(n):
        sources: list[dict[str, Any]] = []
        if i < len(groups) and isinstance(groups[i], dict):
            sources = list(groups[i].get("sources") or [])
        means.append(group_alignment_from_sources(sources, occ_index))
    bundle.group_alignment = means
    labels = np.asarray(bundle.labels, dtype=int)
    for row in bundle.clusters:
        idx = np.where(labels == int(row.cluster_id))[0]
        vals = [means[int(i)] for i in idx.tolist() if 0 <= int(i) < len(means) and means[int(i)] is not None]
        row.n_scored = len(vals)
        row.mean_alignment = _mean(vals) if vals else None
        row.frac_anti = (
            round(sum(1 for v in vals if float(v) <= DEFAULT_ANTI_HIGH) / len(vals), 4) if vals else None
        )
        mid = row.medoid_idx
        row.medoid_alignment = means[int(mid)] if mid is not None and 0 <= int(mid) < len(means) else None


def parse_selection_label(raw: str | None) -> SelectionHint:
    """Parse ``selection:name``, ``filter:ann``, or ``filter:a+selection:b``."""
    if not raw or not str(raw).strip():
        return SelectionHint(raw=None)
    text = str(raw).strip()
    selection: str | None = None
    filters: list[str] = []
    for bit in text.split("+"):
        bit = bit.strip()
        if bit.startswith("selection:"):
            name = bit.split(":", 1)[1].strip()
            selection = name or None
        elif bit.startswith("filter:"):
            name = bit.split(":", 1)[1].strip()
            if name:
                filters.append(name)
    return SelectionHint(raw=text, selection=selection, filter_annotations=tuple(filters))


def _normalize_run_dir_str(run_dir: Path | str) -> str:
    return str(run_dir).strip().replace("\\", "/").rstrip("/")


def infer_corpus_from_run_dir(run_dir: Path | str | None) -> tuple[str | None, str | None]:
    """Return ``(corpus, model_tag)`` from ``.../runs/<corpus>/<tag>``.

    Accepts POSIX or Windows paths. Does not ``resolve()`` first, so a GPU
    ``C:\\…\\runs\\measles2\\qwen3-emb-8b`` still parses on another machine.
    """
    if run_dir is None:
        return None, None
    m = _RUNS_TAIL.search(_normalize_run_dir_str(run_dir))
    if not m:
        return None, None
    return m.group(1), m.group(2)


def portable_run_dir_str(run_dir: Path | str) -> str:
    """Stable JSON value: ``runs/<corpus>/<tag>`` when the path matches layout."""
    slug, tag = infer_corpus_from_run_dir(run_dir)
    if slug and tag:
        return f"runs/{slug}/{tag}"
    return _normalize_run_dir_str(run_dir)


def _pick_stamp_file(paths: list[Path], stamp: str | None) -> Path | None:
    if not paths:
        return None
    if stamp:
        for p in paths:
            if p.stem.endswith(stamp) or p.name.endswith(f"_{stamp}.npy") or p.name.endswith(f"_{stamp}.json"):
                return p
    return max(paths, key=lambda p: p.stat().st_mtime)


def _stamp_from_name(path: Path, prefixes: tuple[str, ...]) -> str | None:
    stem = path.stem
    for pre in prefixes:
        if stem.startswith(pre):
            rest = stem[len(pre) :]
            return rest or None
    return None


def _is_hierarchy_name(name: str) -> bool:
    return name.startswith("hierarchy_") or name.startswith("leaf_labels_") or name.startswith(
        "narrative_labels_"
    )


def discover_cluster_output(
    path: Path,
    *,
    labels: Path | None = None,
) -> ClusterOutput:
    """Resolve a cluster experiment dir, ``result_*.json``, or ``labels_*.npy``."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Cluster output not found: {path}")

    labels_override = Path(labels) if labels is not None else None
    if labels_override is not None and _is_hierarchy_name(labels_override.name):
        raise ValueError(_HIERARCHY_MSG)

    if path.is_file() and path.suffix == ".npy":
        if _is_hierarchy_name(path.name):
            raise ValueError(_HIERARCHY_MSG)
        return _output_from_labels_file(path)

    meta_path: Path | None = None
    directory: Path
    if path.is_file() and path.suffix == ".json":
        if _is_hierarchy_name(path.name):
            raise ValueError(_HIERARCHY_MSG)
        meta_path = path
        directory = path.parent
    elif path.is_dir():
        directory = path
    else:
        raise ValueError(f"Expected a directory, JSON, or .npy labels file; got {path}")

    result_jsons = sorted(directory.glob("result_*.json"))
    flat_npys = sorted(directory.glob("labels_*.npy"))
    hier_jsons = sorted(directory.glob("hierarchy_*.json"))
    leaf_npys = sorted(directory.glob("leaf_labels_*.npy"))

    if meta_path is None:
        if result_jsons:
            meta_path = _pick_stamp_file(result_jsons, None)
        elif hier_jsons or leaf_npys:
            raise ValueError(_HIERARCHY_MSG)

    payload: dict[str, Any] = {}
    if meta_path is not None:
        if _is_hierarchy_name(meta_path.name):
            raise ValueError(_HIERARCHY_MSG)
        raw = claims_io.read_json(meta_path)
        if not isinstance(raw, dict):
            raise ValueError(f"Cluster JSON must be an object: {meta_path}")
        payload = raw
        if "narratives" in payload:
            raise ValueError(_HIERARCHY_MSG)

    stamp = _stamp_from_name(meta_path, ("result_",)) if meta_path is not None else None

    if labels_override is not None:
        labels_path = labels_override
    else:
        labels_path = _pick_stamp_file(flat_npys, stamp)
        if labels_path is None:
            if hier_jsons or leaf_npys:
                raise ValueError(_HIERARCHY_MSG)
            raise FileNotFoundError(
                f"No labels_*.npy in {directory}; re-run cluster with --save-labels"
            )

    run_dir = Path(payload["run_dir"]) if payload.get("run_dir") else None
    n_selected = int(payload["n_selected"]) if payload.get("n_selected") is not None else None
    hint = parse_selection_label(payload.get("selection") if isinstance(payload.get("selection"), str) else None)
    return ClusterOutput(
        directory=directory,
        meta_path=meta_path,
        payload=payload,
        labels_path=labels_path,
        run_dir=run_dir,
        n_selected=n_selected,
        hint=hint,
    )


def _output_from_labels_file(labels_path: Path) -> ClusterOutput:
    directory = labels_path.parent
    payload: dict[str, Any] = {}
    stamp = _stamp_from_name(labels_path, ("labels_",))
    meta_path = _pick_stamp_file(list(directory.glob("result_*.json")), stamp)
    if meta_path is not None:
        raw = claims_io.read_json(meta_path)
        if isinstance(raw, dict):
            if "narratives" in raw or _is_hierarchy_name(meta_path.name):
                raise ValueError(_HIERARCHY_MSG)
            payload = raw
    run_dir = Path(payload["run_dir"]) if payload.get("run_dir") else None
    n_selected = int(payload["n_selected"]) if payload.get("n_selected") is not None else None
    hint = parse_selection_label(payload.get("selection") if isinstance(payload.get("selection"), str) else None)
    return ClusterOutput(
        directory=directory,
        meta_path=meta_path,
        payload=payload,
        labels_path=labels_path,
        run_dir=run_dir,
        n_selected=n_selected,
        hint=hint,
    )


def resolve_run_dir(
    output: ClusterOutput,
    *,
    run_dir: Path | None = None,
    corpus: str | None = None,
    model_tag: str | None = None,
) -> Path:
    if run_dir is not None:
        return Path(run_dir)

    stored = output.run_dir
    inferred_slug, inferred_tag = infer_corpus_from_run_dir(stored)

    if stored is not None:
        raw = _normalize_run_dir_str(stored)
        candidates = [Path(str(stored)), Path(raw)]
        if not Path(raw).is_absolute():
            candidates.append(claims_io.data_root() / raw)
        marker = "apps/claims/data/"
        if marker in raw:
            candidates.append(claims_io.data_root() / raw.split(marker, 1)[1])
        for cand in candidates:
            try:
                if cand.is_dir():
                    return cand
            except OSError:
                continue

    slug = corpus or inferred_slug
    tag = model_tag or inferred_tag
    if slug and tag:
        return corpus_mod.get_corpus(slug).run_dir(tag)
    if corpus:
        raise ValueError("Provide --model-tag (or a cluster JSON with run_dir) to locate the embed run")
    raise ValueError("Could not resolve embed run: pass --run-dir or --corpus/--model-tag")


def resolve_subset_keys(
    *,
    corpus_root: Path | None,
    index: dict[str, Any],
    hint: SelectionHint,
    selection: str | None,
    filter_specs: list[str] | None,
) -> tuple[set[str] | None, str | None, tuple[str, ...]]:
    """AND named selection + live --filter specs; auto-fill selection from cluster JSON."""
    specs = [str(s) for s in (filter_specs or []) if str(s).strip()]
    sel_name = selection or hint.selection

    if hint.filter_annotations and not specs and not sel_name:
        anns = ", ".join(hint.filter_annotations)
        raise ValueError(
            "This cluster run used a live --filter "
            f"({hint.raw}). Re-pass the same --filter SPEC or a named --selection "
            f"(annotation(s): {anns})."
        )

    if sel_name is None and not specs:
        return None, None, ()

    if (sel_name or specs) and corpus_root is None:
        raise ValueError("--selection / --filter require --corpus (to locate selections/annotations)")

    wanted: set[str] | None = None
    if specs:
        assert corpus_root is not None
        clauses = [filt.parse_filter_spec(s) for s in specs]
        resolved = filt.resolve_filter_clauses(
            corpus_root,
            clauses,
            groups_hash=str(index.get("source_hash") or None),
        )
        wanted = set(resolved.keys)
    if sel_name:
        assert corpus_root is not None
        sel = sel_mod.read_selection(corpus_root, sel_name)
        sel_keys = set(sel.keys)
        wanted = sel_keys if wanted is None else (wanted & sel_keys)
    return wanted, sel_name, tuple(specs)


def load_browse_bundle(
    source: Path,
    *,
    corpus: str | None = None,
    model_tag: str | None = None,
    run_dir: Path | None = None,
    labels: Path | None = None,
    selection: str | None = None,
    filter_specs: list[str] | None = None,
    claims_path: Path | None = None,
) -> BrowseBundle:
    """Load one clustering output aligned to the embed-run subset it was built on."""
    output = discover_cluster_output(source, labels=labels)
    resolved_run = resolve_run_dir(output, run_dir=run_dir, corpus=corpus, model_tag=model_tag)
    vectors, index = claims_io.load_run_arrays(resolved_run)

    inferred_corpus, _inferred_tag = infer_corpus_from_run_dir(resolved_run)
    corpus_name = corpus or inferred_corpus
    corpus_root = corpus_mod.get_corpus(corpus_name).root if corpus_name else None

    wanted, applied_sel, applied_filters = resolve_subset_keys(
        corpus_root=corpus_root,
        index=index,
        hint=output.hint,
        selection=selection,
        filter_specs=filter_specs,
    )
    if wanted is not None:
        vectors, index, _rows = filt.subset_vectors_by_keys(
            vectors,
            index,
            sorted(wanted),
            filter_meta={"selection": applied_sel, "filters": list(applied_filters)},
        )

    labels_arr = np.asarray(np.load(output.labels_path), dtype=int)
    n_index = len(sel_mod.claim_keys_from_index(index))
    if labels_arr.shape[0] != n_index:
        extra = ""
        if output.hint.raw:
            extra = f" Cluster JSON selection={output.hint.raw!r}."
        if applied_sel or applied_filters:
            extra += f" Applied selection={applied_sel!r} filters={list(applied_filters)!r}."
        raise ValueError(
            f"labels length ({labels_arr.shape[0]}) != selected claims ({n_index})."
            " Pass the same --selection / --filter used when clustering."
            + extra
        )
    if int(vectors.shape[0]) != labels_arr.shape[0]:
        raise ValueError(
            f"vectors rows ({vectors.shape[0]}) != labels length ({labels_arr.shape[0]})"
        )

    texts = claims_io.claim_texts_from_index(index)
    clusters, twomeans = _catalog(vectors=vectors, labels=labels_arr, claim_texts=texts)

    resolved_claims = claims_path
    if resolved_claims is None and corpus_name:
        cand = corpus_mod.get_corpus(corpus_name).claims
        if cand.is_file():
            resolved_claims = cand

    return BrowseBundle(
        output=output,
        index=index,
        labels=labels_arr,
        vectors=vectors,
        corpus=corpus_name,
        claims_path=resolved_claims,
        applied_selection=applied_sel,
        applied_filters=applied_filters,
        clusters=clusters,
        twomeans=twomeans,
    )


def _catalog(
    *,
    vectors: np.ndarray,
    labels: np.ndarray,
    claim_texts: list[str],
    seed: int = 0,
) -> tuple[list[ClusterRow], dict[int, TwomeansSplit]]:
    rng = np.random.default_rng(seed)
    clusters: list[ClusterRow] = []
    twomeans: dict[int, TwomeansSplit] = {}
    for cid in _sorted_ids(labels):
        idx = np.where(labels == cid)[0]
        medoid_text = ""
        tightness: float | None = None
        medoid_i: int | None = None
        if idx.size:
            tightness = round(float(_per_cluster_mean_intra_cosine(vectors, idx, rng)), 4)
            medoid_i = int(_cluster_medoid(vectors, idx))
            if 0 <= medoid_i < len(claim_texts):
                medoid_text = str(claim_texts[medoid_i])
        split = twomeans_split(vectors, idx, parent_intra=tightness, seed=seed)
        if split is not None:
            twomeans[cid] = split
        clusters.append(
            ClusterRow(
                cluster_id=cid,
                size=int(idx.size),
                mean_intra_cosine=tightness,
                medoid_text=medoid_text,
                medoid_idx=medoid_i,
                split_gain=None if split is None else round(float(split.split_gain), 4),
                child_sizes=None if split is None else split.sizes,
                child_intras=None if split is None else split.intras,
            )
        )
    attach_leaf_neighbors(clusters, vectors)
    clusters.sort(key=lambda r: (-r.size, r.cluster_id))
    return clusters, twomeans


def twomeans_split(
    vectors: np.ndarray,
    idx: np.ndarray,
    *,
    parent_intra: float | None,
    seed: int = 0,
) -> TwomeansSplit | None:
    """Fit k=2 on one leaf. Skip n < 3. ``split_gain`` is weighted child intra − parent intra."""
    idx = np.asarray(idx, dtype=int)
    n = int(idx.size)
    if n < MIN_TWOMEANS_SIZE:
        return None
    sub = np.asarray(vectors[idx], dtype=np.float64)
    try:
        from sklearn.cluster import KMeans

        child = np.asarray(
            KMeans(n_clusters=2, random_state=seed, n_init="auto").fit_predict(sub),
            dtype=int,
        )
    except ValueError:
        return None
    if int(np.unique(child).size) < 2:
        return None
    rng = np.random.default_rng(seed)
    sizes: list[int] = []
    intras: list[float] = []
    for lab in (0, 1):
        child_idx = idx[child == lab]
        sizes.append(int(child_idx.size))
        if child_idx.size == 0:
            return None
        intras.append(float(_per_cluster_mean_intra_cosine(vectors, child_idx, rng)))
    n0, n1 = sizes
    intra0, intra1 = intras
    parent = float(parent_intra) if parent_intra is not None else float(
        _per_cluster_mean_intra_cosine(vectors, idx, rng)
    )
    weighted = (n0 * intra0 + n1 * intra1) / float(n)
    return TwomeansSplit(
        child_labels=child,
        sizes=(n0, n1),
        intras=(round(intra0, 4), round(intra1, 4)),
        split_gain=float(weighted - parent),
    )


DEFAULT_MIN_SPLIT_GAIN = 0.0
MAX_SPLIT_DEPTH = 16


def recursive_twomeans(
    vectors: np.ndarray,
    idx: np.ndarray,
    claim_texts: list[str],
    *,
    min_gain: float = DEFAULT_MIN_SPLIT_GAIN,
    min_child_n: int = 2,
    max_depth: int = MAX_SPLIT_DEPTH,
    seed: int = 0,
    path: str = "root",
    depth: int = 0,
) -> SplitNode:
    """Bisect with 2-means while ``split_gain >= min_gain`` and n ≥ 3.

    In-memory only. Stops when the group is too small, 2-means cannot split,
    the gain is below ``min_gain``, a child would be smaller than
    ``min_child_n``, or ``max_depth`` is reached.
    """
    idx = np.asarray(idx, dtype=int)
    rng = np.random.default_rng(seed)
    n = int(idx.size)
    intra = float(_per_cluster_mean_intra_cosine(vectors, idx, rng)) if n else 1.0
    medoid_i: int | None = int(_cluster_medoid(vectors, idx)) if n else None
    medoid_text = ""
    if medoid_i is not None and 0 <= medoid_i < len(claim_texts):
        medoid_text = str(claim_texts[medoid_i])

    def _leaf(reason: str, gain: float | None = None) -> SplitNode:
        return SplitNode(
            path=path,
            idx=idx,
            size=n,
            intra=round(intra, 4),
            medoid_idx=medoid_i,
            medoid_text=medoid_text,
            split_gain=None if gain is None else round(float(gain), 4),
            stop_reason=reason,
            children=None,
        )

    if n < MIN_TWOMEANS_SIZE:
        return _leaf("n<3")
    if depth >= int(max_depth):
        return _leaf("max depth")
    split = twomeans_split(vectors, idx, parent_intra=intra, seed=seed)
    if split is None:
        return _leaf("could not split")
    if float(split.split_gain) < float(min_gain):
        return _leaf(f"gain {split.split_gain:.4f} < {float(min_gain):.4f}", gain=split.split_gain)
    idx_a = idx[split.child_labels == 0]
    idx_b = idx[split.child_labels == 1]
    if min(int(idx_a.size), int(idx_b.size)) < int(min_child_n):
        return _leaf(
            f"child n<{int(min_child_n)}",
            gain=split.split_gain,
        )
    child_a = recursive_twomeans(
        vectors,
        idx_a,
        claim_texts,
        min_gain=min_gain,
        min_child_n=min_child_n,
        max_depth=max_depth,
        seed=seed,
        path=f"{path}/A",
        depth=depth + 1,
    )
    child_b = recursive_twomeans(
        vectors,
        idx_b,
        claim_texts,
        min_gain=min_gain,
        min_child_n=min_child_n,
        max_depth=max_depth,
        seed=seed,
        path=f"{path}/B",
        depth=depth + 1,
    )
    return SplitNode(
        path=path,
        idx=idx,
        size=n,
        intra=round(intra, 4),
        medoid_idx=medoid_i,
        medoid_text=medoid_text,
        split_gain=round(float(split.split_gain), 4),
        stop_reason="split",
        children=(child_a, child_b),
    )


def split_tree_stats(node: SplitNode) -> dict[str, Any]:
    """Count splits and collect terminal (unsplit) groups in preorder."""
    n_splits = 0
    terminals: list[SplitNode] = []

    def walk(cur: SplitNode) -> None:
        nonlocal n_splits
        if cur.children is None:
            terminals.append(cur)
            return
        n_splits += 1
        walk(cur.children[0])
        walk(cur.children[1])

    walk(node)
    return {"n_splits": n_splits, "n_terminals": len(terminals), "terminals": terminals}


def iter_split_preorder(node: SplitNode):
    """Yield every node, parent before its children."""
    yield node
    if node.children is not None:
        yield from iter_split_preorder(node.children[0])
        yield from iter_split_preorder(node.children[1])


def attach_leaf_neighbors(clusters: list[ClusterRow], vectors: np.ndarray) -> None:
    """Set each leaf's nearest other leaf by medoid cosine (in place)."""
    usable = [c for c in clusters if c.medoid_idx is not None]
    if len(usable) < 2:
        return
    M = np.stack([np.asarray(vectors[int(c.medoid_idx)], dtype=np.float64) for c in usable])
    norms = np.linalg.norm(M, axis=1, keepdims=True)
    M = M / np.clip(norms, 1e-12, None)
    sims = M @ M.T
    np.fill_diagonal(sims, -np.inf)
    nn_j = np.argmax(sims, axis=1)
    for i, row in enumerate(usable):
        j = int(nn_j[i])
        row.nn_id = usable[j].cluster_id
        row.nn_cosine = round(float(sims[i, j]), 4)


def filter_sort_clusters(
    clusters: list[ClusterRow],
    *,
    min_n: int = MIN_TWOMEANS_SIZE,
    sort_by: str = "n",
    descending: bool = True,
    max_alignment: float | None = None,
) -> list[ClusterRow]:
    """Filter by unique grouped-text count (and optional mean alignment), then sort.

    ``max_alignment`` keeps leaves whose mean extract alignment is ≤ the cutoff
    (0 = anti-vaccine, 1 = pro). Leaves with no scores still pass.
    """
    rows = [c for c in clusters if c.size >= int(min_n)]
    if max_alignment is not None:
        cap = float(max_alignment)
        rows = [c for c in rows if c.mean_alignment is None or float(c.mean_alignment) <= cap]
    getters = {
        "n": lambda c: c.size,
        "intra": lambda c: c.mean_intra_cosine,
        "nn_cosine": lambda c: c.nn_cosine,
        "split_gain": lambda c: c.split_gain,
        "alignment": lambda c: c.mean_alignment,
    }
    if sort_by not in getters:
        raise ValueError(f"Unknown sort_by {sort_by!r}")
    get = getters[sort_by]

    def key(c: ClusterRow) -> tuple[int, float, int]:
        val = get(c)
        if val is None:
            return (1, 0.0, c.cluster_id)
        signed = -float(val) if descending else float(val)
        return (0, signed, c.cluster_id)

    return sorted(rows, key=key)


def neighbors_for_cluster(
    bundle: BrowseBundle,
    cluster_id: int,
    *,
    k: int = LEAF_NEIGHBOR_K,
) -> list[tuple[ClusterRow, float]]:
    """Other leaves ranked by medoid cosine (leaf–leaf)."""
    src = next((c for c in bundle.clusters if c.cluster_id == int(cluster_id)), None)
    if src is None or src.medoid_idx is None:
        return []
    med = np.asarray(bundle.vectors[int(src.medoid_idx)], dtype=np.float64)
    med = med / max(float(np.linalg.norm(med)), 1e-12)
    scored: list[tuple[ClusterRow, float]] = []
    for row in bundle.clusters:
        if row.cluster_id == src.cluster_id or row.medoid_idx is None:
            continue
        other = np.asarray(bundle.vectors[int(row.medoid_idx)], dtype=np.float64)
        other = other / max(float(np.linalg.norm(other)), 1e-12)
        scored.append((row, float(np.dot(med, other))))
    scored.sort(key=lambda t: (-t[1], t[0].cluster_id))
    return scored[: max(0, int(k))]


def _sorted_ids(labels: np.ndarray) -> list[int]:
    ids = sorted({int(x) for x in np.asarray(labels).tolist()})
    assigned = [i for i in ids if i != -1]
    if -1 in ids:
        assigned.append(-1)
    return assigned


def members_for_cluster(bundle: BrowseBundle, cluster_id: int) -> list[MemberRow]:
    """Return member claims, sorted by cosine to the leaf medoid (core first)."""
    cid = int(cluster_id)
    idx = np.where(np.asarray(bundle.labels, dtype=int) == cid)[0]
    keys = sel_mod.claim_keys_from_index(bundle.index)
    texts = claims_io.claim_texts_from_index(bundle.index)
    groups = bundle.index.get("groups") or []
    catalog = next((c for c in bundle.clusters if c.cluster_id == cid), None)
    medoid_idx = catalog.medoid_idx if catalog is not None else None
    med = None
    if medoid_idx is not None:
        med = np.asarray(bundle.vectors[int(medoid_idx)], dtype=np.float64)
        med = med / max(float(np.linalg.norm(med)), 1e-12)
    split = bundle.twomeans.get(cid)
    rows: list[MemberRow] = []
    for j, i in enumerate(idx.tolist()):
        i = int(i)
        sources: list[dict[str, Any]] = []
        if groups and i < len(groups) and isinstance(groups[i], dict):
            sources = list(groups[i].get("sources") or [])
            count = int(groups[i].get("count") or len(sources) or 1)
        else:
            count = 1
        cosine: float | None = None
        if med is not None:
            vec = np.asarray(bundle.vectors[i], dtype=np.float64)
            vec = vec / max(float(np.linalg.norm(vec)), 1e-12)
            cosine = round(float(np.dot(med, vec)), 4)
        child: int | None = None
        if split is not None and j < int(split.child_labels.size):
            child = int(split.child_labels[j])
        rows.append(
            MemberRow(
                idx=i,
                claim_key=keys[i] if i < len(keys) else "",
                claim_text=texts[i] if i < len(texts) else "",
                occurrence_count=count,
                sources=sources,
                cosine_to_medoid=cosine,
                twomeans_child=child,
                alignment=bundle.group_alignment[i] if i < len(bundle.group_alignment) else None,
            )
        )
    rows.sort(
        key=lambda r: (
            -(r.cosine_to_medoid if r.cosine_to_medoid is not None else -1.0),
            r.claim_text.casefold(),
            r.idx,
        )
    )
    return rows


def load_occurrence_index(claims_path: Path) -> dict[str, dict[str, Any]]:
    """Map chunk ``task_id`` → slim post metadata + chunk text + claim texts."""
    _payload, posts = load_posts_from_claims_json(claims_path)
    out: dict[str, dict[str, Any]] = {}
    for post in posts:
        chunks = post.get("chunks")
        if not isinstance(chunks, list):
            continue
        slim = {k: post.get(k) for k in POST_META_KEYS}
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            ctx = context_row_for_chunk(post, chunk)
            tid = str(ctx.get("task_id") or stable_task_id({**slim, **chunk}))
            claims_raw = chunk.get("claims") or []
            claim_texts: list[str] = []
            claim_scores: list[float | None] = []
            for c in claims_raw:
                if isinstance(c, dict):
                    claim_texts.append(str(c.get("claim") or ""))
                    claim_scores.append(_alignment_from_claim(c))
                elif c is not None:
                    claim_texts.append(str(c))
                    claim_scores.append(None)
            out[tid] = {
                "task_id": tid,
                "chunk_index": chunk.get("chunk_index"),
                "chunk_text": str(chunk.get("text") or ctx.get("text") or ""),
                "claim_texts": claim_texts,
                "claim_scores": claim_scores,
                "post": slim,
            }
    return out


def occurrences_for_member(
    member: MemberRow,
    occ_index: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Join group sources to post-chunks. Missing task_ids are still listed."""
    rows: list[dict[str, Any]] = []
    sources = member.sources or [{"task_id": None, "claim_index": 0}]
    for src in sources:
        tid = str(src.get("task_id") or "")
        cidx = int(src.get("claim_index") or 0)
        hit = occ_index.get(tid) if tid else None
        post = dict((hit or {}).get("post") or {})
        claim_in_chunk = ""
        if hit:
            cts = hit.get("claim_texts") or []
            if 0 <= cidx < len(cts):
                claim_in_chunk = str(cts[cidx])
        rows.append(
            {
                "task_id": tid,
                "claim_index": cidx,
                "row_id": src.get("row_id"),
                "chunk_index": (hit or {}).get("chunk_index"),
                "chunk_text": (hit or {}).get("chunk_text") or "",
                "claim_in_chunk": claim_in_chunk or member.claim_text,
                "found": hit is not None,
                **{k: post.get(k) for k in POST_META_KEYS},
            }
        )
    return rows
