"""Read-only loaders for browsing one cluster / hierarchy experiment.

Discovers ``cluster`` or ``hierarchy`` output (JSON + label arrays), reapplies
the same ``--selection`` / ``--filter`` used at cluster time, and joins member
claims to nested ``claims.json`` post-chunks.
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

_RUNS_TAIL = re.compile(r"(?:^|/)runs/([^/]+)/([^/]+)/?$")

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
    kind: str  # "hierarchy" | "flat"
    directory: Path
    meta_path: Path | None
    payload: dict[str, Any]
    labels_path: Path
    parent_labels_path: Path | None
    run_dir: Path | None
    n_selected: int | None
    hint: SelectionHint


@dataclass
class ClusterRow:
    cluster_id: int
    size: int
    mean_intra_cosine: float | None = None
    medoid_text: str = ""
    parent_id: int | None = None
    n_children: int | None = None


@dataclass
class MemberRow:
    idx: int
    claim_key: str
    claim_text: str
    occurrence_count: int
    sources: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class BrowseBundle:
    output: ClusterOutput
    index: dict[str, Any]
    labels: np.ndarray
    parent_labels: np.ndarray | None
    corpus: str | None
    claims_path: Path | None
    applied_selection: str | None
    applied_filters: tuple[str, ...]
    clusters: list[ClusterRow]
    narratives: list[ClusterRow] | None


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


def discover_cluster_output(
    path: Path,
    *,
    labels: Path | None = None,
    parent_labels: Path | None = None,
) -> ClusterOutput:
    """Resolve a cluster/hierarchy experiment dir, result JSON, or labels npy."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Cluster output not found: {path}")

    labels_override = Path(labels) if labels is not None else None
    parent_override = Path(parent_labels) if parent_labels is not None else None

    if path.is_file() and path.suffix == ".npy":
        return _output_from_labels_file(path, parent_override)

    meta_path: Path | None = None
    directory: Path
    if path.is_file() and path.suffix == ".json":
        meta_path = path
        directory = path.parent
    elif path.is_dir():
        directory = path
    else:
        raise ValueError(f"Expected a directory, JSON, or .npy labels file; got {path}")

    hier_jsons = sorted(directory.glob("hierarchy_*.json"))
    result_jsons = sorted(directory.glob("result_*.json"))
    leaf_npys = sorted(directory.glob("leaf_labels_*.npy"))
    flat_npys = sorted(directory.glob("labels_*.npy"))
    if meta_path is None:
        if hier_jsons:
            meta_path = _pick_stamp_file(hier_jsons, None)
        elif result_jsons:
            meta_path = _pick_stamp_file(result_jsons, None)

    payload: dict[str, Any] = {}
    kind = "flat"
    if meta_path is not None:
        raw = claims_io.read_json(meta_path)
        if not isinstance(raw, dict):
            raise ValueError(f"Cluster JSON must be an object: {meta_path}")
        payload = raw
        if "narratives" in payload or meta_path.name.startswith("hierarchy_"):
            kind = "hierarchy"
    elif leaf_npys:
        kind = "hierarchy"

    stamp = None
    if meta_path is not None:
        stamp = _stamp_from_name(meta_path, ("hierarchy_", "result_"))

    if labels_override is not None:
        labels_path = labels_override
    elif kind == "hierarchy":
        labels_path = _pick_stamp_file(leaf_npys, stamp)
        if labels_path is None:
            raise FileNotFoundError(
                f"No leaf_labels_*.npy in {directory}; re-run hierarchy with --save-labels"
            )
    else:
        labels_path = _pick_stamp_file(flat_npys, stamp)
        if labels_path is None:
            raise FileNotFoundError(
                f"No labels_*.npy in {directory}; re-run cluster with --save-labels"
            )

    parent_path = parent_override
    if parent_path is None and kind == "hierarchy":
        found_p = list(directory.glob("narrative_labels_*.npy"))
        parent_path = _pick_stamp_file(found_p, stamp)

    run_dir = Path(payload["run_dir"]) if payload.get("run_dir") else None
    n_selected = int(payload["n_selected"]) if payload.get("n_selected") is not None else None
    hint = parse_selection_label(payload.get("selection") if isinstance(payload.get("selection"), str) else None)
    return ClusterOutput(
        kind=kind,
        directory=directory,
        meta_path=meta_path,
        payload=payload,
        labels_path=labels_path,
        parent_labels_path=parent_path,
        run_dir=run_dir,
        n_selected=n_selected,
        hint=hint,
    )


def _output_from_labels_file(labels_path: Path, parent_labels: Path | None) -> ClusterOutput:
    directory = labels_path.parent
    kind = "hierarchy" if labels_path.name.startswith("leaf_labels_") else "flat"
    parent_path = parent_labels
    if parent_path is None and kind == "hierarchy":
        stamp = _stamp_from_name(labels_path, ("leaf_labels_",))
        found = list(directory.glob("narrative_labels_*.npy"))
        parent_path = _pick_stamp_file(found, stamp)
    meta_path = None
    payload: dict[str, Any] = {}
    prefix = "hierarchy_" if kind == "hierarchy" else "result_"
    metas = list(directory.glob(f"{prefix}*.json"))
    stamp = _stamp_from_name(labels_path, ("leaf_labels_", "labels_"))
    meta_path = _pick_stamp_file(metas, stamp)
    if meta_path is not None:
        raw = claims_io.read_json(meta_path)
        if isinstance(raw, dict):
            payload = raw
    run_dir = Path(payload["run_dir"]) if payload.get("run_dir") else None
    n_selected = int(payload["n_selected"]) if payload.get("n_selected") is not None else None
    hint = parse_selection_label(payload.get("selection") if isinstance(payload.get("selection"), str) else None)
    return ClusterOutput(
        kind=kind,
        directory=directory,
        meta_path=meta_path,
        payload=payload,
        labels_path=labels_path,
        parent_labels_path=parent_path,
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
    parent_labels: Path | None = None,
    selection: str | None = None,
    filter_specs: list[str] | None = None,
    claims_path: Path | None = None,
) -> BrowseBundle:
    """Load one clustering output aligned to the embed-run subset it was built on."""
    output = discover_cluster_output(source, labels=labels, parent_labels=parent_labels)
    resolved_run = resolve_run_dir(output, run_dir=run_dir, corpus=corpus, model_tag=model_tag)
    index = claims_io.load_run_index(resolved_run)

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
        index = filt.subset_index_by_keys(
            index,
            wanted,
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

    parent_arr = None
    if output.parent_labels_path is not None:
        parent_arr = np.asarray(np.load(output.parent_labels_path), dtype=int)
        if parent_arr.shape[0] != labels_arr.shape[0]:
            raise ValueError(
                f"parent labels length ({parent_arr.shape[0]}) != labels ({labels_arr.shape[0]})"
            )

    texts = claims_io.claim_texts_from_index(index)
    clusters, narratives = _catalog(
        kind=output.kind,
        payload=output.payload,
        labels=labels_arr,
        parent_labels=parent_arr,
        claim_texts=texts,
    )

    resolved_claims = claims_path
    if resolved_claims is None and corpus_name:
        cand = corpus_mod.get_corpus(corpus_name).claims
        if cand.is_file():
            resolved_claims = cand

    return BrowseBundle(
        output=output,
        index=index,
        labels=labels_arr,
        parent_labels=parent_arr,
        corpus=corpus_name,
        claims_path=resolved_claims,
        applied_selection=applied_sel,
        applied_filters=applied_filters,
        clusters=clusters,
        narratives=narratives,
    )


def _catalog(
    *,
    kind: str,
    payload: dict[str, Any],
    labels: np.ndarray,
    parent_labels: np.ndarray | None,
    claim_texts: list[str],
) -> tuple[list[ClusterRow], list[ClusterRow] | None]:
    leaf_meta: dict[int, dict[str, Any]] = {}
    narrative_meta: dict[int, dict[str, Any]] = {}
    for nar in payload.get("narratives") or []:
        if not isinstance(nar, dict):
            continue
        nid = int(nar.get("narrative_id", -1))
        narrative_meta[nid] = nar
        for leaf in nar.get("leaves") or []:
            if isinstance(leaf, dict) and leaf.get("leaf_id") is not None:
                leaf_meta[int(leaf["leaf_id"])] = leaf

    clusters: list[ClusterRow] = []
    for cid in _sorted_ids(labels):
        idx = np.where(labels == cid)[0]
        meta = leaf_meta.get(cid, {})
        medoid_text = str(meta.get("medoid_claim_text") or "")
        if not medoid_text and idx.size:
            mi = int(meta["medoid_idx"]) if meta.get("medoid_idx") is not None else int(idx[0])
            if 0 <= mi < len(claim_texts):
                medoid_text = str(claim_texts[mi])
            else:
                medoid_text = str(claim_texts[int(idx[0])])
        parent_id = None
        if parent_labels is not None and idx.size:
            parent_id = int(parent_labels[int(idx[0])])
        tightness = meta.get("mean_intra_cosine")
        clusters.append(
            ClusterRow(
                cluster_id=cid,
                size=int(idx.size),
                mean_intra_cosine=(float(tightness) if tightness is not None else None),
                medoid_text=medoid_text,
                parent_id=parent_id,
            )
        )

    if kind != "hierarchy":
        return clusters, None

    narratives: list[ClusterRow] = []
    if parent_labels is None:
        return clusters, narratives
    for nid in _sorted_ids(parent_labels):
        idx = np.where(parent_labels == nid)[0]
        child_ids = sorted({int(c.cluster_id) for c in clusters if c.parent_id == nid})
        meta = narrative_meta.get(nid, {})
        medoid = ""
        child_rows = [c for c in clusters if c.parent_id == nid]
        if child_rows:
            medoid = max(child_rows, key=lambda r: r.size).medoid_text
        tightness_vals = [c.mean_intra_cosine for c in child_rows if c.mean_intra_cosine is not None]
        mean_t = float(np.mean(tightness_vals)) if tightness_vals else None
        narratives.append(
            ClusterRow(
                cluster_id=nid,
                size=int(idx.size),
                mean_intra_cosine=(round(mean_t, 4) if mean_t is not None else None),
                medoid_text=medoid,
                n_children=int(meta.get("n_leaves") or len(child_ids)),
            )
        )
    narratives.sort(key=lambda r: (-r.size, r.cluster_id))
    clusters.sort(key=lambda r: (-r.size, r.cluster_id))
    return clusters, narratives


def _sorted_ids(labels: np.ndarray) -> list[int]:
    ids = sorted({int(x) for x in np.asarray(labels).tolist()})
    assigned = [i for i in ids if i != -1]
    if -1 in ids:
        assigned.append(-1)
    return assigned


def members_for_cluster(bundle: BrowseBundle, cluster_id: int, *, level: str = "leaf") -> list[MemberRow]:
    """Return all member claims for a leaf (or narrative / flat cluster)."""
    labels = bundle.parent_labels if level == "narrative" and bundle.parent_labels is not None else bundle.labels
    idx = np.where(np.asarray(labels, dtype=int) == int(cluster_id))[0]
    keys = sel_mod.claim_keys_from_index(bundle.index)
    texts = claims_io.claim_texts_from_index(bundle.index)
    groups = bundle.index.get("groups") or []
    rows: list[MemberRow] = []
    for i in idx.tolist():
        i = int(i)
        sources: list[dict[str, Any]] = []
        if groups and i < len(groups) and isinstance(groups[i], dict):
            sources = list(groups[i].get("sources") or [])
            count = int(groups[i].get("count") or len(sources) or 1)
        else:
            count = 1
        rows.append(
            MemberRow(
                idx=i,
                claim_key=keys[i] if i < len(keys) else "",
                claim_text=texts[i] if i < len(texts) else "",
                occurrence_count=count,
                sources=sources,
            )
        )
    rows.sort(key=lambda r: (-r.occurrence_count, r.claim_text.casefold(), r.idx))
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
            for c in claims_raw:
                if isinstance(c, dict):
                    claim_texts.append(str(c.get("claim") or ""))
                elif c is not None:
                    claim_texts.append(str(c))
            out[tid] = {
                "task_id": tid,
                "chunk_index": chunk.get("chunk_index"),
                "chunk_text": str(chunk.get("text") or ctx.get("text") or ""),
                "claim_texts": claim_texts,
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
