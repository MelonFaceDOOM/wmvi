"""Clustering CLI commands (file-mode; no SQLite)."""

from __future__ import annotations

import hashlib
import json
from argparse import Namespace
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims import io as claims_io
from apps.claims.cli import paths as path_helpers
from apps.claims.clustering import cluster as clustering
from apps.claims.clustering import hierarchy as cluster_hierarchy
from apps.claims.clustering import metrics as cluster_metrics
from apps.claims.clustering import query_eval
from apps.claims.clustering import score as cluster_score
from apps.claims.clustering.metrics import _cluster_medoid, _per_cluster_mean_intra_cosine
from apps.claims.clustering.score import HierarchyGuards, HierarchyWeights, ObjectiveGuards, ObjectiveWeights
from apps.claims.types import EmbedConfig

INSPECT_MODES = ("largest", "loosest", "tightest", "mixed", "noise", "query")
_DEFAULT_QUERIES = claims_io.labels_dir() / "cluster_eval_queries.json"


def _resolve_run_dir(args: Namespace) -> Path:
    if getattr(args, "run_dir", None) is not None:
        return Path(args.run_dir)
    if getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        tag = path_helpers.resolve_model_tag(args)
        return corpus.run_dir(tag)
    raise ValueError("Provide --run-dir, or --corpus with --model-tag/--model")


def _resolve_out_dir(args: Namespace, *, default_exp: str | None = None) -> Path | None:
    if getattr(args, "out_dir", None) is not None:
        return Path(args.out_dir)
    if default_exp and getattr(args, "corpus", None):
        corpus = path_helpers.require_corpus(args)
        tag = path_helpers.resolve_model_tag(args)
        return corpus.experiment_dir(tag, default_exp)
    return None


def _config_from_index(index: dict[str, Any], *, model_override: str | None = None) -> EmbedConfig:
    return EmbedConfig(
        model_id=str(model_override or index.get("model_id") or ""),
        doc_instruction=str(index.get("doc_instruction") or ""),
        query_instruction=str(index.get("query_instruction") or ""),
        normalize=bool(index.get("normalize", True)),
    )


def _parse_params_json(raw: str | None) -> dict[str, Any]:
    if raw is None or not str(raw).strip():
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("params-json must be a JSON object")
    return data


def _model_cache_key(config: EmbedConfig) -> str:
    raw = f"{config.model_id}|{config.query_instruction}|{int(config.normalize)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def query_vectors_path(out_dir: Path, config: EmbedConfig) -> Path:
    return out_dir / f"query_vectors_{_model_cache_key(config)}.npz"


def load_or_build_query_cache(
    *,
    config: EmbedConfig,
    out_dir: Path,
    queries: list[dict[str, Any]],
    force: bool = False,
) -> dict[str, np.ndarray]:
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_path = query_vectors_path(out_dir, config)
    if cache_path.is_file() and not force:
        data = np.load(cache_path, allow_pickle=False)
        ids = list(data["ids"])
        vectors = np.asarray(data["vectors"], dtype=np.float32)
        return {str(qid): vectors[i] for i, qid in enumerate(ids)}

    ids: list[str] = []
    rows: list[np.ndarray] = []
    for row in queries:
        qid = str(row.get("id") or row.get("query") or len(ids))
        vec = query_eval.embed_query(
            config.model_id,
            str(row["query"]),
            query_instruction=config.query_instruction,
        )
        ids.append(qid)
        rows.append(vec)
    stacked = np.stack(rows, axis=0).astype(np.float32, copy=False)
    np.savez_compressed(
        cache_path,
        ids=np.asarray(ids),
        vectors=stacked,
        model_id=np.asarray(config.model_id),
    )
    return {qid: stacked[i] for i, qid in enumerate(ids)}


def run_eval_with_cached_queries(
    vectors: np.ndarray,
    *,
    config: EmbedConfig,
    labels: np.ndarray,
    queries: list[dict[str, Any]],
    query_vectors: dict[str, np.ndarray],
) -> query_eval.EvalSuiteResult:
    results: list[query_eval.QueryEvalResult] = []
    for row in queries:
        qid = str(row.get("id") or row.get("query") or "")
        qvec = query_vectors.get(qid)
        if qvec is None:
            qvec = query_eval.embed_query(
                config.model_id,
                str(row["query"]),
                query_instruction=config.query_instruction,
            )
        results.append(
            query_eval.eval_query(
                vectors,
                config=config,
                query=str(row["query"]),
                top_k=int(row.get("top_k", 20)),
                labels=labels,
                query_id=qid,
                query_vector=qvec,
            )
        )
    shares = [r.dominant_cluster_share for r in results if r.dominant_cluster_share is not None]
    cosines = [r.mean_top_k_cosine for r in results]
    mean_share = round(float(np.mean(shares)), 4) if shares else None
    mean_cos = round(float(np.mean(cosines)), 4) if cosines else 0.0
    return query_eval.EvalSuiteResult(
        results=results,
        mean_dominant_cluster_share=mean_share,
        mean_top_k_cosine=mean_cos,
    )


def evaluate_clustering(
    *,
    vectors: np.ndarray,
    config: EmbedConfig,
    algorithm: str,
    params: dict[str, Any],
    seed: int,
    queries: list[dict[str, Any]],
    query_vectors: dict[str, np.ndarray],
) -> tuple[np.ndarray, dict[str, Any]]:
    result = clustering.run_clustering(vectors, algorithm=algorithm, params=params, seed=seed)
    metrics = cluster_metrics.compute_cluster_metrics(vectors, result.labels)
    shape = cluster_score.shape_from_labels(result.labels)
    eval_suite = run_eval_with_cached_queries(
        vectors,
        config=config,
        labels=result.labels,
        queries=queries,
        query_vectors=query_vectors,
    )
    score = cluster_score.compute_objective(
        metrics,
        eval_score=eval_suite.mean_dominant_cluster_share,
        weights=ObjectiveWeights(),
        guards=ObjectiveGuards(),
        largest_cluster_share=shape["largest_cluster_share"],
        singleton_frac=shape["singleton_frac"],
    )
    per_query = [
        {
            "id": r.query_id,
            "query": r.query,
            "dominant_cluster_share": r.dominant_cluster_share,
            "label_entropy": r.label_entropy,
            "mean_top_k_cosine": r.mean_top_k_cosine,
            "dominant_cluster_id": r.dominant_cluster_id,
        }
        for r in eval_suite.results
    ]
    payload = {
        "algorithm": algorithm,
        "params": params,
        "seed": seed,
        "n_clusters": result.n_clusters,
        "n_noise": result.n_noise,
        "coverage_pct": metrics.get("coverage_pct"),
        "mean_intra_cosine": metrics.get("mean_intra_cosine"),
        "silhouette": metrics.get("mean_silhouette_cosine"),
        "eval_score": eval_suite.mean_dominant_cluster_share,
        "mean_top_k_cosine": eval_suite.mean_top_k_cosine,
        "objective": score["objective"],
        "objective_base": score["objective_base"],
        "components": score["components"],
        "penalties": score["penalties"],
        "flags": score["flags"],
        "largest_cluster_share": score["largest_cluster_share"],
        "singleton_frac": score["singleton_frac"],
        "prep_meta": result.prep_meta,
        "per_query": per_query,
    }
    return result.labels, payload


def cmd_cluster(args: Namespace) -> int:
    try:
        algorithm = str(args.algorithm)
        if algorithm not in clustering.CLUSTER_ALGORITHMS:
            raise ValueError(f"Unknown algorithm {algorithm!r}")
        params = _parse_params_json(args.params_json)
        run_dir = _resolve_run_dir(args)
        stamp = hashlib.sha1(
            json.dumps({"a": algorithm, "p": params, "s": int(args.seed)}, sort_keys=True).encode()
        ).hexdigest()[:10]
        out_dir = _resolve_out_dir(args, default_exp=f"cluster_{stamp}")
        vectors, index = claims_io.load_run_arrays(run_dir)
        config = _config_from_index(index)
        queries_path = Path(args.queries) if args.queries else _DEFAULT_QUERIES
        queries = query_eval.load_eval_queries(queries_path)
        cache_dir = out_dir if out_dir is not None else run_dir
        query_vectors = load_or_build_query_cache(
            config=config, out_dir=cache_dir, queries=queries, force=False
        )
        labels, payload = evaluate_clustering(
            vectors=vectors,
            config=config,
            algorithm=algorithm,
            params=params,
            seed=int(args.seed),
            queries=queries,
            query_vectors=query_vectors,
        )
        payload["run_dir"] = str(run_dir)
        payload["model_id"] = config.model_id
        if out_dir is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            result_path = out_dir / f"result_{stamp}.json"
            claims_io.write_json(result_path, payload)
            payload["result_path"] = str(result_path)
            if args.save_labels:
                labels_path = out_dir / f"labels_{stamp}.npy"
                np.save(labels_path, labels)
                payload["labels_path"] = str(labels_path)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0


def cmd_hierarchy(args: Namespace) -> int:
    try:
        from apps.claims.clustering import presets as cluster_presets

        preset_name = getattr(args, "preset", None)
        if preset_name:
            preset = cluster_presets.get_hierarchy_preset(str(preset_name))
            leaf_algorithm = str(preset["leaf_algorithm"])
            narrative_algorithm = str(preset["narrative_algorithm"])
            leaf_params = dict(preset["leaf_params"])
            narrative_params = dict(preset["narrative_params"])
        else:
            leaf_algorithm = str(args.leaf_algorithm or "hdbscan")
            narrative_algorithm = str(args.narrative_algorithm or "agglomerative")
            leaf_params = (
                _parse_params_json(args.leaf_params_json)
                if args.leaf_params_json
                else dict(cluster_hierarchy.DEFAULT_LEAF_PARAMS)
            )
            narrative_params = (
                _parse_params_json(args.narrative_params_json)
                if args.narrative_params_json
                else dict(cluster_hierarchy.DEFAULT_NARRATIVE_PARAMS)
            )

        if args.leaf_algorithm:
            leaf_algorithm = str(args.leaf_algorithm)
        if args.narrative_algorithm:
            narrative_algorithm = str(args.narrative_algorithm)
        if args.leaf_params_json:
            leaf_params = _parse_params_json(args.leaf_params_json)
        if args.narrative_params_json:
            narrative_params = _parse_params_json(args.narrative_params_json)

        for name, algo in (("leaf", leaf_algorithm), ("narrative", narrative_algorithm)):
            if algo not in clustering.CLUSTER_ALGORITHMS:
                raise ValueError(f"Unknown {name} algorithm {algo!r}")
        run_dir = _resolve_run_dir(args)
        seed = int(args.seed)
        stamp = hashlib.sha1(
            json.dumps(
                {
                    "leaf_a": leaf_algorithm,
                    "leaf_p": leaf_params,
                    "nar_a": narrative_algorithm,
                    "nar_p": narrative_params,
                    "s": seed,
                    "preset": preset_name,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()[:10]
        default_exp = (
            f"hierarchy_{preset_name}_{stamp}" if preset_name else f"hierarchy_{stamp}"
        )
        out_dir = _resolve_out_dir(args, default_exp=default_exp)
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        config = _config_from_index(index)
        queries_path = Path(args.queries) if args.queries else _DEFAULT_QUERIES
        queries = query_eval.load_eval_queries(queries_path)
        cache_dir = out_dir if out_dir is not None else run_dir
        query_vectors = load_or_build_query_cache(
            config=config, out_dir=cache_dir, queries=queries, force=False
        )
        hier = cluster_hierarchy.build_hierarchy(
            vectors,
            leaf_algorithm=leaf_algorithm,
            leaf_params=leaf_params,
            narrative_algorithm=narrative_algorithm,
            narrative_params=narrative_params,
            seed=seed,
        )
        leaf_metrics = cluster_metrics.compute_cluster_metrics(vectors, hier.leaf_labels)
        narrative_metrics = cluster_metrics.compute_cluster_metrics(vectors, hier.narrative_labels)
        leaf_shape = cluster_score.shape_from_labels(hier.leaf_labels)
        nar_shape = cluster_score.shape_from_labels(hier.narrative_labels)
        eval_suite = run_eval_with_cached_queries(
            vectors,
            config=config,
            labels=hier.narrative_labels,
            queries=queries,
            query_vectors=query_vectors,
        )
        score = cluster_score.compute_hierarchy_objective(
            leaf_metrics,
            narrative_metrics,
            eval_score=eval_suite.mean_dominant_cluster_share,
            weights=HierarchyWeights(),
            guards=HierarchyGuards(),
            leaf_singleton_frac=leaf_shape["singleton_frac"],
            narrative_largest_share=nar_shape["largest_cluster_share"],
        )
        narratives = cluster_hierarchy.nested_hierarchy_payload(
            hierarchy=hier,
            vectors=vectors,
            claim_texts=claim_texts,
            n_samples_per_leaf=int(args.n_samples_per_leaf),
            seed=seed,
        )
        payload: dict[str, Any] = {
            "run_dir": str(run_dir),
            "model_id": config.model_id,
            "leaf_algorithm": leaf_algorithm,
            "leaf_params": leaf_params,
            "narrative_algorithm": narrative_algorithm,
            "narrative_params": narrative_params,
            "preset": preset_name,
            "seed": seed,
            "n_leaves": hier.n_leaves,
            "n_narratives": hier.n_narratives,
            "n_leaf_noise": hier.n_leaf_noise,
            "leaf_coverage_pct": leaf_metrics.get("coverage_pct"),
            "leaf_mean_intra_cosine": leaf_metrics.get("mean_intra_cosine"),
            "eval_score": eval_suite.mean_dominant_cluster_share,
            "objective": score["objective"],
            "objective_base": score["objective_base"],
            "components": score["components"],
            "penalties": score["penalties"],
            "flags": score["flags"],
            "narratives": narratives,
        }
        if out_dir is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            result_path = out_dir / f"hierarchy_{stamp}.json"
            claims_io.write_json(result_path, payload)
            payload["result_path"] = str(result_path)
            if args.save_labels:
                leaf_path = out_dir / f"leaf_labels_{stamp}.npy"
                nar_path = out_dir / f"narrative_labels_{stamp}.npy"
                np.save(leaf_path, hier.leaf_labels)
                np.save(nar_path, hier.narrative_labels)
                payload["leaf_labels_path"] = str(leaf_path)
                payload["narrative_labels_path"] = str(nar_path)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0


def _cluster_stats(vectors: np.ndarray, labels: np.ndarray, *, seed: int = 0) -> list[dict[str, Any]]:
    labels = np.asarray(labels, dtype=int)
    rng = np.random.default_rng(seed)
    rows: list[dict[str, Any]] = []
    for cid in sorted({int(x) for x in labels.tolist() if int(x) != -1}):
        idx = np.where(labels == cid)[0]
        if idx.size == 0:
            continue
        medoid = _cluster_medoid(vectors, idx)
        intra = _per_cluster_mean_intra_cosine(vectors, idx, rng)
        rows.append(
            {
                "cluster_id": cid,
                "size": int(idx.size),
                "mean_intra_cosine": round(float(intra), 4),
                "medoid_idx": int(medoid),
            }
        )
    return rows


def sample_cluster_members(
    *,
    vectors: np.ndarray,
    labels: np.ndarray,
    claim_texts: list[str],
    cluster_id: int,
    n_per_cluster: int,
    seed: int = 0,
    include_outlier: bool = True,
) -> dict[str, Any]:
    labels = np.asarray(labels, dtype=int)
    idx = np.where(labels == int(cluster_id))[0]
    rng = np.random.default_rng(seed + (0 if cluster_id < 0 else cluster_id + 1))

    def _row(role: str, i: int) -> dict[str, Any]:
        i = int(i)
        text = claim_texts[i] if 0 <= i < len(claim_texts) else ""
        return {"role": role, "idx": i, "claim_text": text}

    if idx.size == 0:
        return {"cluster_id": int(cluster_id), "size": 0, "mean_intra_cosine": None, "samples": []}
    if cluster_id == -1:
        k = min(int(n_per_cluster), int(idx.size))
        pick = rng.choice(idx, size=k, replace=False) if k else []
        return {
            "cluster_id": -1,
            "size": int(idx.size),
            "mean_intra_cosine": None,
            "samples": [_row("noise", i) for i in pick],
        }

    medoid = _cluster_medoid(vectors, idx)
    intra = _per_cluster_mean_intra_cosine(vectors, idx, rng)
    samples: list[dict[str, Any]] = [_row("medoid", medoid)]
    remaining = [int(i) for i in idx.tolist() if int(i) != int(medoid)]
    outlier_idx: int | None = None
    if include_outlier and remaining:
        med_vec = vectors[int(medoid)]
        sims = vectors[np.asarray(remaining, dtype=int)] @ med_vec
        outlier_idx = int(remaining[int(np.argmin(sims))])
        samples.append(_row("outlier", outlier_idx))
    member_pool = [i for i in remaining if i != outlier_idx]
    n_members = max(0, int(n_per_cluster) - len(samples))
    if member_pool and n_members > 0:
        k = min(n_members, len(member_pool))
        pick = rng.choice(np.asarray(member_pool, dtype=int), size=k, replace=False)
        samples.extend(_row("member", i) for i in pick.tolist())
    return {
        "cluster_id": int(cluster_id),
        "size": int(idx.size),
        "mean_intra_cosine": round(float(intra), 4),
        "samples": samples,
    }


def select_cluster_ids(
    stats: list[dict[str, Any]],
    *,
    mode: str,
    n_clusters: int,
    min_size: int = 3,
) -> list[int]:
    if mode == "noise":
        return [-1]
    usable = [s for s in stats if int(s["size"]) >= int(min_size)] or list(stats)
    if not usable:
        return []
    n = max(1, int(n_clusters))
    by_size = sorted(usable, key=lambda s: (-int(s["size"]), int(s["cluster_id"])))
    by_loose = sorted(
        usable, key=lambda s: (float(s["mean_intra_cosine"]), -int(s["size"]), int(s["cluster_id"]))
    )
    by_tight = sorted(
        usable, key=lambda s: (-float(s["mean_intra_cosine"]), -int(s["size"]), int(s["cluster_id"]))
    )
    if mode == "largest":
        return [int(s["cluster_id"]) for s in by_size[:n]]
    if mode == "loosest":
        return [int(s["cluster_id"]) for s in by_loose[:n]]
    if mode == "tightest":
        return [int(s["cluster_id"]) for s in by_tight[:n]]
    if mode == "mixed":
        half = max(1, n // 2)
        chosen: list[int] = []
        seen: set[int] = set()
        for s in by_size[:half] + by_loose[: n - half]:
            cid = int(s["cluster_id"])
            if cid not in seen:
                chosen.append(cid)
                seen.add(cid)
            if len(chosen) >= n:
                break
        return chosen
    if mode == "query":
        return [int(s["cluster_id"]) for s in by_size[:n]]
    raise ValueError(f"Unknown inspect mode: {mode!r}")


def cmd_inspect(args: Namespace) -> int:
    try:
        mode = str(args.mode)
        if mode not in INSPECT_MODES:
            raise ValueError(f"Unknown mode {mode!r}")
        labels_path = Path(args.labels)
        run_dir = _resolve_run_dir(args)
        out_dir = _resolve_out_dir(args, default_exp="inspect")
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        labels = np.asarray(np.load(labels_path), dtype=int)
        if labels.shape[0] != vectors.shape[0]:
            raise ValueError("labels length != vectors length")
        parent_labels = None
        if args.parent_labels is not None:
            parent_labels = np.asarray(np.load(Path(args.parent_labels)), dtype=int)
        cluster_ids = None
        if args.cluster_ids:
            cluster_ids = [int(x.strip()) for x in str(args.cluster_ids).split(",") if x.strip()]
        stats = _cluster_stats(vectors, labels, seed=int(args.seed))
        if cluster_ids is not None:
            selected = cluster_ids
        else:
            selected = select_cluster_ids(
                stats, mode=mode, n_clusters=int(args.n_clusters), min_size=int(args.min_size)
            )
        clusters = [
            sample_cluster_members(
                vectors=vectors,
                labels=labels,
                claim_texts=claim_texts,
                cluster_id=cid,
                n_per_cluster=int(args.n_per_cluster),
                seed=int(args.seed),
                include_outlier=(cid != -1),
            )
            for cid in selected
        ]
        if parent_labels is not None:
            for row in clusters:
                row["parent_cluster_id"] = cluster_hierarchy.parent_id_for_cluster(
                    child_labels=labels,
                    parent_labels=parent_labels,
                    cluster_id=int(row["cluster_id"]),
                    vectors=vectors,
                )
        payload = {
            "mode": mode,
            "selected_cluster_ids": selected,
            "clusters": clusters,
            "run_dir": str(run_dir),
            "labels_path": str(labels_path),
        }
        if out_dir is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            path = out_dir / "inspect.json"
            claims_io.write_json(path, payload)
            payload["inspect_path"] = str(path)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0


def cmd_prep_queries(args: Namespace) -> int:
    try:
        config = EmbedConfig(
            model_id=str(args.model),
            query_instruction=str(args.query_instruction or ""),
        )
        queries_path = Path(args.queries) if args.queries else _DEFAULT_QUERIES
        queries = query_eval.load_eval_queries(queries_path)
        cache = load_or_build_query_cache(
            config=config,
            out_dir=Path(args.out_dir),
            queries=queries,
            force=bool(args.force),
        )
        path = query_vectors_path(Path(args.out_dir), config)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(
        {
            "ok": True,
            "model_id": config.model_id,
            "cache_path": str(path),
            "n_queries": len(cache),
            "query_ids": list(cache.keys()),
        }
    )
    return 0


def cmd_sweep(args: Namespace) -> int:
    try:
        configs_raw = json.loads(Path(args.configs).read_text(encoding="utf-8"))
        if not isinstance(configs_raw, list) or not configs_raw:
            raise ValueError("--configs must be a non-empty JSON array")
        run_dir = _resolve_run_dir(args)
        out_dir = _resolve_out_dir(args, default_exp="sweep")
        if out_dir is None:
            raise ValueError("Provide --out-dir, or --corpus with --model-tag")
        vectors, index = claims_io.load_run_arrays(run_dir)
        config = _config_from_index(index)
        queries_path = Path(args.queries) if args.queries else _DEFAULT_QUERIES
        queries = query_eval.load_eval_queries(queries_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        query_vectors = load_or_build_query_cache(
            config=config, out_dir=out_dir, queries=queries, force=False
        )
        sweep_path = out_dir / "sweep.jsonl"
        best: dict[str, Any] | None = None
        rows: list[dict[str, Any]] = []
        with sweep_path.open("w", encoding="utf-8") as fh:
            for i, cfg in enumerate(configs_raw):
                algorithm = str(cfg.get("algorithm") or "")
                params = cfg.get("params") or {}
                cfg_seed = int(cfg.get("seed", args.seed))
                _labels, payload = evaluate_clustering(
                    vectors=vectors,
                    config=config,
                    algorithm=algorithm,
                    params=params,
                    seed=cfg_seed,
                    queries=queries,
                    query_vectors=query_vectors,
                )
                payload["config_index"] = i
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
                rows.append(payload)
                if best is None or float(payload["objective"]) > float(best["objective"]):
                    best = payload
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(
        {"ok": True, "n_configs": len(rows), "sweep_path": str(sweep_path), "best": best}
    )
    return 0


def cmd_doctor(args: Namespace) -> int:
    checks: list[dict[str, Any]] = []
    hard_fail = False
    try:
        claims_io.ensure_data_dirs()
        checks.append({"name": "data_dirs", "ok": True, "detail": str(claims_io.data_root())})
    except Exception as exc:  # noqa: BLE001
        checks.append({"name": "data_dirs", "ok": False, "detail": str(exc)})
        hard_fail = True

    run_dir: Path | None = None
    try:
        if getattr(args, "run_dir", None) is not None or getattr(args, "corpus", None):
            run_dir = _resolve_run_dir(args)
    except Exception as exc:  # noqa: BLE001
        checks.append({"name": "run_dir_resolve", "ok": False, "detail": str(exc)})
        hard_fail = True

    if run_dir is not None:
        try:
            vectors, index = claims_io.load_run_arrays(run_dir)
            texts = claims_io.claim_texts_from_index(index)
            ok = vectors.ndim == 2 and vectors.shape[0] == len(texts) and vectors.shape[0] > 0
            checks.append(
                {
                    "name": "run_arrays",
                    "ok": ok,
                    "detail": f"vectors={tuple(vectors.shape)} claim_texts={len(texts)}",
                }
            )
            if not ok:
                hard_fail = True
        except Exception as exc:  # noqa: BLE001
            checks.append({"name": "run_arrays", "ok": False, "detail": str(exc)})
            hard_fail = True

    queries_path = Path(args.queries) if args.queries else _DEFAULT_QUERIES
    try:
        queries = query_eval.load_eval_queries(queries_path)
        checks.append({"name": "queries_file", "ok": True, "detail": f"{queries_path} ({len(queries)})"})
    except Exception as exc:  # noqa: BLE001
        checks.append({"name": "queries_file", "ok": False, "detail": str(exc)})
        hard_fail = True

    for mod in ("sentence_transformers", "sklearn", "networkx"):
        try:
            __import__(mod)
            checks.append({"name": f"import:{mod}", "ok": True, "detail": "ok"})
        except Exception as exc:  # noqa: BLE001
            checks.append({"name": f"import:{mod}", "ok": False, "detail": str(exc)})
            hard_fail = True

    if run_dir is not None and not args.skip_model:
        try:
            vectors, index = claims_io.load_run_arrays(run_dir)
            config = _config_from_index(index)
            from apps.claims.embedding.encode import encode_texts, load_sentence_transformer

            enc = load_sentence_transformer(config.model_id)
            probe = encode_texts(enc, ["doctor probe"], normalize_embeddings=True)
            checks.append(
                {
                    "name": "model_embed",
                    "ok": True,
                    "detail": f"model_id={config.model_id} dim={int(np.asarray(probe).shape[-1])}",
                }
            )
        except Exception as exc:  # noqa: BLE001
            checks.append({"name": "model_embed", "ok": False, "detail": str(exc)})
            hard_fail = True

    ok = not hard_fail
    claims_io.emit_json({"ok": ok, "checks": checks})
    return 0 if ok else 1
