"""Query-based eval harness for comparing cluster runs."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims import io as claims_io
from apps.claims.types import EmbedConfig

_DEFAULT_QUERIES_PATH = claims_io.labels_dir() / "cluster_eval_queries.json"


@dataclass
class QueryEvalResult:
    query_id: str
    query: str
    top_k: int
    mean_top_k_cosine: float
    dominant_cluster_id: int | None
    dominant_cluster_share: float | None
    label_entropy: float | None
    top_indices: list[int] = field(default_factory=list)


@dataclass
class EvalSuiteResult:
    results: list[QueryEvalResult]
    mean_dominant_cluster_share: float | None
    mean_top_k_cosine: float


def load_eval_queries(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or _DEFAULT_QUERIES_PATH
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {p}")
    return data


def embed_query(model_id: str, text: str, *, query_instruction: str = "") -> np.ndarray:
    from apps.claims.embedding.encode import encode_texts, load_sentence_transformer

    encoder = load_sentence_transformer(model_id)
    instr = (query_instruction or "").strip()
    payload = f"{instr} {text}".strip() if instr else text
    vec = encode_texts(encoder, [payload], normalize_embeddings=True)
    return np.asarray(vec, dtype=np.float32)[0]


def _label_entropy(labels: np.ndarray) -> float:
    labels = np.asarray(labels, dtype=int)
    if labels.size == 0:
        return 0.0
    _, counts = np.unique(labels, return_counts=True)
    probs = counts.astype(np.float64) / counts.sum()
    ent = -float(np.sum(probs * np.log(probs + 1e-12)))
    max_ent = math.log(len(counts)) if len(counts) > 1 else 1.0
    return round(ent / max_ent, 4) if max_ent > 0 else 0.0


def _dominant_cluster_share(labels: np.ndarray) -> tuple[int | None, float | None]:
    labels = np.asarray(labels, dtype=int)
    if labels.size == 0:
        return None, None
    valid = labels[labels != -1]
    if valid.size == 0:
        return None, None
    vals, counts = np.unique(valid, return_counts=True)
    best_i = int(np.argmax(counts))
    share = float(counts[best_i]) / float(labels.size)
    return int(vals[best_i]), round(share, 4)


def eval_query(
    vectors: np.ndarray,
    *,
    config: EmbedConfig,
    query: str,
    top_k: int = 20,
    labels: np.ndarray | None = None,
    query_id: str = "",
    query_vector: np.ndarray | None = None,
) -> QueryEvalResult:
    if query_vector is not None:
        q = np.asarray(query_vector, dtype=np.float64)
    else:
        q = embed_query(config.model_id, query, query_instruction=config.query_instruction)
    scores = vectors @ q
    k = int(min(top_k, len(scores)))
    top = np.argsort(-scores)[:k]
    top_labels = None
    if labels is not None:
        top_labels = np.asarray(labels, dtype=int)[top]
    dom_id, dom_share = (None, None)
    entropy = None
    if top_labels is not None:
        dom_id, dom_share = _dominant_cluster_share(top_labels)
        entropy = _label_entropy(top_labels)
    mean_cos = round(float(np.mean(scores[top])), 4) if k else 0.0
    return QueryEvalResult(
        query_id=query_id,
        query=query,
        top_k=k,
        mean_top_k_cosine=mean_cos,
        dominant_cluster_id=dom_id,
        dominant_cluster_share=dom_share,
        label_entropy=entropy,
        top_indices=[int(i) for i in top],
    )


def run_eval_suite(
    vectors: np.ndarray,
    *,
    config: EmbedConfig,
    labels: np.ndarray | None = None,
    queries: list[dict[str, Any]] | None = None,
    queries_path: Path | None = None,
) -> EvalSuiteResult:
    qrows = queries if queries is not None else load_eval_queries(queries_path)
    results: list[QueryEvalResult] = []
    for row in qrows:
        results.append(
            eval_query(
                vectors,
                config=config,
                query=str(row["query"]),
                top_k=int(row.get("top_k", 20)),
                labels=labels,
                query_id=str(row.get("id", "")),
            )
        )
    shares = [r.dominant_cluster_share for r in results if r.dominant_cluster_share is not None]
    cosines = [r.mean_top_k_cosine for r in results]
    mean_share = round(float(np.mean(shares)), 4) if shares else None
    mean_cos = round(float(np.mean(cosines)), 4) if cosines else 0.0
    return EvalSuiteResult(
        results=results,
        mean_dominant_cluster_share=mean_share,
        mean_top_k_cosine=mean_cos,
    )
