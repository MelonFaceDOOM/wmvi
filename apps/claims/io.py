"""Extended data-dir conventions for claims lifecycle artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims.types import RunPaths

VECTORS_FILE = "vectors.npy"
INDEX_FILE = "index.json"
METRICS_FILE = "metrics.json"
MANIFEST_FILE = "manifest.json"
SPEC_FILE = "spec.json"
LABELS_FILE = "labels.jsonl"
TRIPLETS_FILE = "triplets.jsonl"
ALIAS_FILE = "active.json"


def package_root() -> Path:
    return Path(__file__).resolve().parent


def data_root() -> Path:
    return package_root() / "data"


def models_dir() -> Path:
    return data_root() / "models"


def registered_models_dir() -> Path:
    return models_dir() / "registered"


def labeler_models_dir() -> Path:
    return models_dir() / "labelers"


def embedder_models_dir() -> Path:
    return models_dir() / "embedders"


def fixtures_dir() -> Path:
    """Shared fixtures: cluster eval queries, discovery logs (not training labels)."""
    return data_root() / "fixtures"


def training_dir() -> Path:
    return data_root() / "training"


def labeler_training_dir() -> Path:
    return training_dir() / "labelers"


def embedder_training_dir() -> Path:
    return training_dir() / "embedders"


def runs_dir() -> Path:
    return data_root() / "runs"


def corpora_dir() -> Path:
    return data_root() / "corpora"


def experiments_dir() -> Path:
    return data_root() / "experiments"


def clustering_experiments_dir() -> Path:
    return experiments_dir() / "clustering"


def model_eval_dir() -> Path:
    return experiments_dir() / "model_eval"


def labeler_eval_dir() -> Path:
    return model_eval_dir() / "labelers"


def embedder_eval_dir() -> Path:
    return model_eval_dir() / "embedders"


def run_paths(name_or_dir: str | Path) -> RunPaths:
    """Resolve a run under data/runs/ or an absolute/relative run directory.

    Accepts ``corpus/tag``, legacy ``corpus__tag``, or a filesystem path.
    """
    p = Path(name_or_dir)
    if p.is_absolute() or (len(p.parts) > 1 and p.exists()):
        return RunPaths(run_dir=p)
    raw = str(name_or_dir).strip()
    if "/" in raw and "__" not in Path(raw).name:
        # nested corpus/tag
        p = runs_dir() / raw
    elif "__" in raw and "/" not in raw:
        # legacy flat name corpus__tag → nested
        slug, _, tag = raw.partition("__")
        p = runs_dir() / slug / tag if slug and tag else runs_dir() / raw
    elif len(p.parts) == 1:
        p = runs_dir() / p
    else:
        p = runs_dir() / p
    return RunPaths(run_dir=p)


def ensure_data_dirs() -> None:
    for d in (
        models_dir(),
        registered_models_dir(),
        labeler_models_dir(),
        embedder_models_dir(),
        fixtures_dir(),
        training_dir(),
        labeler_training_dir(),
        embedder_training_dir(),
        runs_dir(),
        corpora_dir(),
        experiments_dir(),
        clustering_experiments_dir(),
        model_eval_dir(),
        labeler_eval_dir(),
        embedder_eval_dir(),
    ):
        d.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any, *, indent: int | None = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=indent), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]], *, append: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with path.open(mode, encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    write_jsonl(path, [row], append=True)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def normalize_index(index: dict[str, Any]) -> dict[str, Any]:
    """Ensure ``groups`` / ``claim_texts`` are present."""
    if index.get("groups"):
        if not index.get("claim_texts"):
            index["claim_texts"] = [g.get("claim_text", "") for g in index["groups"]]
        return index

    claim_texts = index.get("claim_texts") or []
    task_ids = index.get("task_ids") or []
    claim_indices = index.get("claim_indices") or []
    row_ids = index.get("row_ids") or []
    groups: list[dict[str, Any]] = []
    for i, text in enumerate(claim_texts):
        groups.append(
            {
                "claim_text": text,
                "count": 1,
                "sources": [
                    {
                        "task_id": task_ids[i] if i < len(task_ids) else "?",
                        "claim_index": int(claim_indices[i]) if i < len(claim_indices) else 0,
                        "row_id": row_ids[i] if i < len(row_ids) else f"?:{i}",
                    }
                ],
            }
        )
    index["groups"] = groups
    if not index.get("claim_texts"):
        index["claim_texts"] = claim_texts
    return index


def load_run_index(run_dir: Path) -> dict[str, Any]:
    """Load and normalize index.json from a run directory (no vectors)."""
    paths = RunPaths(run_dir=run_dir)
    if not paths.index.is_file():
        raise FileNotFoundError(f"Missing index: {paths.index}")
    return normalize_index(read_json(paths.index))


def load_run_arrays(run_dir: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Load vectors.npy + index.json from a run directory."""
    paths = RunPaths(run_dir=run_dir)
    if not paths.vectors.is_file():
        raise FileNotFoundError(f"Missing vectors: {paths.vectors}")
    index = load_run_index(run_dir)
    vectors = np.load(paths.vectors)
    return np.asarray(vectors, dtype=np.float32), index


def claim_texts_from_index(index: dict[str, Any]) -> list[str]:
    texts = index.get("claim_texts")
    if isinstance(texts, list) and texts:
        return [str(t) for t in texts]
    groups = index.get("groups") or []
    return [str(g.get("claim_text", "")) for g in groups]


def emit_json(payload: dict[str, Any]) -> None:
    """Print one JSON object to stdout (CLI convention)."""
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
