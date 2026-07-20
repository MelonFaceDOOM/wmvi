"""File I/O helpers and data-dir conventions for the claims CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims.types import RunPaths

VECTORS_FILE = "vectors.npy"
INDEX_FILE = "index.json"
METRICS_FILE = "metrics.json"


def package_root() -> Path:
    return Path(__file__).resolve().parent


def data_root() -> Path:
    return package_root() / "data"


def models_dir() -> Path:
    return data_root() / "models"


def labels_dir() -> Path:
    return data_root() / "labels"


def runs_dir() -> Path:
    return data_root() / "runs"


def inputs_dir() -> Path:
    return data_root() / "inputs"


def experiments_dir() -> Path:
    return data_root() / "experiments"


def run_paths(name_or_dir: str | Path) -> RunPaths:
    """Resolve a run name under data/runs/ or an absolute/relative run directory."""
    p = Path(name_or_dir)
    if not p.is_absolute() and len(p.parts) == 1:
        p = runs_dir() / p
    return RunPaths(run_dir=p)


def ensure_data_dirs() -> None:
    for d in (models_dir(), labels_dir(), runs_dir(), inputs_dir(), experiments_dir()):
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


def load_run_arrays(run_dir: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Load vectors.npy + index.json from a run directory."""
    paths = RunPaths(run_dir=run_dir)
    if not paths.vectors.is_file():
        raise FileNotFoundError(f"Missing vectors: {paths.vectors}")
    if not paths.index.is_file():
        raise FileNotFoundError(f"Missing index: {paths.index}")
    vectors = np.load(paths.vectors)
    index = normalize_index(read_json(paths.index))
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
