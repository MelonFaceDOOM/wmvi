"""Load Exp2 hierarchy JSON + label arrays for massage/pack."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from apps.claims.demo import BUNDLE_FILE, MEMBERSHIP_FILE, NAMES_FILE


DEFAULT_EXP_DIR = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "experiments"
    / "clustering"
    / "measles2"
    / "qwen3-emb-8b"
    / "exp2_kmeans_orphan_v2"
)
DEFAULT_CLAIMS = (
    Path(__file__).resolve().parents[1] / "data" / "corpora" / "measles2" / "claims.json"
)
DEFAULT_RUN = Path(__file__).resolve().parents[1] / "data" / "runs" / "measles2" / "qwen3-emb-8b"


@dataclass
class LeafInfo:
    leaf_id: int
    narrative_id: int
    size: int
    medoid: str
    samples: list[str]


@dataclass
class NarrativeInfo:
    narrative_id: int
    size: int
    n_leaves: int
    leaves: list[LeafInfo]


@dataclass
class Catalog:
    exp_dir: Path
    hierarchy_path: Path
    leaf_labels_path: Path
    narrative_labels_path: Path
    narratives: list[NarrativeInfo]
    leaves_by_id: dict[int, LeafInfo] = field(default_factory=dict)


def _pick(files: list[Path]) -> Path:
    if not files:
        raise FileNotFoundError("no matching files")
    return sorted(files)[-1]


def find_exp_files(exp_dir: Path) -> tuple[Path, Path, Path]:
    exp_dir = Path(exp_dir)
    hier = _pick(list(exp_dir.glob("hierarchy_*.json")))
    leaf = _pick(list(exp_dir.glob("leaf_labels_*.npy")))
    nar = _pick(list(exp_dir.glob("narrative_labels_*.npy")))
    return hier, leaf, nar


def load_catalog(exp_dir: Path | None = None) -> Catalog:
    exp_dir = Path(exp_dir or DEFAULT_EXP_DIR)
    hier_path, leaf_path, nar_path = find_exp_files(exp_dir)
    payload = json.loads(hier_path.read_text(encoding="utf-8"))
    narratives: list[NarrativeInfo] = []
    leaves_by_id: dict[int, LeafInfo] = {}
    for row in payload.get("narratives") or []:
        leaves: list[LeafInfo] = []
        nid = int(row["narrative_id"])
        for lf in row.get("leaves") or []:
            info = LeafInfo(
                leaf_id=int(lf["leaf_id"]),
                narrative_id=nid,
                size=int(lf.get("size") or 0),
                medoid=str(lf.get("medoid_claim_text") or ""),
                samples=[str(s) for s in (lf.get("sample_claim_texts") or []) if str(s).strip()],
            )
            leaves.append(info)
            leaves_by_id[info.leaf_id] = info
        narratives.append(
            NarrativeInfo(
                narrative_id=nid,
                size=int(row.get("size") or 0),
                n_leaves=int(row.get("n_leaves") or len(leaves)),
                leaves=leaves,
            )
        )
    return Catalog(
        exp_dir=exp_dir,
        hierarchy_path=hier_path,
        leaf_labels_path=leaf_path,
        narrative_labels_path=nar_path,
        narratives=narratives,
        leaves_by_id=leaves_by_id,
    )


def names_path(exp_dir: Path) -> Path:
    return Path(exp_dir) / NAMES_FILE


def membership_path(exp_dir: Path) -> Path:
    return Path(exp_dir) / MEMBERSHIP_FILE


def bundle_path(exp_dir: Path) -> Path:
    return Path(exp_dir) / BUNDLE_FILE


def resolve_bundle_path(given: Path | str | None, *, exp_dir: Path | None = None) -> Path:
    """Resolve ``--bundle``: cwd file, exp-dir file, or default packed sqlite.

    A bare name like ``measles2_demo.sqlite`` is looked up in *exp_dir*
    (default Exp2 v2) when it is not a file in the current directory.
    """
    default_dir = Path(exp_dir or DEFAULT_EXP_DIR)
    default = bundle_path(default_dir)
    if given is None:
        return default
    path = Path(given).expanduser()
    if path.is_file():
        return path
    if path.is_dir():
        nested = path / BUNDLE_FILE
        return nested if nested.is_file() else nested
    fallback = default_dir / path.name
    if fallback.is_file():
        return fallback
    return path


def load_names(exp_dir: Path) -> dict[str, Any]:
    path = names_path(exp_dir)
    if not path.is_file():
        return {"narratives": [], "leaves": []}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {"narratives": [], "leaves": []}
    raw.setdefault("narratives", [])
    raw.setdefault("leaves", [])
    return raw


def save_names(exp_dir: Path, payload: dict[str, Any]) -> Path:
    path = names_path(exp_dir)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_membership(exp_dir: Path) -> dict[int, int]:
    path = membership_path(exp_dir)
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: dict[int, int] = {}
    for k, v in raw.items():
        if str(k).startswith("_"):
            continue
        out[int(k)] = int(v)
    return out


def save_membership(exp_dir: Path, mapping: dict[int, int]) -> Path:
    path = membership_path(exp_dir)
    payload = {str(int(k)): int(v) for k, v in sorted(mapping.items())}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_label_arrays(catalog: Catalog) -> tuple[np.ndarray, np.ndarray]:
    leaf = np.asarray(np.load(catalog.leaf_labels_path), dtype=int)
    nar = np.asarray(np.load(catalog.narrative_labels_path), dtype=int)
    if leaf.shape != nar.shape:
        raise ValueError(f"label length mismatch {leaf.shape} vs {nar.shape}")
    return leaf, nar
