"""Shared dataclasses for the claims pipeline (in-memory only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(frozen=True)
class ClaimSource:
    task_id: str
    claim_index: int
    row_id: str


@dataclass
class ClaimGroup:
    group_id: int
    claim_text: str
    sources: list[ClaimSource] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.sources)


@dataclass
class ClaimsBundle:
    groups: list[ClaimGroup]
    posts_by_task_id: dict[str, dict[str, Any]]
    source_hash: str
    source_path: str
    source_claim_count: int

    @property
    def claim_count(self) -> int:
        return len(self.groups)

    @property
    def post_count(self) -> int:
        return len(self.posts_by_task_id)


@dataclass
class EmbedConfig:
    """Minimal embed settings (replaces EmbedProfile for file-mode)."""

    model_id: str
    doc_instruction: str = ""
    query_instruction: str = ""
    normalize: bool = True


@dataclass
class ClusterResult:
    labels: np.ndarray  # shape (n,), int; -1 means noise
    n_clusters: int
    n_noise: int
    prep_meta: dict[str, Any] | None = None


@dataclass
class TripletAnchor:
    """Training/eval triplet anchor (file-mode; id may be synthetic)."""

    id: int
    text: str
    pool: str = "eval"
    category: str = ""
    family: str = ""
    too_hard: bool = False
    positives: list[str] = field(default_factory=list)
    negatives: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RunPaths:
    """Paths inside a run directory under data/runs/<name>/."""

    run_dir: Path

    @property
    def vectors(self) -> Path:
        return self.run_dir / "vectors.npy"

    @property
    def index(self) -> Path:
        return self.run_dir / "index.json"

    @property
    def metrics(self) -> Path:
        return self.run_dir / "metrics.json"
