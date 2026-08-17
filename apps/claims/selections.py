"""Keyed claim selections (views over a corpus, not copies).

Layout::

    data/corpora/<corpus>/selections/<name>.json
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims.keys import claim_key


@dataclass
class Selection:
    name: str
    scope: str  # "group" | "claim"
    keys: list[str]
    from_annotation: str | None = None
    predicate: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Selection:
        return cls(
            name=str(data["name"]),
            scope=str(data.get("scope") or "group"),
            keys=[str(k) for k in (data.get("keys") or [])],
            from_annotation=data.get("from_annotation"),
            predicate=dict(data.get("predicate") or {}),
            created_at=str(data.get("created_at") or ""),
        )

    @property
    def key_set(self) -> set[str]:
        return set(self.keys)


def selections_dir(corpus_root: Path) -> Path:
    return Path(corpus_root) / "selections"


def selection_path(corpus_root: Path, name: str) -> Path:
    safe = _safe_name(name)
    return selections_dir(corpus_root) / f"{safe}.json"


def _safe_name(name: str) -> str:
    n = (name or "").strip()
    if not n:
        raise ValueError("selection name must be non-empty")
    if "/" in n or "\\" in n or ".." in n or n.startswith("."):
        raise ValueError(f"Invalid selection name {name!r}")
    return n


def write_selection(
    corpus_root: Path,
    selection: Selection,
    *,
    force: bool = False,
) -> Path:
    path = selection_path(corpus_root, selection.name)
    if path.exists() and not force:
        raise FileExistsError(f"Selection {selection.name!r} already exists at {path}; pass force=True")
    if not selection.created_at:
        selection.created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    claims_io.write_json(path, selection.to_dict())
    return path


def read_selection(corpus_root: Path, name: str) -> Selection:
    path = selection_path(corpus_root, name)
    if not path.is_file():
        raise FileNotFoundError(f"Missing selection: {path}")
    return Selection.from_dict(claims_io.read_json(path))


def list_selections(corpus_root: Path) -> list[Selection]:
    root = selections_dir(corpus_root)
    if not root.is_dir():
        return []
    out: list[Selection] = []
    for path in sorted(root.glob("*.json")):
        try:
            out.append(Selection.from_dict(claims_io.read_json(path)))
        except Exception:  # noqa: BLE001
            continue
    return out


def remove_selection(corpus_root: Path, name: str) -> None:
    path = selection_path(corpus_root, name)
    if not path.exists():
        raise FileNotFoundError(f"Selection {name!r} not found at {path}")
    path.unlink()


def from_predicate(
    annotation: ann_mod.Annotation,
    *,
    name: str,
    predicate_fn: Callable[[Any], bool],
    predicate_meta: dict[str, Any] | None = None,
) -> Selection:
    """Build a selection from an annotation by keeping keys where predicate(v) is true."""
    keys = [k for k, v in annotation.values.items() if predicate_fn(v)]
    return Selection(
        name=_safe_name(name),
        scope=annotation.meta.scope,
        keys=keys,
        from_annotation=annotation.name,
        predicate=dict(predicate_meta or {}),
    )


def from_threshold(
    annotation: ann_mod.Annotation,
    *,
    name: str,
    low: float | None = None,
    high: float | None = None,
    inclusive: bool = True,
) -> Selection:
    """Keep keys whose numeric value is in ``[low, high]`` (None = open end)."""

    def _pred(v: Any) -> bool:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return False
        if low is not None:
            if inclusive and x < low:
                return False
            if not inclusive and x <= low:
                return False
        if high is not None:
            if inclusive and x > high:
                return False
            if not inclusive and x >= high:
                return False
        return True

    return from_predicate(
        annotation,
        name=name,
        predicate_fn=_pred,
        predicate_meta={"op": "threshold", "low": low, "high": high, "inclusive": inclusive},
    )


def claim_keys_from_index(index: dict[str, Any]) -> list[str]:
    """Parallel claim_key list for a run index (same order as vectors rows)."""
    keys = index.get("claim_keys")
    if isinstance(keys, list) and keys:
        return [str(k) for k in keys]
    groups = index.get("groups") or []
    if groups:
        out: list[str] = []
        for g in groups:
            ck = g.get("claim_key")
            if ck:
                out.append(str(ck))
            else:
                out.append(claim_key(str(g.get("claim_text") or "")))
        return out
    texts = index.get("claim_texts") or []
    return [claim_key(str(t)) for t in texts]


def row_indices_for_selection(index: dict[str, Any], selection: Selection) -> list[int]:
    """Map a group-scope selection to row indices in a run's vectors.npy."""
    if selection.scope != "group":
        raise ValueError(
            f"row_indices_for_selection requires scope='group'; got {selection.scope!r}"
        )
    keys = claim_keys_from_index(index)
    wanted = selection.key_set
    return [i for i, k in enumerate(keys) if k in wanted]


def subset_vectors(
    vectors: Any,
    index: dict[str, Any],
    selection: Selection,
) -> tuple[Any, dict[str, Any], list[int]]:
    """Return (vectors[rows], subset_index, row_indices) without re-embedding.

    ``subset_index`` mirrors the parent index but only for selected rows.
    """
    import numpy as np

    rows = row_indices_for_selection(index, selection)
    arr = np.asarray(vectors)
    sub = arr[rows] if rows else arr[:0]
    texts = claims_io.claim_texts_from_index(index)
    keys = claim_keys_from_index(index)
    groups = index.get("groups") or []
    sub_index: dict[str, Any] = {
        "model_id": index.get("model_id"),
        "query_instruction": index.get("query_instruction"),
        "doc_instruction": index.get("doc_instruction"),
        "normalize": index.get("normalize", True),
        "source_hash": index.get("source_hash"),
        "selection": selection.name,
        "parent_row_indices": rows,
        "claim_texts": [texts[i] for i in rows] if texts else [],
        "claim_keys": [keys[i] for i in rows] if keys else [],
        "groups": [groups[i] for i in rows] if groups and len(groups) == len(keys) else [],
    }
    return sub, sub_index, rows
