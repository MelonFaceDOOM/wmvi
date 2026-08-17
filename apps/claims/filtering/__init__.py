"""Ephemeral annotation filter predicates with resolved-selection provenance."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from apps.claims import annotations as ann_mod
from apps.claims import provenance as prov
from apps.claims import selections as sel_mod
from apps.claims.keys import claim_key


@dataclass
class FilterPredicate:
    """Structured predicate over annotation values.

    Supported ops:
      - eq: value equals ``value``
      - range: inclusive ``[low, high]`` (None = open end)
    """

    op: str
    value: Any = None
    low: float | None = None
    high: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FilterPredicate:
        return cls(
            op=str(data.get("op") or "eq"),
            value=data.get("value"),
            low=(float(data["low"]) if data.get("low") is not None else None),
            high=(float(data["high"]) if data.get("high") is not None else None),
        )

    @classmethod
    def eq(cls, value: Any) -> FilterPredicate:
        return cls(op="eq", value=value)

    @classmethod
    def range(cls, low: float | None = None, high: float | None = None) -> FilterPredicate:
        return cls(op="range", low=low, high=high)


@dataclass
class ResolvedFilter:
    annotation_name: str
    annotation_hash: str
    predicate: FilterPredicate
    scope: str
    keys: list[str]
    selected_keys_hash: str
    count: int
    source_hash: str | None = None
    groups_hash: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["predicate"] = self.predicate.to_dict()
        return d

    def provenance(self) -> dict[str, Any]:
        """Compact provenance for consumer manifests (embed/cluster/etc.)."""
        return {
            "annotation": self.annotation_name,
            "annotation_hash": self.annotation_hash,
            "predicate": self.predicate.to_dict(),
            "scope": self.scope,
            "selected_count": self.count,
            "selected_keys_hash": self.selected_keys_hash,
            "source_hash": self.source_hash,
            "groups_hash": self.groups_hash,
        }


def matches(pred: FilterPredicate, v: Any) -> bool:
    if pred.op == "eq":
        if isinstance(pred.value, (int, float)) and not isinstance(pred.value, bool):
            try:
                return float(v) == float(pred.value)
            except (TypeError, ValueError):
                return False
        return v == pred.value
    if pred.op == "range":
        try:
            x = float(v)
        except (TypeError, ValueError):
            return False
        if pred.low is not None and x < float(pred.low):
            return False
        if pred.high is not None and x > float(pred.high):
            return False
        return True
    raise ValueError(f"Unsupported filter op: {pred.op!r}")


def _matches(pred: FilterPredicate, v: Any) -> bool:
    """Backward-compatible alias."""
    return matches(pred, v)


def annotation_content_hash(annotation: ann_mod.Annotation) -> str:
    items = sorted((str(k), annotation.values[k]) for k in annotation.values)
    return prov.sha256_json(items)


def resolve_filter(
    corpus_root: Path,
    annotation_name: str,
    predicate: FilterPredicate,
    *,
    groups_hash: str | None = None,
) -> ResolvedFilter:
    ann = ann_mod.read_annotation(corpus_root, annotation_name)
    keys = sorted(k for k, v in ann.values.items() if _matches(predicate, v))
    return ResolvedFilter(
        annotation_name=ann.name,
        annotation_hash=annotation_content_hash(ann),
        predicate=predicate,
        scope=ann.meta.scope,
        keys=keys,
        selected_keys_hash=prov.selected_keys_hash(keys),
        count=len(keys),
        source_hash=ann.meta.source_hash,
        groups_hash=groups_hash,
    )


@dataclass
class ResolvedFilterSet:
    """One or more annotation filters AND-ed together (key intersection)."""

    clauses: list[ResolvedFilter]
    keys: list[str]
    selected_keys_hash: str
    count: int
    groups_hash: str | None = None

    @property
    def scope(self) -> str:
        scopes = {c.scope for c in self.clauses}
        if len(scopes) != 1:
            raise ValueError(f"Filter clauses must share one scope, got {sorted(scopes)}")
        return next(iter(scopes))

    @property
    def annotation_name(self) -> str:
        return "+".join(c.annotation_name for c in self.clauses)

    def provenance(self) -> dict[str, Any]:
        if len(self.clauses) == 1:
            return self.clauses[0].provenance()
        return {
            "op": "and",
            "clauses": [c.provenance() for c in self.clauses],
            "scope": self.scope,
            "selected_count": self.count,
            "selected_keys_hash": self.selected_keys_hash,
            "groups_hash": self.groups_hash,
        }


def parse_predicate_args(
    *,
    eq: Any = None,
    low: float | None = None,
    high: float | None = None,
) -> FilterPredicate:
    if eq is not None and (low is not None or high is not None):
        raise ValueError("Use either --eq or --low/--high, not both")
    if eq is not None:
        # Try numeric
        try:
            if isinstance(eq, str) and eq.strip() and eq.replace(".", "", 1).isdigit():
                return FilterPredicate.eq(float(eq))
            if isinstance(eq, (int, float)):
                return FilterPredicate.eq(float(eq))
        except (TypeError, ValueError):
            pass
        return FilterPredicate.eq(eq)
    if low is not None or high is not None:
        return FilterPredicate.range(low=low, high=high)
    raise ValueError("Provide --eq VALUE or --low/--high for annotation filter")


def parse_filter_spec(spec: str) -> tuple[str, FilterPredicate]:
    """Parse ``name:eq=1`` / ``name:low=0.5`` / ``name:low=0.33,high=0.66``."""
    raw = str(spec or "").strip()
    if not raw or ":" not in raw:
        raise ValueError(
            f"Invalid --filter {spec!r}; expected 'name:eq=V' or 'name:low=A,high=B'"
        )
    name, rest = raw.split(":", 1)
    name = name.strip()
    if not name or not rest.strip():
        raise ValueError(
            f"Invalid --filter {spec!r}; expected 'name:eq=V' or 'name:low=A,high=B'"
        )
    parts: dict[str, str] = {}
    for piece in rest.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "=" not in piece:
            raise ValueError(
                f"Invalid --filter piece {piece!r} in {spec!r}; use key=value"
            )
        k, v = piece.split("=", 1)
        parts[k.strip()] = v.strip()
    if "eq" in parts and ("low" in parts or "high" in parts):
        raise ValueError(f"Invalid --filter {spec!r}: use eq= or low=/high=, not both")
    if "eq" in parts:
        return name, parse_predicate_args(eq=parts["eq"])
    low = float(parts["low"]) if "low" in parts else None
    high = float(parts["high"]) if "high" in parts else None
    if low is None and high is None:
        raise ValueError(f"Invalid --filter {spec!r}: need eq= or low=/high=")
    return name, parse_predicate_args(low=low, high=high)


def clauses_from_args(args: Any) -> list[tuple[str, FilterPredicate]]:
    """Collect filter clauses from ``--filter`` and/or ``--where-annotation``.

    Repeatable ``--filter`` specs are AND-ed. Legacy ``--where-annotation`` +
    ``--eq``/``--low``/``--high`` adds one more clause (also AND-ed).
    """
    clauses: list[tuple[str, FilterPredicate]] = []
    for spec in getattr(args, "filter", None) or []:
        clauses.append(parse_filter_spec(str(spec)))
    where = getattr(args, "where_annotation", None)
    if where:
        clauses.append(
            (
                str(where),
                parse_predicate_args(
                    eq=getattr(args, "eq", None),
                    low=getattr(args, "low", None),
                    high=getattr(args, "high", None),
                ),
            )
        )
    return clauses


def resolve_filter_clauses(
    corpus_root: Path,
    clauses: list[tuple[str, FilterPredicate]],
    *,
    groups_hash: str | None = None,
) -> ResolvedFilterSet:
    if not clauses:
        raise ValueError("resolve_filter_clauses requires at least one clause")
    resolved_clauses = [
        resolve_filter(corpus_root, name, pred, groups_hash=groups_hash)
        for name, pred in clauses
    ]
    key_sets = [set(c.keys) for c in resolved_clauses]
    keys = sorted(set.intersection(*key_sets)) if key_sets else []
    return ResolvedFilterSet(
        clauses=resolved_clauses,
        keys=keys,
        selected_keys_hash=prov.selected_keys_hash(keys),
        count=len(keys),
        groups_hash=groups_hash,
    )


def resolve_args_filter(
    args: Any,
    corpus_root: Path,
    *,
    groups_hash: str | None = None,
) -> ResolvedFilterSet | None:
    """Resolve CLI filter args; ``None`` when no filter flags are set."""
    clauses = clauses_from_args(args)
    if not clauses:
        return None
    return resolve_filter_clauses(corpus_root, clauses, groups_hash=groups_hash)


def resolve_keys_for_args(
    args: Any,
    corpus_root: Path,
    *,
    groups_hash: str | None = None,
) -> tuple[set[str] | None, dict[str, Any] | None]:
    """Resolve ``--filter``/``--where-annotation`` AND ``--selection`` to a key set.

    Returns ``(None, None)`` when no filter flags are set. When both annotation
    filters and a named selection are present, keys are AND-ed. Honors
    ``--save-selection`` / ``--force-selection`` side effects from annotation
    filters (same behavior as browse/cluster).
    """
    has_filter = bool(clauses_from_args(args))
    sel_name = getattr(args, "selection", None)
    if not has_filter and not sel_name:
        return None, None

    wanted: set[str] | None = None
    filter_meta: dict[str, Any] | None = None

    resolved = resolve_args_filter(args, corpus_root, groups_hash=groups_hash)
    if resolved is not None:
        wanted = set(resolved.keys)
        filter_meta = resolved.provenance()
        save_as = getattr(args, "save_selection", None)
        if save_as:
            maybe_save_selection(
                corpus_root,
                resolved,
                name=str(save_as),
                force=bool(getattr(args, "force_selection", False)),
            )

    if sel_name:
        selection = sel_mod.read_selection(corpus_root, str(sel_name))
        sel_keys = set(selection.keys)
        wanted = sel_keys if wanted is None else (wanted & sel_keys)
        filter_meta = {
            **(filter_meta or {}),
            "selection": selection.name,
            "selection_count": len(selection.keys),
        }

    assert wanted is not None
    return wanted, filter_meta


def sampling_descriptor(
    filter_meta: dict[str, Any] | None,
    *,
    base: str = "random",
) -> str:
    """Compact sampling tag for gold rows (e.g. ``random|filter=ann:eq=1``)."""
    if not filter_meta:
        return base
    if filter_meta.get("selection"):
        return f"{base}|selection={filter_meta['selection']}"
    ann = filter_meta.get("annotation")
    pred = filter_meta.get("predicate") or {}
    if ann and pred.get("op") == "eq":
        return f"{base}|filter={ann}:eq={pred.get('value')}"
    if ann and pred.get("op") == "range":
        low = pred.get("low")
        high = pred.get("high")
        bits = []
        if low is not None:
            bits.append(f"low={low}")
        if high is not None:
            bits.append(f"high={high}")
        return f"{base}|filter={ann}:{','.join(bits)}" if bits else f"{base}|filter={ann}"
    if filter_meta.get("op") == "and" and filter_meta.get("clauses"):
        parts = []
        for c in filter_meta["clauses"]:
            a = c.get("annotation")
            p = c.get("predicate") or {}
            if a and p.get("op") == "eq":
                parts.append(f"{a}:eq={p.get('value')}")
            elif a:
                parts.append(str(a))
        if parts:
            return f"{base}|filter={'+'.join(parts)}"
    return f"{base}|filter"


def subset_index_by_keys(
    index: dict[str, Any],
    keys: list[str] | set[str],
    *,
    filter_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Subset a groups/run index dict by claim_key (no vectors)."""
    from apps.claims import io as claims_io

    rows = row_indices_for_keys(index, keys)
    texts = claims_io.claim_texts_from_index(index)
    all_keys = sel_mod.claim_keys_from_index(index)
    groups = index.get("groups") or []
    sub: dict[str, Any] = {
        **{k: v for k, v in index.items() if k not in ("claim_texts", "claim_keys", "groups")},
        "claim_texts": [texts[i] for i in rows],
        "claim_keys": [all_keys[i] for i in rows],
        "groups": [groups[i] for i in rows] if groups and len(groups) == len(all_keys) else [],
        "parent_row_indices": rows,
    }
    if filter_meta:
        sub["filter"] = filter_meta
    return sub


def row_indices_for_keys(index: dict[str, Any], keys: list[str] | set[str]) -> list[int]:
    wanted = set(keys)
    all_keys = sel_mod.claim_keys_from_index(index)
    return [i for i, k in enumerate(all_keys) if k in wanted]


def subset_vectors_by_keys(
    vectors: Any,
    index: dict[str, Any],
    keys: list[str],
    *,
    filter_meta: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], list[int]]:
    import numpy as np

    from apps.claims import io as claims_io

    rows = row_indices_for_keys(index, keys)
    arr = np.asarray(vectors)
    sub = arr[rows] if rows else arr[:0]
    texts = claims_io.claim_texts_from_index(index)
    all_keys = sel_mod.claim_keys_from_index(index)
    groups = index.get("groups") or []
    sub_index: dict[str, Any] = {
        "model_id": index.get("model_id"),
        "query_instruction": index.get("query_instruction"),
        "doc_instruction": index.get("doc_instruction"),
        "normalize": index.get("normalize", True),
        "source_hash": index.get("source_hash"),
        "parent_row_indices": rows,
        "claim_texts": [texts[i] for i in rows] if texts else [],
        "claim_keys": [all_keys[i] for i in rows] if all_keys else [],
        "groups": [groups[i] for i in rows] if groups and len(groups) == len(all_keys) else [],
    }
    if filter_meta:
        sub_index["filter"] = filter_meta
    return sub, sub_index, rows


def filter_groups(
    groups: list[dict[str, Any]],
    resolved: ResolvedFilter | ResolvedFilterSet,
) -> list[dict[str, Any]]:
    """Filter group dicts by resolved claim_keys (group scope)."""
    if resolved.scope != "group":
        raise ValueError(f"filter_groups requires scope=group, got {resolved.scope}")
    wanted = set(resolved.keys)
    out: list[dict[str, Any]] = []
    for g in groups:
        ck = str(g.get("claim_key") or claim_key(str(g.get("claim_text") or "")))
        if ck in wanted:
            out.append(g)
    return out


def maybe_save_selection(
    corpus_root: Path,
    resolved: ResolvedFilter | ResolvedFilterSet,
    *,
    name: str,
    force: bool = False,
) -> Path:
    """Optional compatibility: persist a named selection from a resolved filter."""
    if isinstance(resolved, ResolvedFilterSet):
        if len(resolved.clauses) == 1:
            from_ann = resolved.clauses[0].annotation_name
            predicate = resolved.clauses[0].predicate.to_dict()
        else:
            from_ann = resolved.annotation_name
            predicate = {
                "op": "and",
                "clauses": [
                    {"annotation": c.annotation_name, **c.predicate.to_dict()}
                    for c in resolved.clauses
                ],
            }
        scope = resolved.scope
    else:
        from_ann = resolved.annotation_name
        predicate = resolved.predicate.to_dict()
        scope = resolved.scope
    sel = sel_mod.Selection(
        name=name,
        scope=scope,
        keys=list(resolved.keys),
        from_annotation=from_ann,
        predicate=predicate,
        created_at=prov.utc_now(),
    )
    return sel_mod.write_selection(corpus_root, sel, force=force)
