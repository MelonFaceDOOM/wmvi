"""Similarity intents, keyed triplet rows, and frozen datasets (file-mode)."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims import provenance as prov
from apps.claims.keys import claim_key

DEFAULT_MIN_GOLD_TOTAL = 20
DEFAULT_PROBE_TARGET = 25
DEFAULT_NEIGHBOR_K = 15
DEFAULT_AGENT_BATCH_SIZE = 20


@dataclass
class SimilaritySpec:
    name: str
    version: int
    instructions: str = ""
    similarity_rubric: str = ""
    eval_frac: float = 0.15  # legacy; ignored for new freeze (gold-held-out)
    split_seed: int = 0  # legacy; ignored for new rows (always train)
    min_gold_total: int = DEFAULT_MIN_GOLD_TOTAL
    probe_target: int = DEFAULT_PROBE_TARGET
    neighbor_k: int = DEFAULT_NEIGHBOR_K
    agent_batch_size: int | None = DEFAULT_AGENT_BATCH_SIZE
    agent_model: str | None = None
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SimilaritySpec:
        batch_raw = data.get("agent_batch_size")
        model_raw = data.get("agent_model")
        return cls(
            name=str(data["name"]),
            version=int(data.get("version") or 1),
            instructions=str(data.get("instructions") or ""),
            similarity_rubric=str(
                data.get("similarity_rubric") or data.get("rubric") or ""
            ),
            eval_frac=float(
                data.get("eval_frac") if data.get("eval_frac") is not None else 0.15
            ),
            split_seed=int(data.get("split_seed") or 0),
            min_gold_total=int(
                data["min_gold_total"]
                if data.get("min_gold_total") is not None
                else DEFAULT_MIN_GOLD_TOTAL
            ),
            probe_target=int(
                data["probe_target"]
                if data.get("probe_target") is not None
                else DEFAULT_PROBE_TARGET
            ),
            neighbor_k=int(
                data["neighbor_k"]
                if data.get("neighbor_k") is not None
                else DEFAULT_NEIGHBOR_K
            ),
            agent_batch_size=(
                int(batch_raw)
                if batch_raw is not None
                else DEFAULT_AGENT_BATCH_SIZE
            ),
            agent_model=(str(model_raw).strip() or None) if model_raw is not None else None,
            created_at=str(data.get("created_at") or ""),
        )


@dataclass
class TripletRow:
    row_id: str
    intent: str
    spec_version: int
    split: str
    anchor_key: str
    anchor_text: str
    positive_keys: list[str]
    positive_texts: list[str]
    negative_keys: list[str]
    negative_texts: list[str]
    producer: dict[str, Any]
    labeled_at: str
    reason: str | None = None
    corpus: str | None = None
    supersedes: str | None = None
    shown_keys: list[str] = field(default_factory=list)
    run_tag: str | None = None
    probe_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TripletRow:
        return cls(
            row_id=str(data["row_id"]),
            intent=str(data["intent"]),
            spec_version=int(data.get("spec_version") or 1),
            split=str(data.get("split") or "train"),
            anchor_key=str(data["anchor_key"]),
            anchor_text=str(data.get("anchor_text") or ""),
            positive_keys=[str(x) for x in (data.get("positive_keys") or [])],
            positive_texts=[str(x) for x in (data.get("positive_texts") or [])],
            negative_keys=[str(x) for x in (data.get("negative_keys") or [])],
            negative_texts=[str(x) for x in (data.get("negative_texts") or [])],
            producer=dict(data.get("producer") or {}),
            labeled_at=str(data.get("labeled_at") or ""),
            reason=(str(data["reason"]) if data.get("reason") is not None else None),
            corpus=(str(data["corpus"]) if data.get("corpus") is not None else None),
            supersedes=(
                str(data["supersedes"]) if data.get("supersedes") is not None else None
            ),
            shown_keys=[str(x) for x in (data.get("shown_keys") or [])],
            run_tag=(str(data["run_tag"]) if data.get("run_tag") is not None else None),
            probe_run_id=(
                str(data["probe_run_id"]) if data.get("probe_run_id") is not None else None
            ),
        )


def intent_dir(name: str) -> Path:
    return claims_io.embedder_training_dir() / prov.safe_slug(name)


def spec_path(name: str) -> Path:
    return intent_dir(name) / claims_io.SPEC_FILE


def triplets_path(name: str) -> Path:
    return intent_dir(name) / claims_io.TRIPLETS_FILE


def datasets_dir(name: str) -> Path:
    return intent_dir(name) / "datasets"


def dataset_manifest_path(name: str, version: str) -> Path:
    return datasets_dir(name) / prov.safe_slug(version) / claims_io.MANIFEST_FILE


def create_intent(
    name: str,
    *,
    instructions: str = "",
    similarity_rubric: str = "",
    eval_frac: float = 0.15,
    split_seed: int = 0,
    min_gold_total: int = DEFAULT_MIN_GOLD_TOTAL,
    probe_target: int = DEFAULT_PROBE_TARGET,
    neighbor_k: int = DEFAULT_NEIGHBOR_K,
    agent_batch_size: int | None = DEFAULT_AGENT_BATCH_SIZE,
    agent_model: str | None = None,
    version: int = 1,
    force: bool = False,
) -> SimilaritySpec:
    slug = prov.safe_slug(name)
    path = spec_path(slug)
    if path.exists() and not force:
        raise FileExistsError(f"Similarity intent already exists: {path}")
    spec = SimilaritySpec(
        name=slug,
        version=int(version),
        instructions=instructions.strip(),
        similarity_rubric=similarity_rubric.strip(),
        eval_frac=float(eval_frac),
        split_seed=int(split_seed),
        min_gold_total=int(min_gold_total),
        probe_target=int(probe_target),
        neighbor_k=int(neighbor_k),
        agent_batch_size=(
            int(agent_batch_size) if agent_batch_size is not None else None
        ),
        agent_model=(str(agent_model).strip() or None) if agent_model else None,
        created_at=prov.utc_now(),
    )
    claims_io.ensure_data_dirs()
    intent_dir(slug).mkdir(parents=True, exist_ok=True)
    claims_io.write_json(path, spec.to_dict())
    if not triplets_path(slug).exists():
        triplets_path(slug).write_text("", encoding="utf-8")
    return spec


def load_spec(name: str) -> SimilaritySpec:
    path = spec_path(name)
    if not path.is_file():
        raise FileNotFoundError(f"Missing similarity intent spec: {path}")
    return SimilaritySpec.from_dict(claims_io.read_json(path))


def list_intents() -> list[dict[str, Any]]:
    root = claims_io.embedder_training_dir()
    if not root.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir() or p.name.startswith("."):
            continue
        try:
            spec = load_spec(p.name)
        except Exception:  # noqa: BLE001
            continue
        n = (
            sum(1 for _ in triplets_path(p.name).open(encoding="utf-8"))
            if triplets_path(p.name).is_file()
            else 0
        )
        out.append({**spec.to_dict(), "n_triplets": n, "path": str(p)})
    return out


def labeled_anchor_keys(name: str) -> set[str]:
    """Distinct anchor keys among resolved training triplets."""
    return {r.anchor_key for r in resolved_triplets(name).values()}


def make_triplet_row(
    *,
    spec: SimilaritySpec,
    anchor_text: str,
    positive_texts: list[str] | None = None,
    negative_texts: list[str] | None = None,
    producer: dict[str, Any],
    reason: str | None = None,
    corpus: str | None = None,
    anchor_key_override: str | None = None,
    positive_keys_override: list[str] | None = None,
    negative_keys_override: list[str] | None = None,
    supersedes: str | None = None,
    labeled_at: str | None = None,
    shown_keys: list[str] | None = None,
    run_tag: str | None = None,
    probe_run_id: str | None = None,
) -> TripletRow:
    """Build a triplet row. Empty pos and/or empty neg are allowed."""
    pos_texts = [str(t) for t in (positive_texts or []) if str(t).strip()]
    neg_texts = [str(t) for t in (negative_texts or []) if str(t).strip()]
    ak = anchor_key_override or claim_key(anchor_text)
    pkeys = list(positive_keys_override) if positive_keys_override is not None else [
        claim_key(t) for t in pos_texts
    ]
    nkeys = list(negative_keys_override) if negative_keys_override is not None else [
        claim_key(t) for t in neg_texts
    ]
    # If only keys provided without texts, pad empty texts
    if positive_keys_override is not None and not pos_texts:
        pos_texts = [""] * len(pkeys)
    if negative_keys_override is not None and not neg_texts:
        neg_texts = [""] * len(nkeys)
    if len(pkeys) != len(pos_texts):
        raise ValueError("key/text length mismatch for positives")
    if len(nkeys) != len(neg_texts):
        raise ValueError("key/text length mismatch for negatives")

    if ak in pkeys or ak in nkeys:
        raise ValueError("positive/negative keys must not include the anchor")
    overlap = set(pkeys) & set(nkeys)
    if overlap:
        raise ValueError(f"positive and negative keys must be disjoint: {sorted(overlap)[:5]}")

    ts = labeled_at or prov.utc_now()
    # Gold-held-out scheme: all training log rows are train; eval is gold.
    split = "train"
    producer = dict(producer or {})
    if "type" not in producer:
        producer["type"] = "unknown"
    rid = prov.triplet_row_id(
        intent=spec.name,
        anchor_key=ak,
        positive_keys=pkeys,
        negative_keys=nkeys,
        labeled_at=ts,
    )
    return TripletRow(
        row_id=rid,
        intent=spec.name,
        spec_version=spec.version,
        split=split,
        anchor_key=ak,
        anchor_text=anchor_text,
        positive_keys=list(pkeys),
        positive_texts=list(pos_texts),
        negative_keys=list(nkeys),
        negative_texts=list(neg_texts),
        producer=producer,
        labeled_at=ts,
        reason=reason,
        corpus=corpus,
        supersedes=supersedes,
        shown_keys=list(shown_keys or []),
        run_tag=run_tag,
        probe_run_id=probe_run_id,
    )


def append_triplet(row: TripletRow) -> Path:
    path = triplets_path(row.intent)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        for existing in claims_io.read_jsonl(path):
            if str(existing.get("row_id")) == row.row_id:
                raise FileExistsError(f"Triplet row already exists: {row.row_id}")
            # Dedupe: same anchor + same pos/neg key sets
            if (
                str(existing.get("anchor_key")) == row.anchor_key
                and sorted(existing.get("positive_keys") or []) == sorted(row.positive_keys)
                and sorted(existing.get("negative_keys") or []) == sorted(row.negative_keys)
                and not existing.get("supersedes")
            ):
                raise FileExistsError(
                    f"Duplicate triplet for anchor {row.anchor_key} "
                    f"(existing row_id={existing.get('row_id')})"
                )
    claims_io.append_jsonl(path, row.to_dict())
    return path


def load_triplets(name: str) -> list[TripletRow]:
    return [TripletRow.from_dict(r) for r in claims_io.read_jsonl(triplets_path(name))]


def resolved_triplets(name: str) -> dict[str, TripletRow]:
    """Latest non-superseded triplet per row_id; keyed by row_id."""
    rows = load_triplets(name)
    superseded = {r.supersedes for r in rows if r.supersedes}
    return {r.row_id: r for r in rows if r.row_id not in superseded}


def freeze_dataset(name: str, version: str, *, force: bool = False) -> dict[str, Any]:
    """Freeze train-only dataset; exclude gold anchor keys (labeler corollary)."""
    from apps.claims.embedding import gold as gold_mod

    spec = load_spec(name)
    resolved = resolved_triplets(name)
    if not resolved:
        raise ValueError(f"No triplets to freeze for intent {name!r}")
    out_path = dataset_manifest_path(name, version)
    if out_path.exists() and not force:
        raise FileExistsError(f"Dataset already exists: {out_path}")

    gkeys = gold_mod.gold_anchor_keys(name)
    train_rows = [r for r in resolved.values() if r.anchor_key not in gkeys]
    excluded = [r for r in resolved.values() if r.anchor_key in gkeys]
    train_keys = {r.anchor_key for r in train_rows}
    leak = train_keys & gkeys
    if leak:
        raise ValueError(
            f"Gold anchor keys leaked into train set for intent {name!r}: "
            f"{sorted(leak)[:5]}"
        )

    train_ids = sorted(r.row_id for r in train_rows)
    by_id = {r.row_id: r for r in train_rows}
    gold_refs: dict[str, Any] = {}
    for corpus in gold_mod.list_gold_corpora(name):
        rows = gold_mod.resolved_gold(name, corpus)
        gold_refs[corpus] = {
            "n": len(rows),
            "hash": gold_mod.gold_hash(name, corpus),
        }

    manifest = {
        "intent": spec.name,
        "spec_version": spec.version,
        "dataset_version": prov.safe_slug(version),
        "created_at": prov.utc_now(),
        "n_total": len(train_rows),
        "n_train": len(train_ids),
        "train_row_ids": train_ids,
        "n_excluded_gold_overlap": len(excluded),
        "gold_refs": gold_refs,
        "triplets_hash": prov.sha256_json([by_id[i].to_dict() for i in sorted(by_id)]),
        "spec_hash": prov.sha256_json(spec.to_dict()),
    }
    claims_io.write_json(out_path, manifest)
    return manifest


def load_dataset_manifest(name: str, version: str) -> dict[str, Any]:
    path = dataset_manifest_path(name, version)
    if not path.is_file():
        raise FileNotFoundError(f"Missing dataset manifest: {path}")
    return claims_io.read_json(path)


def dataset_rows(name: str, version: str, *, split: str | None = "train") -> list[TripletRow]:
    """Return frozen train rows. ``split`` must be ``train`` or None (same)."""
    manifest = load_dataset_manifest(name, version)
    if split not in (None, "train"):
        # Backward compat: old manifests may have eval_row_ids
        if split == "eval" and manifest.get("eval_row_ids"):
            wanted = set(manifest.get("eval_row_ids") or [])
            by_id = {r.row_id: r for r in load_triplets(name)}
            missing = wanted - set(by_id)
            if missing:
                raise ValueError(
                    f"Dataset references missing triplet rows: {sorted(missing)[:5]}"
                )
            return [by_id[i] for i in sorted(wanted)]
        raise ValueError(
            "Datasets are train-only under the gold-eval scheme; "
            "use gold triplets for evaluation (split must be 'train' or None)"
        )
    wanted = set(manifest.get("train_row_ids") or [])
    by_id = {r.row_id: r for r in load_triplets(name)}
    missing = wanted - set(by_id)
    if missing:
        raise ValueError(f"Dataset references missing triplet rows: {sorted(missing)[:5]}")
    return [by_id[i] for i in sorted(wanted)]
