"""Lightweight pipeline runner: Ctx + step() with skip-if-fresh annotations."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from apps.claims import annotations as ann_mod
from apps.claims import corpus as corpus_mod
from apps.claims import io as claims_io


@dataclass
class Ctx:
    """Shared pipeline context for one corpus run."""

    corpus: corpus_mod.CorpusPaths
    model_tag: str | None = None
    force: bool = False
    log: list[str] = field(default_factory=list)

    @property
    def root(self) -> Path:
        return self.corpus.root

    def note(self, msg: str) -> None:
        self.log.append(msg)
        print(msg)

    @classmethod
    def for_corpus(cls, name: str, *, model_tag: str | None = None, force: bool = False) -> Ctx:
        return cls(corpus=corpus_mod.get_corpus(name), model_tag=model_tag, force=force)


def groups_source_hash(ctx: Ctx) -> str | None:
    if not ctx.corpus.groups.is_file():
        return None
    try:
        data = claims_io.read_json(ctx.corpus.groups)
        return str(data.get("source_hash") or "") or None
    except Exception:  # noqa: BLE001
        return None


def artifact_fresh(
    *,
    output_path: Path | None = None,
    expected_hashes: dict[str, str] | None = None,
    recorded_hashes: dict[str, str] | None = None,
) -> bool:
    """True when output exists and recorded hashes match expected (if provided)."""
    if output_path is not None and not Path(output_path).exists():
        return False
    if not expected_hashes:
        return output_path is None or Path(output_path).exists()
    recorded = recorded_hashes or {}
    for k, v in expected_hashes.items():
        if not v:
            continue
        if str(recorded.get(k) or "") != str(v):
            return False
    return True


def step(
    ctx: Ctx,
    *,
    name: str,
    output_annotation: str | None = None,
    source_hash: str | None = None,
    expected_hashes: dict[str, str] | None = None,
    run: Callable[[Ctx], Any],
) -> Any:
    """Run ``run(ctx)`` unless output is fresh.

    Freshness:
      - annotation mode: meta ``source_hash`` (+ optional extra hashes in meta.params)
      - hash mode: ``expected_hashes`` vs recorded (annotation params or caller)
    """
    if not ctx.force and output_annotation:
        src = source_hash if source_hash is not None else groups_source_hash(ctx)
        if ann_mod.annotation_is_fresh(ctx.root, output_annotation, source_hash=src):
            if expected_hashes:
                try:
                    ann = ann_mod.read_annotation(ctx.root, output_annotation)
                    recorded = {
                        "source_hash": ann.meta.source_hash or "",
                        "model_hash": ann.meta.model_hash or "",
                        **dict(ann.meta.params or {}),
                    }
                    if artifact_fresh(expected_hashes=expected_hashes, recorded_hashes=recorded):
                        ctx.note(f"[skip] {name}: annotation {output_annotation!r} is fresh")
                        return ann
                except Exception:  # noqa: BLE001
                    pass
            else:
                ctx.note(f"[skip] {name}: annotation {output_annotation!r} is fresh")
                return ann_mod.read_annotation(ctx.root, output_annotation)
    ctx.note(f"[run]  {name}")
    return run(ctx)
