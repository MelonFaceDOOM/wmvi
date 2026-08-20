"""Fine-tune a sentence-transformer from triplet anchors (file-mode, no SQLite)."""

from __future__ import annotations

import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np

from apps.claims.embedding.encode import _preload_sklearn_on_windows

_preload_sklearn_on_windows()
import torch.nn as nn  # noqa: E402  — sklearn before torch on Windows (see encode.py)

from apps.claims import io as claims_io
from apps.claims.embedding import eval_triplets as eval_mod
from apps.claims.types import TripletAnchor

LOSS_CHOICES: tuple[str, ...] = ("MultipleNegativesRankingLoss", "TripletLoss")
DEFAULT_LOSS = "MultipleNegativesRankingLoss"
DEFAULT_BATCH_SIZE = 16
DEFAULT_LEARNING_RATE = 2e-5
DEFAULT_EPOCHS = 3
DEFAULT_LORA_R = 16
DEFAULT_LORA_ALPHA = 32
DEFAULT_LORA_TARGETS: tuple[str, ...] = (
    "q_proj",
    "k_proj",
    "v_proj",
    "o_proj",
    "gate_proj",
    "up_proj",
    "down_proj",
)


@dataclass
class TrainingResult:
    output_dir: str
    loss_curve: list[float] = field(default_factory=list)
    dev_acc_per_epoch: list[float] = field(default_factory=list)
    best_epoch: int = 0
    best_dev_acc: float = 0.0
    wall_seconds: float = 0.0
    epochs: int = 0
    lora: bool = False
    hyperparameters: dict[str, Any] = field(default_factory=dict)


def _apply_prompt(text: str, prompt: str | None) -> str:
    p = (prompt or "").strip()
    if not p:
        return text
    return f"{p}{text}"


def _build_examples(
    anchors: list[TripletAnchor],
    *,
    loss: str,
    prompt: str | None = None,
) -> list[Any]:
    from sentence_transformers import InputExample

    examples: list[InputExample] = []
    for anchor in anchors:
        if anchor.too_hard:
            continue
        positives = anchor.positives or []
        negatives = anchor.negatives or []
        if not positives:
            continue
        a_text = _apply_prompt(anchor.text, prompt)
        if loss == "TripletLoss":
            for pos in positives:
                for neg in negatives:
                    examples.append(
                        InputExample(
                            texts=[
                                a_text,
                                _apply_prompt(pos, prompt),
                                _apply_prompt(neg, prompt),
                            ]
                        )
                    )
        else:
            for pos in positives:
                examples.append(
                    InputExample(texts=[a_text, _apply_prompt(pos, prompt)])
                )
    return examples


def _embed_fn_for_model(model: Any, *, prompt: str | None = None) -> Callable[[list[str]], np.ndarray]:
    from apps.claims.embedding.encode import encode_texts

    def _fn(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, 0), dtype=np.float32)
        arr = encode_texts(
            model,
            texts,
            normalize_embeddings=True,
            prompt=(prompt or "").strip() or None,
        )
        return np.asarray(arr, dtype=np.float32)

    return _fn


def _dev_pairwise_accuracy(
    model: Any,
    dev_anchors: list[TripletAnchor],
    *,
    prompt: str | None = None,
) -> float:
    embed_fn = _embed_fn_for_model(model, prompt=prompt)
    scores: dict[int, Any] = {}
    for anchor in dev_anchors:
        if anchor.too_hard or not anchor.positives or not anchor.negatives:
            continue
        scores[anchor.id] = eval_mod.score_anchor(
            anchor.text,
            anchor.positives or [],
            anchor.negatives or [],
            embed_fn=embed_fn,
        )
    overall, _ = eval_mod.aggregate_scores(dev_anchors, scores)
    return overall


class _DevPairwiseEvaluator:
    def __init__(self, dev_anchors: list[TripletAnchor], *, prompt: str | None = None) -> None:
        self.dev_anchors = dev_anchors
        self.prompt = prompt
        self.scores: list[float] = []

    def __call__(self, model, output_path: str | None = None, epoch: int = -1, steps: int = -1) -> float:
        acc = _dev_pairwise_accuracy(model, self.dev_anchors, prompt=self.prompt)
        self.scores.append(acc)
        return acc


class _LossTracker(nn.Module):
    def __init__(self, inner: nn.Module) -> None:
        super().__init__()
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "step_losses", [])

    def forward(self, sentence_features: Any, labels: Any) -> Any:
        result = self._inner(sentence_features, labels)
        try:
            self.step_losses.append(float(result.detach().cpu()))
        except Exception:  # noqa: BLE001
            pass
        return result

    @property
    def model(self) -> Any:
        return self._inner.model

    @model.setter
    def model(self, value: Any) -> None:
        self._inner.model = value


def build_lora_config(
    *,
    r: int = DEFAULT_LORA_R,
    alpha: int = DEFAULT_LORA_ALPHA,
    target_modules: tuple[str, ...] | list[str] | None = None,
) -> Any:
    """Build a PEFT LoraConfig (import peft lazily)."""
    from peft import LoraConfig, TaskType

    targets = list(target_modules) if target_modules is not None else list(DEFAULT_LORA_TARGETS)
    return LoraConfig(
        r=int(r),
        lora_alpha=int(alpha),
        lora_dropout=0.05,
        bias="none",
        task_type=TaskType.FEATURE_EXTRACTION,
        target_modules=targets,
    )


def _apply_lora(
    model: Any,
    *,
    r: int,
    alpha: int,
    target_modules: tuple[str, ...] | list[str] | None = None,
) -> Any:
    """Wrap the inner transformer with LoRA + enable gradient checkpointing."""
    from peft import get_peft_model

    # SentenceTransformer: first module is usually Transformer
    auto_model = None
    if hasattr(model, "_first_module"):
        first = model._first_module()
        auto_model = getattr(first, "auto_model", None)
    if auto_model is None:
        raise ValueError("Could not locate transformer auto_model for LoRA wrapping")

    if hasattr(auto_model, "gradient_checkpointing_enable"):
        auto_model.gradient_checkpointing_enable()
    if hasattr(auto_model, "enable_input_require_grads"):
        auto_model.enable_input_require_grads()
    elif hasattr(auto_model, "get_input_embeddings"):
        emb = auto_model.get_input_embeddings()
        if emb is not None and hasattr(emb, "weight"):

            def _make_inputs_require_grad(_module, _inp, output):  # type: ignore[no-untyped-def]
                output.requires_grad_(True)

            emb.register_forward_hook(_make_inputs_require_grad)

    lora_cfg = build_lora_config(r=r, alpha=alpha, target_modules=target_modules)
    peft_model = get_peft_model(auto_model, lora_cfg)
    first.auto_model = peft_model  # type: ignore[union-attr]
    return model


def run(
    *,
    base_model_id: str,
    output_name: str,
    train_anchors: list[TripletAnchor],
    dev_anchors: list[TripletAnchor],
    loss: str = DEFAULT_LOSS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    learning_rate: float = DEFAULT_LEARNING_RATE,
    epochs: int = DEFAULT_EPOCHS,
    models_root: Path | None = None,
    allow_overwrite: bool = True,
    lora: bool = False,
    lora_r: int = DEFAULT_LORA_R,
    lora_alpha: int = DEFAULT_LORA_ALPHA,
    doc_instruction: str = "",
    max_seq_length: int | None = 512,
    dtype: str = "auto",
    device: str | None = None,
) -> TrainingResult:
    from apps.claims.embedding.encode import load_sentence_transformer
    from sentence_transformers import losses
    from sentence_transformers.evaluation import SequentialEvaluator
    from torch.utils.data import DataLoader

    if loss not in LOSS_CHOICES:
        raise ValueError(f"Unknown loss: {loss}")

    prompt = (doc_instruction or "").strip() or None
    train_examples = _build_examples(train_anchors, loss=loss, prompt=prompt)
    if not train_examples:
        raise ValueError("No training examples (need training-pool anchors with pos/neg, not too_hard)")

    out_root = models_root or claims_io.models_dir()
    out_root.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in output_name.strip()) or "trained_model"
    output_dir = out_root / safe_name
    if output_dir.exists():
        if not allow_overwrite:
            raise FileExistsError(f"Model output already exists (immutable): {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    model = load_sentence_transformer(
        base_model_id,
        device=device,
        dtype=dtype,
        max_seq_length=max_seq_length,
    )
    if lora:
        try:
            model = _apply_lora(model, r=lora_r, alpha=lora_alpha)
        except ImportError as exc:
            raise ImportError(
                "LoRA training requires peft; pip install -r apps/claims/requirements-embed.txt"
            ) from exc

    train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=batch_size)
    if loss == "TripletLoss":
        base_loss = losses.TripletLoss(model)
    else:
        base_loss = losses.MultipleNegativesRankingLoss(model)
    train_loss = _LossTracker(base_loss)
    # Dev evaluator encodes raw texts with prompt= (not pre-prefixed examples)
    dev_evaluator = _DevPairwiseEvaluator(dev_anchors, prompt=prompt)
    evaluator = SequentialEvaluator([dev_evaluator])

    t0 = time.monotonic()
    steps_per_epoch = max(1, len(train_examples) // batch_size)
    warmup_steps = max(10, steps_per_epoch)
    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        evaluator=evaluator,
        epochs=epochs,
        warmup_steps=warmup_steps,
        output_path=str(output_dir),
        optimizer_params={"lr": learning_rate},
        show_progress_bar=True,
        checkpoint_save_steps=steps_per_epoch,
        checkpoint_save_total_limit=1,
        evaluation_steps=steps_per_epoch,
    )
    wall_seconds = time.monotonic() - t0

    # Persist encode metadata alongside ST artifacts
    meta = {
        "doc_instruction": doc_instruction or "",
        "max_seq_length": max_seq_length,
        "dtype": dtype,
        "lora": bool(lora),
        "lora_r": int(lora_r) if lora else None,
        "lora_alpha": int(lora_alpha) if lora else None,
        "base_model_id": base_model_id,
    }
    claims_io.write_json(output_dir / "claims_encode_meta.json", meta)

    dev_acc = dev_evaluator.scores
    best_epoch = 0
    best_dev_acc = 0.0
    if dev_acc:
        best_dev_acc = max(dev_acc)
        best_epoch = int(dev_acc.index(best_dev_acc)) + 1

    loss_curve: list[float] = []
    if train_loss.step_losses:
        for ep in range(epochs):
            start = ep * steps_per_epoch
            end = min((ep + 1) * steps_per_epoch, len(train_loss.step_losses))
            chunk = train_loss.step_losses[start:end]
            loss_curve.append(float(np.mean(chunk)) if chunk else 0.0)

    hyper: dict[str, Any] = {
        "batch_size": batch_size,
        "learning_rate": learning_rate,
        "epochs": epochs,
        "lora": bool(lora),
        "lora_r": int(lora_r) if lora else None,
        "lora_alpha": int(lora_alpha) if lora else None,
        "doc_instruction": doc_instruction or "",
        "max_seq_length": max_seq_length,
        "dtype": dtype,
    }
    return TrainingResult(
        output_dir=str(output_dir.resolve()),
        loss_curve=loss_curve,
        dev_acc_per_epoch=dev_acc,
        best_epoch=best_epoch,
        best_dev_acc=best_dev_acc,
        wall_seconds=wall_seconds,
        epochs=epochs,
        lora=bool(lora),
        hyperparameters=hyper,
    )
