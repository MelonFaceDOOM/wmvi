"""Encoder load/encode options and LoRA config (no model download)."""

from __future__ import annotations

from argparse import Namespace
from typing import Any

import numpy as np
import pytest

from apps.claims.embedding import encode as encode_mod
from apps.claims.embedding import train as train_mod


def test_resolve_load_kwargs_cuda_defaults_bf16(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(encode_mod, "resolve_device", lambda device=None: "cuda")
    opts = encode_mod.resolve_load_kwargs(
        "Qwen/Qwen3-Embedding-8B",
        device="auto",
        dtype="auto",
        max_seq_length=512,
    )
    assert opts["device"] == "cuda"
    assert opts["dtype_name"] == "bfloat16"
    assert opts["torch_dtype"] is not None
    assert opts["max_seq_length"] == 512
    assert opts["model_kwargs"].get("attn_implementation") == "sdpa"
    assert opts["model_kwargs"].get("torch_dtype") is not None
    assert opts["warn_cpu_large"] is False


def test_resolve_load_kwargs_cpu_no_forced_bf16(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(encode_mod, "resolve_device", lambda device=None: "cpu")
    opts = encode_mod.resolve_load_kwargs(
        "BAAI/bge-small-en-v1.5",
        device="cpu",
        dtype="auto",
        max_seq_length=256,
    )
    assert opts["device"] == "cpu"
    assert opts["dtype_name"] is None
    assert opts["torch_dtype"] is None
    assert "torch_dtype" not in opts["model_kwargs"]
    assert opts["max_seq_length"] == 256
    assert opts["warn_cpu_large"] is False


def test_resolve_load_kwargs_warns_large_on_cpu(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(encode_mod, "resolve_device", lambda device=None: "cpu")
    opts = encode_mod.resolve_load_kwargs(
        "Qwen/Qwen3-Embedding-8B",
        device="cpu",
        dtype="auto",
    )
    assert opts["warn_cpu_large"] is True


def test_preload_sklearn_noop_non_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(encode_mod.sys, "platform", "linux")
    encode_mod._preload_sklearn_on_windows()


def test_preload_sklearn_on_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    fake = types.ModuleType("sklearn")
    monkeypatch.setattr(encode_mod.sys, "platform", "win32")
    monkeypatch.setitem(sys.modules, "sklearn", fake)
    encode_mod._preload_sklearn_on_windows()


def test_looks_large_model() -> None:
    assert encode_mod._looks_large_model("Qwen/Qwen3-Embedding-8B")
    assert encode_mod._looks_large_model("org/model-7b-instruct")
    assert not encode_mod._looks_large_model("BAAI/bge-large-en-v1.5")
    assert not encode_mod._looks_large_model("BAAI/bge-small-en-v1.5")


def test_encode_texts_passes_prompt() -> None:
    class FakeEncoder:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] = {}

        def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
            self.kwargs = kwargs
            return np.zeros((len(texts), 4), dtype=np.float32)

    enc = FakeEncoder()
    out = encode_mod.encode_texts(
        enc,
        ["a", "b"],
        batch_size=2,
        prompt="Instruct: test\nQuery:",
    )
    assert out.shape == (2, 4)
    assert enc.kwargs.get("prompt") == "Instruct: test\nQuery:"
    assert enc.kwargs.get("batch_size") == 2


def test_encode_texts_omits_empty_prompt() -> None:
    class FakeEncoder:
        def __init__(self) -> None:
            self.kwargs: dict[str, Any] = {}

        def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
            self.kwargs = kwargs
            return np.zeros((len(texts), 2), dtype=np.float32)

    enc = FakeEncoder()
    encode_mod.encode_texts(enc, ["x"], prompt="")
    assert "prompt" not in enc.kwargs


def test_build_lora_config_shape() -> None:
    peft = pytest.importorskip("peft")
    cfg = train_mod.build_lora_config(r=8, alpha=16)
    assert cfg.r == 8
    assert cfg.lora_alpha == 16
    assert "q_proj" in cfg.target_modules
    assert "down_proj" in cfg.target_modules
    assert peft is not None


def test_train_encode_kwargs_from_args() -> None:
    from apps.claims.cli import embedder_cmd

    args = Namespace(
        lora=True,
        lora_r=8,
        lora_alpha=16,
        doc_instruction="Instruct: x\nQuery:",
        max_seq_length=256,
        dtype="bfloat16",
    )
    kw = embedder_cmd._train_encode_kwargs(args)
    assert kw == {
        "lora": True,
        "lora_r": 8,
        "lora_alpha": 16,
        "doc_instruction": "Instruct: x\nQuery:",
        "max_seq_length": 256,
        "dtype": "bfloat16",
    }


def test_apply_prompt_prefix() -> None:
    assert train_mod._apply_prompt("claim", "Instruct:\nQuery:") == "Instruct:\nQuery:claim"
    assert train_mod._apply_prompt("claim", "") == "claim"
    assert train_mod._apply_prompt("claim", None) == "claim"


def test_build_examples_prefixes_prompt() -> None:
    from apps.claims.types import TripletAnchor

    anchors = [
        TripletAnchor(
            id=1,
            text="anchor",
            positives=["pos"],
            negatives=["neg"],
            pool="training",
        )
    ]
    examples = train_mod._build_examples(
        anchors,
        loss="MultipleNegativesRankingLoss",
        prompt="P:",
    )
    assert len(examples) == 1
    assert examples[0].texts == ["P:anchor", "P:pos"]

    trip = train_mod._build_examples(anchors, loss="TripletLoss", prompt="P:")
    assert trip[0].texts == ["P:anchor", "P:pos", "P:neg"]
