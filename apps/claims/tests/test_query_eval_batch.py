"""Query eval encodes a batch with one encoder load (avoid multi-Qwen CUDA OOM)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np

from apps.claims.cli import cluster_cmd
from apps.claims.clustering import query_eval
from apps.claims.types import EmbedConfig


def test_embed_queries_loads_encoder_once() -> None:
    loads: list[str] = []

    class FakeEnc:
        def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
            return np.ones((len(texts), 3), dtype=np.float32)

    def fake_load(model_id: str, **kwargs: Any) -> FakeEnc:
        loads.append(model_id)
        return FakeEnc()

    with patch("apps.claims.embedding.encode.load_sentence_transformer", side_effect=fake_load):
        out = query_eval.embed_queries(
            "Qwen/Qwen3-Embedding-8B",
            ["a", "b", "c", "d", "e"],
            doc_instruction="Instruct:\nQuery:",
        )
    assert loads == ["Qwen/Qwen3-Embedding-8B"]
    assert out.shape == (5, 3)


def test_embed_query_reuses_encoder() -> None:
    enc = MagicMock()
    enc.encode.return_value = np.array([[1.0, 0.0]], dtype=np.float32)
    with patch("apps.claims.embedding.encode.load_sentence_transformer") as load:
        query_eval.embed_query("m", "hello", encoder=enc)
    load.assert_not_called()
    enc.encode.assert_called_once()


def test_load_or_build_query_cache_embeds_once(tmp_path: Path) -> None:
    loads: list[str] = []

    class FakeEnc:
        def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
            return np.eye(len(texts), 4, dtype=np.float32)

    def fake_load(model_id: str, **kwargs: Any) -> FakeEnc:
        loads.append(model_id)
        return FakeEnc()

    cfg = EmbedConfig(
        model_id="Qwen/Qwen3-Embedding-8B",
        doc_instruction="Instruct:\nQuery:",
        max_seq_length=512,
        dtype="bfloat16",
        device="cuda",
    )
    queries = [{"id": f"q{i}", "query": f"claim {i}"} for i in range(5)]
    with patch("apps.claims.embedding.encode.load_sentence_transformer", side_effect=fake_load):
        cache = cluster_cmd.load_or_build_query_cache(
            config=cfg,
            out_dir=tmp_path,
            queries=queries,
        )
    assert loads == ["Qwen/Qwen3-Embedding-8B"]
    assert len(cache) == 5
    npz = list(tmp_path.glob("query_vectors_*.npz"))
    assert len(npz) == 1

    loads.clear()
    with patch("apps.claims.embedding.encode.load_sentence_transformer", side_effect=fake_load):
        again = cluster_cmd.load_or_build_query_cache(
            config=cfg,
            out_dir=tmp_path,
            queries=queries,
        )
    assert loads == []
    assert again.keys() == cache.keys()


def _cfg() -> EmbedConfig:
    return EmbedConfig(
        model_id="Qwen/Qwen3-Embedding-8B",
        doc_instruction="Instruct:\nQuery:",
        max_seq_length=512,
        dtype="bfloat16",
        device="cuda",
    )


def test_load_or_build_reuses_sibling_cache(tmp_path: Path) -> None:
    cfg = _cfg()
    queries = [{"id": f"q{i}", "query": f"claim {i}"} for i in range(3)]
    sibling = tmp_path / "old_exp"
    dest = tmp_path / "new_exp"
    sibling.mkdir()
    dest.mkdir()

    class FakeEnc:
        def encode(self, texts: list[str], **kwargs: Any) -> np.ndarray:
            return np.eye(len(texts), 4, dtype=np.float32)

    with patch(
        "apps.claims.embedding.encode.load_sentence_transformer",
        return_value=FakeEnc(),
    ):
        cluster_cmd.load_or_build_query_cache(config=cfg, out_dir=sibling, queries=queries)

    loads: list[str] = []

    def boom(model_id: str, **kwargs: Any) -> None:
        loads.append(model_id)
        raise AssertionError("should not embed")

    with patch("apps.claims.embedding.encode.load_sentence_transformer", side_effect=boom):
        cache = cluster_cmd.load_or_build_query_cache(
            config=cfg,
            out_dir=dest,
            queries=queries,
            search_dirs=[tmp_path],
            allow_embed=False,
        )
    assert loads == []
    assert len(cache) == 3
    copied = list(dest.glob("query_vectors_*.npz"))
    assert len(copied) == 1


def test_prepare_query_eval_skips_when_cuda_missing(tmp_path: Path) -> None:
    cfg = _cfg()
    queries = [{"id": "q0", "query": "hello"}]
    dest = tmp_path / "exp"
    dest.mkdir()
    with patch(
        "apps.claims.embedding.encode.resolve_device",
        side_effect=ValueError("device=cuda requested but torch.cuda.is_available() is False"),
    ):
        eval_queries, vecs, skip = cluster_cmd.prepare_query_eval(
            config=cfg,
            cache_dir=dest,
            run_dir=None,
            queries=queries,
        )
    assert eval_queries == []
    assert vecs == {}
    assert skip is not None
    assert "cuda" in skip
