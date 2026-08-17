"""Lifecycle contracts: labels, gold, triplets, filters, registries (no lab imports)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from apps.claims import annotations as ann_mod
from apps.claims import filtering as filt
from apps.claims import io as claims_io
from apps.claims import labeling as label_data
from apps.claims import provenance as prov
from apps.claims.embedding import triplets as trip_data
from apps.claims.keys import claim_key
from apps.claims.labeling import gold as gold_mod
from apps.claims.labeling import lifecycle as label_life
from apps.claims.labeling import probes as probes_mod
from apps.claims.labeling import registry as label_reg


@pytest.fixture()
def claims_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "data"
    monkeypatch.setattr(claims_io, "data_root", lambda: root)
    claims_io.ensure_data_dirs()
    return root


def _seed_gold(intent: str, corpus: str, n0: int = 10, n1: int = 40) -> None:
    spec = label_data.load_spec(intent)
    for i in range(n0):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=f"gold zero claim {i} about vaccines",
                value=0.0,
                corpus=corpus,
            )
        )
    for i in range(n1):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=f"gold one claim {i} about vaccines",
                value=1.0,
                corpus=corpus,
            )
        )


def test_label_intent_freeze_excludes_gold(claims_data_root: Path):
    spec = label_data.create_intent(
        "toy_value",
        instructions="toy",
        value_type="binary",
        min_gold_total=10,
        min_gold_per_class=2,
    )
    for i in range(20):
        row = label_data.make_label_row(
            spec=spec,
            text=f"claim text number {i} about vaccines",
            value=float(i % 2),
            producer={"type": "test"},
            corpus="measles",
        )
        label_data.append_label(row)
    # Gold overlaps first 3 train keys
    for i in range(3):
        text = f"claim text number {i} about vaccines"
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=text,
                value=float(i % 2),
                corpus="measles",
            )
        )
    manifest = label_data.freeze_dataset("toy_value", "v1")
    assert manifest["n_excluded_gold_overlap"] == 3
    assert manifest["n_train"] == 17
    train = label_data.dataset_rows("toy_value", "v1", split="train")
    assert len(train) == 17
    gkeys = gold_mod.gold_keys("toy_value")
    assert all(r.claim_key not in gkeys for r in train)
    with pytest.raises(FileExistsError):
        label_data.freeze_dataset("toy_value", "v1")


def test_gold_gate_and_human_only(claims_data_root: Path):
    label_data.create_intent(
        "gate_bin",
        instructions="x",
        value_type="binary",
        min_gold_total=5,
        min_gold_per_class=2,
    )
    status = gold_mod.gold_status("gate_bin", "resp")
    assert status["gate_ok"] is False
    with pytest.raises(ValueError, match="Insufficient gold"):
        gold_mod.require_gold_gate("gate_bin", "resp")
    _seed_gold("gate_bin", "resp", n0=2, n1=3)
    status2 = gold_mod.gold_status("gate_bin", "resp")
    assert status2["gate_ok"] is True
    # Reject non-human producer
    bad = gold_mod.GoldRow(
        claim_key="abc",
        claim_text="x",
        value=1.0,
        corpus="resp",
        intent="gate_bin",
        spec_version=1,
        labeled_at=prov.utc_now(),
        producer={"type": "agent_label"},
    )
    with pytest.raises(ValueError, match="human"):
        gold_mod.append_gold(bad)


def test_float_class_keys_match_label_buckets(claims_data_root: Path):
    """0.0/1.0 must count under spec keys '0.0'/'1.0', not '0'/'1'."""
    label_data.create_intent(
        "stance_gate",
        instructions="x",
        value_type="float",
        labels={
            "0.0": "strong_anti",
            "0.25": "soft_anti",
            "0.5": "neutral",
            "0.75": "soft_pro",
            "1.0": "strong_pro",
        },
        min_gold_total=10,
        min_gold_per_class=2,
    )
    labels = {
        "0.0": "strong_anti",
        "0.25": "soft_anti",
        "0.5": "neutral",
        "0.75": "soft_pro",
        "1.0": "strong_pro",
    }
    for k in ("0.0", "0.25", "0.5", "0.75", "1.0"):
        assert gold_mod._class_key(float(k), value_type="float", labels=labels) == k
    spec = label_data.load_spec("stance_gate")
    for bucket, n in [("0.0", 2), ("0.25", 2), ("0.5", 2), ("0.75", 2), ("1.0", 2)]:
        for i in range(n):
            gold_mod.append_gold(
                gold_mod.make_gold_row(
                    intent=spec.name,
                    spec_version=spec.version,
                    text=f"stance gold {bucket} {i}",
                    value=float(bucket),
                    corpus="measles",
                )
            )
    status = gold_mod.gold_status("stance_gate", "measles")
    corp = status["corpora"]["measles"]
    assert corp["per_class"]["0.0"] == 2
    assert corp["per_class"]["1.0"] == 2
    assert "0" not in corp["per_class"]
    assert "1" not in corp["per_class"]
    assert corp["gate_ok"] is True


def test_stratified_probes_prefer_rare_class(claims_data_root: Path):
    """With imbalanced gold, small probe draws should still hit the rare class."""
    import random

    label_data.create_intent(
        "strat_bin",
        instructions="x",
        value_type="binary",
        min_gold_total=10,
        min_gold_per_class=2,
        probe_target=25,
    )
    _seed_gold("strat_bin", "measles", n0=3, n1=40)
    unused = list(gold_mod.resolved_gold("strat_bin", "measles").values())
    picked = probes_mod.pick_stratified_probes(
        unused,
        6,
        value_type="binary",
        labels={"0": "no", "1": "yes"},
        rng=random.Random(0),
    )
    assert len(picked) == 6
    values = {float(r.value) for r in picked}
    assert 0.0 in values
    assert 1.0 in values
    # First slots favor rare class: among 6 picks with 3 zeros, all 3 zeros should appear
    n0 = sum(1 for r in picked if float(r.value) == 0.0)
    assert n0 == 3


def test_probe_injection_and_agent_eval(claims_data_root: Path):
    from apps.claims import corpus as corpus_mod

    label_data.create_intent(
        "probe_bin",
        instructions="x",
        value_type="binary",
        min_gold_total=10,
        min_gold_per_class=3,
        probe_target=5,
    )
    _seed_gold("probe_bin", "measles", n0=5, n1=10)
    # Build a tiny corpus groups.json
    corp = corpus_mod.create_corpus("measles")
    groups = []
    for i in range(30):
        text = f"ordinary training claim {i} vaccines"
        groups.append(
            {
                "group_id": i,
                "claim_key": claim_key(text),
                "claim_text": text,
                "count": 1,
                "sources": [],
            }
        )
    claims_io.write_json(
        corp.groups,
        {
            "source_path": "test",
            "source_hash": "h",
            "source_claim_count": 30,
            "claim_count": 30,
            "groups": groups,
        },
    )

    batch = probes_mod.sample_labeling_batch(
        intent="probe_bin",
        corpus="measles",
        n=10,
        run_size=50,
        seed=0,
    )
    assert batch["n_probes_injected"] >= 1
    assert batch["run_id"]
    run_id = batch["run_id"]
    probe_keys = probes_mod.served_probe_keys("probe_bin", run_id)
    assert probe_keys

    # Label all served claims (including probes) as agent
    spec = label_data.load_spec("probe_bin")
    gold = gold_mod.resolved_gold("probe_bin", "measles")
    for c in batch["claims"]:
        ck = c["claim_key"]
        # agent sometimes wrong on gold
        if ck in gold:
            val = 1.0 - float(gold[ck].value)  # disagree
        else:
            val = 1.0
        label_data.append_label(
            label_data.make_label_row(
                spec=spec,
                text=c["text"],
                value=val,
                producer={"type": "agent_label"},
                corpus="measles",
                claim_key_override=ck,
                probe_run_id=run_id if ck in probe_keys else None,
            )
        )

    # Force reportable by lowering floor
    ev = label_life.evaluate_agent_labeler(
        intent="probe_bin",
        corpus="measles",
        run_id=run_id,
        min_probes=1,
    )
    assert ev["reportable"] is True
    assert ev["n_probes"] >= 1
    assert ev["metrics"]["accuracy"] is not None


def test_labeler_train_eval_promote_apply(claims_data_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("sklearn")
    import numpy as np

    class _FakeEnc:
        def encode(self, texts, **kwargs):
            _ = kwargs
            return np.stack([np.ones(8) * (hash(t) % 7) for t in texts], axis=0).astype(np.float64)

    monkeypatch.setattr(
        "apps.claims.labeling.train.load_sentence_transformer",
        lambda _mid=None: _FakeEnc(),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.predict.load_sentence_transformer",
        lambda _mid=None: _FakeEnc(),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.train.encode_texts",
        lambda model, texts, **kw: model.encode(texts),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.predict.encode_texts",
        lambda model, texts, **kw: model.encode(texts),
    )

    spec = label_data.create_intent(
        "tiny_bin",
        instructions="x",
        value_type="binary",
        min_gold_total=4,
        min_gold_per_class=2,
    )
    for i in range(8):
        label_data.append_label(
            label_data.make_label_row(
                spec=spec,
                text=f"synthetic claim {i} vaccines matter here",
                value=1.0 if i % 2 == 0 else 0.0,
                producer={"type": "test"},
                corpus="toy",
            )
        )
    # Separate gold (no overlap with train texts)
    for i in range(2):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=f"gold eval claim {i} vaccines",
                value=float(i % 2),
                corpus="toy",
            )
        )
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=f"gold eval claim b{i} vaccines",
                value=float((i + 1) % 2),
                corpus="toy",
            )
        )

    label_data.freeze_dataset("tiny_bin", "v1")
    out = label_life.train_labeler(
        intent="tiny_bin",
        dataset_version="v1",
        model_version="m1",
        encoder_model_id="fake-encoder",
        set_active=True,
    )
    assert Path(out["path"]).is_dir()
    with pytest.raises(FileExistsError):
        label_life.train_labeler(
            intent="tiny_bin",
            dataset_version="v1",
            model_version="m1",
            encoder_model_id="fake-encoder",
        )
    ev = label_life.evaluate_labeler(
        intent="tiny_bin",
        model_ref="tiny_bin@active",
        corpus="toy",
    )
    assert "toy" in ev["per_corpus"]
    assert ev["per_corpus"]["toy"]["n"] == 4

    corpus_root = tmp_path / "corp"
    corpus_root.mkdir()
    train_texts = [f"synthetic claim {i} vaccines matter here" for i in range(8)]
    groups = {
        "source_hash": "abc",
        "groups": [
            {
                "claim_text": train_texts[0],
                "claim_key": claim_key(train_texts[0]),
            },
            {
                "claim_text": train_texts[1],
                "claim_key": claim_key(train_texts[1]),
            },
            {
                "claim_text": "gold eval claim 0 vaccines",
                "claim_key": claim_key("gold eval claim 0 vaccines"),
            },
        ],
    }
    groups_path = corpus_root / "groups.json"
    claims_io.write_json(groups_path, groups)
    ann = label_life.apply_labeler(
        corpus_root=corpus_root,
        groups_path=groups_path,
        model_ref="tiny_bin/m1",
        annotation_name="pred_v1",
        intent="tiny_bin",
        value_type="binary",
    )
    assert ann.meta.count == 3
    assert ann.meta.producer_kind == "model_prediction"
    assert "labeled_texts" not in ann.meta.params
    assert label_reg.resolve_alias("tiny_bin", "active").name == "m1"

    from apps.claims import corpus as corpus_mod

    cpaths = corpus_mod.create_corpus("toy")
    claims_io.write_json(cpaths.groups, groups)
    ann_mod.write_annotation(
        cpaths.root,
        "pred_v1",
        dict(ann.values),
        producer="test",
        force=True,
    )
    aev = label_life.evaluate_annotation(
        corpus="toy",
        annotation_name="pred_v1",
        intent="tiny_bin",
    )
    assert aev["gold_metrics"] is not None
    assert aev["fit_vs_train_labels"] is not None
    assert aev["fit_vs_train_labels"]["n_scored"] >= 1

    # Existing annotation without --force must fail before scoring
    with pytest.raises(FileExistsError, match="already exists"):
        label_life.apply_labeler(
            corpus_root=corpus_root,
            groups_path=groups_path,
            model_ref="tiny_bin/m1",
            annotation_name="pred_v1",
            intent="tiny_bin",
            value_type="binary",
            force=False,
        )


def test_apply_labeler_skips_predict_when_annotation_exists(
    claims_data_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """FileExistsError must happen before model load / encode."""
    corpus_root = tmp_path / "corp"
    corpus_root.mkdir()
    ann_mod.write_annotation(corpus_root, "already", {"k": 1.0}, producer="t")
    groups_path = corpus_root / "groups.json"
    claims_io.write_json(groups_path, {"source_hash": "h", "groups": []})

    def _boom(*_a, **_k):
        raise AssertionError("FieldPredictor.load should not run when annotation exists")

    monkeypatch.setattr(
        "apps.claims.labeling.lifecycle.FieldPredictor.load",
        _boom,
    )
    with pytest.raises(FileExistsError, match="already exists"):
        label_life.apply_labeler(
            corpus_root=corpus_root,
            groups_path=groups_path,
            model_ref="unused/ref",
            annotation_name="already",
            force=False,
        )


def test_triplet_intent_freeze(claims_data_root: Path):
    spec = trip_data.create_intent("sim_toy", instructions="similar beliefs")
    for i in range(6):
        trip_data.append_triplet(
            trip_data.make_triplet_row(
                spec=spec,
                anchor_text=f"anchor {i} vaccine schedule",
                positive_texts=[f"pos {i} same belief"],
                negative_texts=[f"neg {i} unrelated"],
                producer={"type": "test"},
            )
        )
    man = trip_data.freeze_dataset("sim_toy", "v1")
    assert man["n_total"] == 6
    assert man["n_train"] == 6
    assert man.get("n_excluded_gold_overlap", 0) == 0
    with pytest.raises(FileExistsError):
        trip_data.append_triplet(
            trip_data.make_triplet_row(
                spec=spec,
                anchor_text="anchor 0 vaccine schedule",
                positive_texts=["pos 0 same belief"],
                negative_texts=["neg 0 unrelated"],
                producer={"type": "test"},
            )
        )


def test_triplet_empty_sides_and_disjoint(claims_data_root: Path):
    spec = trip_data.create_intent("sim_empty", instructions="x")
    # Empty neg OK
    row = trip_data.make_triplet_row(
        spec=spec,
        anchor_text="anchor only pos",
        positive_texts=["pos same"],
        negative_texts=[],
        producer={"type": "test"},
    )
    assert row.positive_keys and not row.negative_keys
    trip_data.append_triplet(row)
    # Empty pos OK
    row2 = trip_data.make_triplet_row(
        spec=spec,
        anchor_text="anchor only neg",
        positive_texts=[],
        negative_texts=["neg other"],
        producer={"type": "test"},
    )
    assert not row2.positive_keys and row2.negative_keys
    # Overlap with anchor rejected
    with pytest.raises(ValueError, match="must not include the anchor"):
        trip_data.make_triplet_row(
            spec=spec,
            anchor_text="anchor A",
            positive_texts=["anchor A"],
            negative_texts=["neg"],
            producer={"type": "test"},
        )
    # Pos/neg disjoint
    with pytest.raises(ValueError, match="disjoint"):
        trip_data.make_triplet_row(
            spec=spec,
            anchor_text="anchor B",
            positive_texts=["same text"],
            negative_texts=["same text"],
            producer={"type": "test"},
        )


def test_triplet_freeze_excludes_gold_anchors(claims_data_root: Path):
    from apps.claims.embedding import gold as gold_mod

    spec = trip_data.create_intent(
        "sim_gold_excl", instructions="x", min_gold_total=2
    )
    for i in range(4):
        trip_data.append_triplet(
            trip_data.make_triplet_row(
                spec=spec,
                anchor_text=f"train anchor {i} measles",
                positive_texts=[f"pos {i}"],
                negative_texts=[f"neg {i}"],
                producer={"type": "test"},
                corpus="measles",
            )
        )
    # Gold overlaps first two anchors
    for i in range(2):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                anchor_text=f"train anchor {i} measles",
                positive_texts=[f"gold pos {i}"],
                negative_texts=[f"gold neg {i}"],
                corpus="measles",
            )
        )
    man = trip_data.freeze_dataset("sim_gold_excl", "v1")
    assert man["n_excluded_gold_overlap"] == 2
    assert man["n_train"] == 2
    train = trip_data.dataset_rows("sim_gold_excl", "v1", split="train")
    gkeys = gold_mod.gold_anchor_keys("sim_gold_excl")
    assert all(r.anchor_key not in gkeys for r in train)


def test_pick_train_compare_winner():
    from apps.claims.embedding import compare as cmp_mod

    pick = cmp_mod.pick_train_compare_winner(
        {"pass_pct": 0.8, "mean_margin": 0.1, "n_empty_eval": 0},
        {"pass_pct": 0.7, "mean_margin": 0.2, "n_empty_eval": 0},
    )
    assert pick["winner"] == "mnrl"
    pick2 = cmp_mod.pick_train_compare_winner(
        {"pass_pct": 0.5, "mean_margin": 0.05, "n_empty_eval": 2},
        {"pass_pct": 0.5, "mean_margin": 0.2, "n_empty_eval": 2},
    )
    assert pick2["winner"] == "triplet"
    pick3 = cmp_mod.pick_train_compare_winner(
        {"pass_pct": 0.5, "mean_margin": 0.1, "n_empty_eval": 0},
        {"pass_pct": 0.5, "mean_margin": 0.1, "n_empty_eval": 3},
    )
    assert pick3["winner"] == "mnrl"


def test_set_agreement_and_agent_eval(claims_data_root: Path):
    from apps.claims.embedding import compare as cmp_mod
    from apps.claims.embedding import gold as gold_mod
    from apps.claims.embedding import probes as probes_mod

    m = cmp_mod.set_agreement({"a", "b"}, {"b", "c"})
    assert m["n_tp"] == 1
    assert abs(m["jaccard"] - 1 / 3) < 1e-9

    spec = trip_data.create_intent(
        "sim_agent", instructions="x", min_gold_total=2, probe_target=2
    )
    for i in range(3):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                anchor_text=f"gold anchor {i} vaccines",
                positive_texts=[f"gold pos {i}a", f"gold pos {i}b"],
                negative_texts=[f"gold neg {i}"],
                corpus="measles",
            )
        )
    status = gold_mod.gold_status("sim_agent", "measles")
    assert status["gate_ok"] is True

    ledger = probes_mod.ensure_run_ledger(
        "sim_agent", corpus="measles", run_id="run_test1"
    )
    gkeys = list(gold_mod.resolved_gold("sim_agent", "measles").keys())
    probes_mod.record_served_probes("sim_agent", "run_test1", gkeys[:2])

    # Agent labels with partial agreement on first gold, full on second
    g0 = gold_mod.resolved_gold("sim_agent", "measles")[gkeys[0]]
    trip_data.append_triplet(
        trip_data.make_triplet_row(
            spec=spec,
            anchor_text=g0.claim_text,
            positive_texts=[g0.positive_texts[0]],  # miss one pos
            negative_texts=list(g0.negative_texts),
            producer={"type": "agent_label"},
            corpus="measles",
            anchor_key_override=g0.claim_key,
            probe_run_id="run_test1",
            reason="partial agreement on first probe",
        )
    )
    g1 = gold_mod.resolved_gold("sim_agent", "measles")[gkeys[1]]
    trip_data.append_triplet(
        trip_data.make_triplet_row(
            spec=spec,
            anchor_text=g1.claim_text,
            positive_texts=list(g1.positive_texts),
            negative_texts=list(g1.negative_texts),
            producer={"type": "agent_label"},
            corpus="measles",
            anchor_key_override=g1.claim_key,
            probe_run_id="run_test1",
            reason="full agreement on second probe",
        )
    )
    ev = cmp_mod.evaluate_agent_triplets(
        intent="sim_agent",
        corpus="measles",
        run_id="run_test1",
        min_probes=1,
    )
    assert ev["reportable"] is True
    assert ev["metrics"]["n_labeled"] == 2
    assert ev["metrics"]["pos_recall"] is not None
    assert 0.0 < float(ev["metrics"]["pos_recall"]) < 1.0 or float(
        ev["metrics"]["pos_recall"]
    ) == 1.0


def test_sample_neighbors_and_triplets_import(claims_data_root: Path, tmp_path: Path):
    import numpy as np
    from apps.claims import corpus as corpus_mod
    from apps.claims.embedding import gold as gold_mod
    from apps.claims.embedding import probes as probes_mod

    spec = trip_data.create_intent(
        "sim_sample",
        instructions="x",
        min_gold_total=2,
        probe_target=2,
        neighbor_k=3,
    )
    corp = corpus_mod.create_corpus("measles")
    texts = [f"claim text number {i} about vaccines measles" for i in range(20)]
    # Put gold texts in the pool
    gold_texts = [f"gold probe anchor {i} vaccines" for i in range(3)]
    all_texts = texts + gold_texts
    groups = []
    for i, text in enumerate(all_texts):
        groups.append(
            {
                "group_id": i,
                "claim_key": claim_key(text),
                "claim_text": text,
                "count": 1,
                "sources": [],
            }
        )
    claims_io.write_json(
        corp.groups,
        {
            "source_path": "test",
            "source_hash": "h",
            "source_claim_count": len(groups),
            "claim_count": len(groups),
            "groups": groups,
        },
    )
    # Tiny embed run (normalized random vectors)
    run_dir = corp.run_dir("toy-tag")
    run_dir.mkdir(parents=True)
    rng = np.random.default_rng(0)
    vectors = rng.normal(size=(len(groups), 8)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True) + 1e-9
    np.save(run_dir / "vectors.npy", vectors)
    claims_io.write_json(
        run_dir / "index.json",
        {
            "claim_texts": [g["claim_text"] for g in groups],
            "claim_keys": [g["claim_key"] for g in groups],
            "groups": groups,
        },
    )
    claims_io.write_json(run_dir / "metrics.json", {"model_id": "toy"})

    for i, text in enumerate(gold_texts):
        # Gold with neighbors drawn from pool texts
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                anchor_text=text,
                positive_texts=[texts[i]],
                negative_texts=[texts[i + 5]],
                corpus="measles",
            )
        )

    batch = probes_mod.sample_triplet_batch(
        intent="sim_sample",
        corpus="measles",
        model_tag="toy-tag",
        n=5,
        run_size=20,
        seed=0,
    )
    assert batch["n_returned"] == 5
    assert batch["n_probes_injected"] >= 1
    assert all(c.get("neighbors") for c in batch["claims"])
    assert all(len(c["neighbors"]) <= 3 for c in batch["claims"])
    run_id = batch["run_id"]

    # Import judgments using 1-based indices from first claim
    first = batch["claims"][0]
    judged = [
        {
            "claim_key": first["claim_key"],
            "text": first["text"],
            "pos": [1],
            "neg": [2] if len(first["neighbors"]) >= 2 else [],
            "reason": "unique reason for first anchor judgment",
        }
    ]
    judged_path = tmp_path / "judged.jsonl"
    claims_io.write_jsonl(judged_path, judged)
    sample_path = tmp_path / "sample.json"
    claims_io.write_json(sample_path, batch)

    from apps.claims.cli.embedder_cmd import cmd_embedder_triplets_import
    from argparse import Namespace

    rc = cmd_embedder_triplets_import(
        Namespace(
            intent="sim_sample",
            from_path=judged_path,
            corpus="measles",
            run_id=run_id,
            sample=sample_path,
            producer_type="agent_label",
            model_tag="toy-tag",
        )
    )
    assert rc == 0
    rows = trip_data.load_triplets("sim_sample")
    assert len(rows) == 1
    assert rows[0].positive_keys
    assert rows[0].anchor_key == first["claim_key"]


def test_filter_resolve_and_provenance(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    ann_mod.write_annotation(
        root,
        "epi",
        {"a": 1, "b": 0, "c": 1},
        scope="group",
        producer="test",
        producer_kind="agent_label",
        source_hash="hhh",
        value_type="binary",
    )
    resolved = filt.resolve_filter(root, "epi", filt.FilterPredicate.eq(1.0), groups_hash="hhh")
    assert resolved.count == 2
    assert set(resolved.keys) == {"a", "c"}
    prov_dict = resolved.provenance()
    assert prov_dict["annotation"] == "epi"
    assert prov_dict["selected_count"] == 2
    assert "selected_keys_hash" in prov_dict


def test_parse_filter_spec_and_resolve_and(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    ann_mod.write_annotation(
        root,
        "quality",
        {"a": 0.9, "b": 0.1, "c": 0.8, "d": 0.7},
        scope="group",
        producer="test",
        value_type="float",
    )
    ann_mod.write_annotation(
        root,
        "stance",
        {"a": 0.9, "b": 0.9, "c": 0.2, "d": 0.95},
        scope="group",
        producer="test",
        value_type="float",
    )
    name, pred = filt.parse_filter_spec("quality:low=0.5")
    assert name == "quality"
    assert pred.op == "range" and pred.low == 0.5
    name2, pred2 = filt.parse_filter_spec("stance:low=0.875,high=1")
    assert name2 == "stance" and pred2.low == 0.875 and pred2.high == 1.0

    combined = filt.resolve_filter_clauses(
        root,
        [("quality", pred), ("stance", pred2)],
    )
    assert set(combined.keys) == {"a", "d"}
    assert combined.provenance()["op"] == "and"
    assert combined.count == 2


def test_annotation_strips_bulky_params(tmp_path: Path):
    root = tmp_path / "c"
    root.mkdir()
    ann = ann_mod.write_annotation(
        root,
        "x",
        {"k": 1},
        scope="group",
        producer="t",
        params={"labeled_texts": {"k": "huge"}, "rubric": "ok"},
    )
    assert "labeled_texts" not in ann.meta.params
    assert ann.meta.params.get("rubric") == "ok"


def _write_toy_groups(corpus_name: str, texts: list[str]) -> list[str]:
    from apps.claims import corpus as corpus_mod

    corp = corpus_mod.create_corpus(corpus_name)
    keys = [claim_key(t) for t in texts]
    claims_io.write_json(
        corp.groups,
        {
            "source_path": "test",
            "source_hash": "h",
            "source_claim_count": len(texts),
            "claim_count": len(texts),
            "groups": [
                {
                    "group_id": i,
                    "claim_key": keys[i],
                    "claim_text": texts[i],
                    "count": 1,
                    "sources": [],
                }
                for i in range(len(texts))
            ],
        },
    )
    return keys


def test_gold_sample_respects_filter(claims_data_root: Path):
    from argparse import Namespace

    from apps.claims import corpus as corpus_mod
    from apps.claims.cli import labeler_cmd

    label_data.create_intent("filt_gold", instructions="x", value_type="binary")
    texts = [f"filt claim {i} vaccines" for i in range(8)]
    keys = _write_toy_groups("measles", texts)
    corp = corpus_mod.get_corpus("measles")
    # Keep only even-indexed claims
    ann_mod.write_annotation(
        corp.root,
        "standalone",
        {keys[i]: (1.0 if i % 2 == 0 else 0.0) for i in range(len(keys))},
        producer="test",
        value_type="binary",
    )
    args = Namespace(
        intent="filt_gold",
        corpus="measles",
        n=10,
        seed=0,
        human=False,
        filter=["standalone:eq=1"],
        where_annotation=None,
        eq=None,
        low=None,
        high=None,
        selection=None,
        save_selection=None,
        force_selection=False,
    )
    assert labeler_cmd.cmd_labeler_gold_sample(args) == 0
    # Capture via re-running the core path
    allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
    assert allow == {keys[i] for i in range(0, 8, 2)}
    assert filter_meta is not None
    from apps.claims import claim_sample

    index, claim_texts, claim_keys = claim_sample.load_corpus_pool(
        "measles", allow_keys=allow
    )
    assert set(claim_keys) == allow
    assert len(claim_texts) == 4


def test_gold_sampling_descriptor_and_make_row(claims_data_root: Path):
    label_data.create_intent("samp_tag", instructions="x", value_type="binary")
    meta = {"annotation": "standalone_pred_m1", "predicate": {"op": "eq", "value": 1.0}}
    tag = filt.sampling_descriptor(meta)
    assert tag == "random|filter=standalone_pred_m1:eq=1.0"
    row = gold_mod.make_gold_row(
        intent="samp_tag",
        spec_version=1,
        text="a standalone claim about vaccines",
        value=1.0,
        corpus="measles",
        sampling=tag,
    )
    assert row.sampling == tag


def test_sample_labeling_batch_filters_ordinary_and_probes(claims_data_root: Path):
    from apps.claims import corpus as corpus_mod

    label_data.create_intent(
        "filt_probe",
        instructions="x",
        value_type="binary",
        min_gold_total=10,
        min_gold_per_class=3,
        probe_target=5,
    )
    # Gold: half will be outside the filter
    inside_gold_keys: list[str] = []
    outside_gold_keys: list[str] = []
    for i in range(5):
        text = f"gold inside {i} vaccines"
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent="filt_probe",
                spec_version=1,
                text=text,
                value=0.0,
                corpus="measles",
            )
        )
        inside_gold_keys.append(claim_key(text))
    for i in range(10):
        text = f"gold outside {i} vaccines"
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent="filt_probe",
                spec_version=1,
                text=text,
                value=1.0,
                corpus="measles",
            )
        )
        outside_gold_keys.append(claim_key(text))

    ordinary_texts = [f"ordinary filt claim {i} vaccines" for i in range(40)]
    all_texts = (
        [f"gold inside {i} vaccines" for i in range(5)]
        + [f"gold outside {i} vaccines" for i in range(10)]
        + ordinary_texts
    )
    keys = _write_toy_groups("measles", all_texts)
    corp = corpus_mod.get_corpus("measles")
    allow = set(inside_gold_keys) | {claim_key(t) for t in ordinary_texts[:20]}
    # Annotation marks allow-set as 1
    ann_mod.write_annotation(
        corp.root,
        "standalone",
        {k: (1.0 if k in allow else 0.0) for k in keys},
        producer="test",
        value_type="binary",
    )
    resolved = filt.resolve_filter(
        corp.root, "standalone", filt.FilterPredicate.eq(1.0)
    )
    assert set(resolved.keys) == allow

    batch = probes_mod.sample_labeling_batch(
        intent="filt_probe",
        corpus="measles",
        n=15,
        run_size=50,
        seed=0,
        allow_keys=allow,
        filter_meta=resolved.provenance(),
    )
    returned = {c["claim_key"] for c in batch["claims"]}
    assert returned <= allow
    assert not (returned & set(outside_gold_keys))
    assert batch["n_probe_pool"] == len(inside_gold_keys)
    assert batch["n_probe_pool_unfiltered"] == len(inside_gold_keys) + len(outside_gold_keys)
    assert batch["filter"] is not None
    served = probes_mod.served_probe_keys("filt_probe", batch["run_id"])
    assert served <= set(inside_gold_keys)


def test_apply_labeler_with_filter(claims_data_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("sklearn")
    import numpy as np

    class _FakeEnc:
        def encode(self, texts, **kwargs):
            _ = kwargs
            return np.stack([np.ones(8) * (hash(t) % 7) for t in texts], axis=0).astype(
                np.float64
            )

    monkeypatch.setattr(
        "apps.claims.labeling.train.load_sentence_transformer",
        lambda _mid=None: _FakeEnc(),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.predict.load_sentence_transformer",
        lambda _mid=None: _FakeEnc(),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.train.encode_texts",
        lambda model, texts, **kw: model.encode(texts),
    )
    monkeypatch.setattr(
        "apps.claims.labeling.predict.encode_texts",
        lambda model, texts, **kw: model.encode(texts),
    )

    spec = label_data.create_intent(
        "filt_apply",
        instructions="x",
        value_type="binary",
        min_gold_total=4,
        min_gold_per_class=2,
    )
    for i in range(8):
        label_data.append_label(
            label_data.make_label_row(
                spec=spec,
                text=f"apply filt claim {i} vaccines",
                value=1.0 if i % 2 == 0 else 0.0,
                producer={"type": "test"},
                corpus="toy",
            )
        )
    for i in range(4):
        gold_mod.append_gold(
            gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                text=f"apply filt gold {i} vaccines",
                value=float(i % 2),
                corpus="toy",
            )
        )
    label_data.freeze_dataset("filt_apply", "v1")
    label_life.train_labeler(
        intent="filt_apply",
        dataset_version="v1",
        model_version="m1",
        encoder_model_id="fake-encoder",
        set_active=True,
    )

    corpus_root = tmp_path / "corp"
    corpus_root.mkdir()
    texts = [f"apply filt claim {i} vaccines" for i in range(6)]
    keys = [claim_key(t) for t in texts]
    groups_path = corpus_root / "groups.json"
    claims_io.write_json(
        groups_path,
        {
            "source_hash": "abc",
            "groups": [
                {"claim_text": texts[i], "claim_key": keys[i]} for i in range(len(texts))
            ],
        },
    )
    allow = {keys[0], keys[2], keys[4]}
    filter_meta = {"annotation": "standalone", "predicate": {"op": "eq", "value": 1}}
    ann = label_life.apply_labeler(
        corpus_root=corpus_root,
        groups_path=groups_path,
        model_ref="filt_apply/m1",
        annotation_name="pred_filt",
        intent="filt_apply",
        value_type="binary",
        allow_keys=allow,
        filter_meta=filter_meta,
    )
    assert set(ann.values.keys()) == allow
    assert ann.meta.count == 3
    assert ann.meta.params["n_scored"] == 3
    assert ann.meta.params["n_skipped"] == 3
    assert ann.meta.params["n_groups_total"] == 6
    assert ann.meta.params["filter"] == filter_meta


def test_no_lab_imports_in_claims_package():
    root = Path(__file__).resolve().parents[1]  # apps/claims
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path.name.startswith("test_") or "tests" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "apps.labeler_lab" in text or "apps.embedding_lab" in text:
            offenders.append(str(path.relative_to(root.parent.parent)))
    assert offenders == []
