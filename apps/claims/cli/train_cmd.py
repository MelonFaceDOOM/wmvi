from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims.embedding import discover_triplets as discover_mod
from apps.claims.embedding import eval_triplets as eval_mod
from apps.claims.embedding import train as train_mod
from apps.claims.types import EmbedConfig


def cmd_train(args: Namespace) -> int:
    try:
        from apps.claims import provenance as prov

        anchors = eval_mod.load_triplets_json(Path(args.triplets))
        train_anchors = [a for a in anchors if a.pool == "training"]
        dev_anchors = [a for a in anchors if a.pool == "dev"]
        result = train_mod.run(
            base_model_id=str(args.base_model),
            output_name=str(args.output_name),
            train_anchors=train_anchors,
            dev_anchors=dev_anchors,
            loss=str(args.loss),
            batch_size=int(args.batch_size),
            learning_rate=float(args.learning_rate),
            epochs=int(args.epochs),
            allow_overwrite=True,  # legacy CLI behavior
        )
        # Persist training metadata beside the model (was stdout-only)
        out_dir = Path(result.output_dir)
        meta = {
            "kind": "embedder_legacy_train",
            "created_at": prov.utc_now(),
            "base_model_id": str(args.base_model),
            "triplets": str(Path(args.triplets).resolve()),
            "loss": str(args.loss),
            "hyperparameters": {
                "batch_size": int(args.batch_size),
                "learning_rate": float(args.learning_rate),
                "epochs": int(args.epochs),
            },
            "metrics": {
                "best_epoch": result.best_epoch,
                "best_dev_acc": result.best_dev_acc,
                "dev_acc_per_epoch": result.dev_acc_per_epoch,
                "wall_seconds": result.wall_seconds,
            },
            "loss_curve": result.loss_curve,
        }
        claims_io.write_json(out_dir / "train_meta.json", meta)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(
        {
            "ok": True,
            "output_dir": result.output_dir,
            "best_epoch": result.best_epoch,
            "best_dev_acc": result.best_dev_acc,
            "wall_seconds": result.wall_seconds,
            "loss_curve": result.loss_curve,
            "dev_acc_per_epoch": result.dev_acc_per_epoch,
            "train_meta": str(out_dir / "train_meta.json"),
        }
    )
    return 0


def cmd_eval_triplets(args: Namespace) -> int:
    try:
        from apps.claims.cli import paths as path_helpers

        if getattr(args, "run_dir", None) is not None:
            run_dir = Path(args.run_dir)
        elif getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            tag = path_helpers.resolve_model_tag(args)
            run_dir = corpus.run_dir(tag)
        else:
            raise ValueError("Provide --run-dir, or --corpus with --model-tag")
        vectors, index = claims_io.load_run_arrays(run_dir)
        config = EmbedConfig(
            model_id=str(index.get("model_id") or ""),
            doc_instruction=str(index.get("doc_instruction") or ""),
            normalize=bool(index.get("normalize", True)),
            batch_size=int(index.get("batch_size") or 16),
            max_seq_length=int(index.get("max_seq_length") or 512),
            dtype=str(index.get("dtype") or "auto"),
            device=str(index.get("device") or "auto"),
        )
        if not config.model_id:
            raise ValueError("run index.json missing model_id")
        anchors = eval_mod.load_triplets_json(Path(args.triplets))
        payload = eval_mod.run(anchors=anchors, config=config, pool=str(args.pool))
        payload["run_dir"] = str(run_dir)
        _ = vectors
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json(payload)
    return 0


def cmd_discover_triplets(args: Namespace) -> int:
    try:
        from apps.claims.cli import paths as path_helpers

        if getattr(args, "run_dir", None) is not None:
            run_dir = Path(args.run_dir)
        elif getattr(args, "corpus", None):
            corpus = path_helpers.require_corpus(args)
            tag = path_helpers.resolve_model_tag(args)
            run_dir = corpus.run_dir(tag)
        else:
            raise ValueError("Provide --run-dir, or --corpus with --model-tag")
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        existing = (
            eval_mod.load_triplets_json(Path(args.existing)) if args.existing else []
        )
        out_path = Path(args.out)
        unusable = Path(args.unusable_log) if args.unusable_log else None
        payload = discover_mod.run(
            vectors=vectors,
            claim_texts=claim_texts,
            model=str(args.model),
            n_claims=int(args.n_claims),
            existing=existing,
            unusable_log=unusable,
            out_path=out_path,
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    claims_io.emit_json({"ok": True, **payload})
    return 0
