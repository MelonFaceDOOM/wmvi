"""CLI: similarity intents, triplets, datasets, embedder train/eval/promote."""

from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from typing import Any

from apps.claims import io as claims_io
from apps.claims.embedding import registry as emb_reg
from apps.claims.embedding import triplets as trip_data
from apps.claims.types import TripletAnchor


def _parse_text_list(raw: str | None) -> list[str]:
    if raw is None or raw == "":
        return []
    s = str(raw).strip()
    if s.startswith("["):
        val = json.loads(s)
        if isinstance(val, str):
            return [val]
        return [str(x) for x in val]
    return [s]


def _parse_index_list(raw: str) -> list[int]:
    """Parse neighbor indices from '1 3 4', '1,3,4', or mixed separators."""
    toks = str(raw).replace(",", " ").replace(";", " ").split()
    out: list[int] = []
    for tok in toks:
        if tok.isdigit():
            out.append(int(tok))
    return out


def _train_encode_kwargs(args: Namespace) -> dict[str, Any]:
    """Shared encode/LoRA kwargs for embedder train / train-compare."""
    return {
        "lora": bool(getattr(args, "lora", False)),
        "lora_r": int(getattr(args, "lora_r", 16) or 16),
        "lora_alpha": int(getattr(args, "lora_alpha", 32) or 32),
        "doc_instruction": str(getattr(args, "doc_instruction", "") or ""),
        "max_seq_length": int(getattr(args, "max_seq_length", 512) or 512),
        "dtype": str(getattr(args, "dtype", "auto") or "auto"),
    }


def _encode_meta_from_model_path(model_id: str | Path) -> dict[str, Any]:
    """Load claims_encode_meta.json from a trained model dir if present."""
    p = Path(model_id)
    meta_path = p / "claims_encode_meta.json" if p.is_dir() else None
    if meta_path is not None and meta_path.is_file():
        try:
            return claims_io.read_json(meta_path)
        except Exception:  # noqa: BLE001
            return {}
    return {}


def _gold_eval_kwargs(model_id: str, args: Namespace | None = None) -> dict[str, Any]:
    meta = _encode_meta_from_model_path(model_id)
    doc = ""
    if args is not None:
        doc = str(getattr(args, "doc_instruction", "") or "")
    if not doc:
        doc = str(meta.get("doc_instruction") or "")
    dtype = "auto"
    if args is not None and getattr(args, "dtype", None):
        dtype = str(args.dtype)
    elif meta.get("dtype"):
        dtype = str(meta["dtype"])
    max_seq = None
    if args is not None and getattr(args, "max_seq_length", None) is not None:
        max_seq = int(args.max_seq_length)
    elif meta.get("max_seq_length") is not None:
        max_seq = int(meta["max_seq_length"])
    return {
        "doc_instruction": doc,
        "dtype": dtype,
        "max_seq_length": max_seq,
    }


def cmd_embedder_intent_create(args: Namespace) -> int:
    try:
        spec = trip_data.create_intent(
            str(args.name),
            instructions=str(args.instructions or ""),
            similarity_rubric=str(getattr(args, "rubric", None) or ""),
            eval_frac=float(args.eval_frac),
            split_seed=int(args.split_seed),
            min_gold_total=int(getattr(args, "min_gold_total", 20) or 20),
            probe_target=int(getattr(args, "probe_target", 25) or 25),
            neighbor_k=int(getattr(args, "neighbor_k", 15) or 15),
            agent_batch_size=(
                int(args.agent_batch_size)
                if getattr(args, "agent_batch_size", None) is not None
                else 20
            ),
            agent_model=getattr(args, "agent_model", None),
            force=bool(args.force),
        )
        claims_io.emit_json({"ok": True, "spec": spec.to_dict()})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_intent_list(args: Namespace) -> int:
    _ = args
    claims_io.emit_json({"ok": True, "intents": trip_data.list_intents()})
    return 0


def cmd_embedder_intent_show(args: Namespace) -> int:
    try:
        spec = trip_data.load_spec(str(args.name))
        claims_io.emit_json({"ok": True, "spec": spec.to_dict()})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_triplets_add(args: Namespace) -> int:
    try:
        spec = trip_data.load_spec(str(args.intent))
        positives = _parse_text_list(args.positives)
        negatives = _parse_text_list(args.negatives)
        producer = {"type": str(args.producer_type or "manual")}
        if args.producer_json:
            producer.update(json.loads(args.producer_json))
        row = trip_data.make_triplet_row(
            spec=spec,
            anchor_text=str(args.anchor),
            positive_texts=positives,
            negative_texts=negatives,
            producer=producer,
            reason=args.reason,
            corpus=args.corpus,
            anchor_key_override=args.anchor_key,
        )
        path = trip_data.append_triplet(row)
        claims_io.emit_json(
            {
                "ok": True,
                "row_id": row.row_id,
                "anchor_key": row.anchor_key,
                "split": row.split,
                "n_pos": len(row.positive_keys),
                "n_neg": len(row.negative_keys),
                "path": str(path),
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_triplets_import_neighbors(args: Namespace) -> int:
    """Import a neighbors CLI JSON payload as a triplet row (legacy)."""
    try:
        spec = trip_data.load_spec(str(args.intent))
        payload = claims_io.read_json(Path(args.from_path))
        items = payload.get("results") if isinstance(payload.get("results"), list) else [payload]
        added = 0
        skipped = 0
        for item in items:
            anchor_text = str(item.get("text") or item.get("anchor_text") or "")
            anchor_key = item.get("claim_key") or item.get("anchor_key")
            neighbors = item.get("neighbors") or []
            pos_texts = [str(x) for x in (item.get("positives") or [])]
            neg_texts = [str(x) for x in (item.get("negatives") or [])]
            pos_keys = item.get("positive_keys")
            neg_keys = item.get("negative_keys")
            if not pos_texts and not neg_texts and neighbors and args.auto_split:
                mid = max(1, len(neighbors) // 3)
                pos_texts = [str(n.get("text") or "") for n in neighbors[:mid]]
                neg_texts = [str(n.get("text") or "") for n in neighbors[-mid:]]
                pos_keys = [str(n.get("claim_key") or "") for n in neighbors[:mid]]
                neg_keys = [str(n.get("claim_key") or "") for n in neighbors[-mid:]]
            if not pos_texts and not neg_texts and not pos_keys and not neg_keys:
                skipped += 1
                continue
            producer = {"type": "neighbors_import"}
            try:
                row = trip_data.make_triplet_row(
                    spec=spec,
                    anchor_text=anchor_text,
                    positive_texts=pos_texts,
                    negative_texts=neg_texts,
                    producer=producer,
                    corpus=args.corpus,
                    anchor_key_override=str(anchor_key) if anchor_key else None,
                    positive_keys_override=list(pos_keys) if pos_keys else None,
                    negative_keys_override=list(neg_keys) if neg_keys else None,
                )
                trip_data.append_triplet(row)
                added += 1
            except FileExistsError:
                skipped += 1
        claims_io.emit_json({"ok": True, "added": added, "skipped": skipped})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_triplets_import(args: Namespace) -> int:
    """Import judged sample batch: {claim_key, pos, neg, reason} with 1-based neighbor indices."""
    try:
        from apps.claims.embedding import probes as probes_mod

        intent = str(args.intent)
        spec = trip_data.load_spec(intent)
        src = Path(args.from_path)
        rows_in = (
            claims_io.read_jsonl(src)
            if src.suffix == ".jsonl"
            else claims_io.read_json(src)
        )
        if isinstance(rows_in, dict):
            rows_in = rows_in.get("judgments") or rows_in.get("claims") or [rows_in]
        if not isinstance(rows_in, list):
            raise ValueError("triplets-import expects jsonl or a JSON array")

        run_id = getattr(args, "run_id", None) or getattr(args, "probe_run_id", None)
        neighbor_map: dict[str, list[str]] = {}
        if run_id:
            neighbor_map = probes_mod.latest_sample_neighbor_map(intent, str(run_id))

        # Optional sample payload with full neighbor texts
        sample_path = getattr(args, "sample", None)
        sample_by_key: dict[str, dict[str, Any]] = {}
        if sample_path:
            sample_payload = claims_io.read_json(Path(sample_path))
            for c in sample_payload.get("claims") or []:
                ck = str(c.get("claim_key") or "")
                if ck:
                    sample_by_key[ck] = c
                    if ck not in neighbor_map:
                        neighbor_map[ck] = [
                            str(n.get("claim_key") or "")
                            for n in (c.get("neighbors") or [])
                        ]

        producer_type = str(getattr(args, "producer_type", None) or "agent_label")
        corpus = getattr(args, "corpus", None)
        require_reason = producer_type == "agent_label"
        reasons: list[str] = []
        added = 0
        skipped = 0
        errors: list[str] = []

        for raw in rows_in:
            ck = str(raw.get("claim_key") or raw.get("anchor_key") or "")
            if not ck:
                skipped += 1
                continue
            reason = raw.get("reason")
            if require_reason and not (reason and str(reason).strip()):
                errors.append(f"{ck}: missing reason")
                skipped += 1
                continue
            if reason:
                reasons.append(str(reason).strip())

            sample_row = sample_by_key.get(ck) or {}
            neighbors = sample_row.get("neighbors") or []
            n_keys = neighbor_map.get(ck) or [
                str(n.get("claim_key") or "") for n in neighbors
            ]
            n_texts = [str(n.get("text") or "") for n in neighbors] if neighbors else None
            anchor_text = str(
                raw.get("text")
                or raw.get("anchor_text")
                or sample_row.get("text")
                or ""
            )
            try:
                pk, pt, nk, nt = probes_mod.resolve_pos_neg_from_judgment(
                    neighbor_keys=n_keys,
                    neighbor_texts=n_texts,
                    pos=raw.get("pos") or raw.get("positives") or [],
                    neg=raw.get("neg") or raw.get("negatives") or [],
                )
                # Prefer texts from judgment if provided as strings lists under positive_texts
                if raw.get("positive_texts"):
                    pt = [str(x) for x in raw["positive_texts"]]
                    pk = list(raw.get("positive_keys") or pk)
                if raw.get("negative_texts"):
                    nt = [str(x) for x in raw["negative_texts"]]
                    nk = list(raw.get("negative_keys") or nk)

                row = trip_data.make_triplet_row(
                    spec=spec,
                    anchor_text=anchor_text or ck,
                    positive_texts=pt,
                    negative_texts=nt,
                    producer={"type": producer_type},
                    reason=str(reason).strip() if reason else None,
                    corpus=str(corpus or raw.get("corpus") or ""),
                    anchor_key_override=ck,
                    positive_keys_override=pk,
                    negative_keys_override=nk,
                    shown_keys=list(n_keys),
                    run_tag=getattr(args, "model_tag", None)
                    or raw.get("run_tag"),
                    probe_run_id=str(run_id) if run_id else None,
                )
                trip_data.append_triplet(row)
                added += 1
            except FileExistsError:
                skipped += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{ck}: {exc}")
                skipped += 1

        if require_reason and reasons and len(set(reasons)) < len(reasons):
            claims_io.emit_json(
                {
                    "ok": False,
                    "error": "duplicate reasons in batch (require unique claim-specific reasons)",
                    "added": added,
                    "skipped": skipped,
                    "unique_reason_count": len(set(reasons)),
                    "n_reasons": len(reasons),
                }
            )
            return 1

        claims_io.emit_json(
            {
                "ok": True,
                "added": added,
                "skipped": skipped,
                "unique_reason_count": len(set(reasons)),
                "errors": errors[:20],
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_sample(args: Namespace) -> int:
    try:
        from apps.claims import filtering as filt
        from apps.claims import corpus as corpus_mod
        from apps.claims.cli import paths as path_helpers
        from apps.claims.embedding import probes as probes_mod

        corp = corpus_mod.get_corpus(str(args.corpus))
        allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
        model_tag = path_helpers.resolve_model_tag(args)
        run_dir = Path(args.run_dir) if getattr(args, "run_dir", None) else None
        payload = probes_mod.sample_triplet_batch(
            intent=str(args.intent),
            corpus=str(args.corpus),
            model_tag=model_tag,
            n=int(args.n),
            run_size=int(args.run_size) if args.run_size is not None else None,
            run_id=args.run_id,
            seed=int(args.seed),
            allow_keys=allow,
            filter_meta=filter_meta,
            run_dir=run_dir,
            neighbor_k=int(args.neighbor_k) if getattr(args, "neighbor_k", None) else None,
        )
        claims_io.emit_json(payload)
        if getattr(args, "human", False):
            _print_sample_human(payload.get("claims") or [])
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def _print_sample_human(claims: list[dict[str, Any]]) -> None:
    for i, c in enumerate(claims):
        if i:
            print()
        text = " ".join(str(c.get("text") or "").split())
        print(f"Anchor [key={c.get('claim_key')}]: {text}")
        print("Neighbors:")
        for n in c.get("neighbors") or []:
            nt = " ".join(str(n.get("text") or "").split())
            print(
                f"  {n.get('n')}. [{n.get('score', 0):.3f}] "
                f"key={n.get('claim_key')}  {nt}"
            )


def cmd_embedder_gold_sample(args: Namespace) -> int:
    """Sample anchors with neighbors for human gold (no probe injection)."""
    try:
        from apps.claims import filtering as filt
        from apps.claims import corpus as corpus_mod
        from apps.claims import selections as sel_mod
        from apps.claims.cli import paths as path_helpers
        from apps.claims.embedding import gold as gold_mod
        from apps.claims.embedding.triplet_neighbors import neighbors_for_claim_index
        import random

        intent = str(args.intent)
        corpus = str(args.corpus)
        n = int(args.n)
        if n < 1:
            raise ValueError("--n must be >= 1")
        spec = trip_data.load_spec(intent)
        k = int(getattr(args, "neighbor_k", None) or spec.neighbor_k)
        corp = corpus_mod.get_corpus(corpus)
        allow, filter_meta = filt.resolve_keys_for_args(args, corp.root)
        model_tag = path_helpers.resolve_model_tag(args)
        run_dir = Path(args.run_dir) if getattr(args, "run_dir", None) else corp.run_dir(model_tag)
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        claim_keys = sel_mod.claim_keys_from_index(index)
        exclude = gold_mod.gold_anchor_keys(intent, corpus)
        candidates = [
            i
            for i in range(len(vectors))
            if (claim_texts[i] or "").strip()
            and claim_keys[i]
            and claim_keys[i] not in exclude
            and (allow is None or claim_keys[i] in allow)
        ]
        rng = random.Random(int(args.seed))
        if len(candidates) <= n:
            picked = candidates
        else:
            picked = rng.sample(candidates, n)
        claims = []
        for idx in picked:
            neighbors = []
            for rank, (ni, score, text) in enumerate(
                neighbors_for_claim_index(
                    idx, vectors=vectors, claim_texts=claim_texts, top_k=k
                ),
                start=1,
            ):
                neighbors.append(
                    {
                        "n": rank,
                        "claim_key": claim_keys[ni] if ni < len(claim_keys) else "",
                        "text": text,
                        "score": float(score),
                        "idx": int(ni),
                    }
                )
            claims.append(
                {
                    "claim_key": claim_keys[idx],
                    "text": claim_texts[idx],
                    "idx": idx,
                    "neighbors": neighbors,
                }
            )
        payload = {
            "ok": True,
            "intent": intent,
            "corpus": corpus,
            "model_tag": model_tag,
            "run_dir": str(run_dir.resolve()),
            "neighbor_k": k,
            "n_returned": len(claims),
            "filter": filter_meta,
            "claims": claims,
        }
        claims_io.emit_json(payload)
        if getattr(args, "human", False):
            _print_sample_human(claims)
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_gold_add(args: Namespace) -> int:
    try:
        from apps.claims.embedding import gold as gold_mod

        spec = trip_data.load_spec(str(args.intent))
        pos = _parse_text_list(getattr(args, "positives", None))
        neg = _parse_text_list(getattr(args, "negatives", None))
        pos_keys = (
            json.loads(args.positive_keys)
            if getattr(args, "positive_keys", None)
            else None
        )
        neg_keys = (
            json.loads(args.negative_keys)
            if getattr(args, "negative_keys", None)
            else None
        )
        shown = (
            json.loads(args.shown_keys) if getattr(args, "shown_keys", None) else None
        )
        row = gold_mod.make_gold_row(
            intent=spec.name,
            spec_version=spec.version,
            anchor_text=str(args.anchor),
            positive_texts=pos,
            negative_texts=neg,
            corpus=str(args.corpus),
            reason=args.reason,
            claim_key_override=args.claim_key,
            positive_keys_override=[str(x) for x in pos_keys] if pos_keys else None,
            negative_keys_override=[str(x) for x in neg_keys] if neg_keys else None,
            shown_keys=[str(x) for x in shown] if shown else None,
            run_tag=getattr(args, "model_tag", None),
        )
        path = gold_mod.append_gold(row)
        claims_io.emit_json(
            {
                "ok": True,
                "claim_key": row.claim_key,
                "n_pos": len(row.positive_keys),
                "n_neg": len(row.negative_keys),
                "corpus": row.corpus,
                "path": str(path),
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_gold_import(args: Namespace) -> int:
    try:
        from apps.claims.embedding import gold as gold_mod

        spec = trip_data.load_spec(str(args.intent))
        src = Path(args.from_path)
        rows_in = (
            claims_io.read_jsonl(src)
            if src.suffix == ".jsonl"
            else claims_io.read_json(src)
        )
        if not isinstance(rows_in, list):
            raise ValueError("gold-import expects a jsonl list or JSON array")
        added = 0
        skipped = 0
        for raw in rows_in:
            try:
                row = gold_mod.make_gold_row(
                    intent=spec.name,
                    spec_version=int(raw.get("spec_version") or spec.version),
                    anchor_text=str(
                        raw.get("claim_text") or raw.get("anchor_text") or raw.get("text") or ""
                    ),
                    positive_texts=[str(x) for x in (raw.get("positive_texts") or [])],
                    negative_texts=[str(x) for x in (raw.get("negative_texts") or [])],
                    corpus=str(args.corpus or raw.get("corpus")),
                    reason=raw.get("reason"),
                    claim_key_override=(
                        str(raw.get("claim_key") or raw.get("anchor_key"))
                        if (raw.get("claim_key") or raw.get("anchor_key"))
                        else None
                    ),
                    positive_keys_override=(
                        [str(x) for x in raw["positive_keys"]]
                        if raw.get("positive_keys") is not None
                        else None
                    ),
                    negative_keys_override=(
                        [str(x) for x in raw["negative_keys"]]
                        if raw.get("negative_keys") is not None
                        else None
                    ),
                    shown_keys=[str(x) for x in (raw.get("shown_keys") or [])],
                    run_tag=raw.get("run_tag"),
                    labeled_at=raw.get("labeled_at"),
                )
                gold_mod.append_gold(row)
                added += 1
            except FileExistsError:
                skipped += 1
        claims_io.emit_json({"ok": True, "added": added, "skipped": skipped})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_gold_status(args: Namespace) -> int:
    try:
        from apps.claims.embedding import gold as gold_mod

        status = gold_mod.gold_status(str(args.intent), args.corpus)
        claims_io.emit_json({"ok": True, **status})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_gold_label(args: Namespace) -> int:
    """Interactive gold loop: sample anchors, prompt pos:/neg: index lines."""
    import secrets

    from apps.claims.embedding import gold as gold_mod

    try:
        from apps.claims import filtering as filt
        from apps.claims import corpus as corpus_mod
        from apps.claims import selections as sel_mod
        from apps.claims.cli import paths as path_helpers
        from apps.claims.embedding.triplet_neighbors import neighbors_for_claim_index
        import random

        intent = str(args.intent)
        corpus = str(args.corpus)
        n = int(args.n)
        seed = int(args.seed) if args.seed is not None else secrets.randbelow(2**31)
        spec = trip_data.load_spec(intent)
        k = int(getattr(args, "neighbor_k", None) or spec.neighbor_k)
        corp = corpus_mod.get_corpus(corpus)
        allow, _filter_meta = filt.resolve_keys_for_args(args, corp.root)
        model_tag = path_helpers.resolve_model_tag(args)
        run_dir = (
            Path(args.run_dir) if getattr(args, "run_dir", None) else corp.run_dir(model_tag)
        )
        vectors, index = claims_io.load_run_arrays(run_dir)
        claim_texts = claims_io.claim_texts_from_index(index)
        claim_keys = sel_mod.claim_keys_from_index(index)
        exclude = gold_mod.gold_anchor_keys(intent, corpus)
        candidates = [
            i
            for i in range(len(vectors))
            if (claim_texts[i] or "").strip()
            and claim_keys[i]
            and claim_keys[i] not in exclude
            and (allow is None or claim_keys[i] in allow)
        ]
        rng = random.Random(seed)
        picked = candidates if len(candidates) <= n else rng.sample(candidates, n)

        print(f"intent={intent} corpus={corpus} model_tag={model_tag} n={len(picked)}")
        print(
            "Enter pos: 1 3 4   and/or   neg: 8 11   "
            "(spaces or commas; empty = skip side; 's' skip anchor)"
        )
        if spec.instructions:
            print(f"instructions: {spec.instructions}")
        if spec.similarity_rubric:
            print(f"rubric: {spec.similarity_rubric}")

        added = 0
        for idx in picked:
            neighbors = []
            for rank, (ni, score, text) in enumerate(
                neighbors_for_claim_index(
                    idx, vectors=vectors, claim_texts=claim_texts, top_k=k
                ),
                start=1,
            ):
                neighbors.append(
                    {
                        "n": rank,
                        "claim_key": claim_keys[ni] if ni < len(claim_keys) else "",
                        "text": text,
                        "score": float(score),
                    }
                )
            print()
            print(f"Anchor [key={claim_keys[idx]}]: {claim_texts[idx]}")
            for n in neighbors:
                print(
                    f"  {n['n']}. [{n['score']:.3f}] {n['claim_key']}  "
                    f"{' '.join(str(n['text']).split())}"
                )
            line = input("pos:/neg: > ").strip()
            if line.lower() in {"s", "skip", "q", "quit"}:
                if line.lower() in {"q", "quit"}:
                    break
                continue
            pos_idx: list[int] = []
            neg_idx: list[int] = []
            # Allow "pos: 1 2  neg: 5 6" or just numbers for pos then ask neg
            if "pos:" in line.lower() or "neg:" in line.lower():
                parts = (
                    line.lower()
                    .replace(",", " ")
                    .replace(";", " ")
                    .replace("pos:", "|POS|")
                    .replace("neg:", "|NEG|")
                )
                cur = None
                for tok in parts.split():
                    if tok == "|POS|":
                        cur = "pos"
                    elif tok == "|NEG|":
                        cur = "neg"
                    elif tok.isdigit() and cur:
                        (pos_idx if cur == "pos" else neg_idx).append(int(tok))
            else:
                pos_idx = _parse_index_list(line)
                neg_line = input("neg: > ").strip()
                if neg_line.lower() not in {"s", "skip"}:
                    neg_idx = _parse_index_list(neg_line)

            from apps.claims.embedding import probes as probes_mod

            n_keys = [str(x["claim_key"]) for x in neighbors]
            n_texts = [str(x["text"]) for x in neighbors]
            pk, pt, nk, nt = probes_mod.resolve_pos_neg_from_judgment(
                neighbor_keys=n_keys,
                neighbor_texts=n_texts,
                pos=pos_idx,
                neg=neg_idx,
            )
            row = gold_mod.make_gold_row(
                intent=spec.name,
                spec_version=spec.version,
                anchor_text=claim_texts[idx],
                positive_texts=pt,
                negative_texts=nt,
                corpus=corpus,
                claim_key_override=claim_keys[idx],
                positive_keys_override=pk,
                negative_keys_override=nk,
                shown_keys=n_keys,
                run_tag=model_tag,
            )
            gold_mod.append_gold(row)
            added += 1
            print(f"  saved n_pos={len(pk)} n_neg={len(nk)}")
        claims_io.emit_json({"ok": True, "added": added, "seed": seed})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_dataset_freeze(args: Namespace) -> int:
    try:
        manifest = trip_data.freeze_dataset(
            str(args.intent),
            str(args.version),
            force=bool(args.force),
        )
        claims_io.emit_json({"ok": True, "manifest": manifest})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def _rows_to_anchors(rows: list[trip_data.TripletRow], *, pool: str) -> list[TripletAnchor]:
    out: list[TripletAnchor] = []
    for i, r in enumerate(rows):
        out.append(
            TripletAnchor(
                id=i,
                text=r.anchor_text,
                positives=[t for t in r.positive_texts if (t or "").strip()],
                negatives=[t for t in r.negative_texts if (t or "").strip()],
                pool=pool,
                too_hard=False,
                category="",
                family="",
            )
        )
    return out


def _gold_dev_anchors(intent: str, corpus: str | None) -> list[TripletAnchor]:
    from apps.claims.embedding import gold as gold_mod

    rows = gold_mod.read_gold(intent, corpus)
    out: list[TripletAnchor] = []
    for i, r in enumerate(rows):
        pos = [t for t in r.positive_texts if (t or "").strip()]
        neg = [t for t in r.negative_texts if (t or "").strip()]
        if not pos or not neg:
            continue
        out.append(
            TripletAnchor(
                id=i,
                text=r.claim_text,
                positives=pos,
                negatives=neg,
                pool="dev",
                too_hard=False,
                category="",
                family="",
            )
        )
    return out


def cmd_embedder_train(args: Namespace) -> int:
    try:
        from apps.claims.embedding import train as train_mod
        from apps.claims import provenance as prov

        intent = str(args.intent)
        dataset_version = str(args.dataset)
        model_version = str(args.version)
        spec = trip_data.load_spec(intent)
        ds = trip_data.load_dataset_manifest(intent, dataset_version)
        train_rows = trip_data.dataset_rows(intent, dataset_version, split="train")
        # Prefer gold as dev when available; fall back to empty
        corpus = getattr(args, "corpus", None)
        if corpus:
            dev_anchors = _gold_dev_anchors(intent, str(corpus))
        else:
            # Use any gold corpus
            from apps.claims.embedding import gold as gold_mod

            corpora = gold_mod.list_gold_corpora(intent)
            dev_anchors = _gold_dev_anchors(intent, corpora[0]) if corpora else []
        out_dir = emb_reg.model_dir(intent, model_version)
        if out_dir.exists():
            raise FileExistsError(f"Model version already exists (immutable): {out_dir}")

        train_anchors = _rows_to_anchors(train_rows, pool="training")
        train_kw = _train_encode_kwargs(args)
        result = train_mod.run(
            base_model_id=str(args.base_model),
            output_name=model_version,
            train_anchors=train_anchors,
            dev_anchors=dev_anchors,
            loss=str(args.loss),
            batch_size=int(args.batch_size),
            learning_rate=float(args.learning_rate),
            epochs=int(args.epochs),
            models_root=emb_reg.intent_models_dir(intent),
            allow_overwrite=False,
            **train_kw,
        )
        metrics = {
            "best_epoch": result.best_epoch,
            "best_dev_acc": result.best_dev_acc,
            "dev_acc_per_epoch": result.dev_acc_per_epoch,
            "wall_seconds": result.wall_seconds,
            "n_train": len(train_rows),
            "n_dev_gold": len(dev_anchors),
            "lora": result.lora,
        }
        hyper = dict(result.hyperparameters)
        hyper.setdefault("batch_size", int(args.batch_size))
        hyper.setdefault("learning_rate", float(args.learning_rate))
        hyper.setdefault("epochs", int(args.epochs))
        manifest = emb_reg.write_model_manifest(
            intent,
            model_version,
            base_model_id=str(args.base_model),
            base_model_revision=None,
            dataset_version=dataset_version,
            dataset_hash=str(ds.get("triplets_hash") or ""),
            spec_hash=str(ds.get("spec_hash") or prov.sha256_json(spec.to_dict())),
            loss=str(args.loss),
            hyperparameters=hyper,
            metrics=metrics,
            loss_curve=result.loss_curve,
        )
        if args.set_active:
            emb_reg.set_alias(intent, model_version, alias="active")
        claims_io.emit_json(
            {
                "ok": True,
                "output_dir": result.output_dir,
                "manifest": manifest,
                **metrics,
                "loss_curve": result.loss_curve,
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_train_compare(args: Namespace) -> int:
    try:
        from apps.claims.embedding import compare as cmp_mod
        from apps.claims.embedding import gold as gold_mod
        from apps.claims.embedding import train as train_mod
        from apps.claims import provenance as prov

        intent = str(args.intent)
        dataset_version = str(args.dataset)
        version = str(args.version)
        corpus = str(args.corpus)
        gold_mod.require_gold_gate(intent, corpus)
        status = gold_mod.gold_status(intent, corpus)
        corp_status = status["corpora"].get(corpus) or {}
        if int(corp_status.get("n_pairwise") or 0) < 1:
            raise ValueError(
                "Gold has no pos+neg pairwise anchors; refuse train-compare --set-active"
            )

        spec = trip_data.load_spec(intent)
        ds = trip_data.load_dataset_manifest(intent, dataset_version)
        train_rows = trip_data.dataset_rows(intent, dataset_version, split="train")
        train_anchors = _rows_to_anchors(train_rows, pool="training")
        dev_anchors = _gold_dev_anchors(intent, corpus)

        results: dict[str, Any] = {}
        for suffix, loss in (
            ("mnrl", "MultipleNegativesRankingLoss"),
            ("triplet", "TripletLoss"),
        ):
            model_version = f"{version}_{suffix}"
            out_dir = emb_reg.model_dir(intent, model_version)
            if out_dir.exists() and not getattr(args, "force", False):
                raise FileExistsError(f"Model version already exists: {out_dir}")
            train_kw = _train_encode_kwargs(args)
            result = train_mod.run(
                base_model_id=str(args.base_model),
                output_name=model_version,
                train_anchors=train_anchors,
                dev_anchors=dev_anchors,
                loss=loss,
                batch_size=int(args.batch_size),
                learning_rate=float(args.learning_rate),
                epochs=int(args.epochs),
                models_root=emb_reg.intent_models_dir(intent),
                allow_overwrite=bool(getattr(args, "force", False)),
                **train_kw,
            )
            gold_metrics = cmp_mod.gold_pairwise_metrics(
                intent=intent,
                corpus=corpus,
                model_id=result.output_dir,
                **_gold_eval_kwargs(result.output_dir, args),
            )
            metrics = {
                "best_epoch": result.best_epoch,
                "best_dev_acc": result.best_dev_acc,
                "wall_seconds": result.wall_seconds,
                "n_train": len(train_rows),
                "lora": result.lora,
                **gold_metrics,
            }
            hyper = dict(result.hyperparameters)
            hyper.setdefault("batch_size", int(args.batch_size))
            hyper.setdefault("learning_rate", float(args.learning_rate))
            hyper.setdefault("epochs", int(args.epochs))
            emb_reg.write_model_manifest(
                intent,
                model_version,
                base_model_id=str(args.base_model),
                base_model_revision=None,
                dataset_version=dataset_version,
                dataset_hash=str(ds.get("triplets_hash") or ""),
                spec_hash=str(ds.get("spec_hash") or prov.sha256_json(spec.to_dict())),
                loss=loss,
                hyperparameters=hyper,
                metrics=metrics,
                loss_curve=result.loss_curve,
            )
            results[suffix] = {
                "version": model_version,
                "loss": loss,
                "output_dir": result.output_dir,
                "metrics": metrics,
            }

        pick = cmp_mod.pick_train_compare_winner(
            results["mnrl"]["metrics"],
            results["triplet"]["metrics"],
        )
        winner_version = results[pick["winner"]]["version"]
        comparison = {
            "intent": intent,
            "corpus": corpus,
            "dataset": dataset_version,
            "base_model": str(args.base_model),
            "created_at": prov.utc_now(),
            "mnrl": results["mnrl"],
            "triplet": results["triplet"],
            **pick,
            "winner_version": winner_version,
        }
        out_dir = claims_io.embedder_eval_dir() / (
            f"compare__{prov.safe_slug(intent)}__{prov.safe_slug(version)}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        claims_io.write_json(out_dir / "comparison.json", comparison)

        if args.set_active:
            if int(corp_status.get("n_pairwise") or 0) < 1:
                raise ValueError("Cannot --set-active without gold pairwise anchors")
            emb_reg.set_alias(intent, winner_version, alias="active")
            comparison["active"] = winner_version

        claims_io.emit_json({"ok": True, "out_dir": str(out_dir), **comparison})
        if getattr(args, "human", False):
            print(
                f"train-compare  winner={pick['winner']}  "
                f"mnrl_pass={pick['mnrl_pass_pct']:.3f}  "
                f"triplet_pass={pick['triplet_pass_pct']:.3f}  "
                f"active={comparison.get('active')}"
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_eval(args: Namespace) -> int:
    try:
        from apps.claims.embedding import compare as cmp_mod
        from apps.claims import provenance as prov

        intent = str(args.intent)
        corpus = str(args.corpus)
        model_ref = emb_reg.resolve_model_ref(str(args.model))
        model_id = str(model_ref)
        metrics = cmp_mod.gold_pairwise_metrics(
            intent=intent,
            corpus=corpus,
            model_id=model_id,
            **_gold_eval_kwargs(model_id, args),
        )
        stamp = (
            args.name
            or f"{prov.safe_slug(intent)}__{prov.safe_slug(Path(model_id).name)}__{prov.safe_slug(corpus)}"
        )
        out_dir = claims_io.embedder_eval_dir() / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "intent": intent,
            "corpus": corpus,
            "model": model_id,
            "created_at": prov.utc_now(),
            **metrics,
        }
        claims_io.write_json(out_dir / "metrics.json", payload)
        claims_io.emit_json({"ok": True, "out_dir": str(out_dir), **payload})
        if getattr(args, "human", False):
            print(
                f"embedder-eval  pass={metrics.get('pass_pct'):.3f}  "
                f"n_scored={metrics.get('n_scored')}  "
                f"n_empty={metrics.get('n_empty_eval')}  "
                f"mean_margin={metrics.get('mean_margin'):.4f}"
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_agent_eval(args: Namespace) -> int:
    try:
        from apps.claims.embedding import compare as cmp_mod
        from apps.claims import provenance as prov

        payload = cmp_mod.evaluate_agent_triplets(
            intent=str(args.intent),
            corpus=str(args.corpus),
            run_id=args.run_id,
            min_probes=int(getattr(args, "min_probes", 1) or 1),
        )
        stamp = (
            f"agent__{prov.safe_slug(args.intent)}__{prov.safe_slug(args.corpus)}"
            + (f"__{prov.safe_slug(args.run_id)}" if args.run_id else "")
        )
        out_dir = claims_io.embedder_eval_dir() / stamp
        out_dir.mkdir(parents=True, exist_ok=True)
        claims_io.write_json(out_dir / "metrics.json", {**payload, "out_dir": str(out_dir)})
        claims_io.emit_json({**payload, "out_dir": str(out_dir)})
        if getattr(args, "human", False) and payload.get("metrics"):
            m = payload["metrics"]
            print(
                f"agent-eval  intent={args.intent}  corpus={args.corpus}  "
                f"n_probes={m.get('n_probes')}  "
                f"pos_j={m.get('pos_jaccard')}  neg_j={m.get('neg_jaccard')}  "
                f"mean_j={m.get('mean_jaccard')}"
            )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_embedder_promote(args: Namespace) -> int:
    try:
        payload = emb_reg.set_alias(
            str(args.intent),
            str(args.version),
            alias=str(args.alias or "active"),
            force=True,
        )
        claims_io.emit_json({"ok": True, **payload})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0
