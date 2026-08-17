"""CLI for annotations and selections."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from apps.claims import annotations as ann_mod
from apps.claims import io as claims_io
from apps.claims import selections as sel_mod
from apps.claims.cli import paths as path_helpers


def _corpus_root(args: Namespace) -> Path:
    corpus = path_helpers.require_corpus(args)
    return corpus.root


def cmd_annotations_list(args: Namespace) -> int:
    try:
        root = _corpus_root(args)
        metas = ann_mod.list_annotations(root)
        claims_io.emit_json(
            {
                "ok": True,
                "corpus": str(getattr(args, "corpus")),
                "annotations": [m.to_dict() for m in metas],
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_annotations_show(args: Namespace) -> int:
    try:
        root = _corpus_root(args)
        ann = ann_mod.read_annotation(root, str(args.name))
        items = list(ann.values.items())
        # Optional value/range filter for browsing
        eq = getattr(args, "eq", None)
        low = getattr(args, "low", None)
        high = getattr(args, "high", None)
        if eq is not None or low is not None or high is not None:
            from apps.claims import filtering as filt

            pred = filt.parse_predicate_args(eq=eq, low=low, high=high)
            items = [(k, v) for k, v in items if filt.matches(pred, v)]
        sample = items[: int(args.limit)]
        claims_io.emit_json(
            {
                "ok": True,
                "meta": ann.meta.to_dict(),
                "n_matched": len(items),
                "sample": [{"k": k, "v": v} for k, v in sample],
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_annotations_diff(args: Namespace) -> int:
    """Compare two annotations (or annotation vs gold label intent keys)."""
    try:
        root = _corpus_root(args)
        left = ann_mod.read_annotation(root, str(args.left))
        right = ann_mod.read_annotation(root, str(args.right))
        keys = set(left.values) | set(right.values)
        only_left = sorted(set(left.values) - set(right.values))
        only_right = sorted(set(right.values) - set(left.values))
        both = set(left.values) & set(right.values)
        disagree = sorted(k for k in both if left.values[k] != right.values[k])
        claims_io.emit_json(
            {
                "ok": True,
                "left": left.name,
                "right": right.name,
                "n_left": len(left.values),
                "n_right": len(right.values),
                "n_only_left": len(only_left),
                "n_only_right": len(only_right),
                "n_disagree": len(disagree),
                "sample_disagree": [
                    {"k": k, "left": left.values[k], "right": right.values[k]}
                    for k in disagree[: int(args.limit)]
                ],
                "sample_only_left": only_left[: int(args.limit)],
                "sample_only_right": only_right[: int(args.limit)],
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_annotations_rm(args: Namespace) -> int:
    try:
        root = _corpus_root(args)
        ann_mod.remove_annotation(root, str(args.name))
        claims_io.emit_json({"ok": True, "removed": str(args.name)})
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_select(args: Namespace) -> int:
    """Build a selection from an annotation + threshold predicate."""
    try:
        root = _corpus_root(args)
        ann = ann_mod.read_annotation(root, str(args.annotation))
        low = float(args.low) if args.low is not None else None
        high = float(args.high) if args.high is not None else None
        if low is None and high is None:
            raise ValueError("Provide --low and/or --high")
        sel = sel_mod.from_threshold(
            ann,
            name=str(args.name),
            low=low,
            high=high,
            inclusive=not bool(args.exclusive),
        )
        path = sel_mod.write_selection(root, sel, force=bool(args.force))
        claims_io.emit_json(
            {
                "ok": True,
                "name": sel.name,
                "count": len(sel.keys),
                "from_annotation": sel.from_annotation,
                "predicate": sel.predicate,
                "path": str(path),
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0


def cmd_selections_list(args: Namespace) -> int:
    try:
        root = _corpus_root(args)
        sels = sel_mod.list_selections(root)
        claims_io.emit_json(
            {
                "ok": True,
                "corpus": str(getattr(args, "corpus")),
                "selections": [
                    {
                        "name": s.name,
                        "scope": s.scope,
                        "count": len(s.keys),
                        "from_annotation": s.from_annotation,
                        "predicate": s.predicate,
                    }
                    for s in sels
                ],
            }
        )
    except Exception as exc:  # noqa: BLE001
        claims_io.emit_json({"error": str(exc)})
        return 1
    return 0
