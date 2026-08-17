"""Migrate labeler labels/specs off hash-based train/eval splits.

- Strip ``split`` from every row in labels.jsonl
- Strip ``eval_frac`` / ``split_seed`` from specs; add gold defaults
- Bump spec version to 3 when still below 3

Leaves existing datasets/ and models/ untouched (legacy).

Usage (repo root):

  python -m apps.claims.scripts.migrate_eval_split --dry-run
  python -m apps.claims.scripts.migrate_eval_split
"""

from __future__ import annotations

import argparse
from pathlib import Path

from apps.claims import io as claims_io
from apps.claims import provenance as prov


def _migrate_intent(intent_dir: Path, *, dry_run: bool) -> dict:
    name = intent_dir.name
    spec_path = intent_dir / claims_io.SPEC_FILE
    labels_path = intent_dir / claims_io.LABELS_FILE
    report: dict = {"intent": name, "spec": None, "labels": None}

    if spec_path.is_file():
        spec = claims_io.read_json(spec_path)
        changed = False
        for key in ("eval_frac", "split_seed"):
            if key in spec:
                del spec[key]
                changed = True
        if "min_gold_total" not in spec:
            spec["min_gold_total"] = 50
            changed = True
        if "min_gold_per_class" not in spec:
            spec["min_gold_per_class"] = 10
            changed = True
        if "probe_target" not in spec:
            spec["probe_target"] = 25
            changed = True
        ver = int(spec.get("version") or 1)
        if ver < 3:
            spec["version"] = 3
            changed = True
        report["spec"] = {"changed": changed, "version": spec.get("version")}
        if changed and not dry_run:
            claims_io.write_json(spec_path, spec)

    if labels_path.is_file():
        rows = claims_io.read_jsonl(labels_path)
        n_split = 0
        out_rows = []
        for row in rows:
            if "split" in row:
                n_split += 1
                row = {k: v for k, v in row.items() if k != "split"}
            out_rows.append(row)
        report["labels"] = {"n_rows": len(rows), "n_had_split": n_split}
        if n_split and not dry_run:
            # rewrite file
            labels_path.write_text("", encoding="utf-8")
            for row in out_rows:
                claims_io.append_jsonl(labels_path, row)

    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--intent",
        action="append",
        default=None,
        help="Limit to intent slug(s); default all under training/labelers/",
    )
    args = ap.parse_args(argv)

    root = claims_io.labeler_training_dir()
    if not root.is_dir():
        print(f"No labeler training dir at {root}")
        return 0

    intents = sorted(
        p for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")
    )
    if args.intent:
        wanted = {prov.safe_slug(x) for x in args.intent}
        intents = [p for p in intents if p.name in wanted]

    reports = []
    for intent_dir in intents:
        reports.append(_migrate_intent(intent_dir, dry_run=bool(args.dry_run)))

    claims_io.emit_json(
        {
            "ok": True,
            "dry_run": bool(args.dry_run),
            "n_intents": len(reports),
            "reports": reports,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
