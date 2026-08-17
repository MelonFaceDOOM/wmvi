"""CLI wrapper for claims run export/import (replaces embedding_lab.transfer)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def main(argv: list[str] | None = None) -> int:
    from apps.claims import runs_xfer

    ap = argparse.ArgumentParser(description="Export/import claims embed runs (zip)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    exp = sub.add_parser("export", help="Zip a run directory")
    exp.add_argument("--run-dir", type=Path, required=True)
    exp.add_argument("--out", type=Path, required=True)

    imp = sub.add_parser("import", help="Extract a run zip under data/runs/")
    imp.add_argument("--from", dest="from_zip", type=Path, required=True)
    imp.add_argument("--run-name", type=str, required=True)
    imp.add_argument("--force", action="store_true")

    args = ap.parse_args(argv)
    if args.cmd == "export":
        result = runs_xfer.export_run(run_dir=Path(args.run_dir), out_zip=Path(args.out))
        print(result)
        return 0
    if args.cmd == "import":
        result = runs_xfer.import_run(
            from_zip=Path(args.from_zip),
            run_name=str(args.run_name),
            force=bool(args.force),
        )
        print(result)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
