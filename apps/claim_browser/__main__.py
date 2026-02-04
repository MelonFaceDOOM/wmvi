from __future__ import annotations

import os
import sys
from pathlib import Path


"""
Run from repo root with:

  # dev (default)
  python -m apps.claim_browser

  # prod
  python -m apps.claim_browser --prod

to add arguments, use this syntax:
python -m apps.claim_browser -- --prefix DEV

Note: the double -- matters: first belongs to the launcher,
second is the Streamlit convention for “pass-through args to the script”.)
"""


def main() -> None:
    # repo root: .../wmvi
    root = Path(__file__).resolve().parents[2]
    app_file = root / "apps" / "claim_browser" / "claim_browser_app.py"

    # Ensure top-level imports like `from db.db import ...` work
    os.environ["PYTHONPATH"] = str(root) + (
        os.pathsep +
        os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else ""
    )

    # Run: python -m streamlit run <app_file> -- <your args>
    # (anything after `--` is passed to your Streamlit script)
    cmd = [
        sys.executable, "-m", "streamlit", "run", str(app_file),
        "--server.runOnSave=true",
    ]

    # Forward any extra CLI args you passed to this module into the app
    if len(sys.argv) > 1:
        cmd += ["--"] + sys.argv[1:]

    raise SystemExit(_run(cmd))


def _run(cmd: list[str]) -> int:
    import subprocess
    return subprocess.call(cmd)


if __name__ == "__main__":
    main()
