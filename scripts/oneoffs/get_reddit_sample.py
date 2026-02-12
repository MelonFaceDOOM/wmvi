from __future__ import annotations

import argparse
from pathlib import Path

from db.db import init_pool, close_pool, getcursor

SQL = """
SELECT id
FROM sm.reddit_submission
WHERE
    tsv_en @@ plainto_tsquery('english', 'autism')
 OR tsv_en @@ plainto_tsquery('english', 'mmr')
 OR tsv_en @@ plainto_tsquery('english', 'measles')
 OR tsv_en @@ plainto_tsquery('english', 'wakefield')
 OR tsv_en @@ plainto_tsquery('english', 'thimerosal')
ORDER BY created_at_ts DESC
LIMIT %s
"""


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export sm.reddit_submission ids where tsv_en matches autism/mmr/measles to a newline-separated txt file."
    )
    ap.add_argument("--prod", action="store_true",
                    help="Use prod DB (default: dev).")
    ap.add_argument("--out", default="submission_ids.txt",
                    help="Output txt path (default: submission_ids.txt).")
    ap.add_argument("--limit", type=int, default=500_000,
                    help="Max ids to export (default: 500000).")
    args = ap.parse_args()

    init_pool(prefix="prod" if args.prod else "dev")
    out_path = Path(args.out)

    try:
        with getcursor() as cur:
            cur.execute(SQL, (args.limit,))
            ids = [row[0] for row in cur.fetchall()]

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join(ids) + ("\n" if ids else ""), encoding="utf-8")

        print(f"Wrote {len(ids)} submission ids to {out_path}")
    finally:
        close_pool()


if __name__ == "__main__":
    main()
