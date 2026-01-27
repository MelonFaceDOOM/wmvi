from __future__ import annotations

from datetime import date, timedelta
from typing import List, Tuple

from dotenv import load_dotenv

from db.db import init_pool, close_pool, getcursor

load_dotenv()


SQL = """
SELECT date_entered::date AS day, COUNT(*)::bigint AS n
FROM sm.reddit_submission
WHERE date_entered >= %s::date
GROUP BY 1
ORDER BY 1 DESC
LIMIT 7;
"""


def fetch_counts(prefix: str) -> List[Tuple[date, int]]:
    init_pool(prefix=prefix)
    try:
        # wide enough to find 7 "most recent days entered"
        start_day = date.today() - timedelta(days=60)
        with getcursor() as cur:
            cur.execute(SQL, (start_day,))
            rows = cur.fetchall()
        return [(r[0], int(r[1])) for r in rows]
    finally:
        close_pool()


def print_table(prefix: str, rows: List[Tuple[date, int]]) -> None:
    print("=" * 72)
    print(f"{prefix}: sm.reddit_submission rows for 7 most recent days (by date_entered)")
    print("=" * 72)
    if not rows:
        print("(no rows found)")
        return
    for day, n in rows:
        print(f"{day.isoformat()}  {n}")


def main() -> None:
    for prefix in ("DEV", "PROD"):
        try:
            rows = fetch_counts(prefix)
            print_table(prefix, rows)
        except Exception as e:
            print("=" * 72)
            print(f"{prefix}: ERROR: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
