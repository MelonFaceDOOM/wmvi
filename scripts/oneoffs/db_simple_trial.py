from __future__ import annotations

from db.db import init_pool, close_pool, getcursor


def _print_one(label: str, sql: str, params: tuple = ()) -> None:
    with getcursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    print(f"{label}: {row}")


def _identity_mode(schema: str, table: str, column: str = "id") -> str:
    with getcursor() as cur:
        cur.execute(
            """
            SELECT identity_generation
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table, column),
        )
        row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else "(none)"


def _try_explicit_id_insert(schema: str, table: str, id_value: int, extra_cols_sql: str, extra_vals_sql: str) -> None:
    """
    Attempts an explicit-id insert and rolls it back (so DB is unchanged).
    extra_cols_sql / extra_vals_sql let us satisfy NOT NULL columns.

    Example:
      extra_cols_sql="name, type"
      extra_vals_sql="%s, %s"
    """
    fq = f"{schema}.{table}"
    with getcursor() as cur:
        cur.execute("BEGIN")
        try:
            # Ensure we won't collide with an existing row.
            cur.execute(f"SELECT 1 FROM {fq} WHERE id = %s", (id_value,))
            if cur.fetchone() is not None:
                # If collision, just choose a higher id and retry once.
                cur.execute(f"SELECT COALESCE(MAX(id), 0) + 1000000 FROM {fq}")
                (id_value,) = cur.fetchone()

            sql = f"INSERT INTO {fq} (id, {extra_cols_sql}) VALUES (%s, {
                extra_vals_sql})"
            # Build params: id + extras
            if fq == "taxonomy.vaccine_term":
                params = (id_value, f"__id_test_{id_value}", "test")
            elif fq == "podcasts.shows":
                params = (id_value, f"__id_test_{id_value}", None)
            else:
                raise RuntimeError(f"Unhandled table {fq}")

            cur.execute(sql, params)
            # If BY DEFAULT, this should succeed.
            cur.execute("ROLLBACK")
            print(f"✅ explicit id insert works for {fq} (id={id_value})")
        except Exception as e:
            cur.execute("ROLLBACK")
            raise RuntimeError(f"❌ explicit id insert failed for {
                               fq}: {type(e).__name__}: {e}") from e


def _sequence_sanity(schema: str, table: str, column: str = "id") -> None:
    """
    Prints pg_get_serial_sequence + last_value vs max(id).
    (Helps catch 'sequence behind max id' issues after copying.)
    """
    fq = f"{schema}.{table}"
    with getcursor() as cur:
        cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (fq, column))
        (seq,) = cur.fetchone()
        if not seq:
            print(f"[seq] {fq}.{column}: (no sequence)")
            return

        cur.execute(f"SELECT COALESCE(MAX({column}), 0) FROM {fq}")
        (mx,) = cur.fetchone()

        # last_value exists on sequences; is_called helps interpret next nextval
        cur.execute(f"SELECT last_value, is_called FROM {seq}")
        last_value, is_called = cur.fetchone()

    print(f"[seq] {fq}.{column}: seq={seq} max_id={
          mx} last_value={last_value} is_called={is_called}")


def main() -> None:
    init_pool(prefix="DEV")
    try:
        print("=== Basic ranges ===")
        _print_one(
            "sm.post_registry (min,max,count)",
            "SELECT COALESCE(MIN(id),0), COALESCE(MAX(id),0), COUNT(*) FROM sm.post_registry",
        )

        print("\n=== Identity mode checks (should be 'BY DEFAULT' after migration 003) ===")
        vt_mode = _identity_mode("taxonomy", "vaccine_term", "id")
        sh_mode = _identity_mode("podcasts", "shows", "id")
        print(f"taxonomy.vaccine_term.id identity_generation: {vt_mode}")
        print(f"podcasts.shows.id identity_generation: {sh_mode}")

        if vt_mode.upper() != "BY DEFAULT":
            raise RuntimeError(
                f"taxonomy.vaccine_term.id is not BY DEFAULT (got {vt_mode})")
        if sh_mode.upper() != "BY DEFAULT":
            raise RuntimeError(
                f"podcasts.shows.id is not BY DEFAULT (got {sh_mode})")

        print("\n=== Explicit-id insert sanity (rolled back; DB unchanged) ===")
        _try_explicit_id_insert(
            "taxonomy",
            "vaccine_term",
            id_value=2_000_000_000,
            extra_cols_sql="name, type",
            extra_vals_sql="%s, %s",
        )
        _try_explicit_id_insert(
            "podcasts",
            "shows",
            id_value=2_000_000_000,
            extra_cols_sql="title, rss_url",
            extra_vals_sql="%s, %s",
        )

        print("\n=== Sequence sanity (important after cloning) ===")
        _sequence_sanity("taxonomy", "vaccine_term", "id")
        _sequence_sanity("podcasts", "shows", "id")

        print("\n✅ All sanity checks passed.")

    finally:
        close_pool()


if __name__ == "__main__":
    main()
