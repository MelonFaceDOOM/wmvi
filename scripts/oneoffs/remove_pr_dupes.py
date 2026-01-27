from db.db import init_pool, getcursor, close_pool

init_pool(prefix="dev")
with getcursor(commit=True) as cur:
    cur.execute(
        """
        WITH ranked AS (

            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY platform, key1, key2
                    ORDER BY id
                ) AS rn
            FROM sm.post_registry
        )
        DELETE FROM sm.post_registry pr
        USING ranked r
        WHERE pr.id = r.id
          AND r.rn > 1;
    """
    )

close_pool()
