"""most data sources are automatically entered into post_registry via triggers
as of now, yt/podcast episodes can be entered using the following utility:"""


def ensure_post_registered(
    cur,
    *,
    platform: str,
    key1: str,
    key2: str = "",
) -> None:
    cur.execute(
        """
        INSERT INTO sm.post_registry (platform, key1, key2)
        VALUES (%s, %s, %s)
        ON CONFLICT (platform, key1, key2) DO NOTHING
        """,
        (platform, key1, key2),
    )
