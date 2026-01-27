import uuid
from db.db import getcursor

"""
NOTE: The actual migration call is done in conftest.py,
since it's used in other tests as well
"""


def test_migration_insert_and_queries_with_triggers(prepared_fresh_db):
    # ---------- Reference data ----------
    # Scrape job
    with getcursor() as cur:
        cur.execute(
            "INSERT INTO scrape.job(name, description, platforms, status) VALUES (%s,%s,%s,%s) RETURNING id",
            ("test-job", "pytest job", ["tweet", "youtube_comment"], "completed"),
        )
        (job_id,) = cur.fetchone()

    # Taxonomy term
    with getcursor() as cur:
        cur.execute(
            "INSERT INTO taxonomy.vaccine_term(name, type) VALUES (%s,%s) "
            "ON CONFLICT (name) DO NOTHING RETURNING id",
            ("mrna vaccine", "vaccine"),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM taxonomy.vaccine_term WHERE name=%s", ("mrna vaccine",))
            (term_id,) = cur.fetchone()
        else:
            (term_id,) = row

    # ---------- Insert a tweet (single-key source) ----------
    tw_id = 10_000_000_000 + int(uuid.uuid4().int % 1_000_000)
    with getcursor() as cur:
        cur.execute(
            """
            INSERT INTO sm.tweet(id, source, conversation_id, created_at_ts, tweet_text, filtered_text,
                                 retweet_count, like_count, reply_count, quote_count, is_en)
            VALUES (%s,'api',%s, now() - interval '1 day', %s, %s, 1, 2, 0, 0, true)
            ON CONFLICT (id) DO NOTHING
            """,
            (tw_id, tw_id, "Hello mRNA world", "Hello mRNA world"),
        )

    # Trigger should have auto-registered the tweet in post_registry
    with getcursor() as cur:
        cur.execute(
            "SELECT id FROM sm.post_registry WHERE platform='tweet' AND key1=%s AND key2 IS NULL",
            (str(tw_id),),
        )
        (tw_post_id,) = cur.fetchone()

    # Link tweet to scrape job and term
    with getcursor() as cur:
        cur.execute(
            "INSERT INTO scrape.post_scrape(scrape_job_id, post_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (job_id, tw_post_id),
        )
        cur.execute(
            "INSERT INTO matches.post_term_match(post_id, term_id, matcher_version, confidence) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (tw_post_id, term_id, "rule_v1", 0.95),
        )

    # ---------- Insert a YouTube comment (composite-key source) ----------
    vid = "v_" + uuid.uuid4().hex[:8]
    cid = "c_" + uuid.uuid4().hex[:8]

    with getcursor() as cur:
        # Parent video
        cur.execute(
            """
            INSERT INTO sm.youtube_video(video_id, url, title, filtered_text, created_at_ts, channel_id, channel_title)
            VALUES (%s, %s, %s, %s, now() - interval '2 days', %s, %s)
            ON CONFLICT (video_id) DO NOTHING
            """,
            (vid, f"https://youtube.com/watch?v={vid}", "Title about mRNA", "Title about mRNA", "ch_1", "Chan"),
        )
        # Comment
        cur.execute(
            """
            INSERT INTO sm.youtube_comment(video_id, comment_id, comment_url, text, filtered_text, created_at_ts, like_count, raw)
            VALUES (%s,%s,%s,%s,%s, now() - interval '12 hours', 3, '{}'::jsonb)
            ON CONFLICT (video_id, comment_id) DO NOTHING
            """,
            (vid, cid, f"https://www.youtube.com/watch?v={vid}&lc={cid}", "mRNA comment text", "mRNA comment text"),
        )

    # Trigger should have auto-registered the YT comment
    with getcursor() as cur:
        cur.execute(
            "SELECT id FROM sm.post_registry WHERE platform='youtube_comment' AND key1=%s AND key2=%s",
            (vid, cid),
        )
        (yc_post_id,) = cur.fetchone()

    # Link YT comment to scrape job (leave unmatched to term)
    with getcursor() as cur:
        cur.execute(
            "INSERT INTO scrape.post_scrape(scrape_job_id, post_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (job_id, yc_post_id),
        )

    # ---------- Assertions / Queries ----------
    # 1) Unified view returns both posts
    with getcursor() as cur:
        cur.execute(
            "SELECT platform, native_id, text FROM sm.posts_unified WHERE post_id IN (%s,%s) ORDER BY platform",
            (tw_post_id, yc_post_id),
        )
        rows = cur.fetchall()
    platforms = {r[0] for r in rows}
    assert platforms == {"tweet", "youtube_comment"}
    assert any("Hello" in r[2] for r in rows)
    assert any("comment text" in r[2] for r in rows)

    # 2) Fetch all posts for the scrape job via join
    with getcursor() as cur:
        cur.execute(
            """
            SELECT u.platform, u.native_id
            FROM scrape.post_scrape ps
            JOIN sm.posts_unified u ON u.post_id = ps.post_id
            WHERE ps.scrape_job_id = %s
            ORDER BY u.created_at_ts
            """,
            (job_id,),
        )
        by_job = cur.fetchall()
    assert len(by_job) >= 2
    assert {p for (p, _) in by_job} == {"tweet", "youtube_comment"}

    # 3) Term-centric: find posts matched to our taxonomy term (should be only the tweet)
    with getcursor() as cur:
        cur.execute(
            """
            SELECT u.platform, u.native_id, u.text
            FROM matches.post_term_match m
            JOIN sm.posts_unified u ON u.post_id = m.post_id
            WHERE m.term_id = %s
            """,
            (term_id,),
        )
        matched = cur.fetchall()
    assert matched and all(row[0] == "tweet" for row in matched)

    # 4) Delete-trigger behavior: deleting the tweet should remove its registry row
    with getcursor() as cur:
        cur.execute("DELETE FROM sm.tweet WHERE id=%s", (tw_id,))
    with getcursor() as cur:
        cur.execute(
            "SELECT 1 FROM sm.post_registry WHERE platform='tweet' AND key1=%s AND key2 IS NULL",
            (str(tw_id),),
        )
        assert cur.fetchone() is None
