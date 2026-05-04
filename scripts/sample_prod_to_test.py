from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

load_dotenv()

REQUIRED_SCHEMAS = ("taxonomy", "sm", "youtube", "podcasts", "matches", "news")


@dataclass(frozen=True)
class PgCreds:
    host: str
    port: str
    user: str
    password: str
    database: str
    sslmode: str = "require"


def _get_creds(prefix: str) -> PgCreds:
    ssl_key = f"{prefix}_PGSSLMODE"
    sslmode = os.environ.get(ssl_key)
    if sslmode is None:
        host = os.environ[f"{prefix}_PGHOST"]
        sslmode = "disable" if host in ("localhost", "127.0.0.1", "::1") else "require"
    return PgCreds(
        host=os.environ[f"{prefix}_PGHOST"],
        port=os.environ.get(f"{prefix}_PGPORT", "5432"),
        user=os.environ[f"{prefix}_PGUSER"],
        password=os.environ[f"{prefix}_PGPASSWORD"],
        database=os.environ[f"{prefix}_PGDATABASE"],
        sslmode=sslmode,
    )


def _connect(creds: PgCreds) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        password=creds.password,
        dbname=creds.database,
        sslmode=creds.sslmode,
    )


def _subproc_env(creds: PgCreds) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PGHOST": creds.host,
            "PGPORT": creds.port,
            "PGUSER": creds.user,
            "PGPASSWORD": creds.password,
            "PGDATABASE": creds.database,
            "PGSSLMODE": creds.sslmode,
        }
    )
    return env


def _ensure_destination_schema(dst: PgCreds, dst_conn: psycopg2.extensions.connection) -> None:
    with dst_conn.cursor() as cur:
        cur.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = ANY(%s)
            ORDER BY schema_name
            """,
            (list(REQUIRED_SCHEMAS),),
        )
        existing = [r[0] for r in cur.fetchall()]
    needs_bootstrap = len(existing) != len(REQUIRED_SCHEMAS)
    drift_reason = None
    if not needs_bootstrap:
        # Detect known schema drift that breaks copying (e.g. stale episodes.audio_path).
        with dst_conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'podcasts'
                  AND table_name = 'episodes'
                  AND column_name = 'audio_path'
                """
            )
            if cur.fetchone() is not None:
                needs_bootstrap = True
                drift_reason = "stale podcasts.episodes.audio_path column exists"

    if not needs_bootstrap:
        return

    schema_path = Path(__file__).resolve().parent.parent / "schema_prod.sql"
    if not schema_path.exists():
        raise RuntimeError(f"Destination missing schemas and schema file not found: {schema_path}")

    print(f"[sample] destination schemas missing/drifted; rebuilding from {schema_path}", flush=True)
    with dst_conn.cursor() as cur:
        for sch in REQUIRED_SCHEMAS:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(sch)))
    dst_conn.commit()
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-f", str(schema_path)]
    try:
        subprocess.run(cmd, env=_subproc_env(dst), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Schema bootstrap failed via psql ({e.returncode})") from e


def _hard_reset_database(dst: PgCreds, maint_db: str = "postgres") -> None:
    conn = psycopg2.connect(
        host=dst.host,
        port=dst.port,
        user=dst.user,
        password=dst.password,
        dbname=maint_db,
        sslmode=dst.sslmode,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (dst.database,),
            )
            cur.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(dst.database)))
            cur.execute(sql.SQL('CREATE DATABASE {} OWNER {}').format(sql.Identifier(dst.database), sql.Identifier(dst.user)))
    finally:
        conn.close()


def _fetchall(cur: psycopg2.extensions.cursor, q: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    cur.execute(q, params)
    return cur.fetchall()


def _column_names(cur: psycopg2.extensions.cursor, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _insertable_destination_columns(cur: psycopg2.extensions.cursor, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND is_generated = 'NEVER'
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _required_destination_columns(cur: psycopg2.extensions.cursor, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND is_generated = 'NEVER'
          AND is_nullable = 'NO'
          AND column_default IS NULL
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _align_rows_to_columns(
    rows: list[tuple[Any, ...]],
    src_columns: list[str],
    dst_columns: list[str],
) -> tuple[list[str], list[tuple[Any, ...]]]:
    if not rows:
        return [], []
    src_idx = {c: i for i, c in enumerate(src_columns)}
    aligned_cols = [c for c in dst_columns if c in src_idx]
    aligned_rows: list[tuple[Any, ...]] = []
    for r in rows:
        aligned_rows.append(tuple(r[src_idx[c]] for c in aligned_cols))
    return aligned_cols, aligned_rows


def _insert_rows(
    cur: psycopg2.extensions.cursor,
    schema: str,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    if not columns:
        raise RuntimeError(f"No aligned columns available for insert into {schema}.{table}")
    q = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT DO NOTHING").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )
    normalized_rows: list[tuple[Any, ...]] = []
    for r in rows:
        nr: list[Any] = []
        for v in r:
            if isinstance(v, (dict, list)):
                nr.append(Json(v))
            else:
                nr.append(v)
        normalized_rows.append(tuple(nr))
    before = cur.rowcount
    execute_values(cur, q.as_string(cur.connection), normalized_rows, page_size=1000)
    after = cur.rowcount
    if after is None or after < 0:
        return len(rows)
    if before is None or before < 0:
        return after
    return max(0, after - before)


def _in_clause(values: Iterable[Any]) -> tuple[str, tuple[Any, ...]]:
    vals = tuple(values)
    if not vals:
        return "(NULL)", ()
    placeholders = ", ".join(["%s"] * len(vals))
    return f"({placeholders})", vals


def _build_post_id_remap(
    src_cur: psycopg2.extensions.cursor,
    dst_cur: psycopg2.extensions.cursor,
    src_post_ids: list[int],
) -> dict[int, int]:
    if not src_post_ids:
        return {}
    in_sql, in_params = _in_clause(src_post_ids)
    src_cur.execute(
        f"""
        SELECT id, platform, key1, key2
        FROM sm.post_registry
        WHERE id IN {in_sql}
        """,
        in_params,
    )
    src_rows = src_cur.fetchall()
    key_to_src_id = {(r[1], r[2], r[3]): int(r[0]) for r in src_rows}
    if not key_to_src_id:
        return {}
    tuples_sql = ", ".join(["(%s,%s,%s)"] * len(key_to_src_id))
    params: list[Any] = []
    for platform, key1, key2 in key_to_src_id.keys():
        params.extend([platform, key1, key2])
    dst_cur.execute(
        f"""
        SELECT pr.id, pr.platform, pr.key1, pr.key2
        FROM sm.post_registry pr
        JOIN (VALUES {tuples_sql}) AS v(platform, key1, key2)
          ON pr.platform = v.platform
         AND pr.key1 = v.key1
         AND pr.key2 = v.key2
        """,
        tuple(params),
    )
    remap: dict[int, int] = {}
    for dst_id, platform, key1, key2 in dst_cur.fetchall():
        src_id = key_to_src_id.get((platform, key1, key2))
        if src_id is not None:
            remap[src_id] = int(dst_id)
    return remap


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Copy small, relationship-consistent slices from PROD into TEST using "
            "two direct DB connections (no init_pool)."
        )
    )
    ap.add_argument("--src-prefix", default="PROD", help="Source env prefix (default: PROD)")
    ap.add_argument("--dst-prefix", default="TEST", help="Destination env prefix (default: TEST)")
    ap.add_argument("--reddit-submissions", type=int, default=50)
    ap.add_argument("--reddit-comments-per-submission", type=int, default=20)
    ap.add_argument("--youtube-videos", type=int, default=300)
    ap.add_argument("--youtube-comments-per-video", type=int, default=100)
    ap.add_argument("--telegram-posts", type=int, default=100)
    ap.add_argument("--podcast-shows", type=int, default=15)
    ap.add_argument("--episodes-per-show", type=int, default=5)
    ap.add_argument("--tweets", type=int, default=100)
    ap.add_argument("--news-articles", type=int, default=50)
    ap.add_argument("--truncate-first", action="store_true", help="TRUNCATE destination tables before insert")
    args = ap.parse_args(argv)

    if args.dst_prefix.upper() == "PROD":
        raise SystemExit("Refusing to write into PROD destination.")

    src = _get_creds(args.src_prefix)
    dst = _get_creds(args.dst_prefix)
    print(f"[sample] src={src.host}:{src.port}/{src.database} -> dst={dst.host}:{dst.port}/{dst.database}", flush=True)

    if args.truncate_first:
        maint_db = os.environ.get(f"{args.dst_prefix.upper()}_PG_MAINT_DB", "postgres")
        print(f"[sample] hard-resetting destination database {dst.database} via {maint_db}", flush=True)
        _hard_reset_database(dst, maint_db=maint_db)

    src_conn = _connect(src)
    dst_conn = _connect(dst)
    src_conn.autocommit = False
    dst_conn.autocommit = False

    try:
        _ensure_destination_schema(dst, dst_conn)
        try:
            dst_conn.close()
        except Exception:
            pass
        dst_conn = _connect(dst)
        dst_conn.autocommit = False

        with src_conn.cursor() as s, dst_conn.cursor() as d:
            reddit_sub_rows = _fetchall(
                s,
                """
                SELECT * FROM sm.reddit_submission
                ORDER BY created_at_ts DESC NULLS LAST, date_entered DESC
                LIMIT %s
                """,
                (args.reddit_submissions,),
            )
            reddit_sub_ids = [r[0] for r in reddit_sub_rows]

            youtube_video_rows = _fetchall(
                s,
                """
                SELECT * FROM youtube.video
                ORDER BY created_at_ts DESC NULLS LAST, date_entered DESC
                LIMIT %s
                """,
                (args.youtube_videos,),
            )
            youtube_video_ids = [r[0] for r in youtube_video_rows]

            telegram_rows = _fetchall(
                s,
                """
                SELECT * FROM sm.telegram_post
                ORDER BY created_at_ts DESC NULLS LAST, date_entered DESC
                LIMIT %s
                """,
                (args.telegram_posts,),
            )

            show_rows = _fetchall(
                s,
                """
                SELECT * FROM podcasts.shows
                ORDER BY date_entered DESC, id DESC
                LIMIT %s
                """,
                (args.podcast_shows,),
            )
            show_ids = [r[0] for r in show_rows]

            tweet_rows = _fetchall(
                s,
                """
                SELECT * FROM sm.tweet
                ORDER BY created_at_ts DESC NULLS LAST, date_entered DESC
                LIMIT %s
                """,
                (args.tweets,),
            )

            article_rows = _fetchall(
                s,
                """
                SELECT * FROM news.article
                ORDER BY created_at_ts DESC NULLS LAST, date_entered DESC
                LIMIT %s
                """,
                (args.news_articles,),
            )

            reddit_comment_rows: list[tuple[Any, ...]] = []
            if reddit_sub_ids:
                in_sql, in_params = _in_clause(reddit_sub_ids)
                s.execute(
                    f"""
                    SELECT *
                    FROM sm.reddit_comment c
                    WHERE c.link_id IN {in_sql}
                      AND c.parent_comment_id IS NULL
                    ORDER BY c.created_at_ts DESC NULLS LAST, c.date_entered DESC
                    LIMIT %s
                    """,
                    (*in_params, max(1, args.reddit_submissions * args.reddit_comments_per_submission)),
                )
                reddit_comment_rows = s.fetchall()

            youtube_comment_rows: list[tuple[Any, ...]] = []
            if youtube_video_ids:
                in_sql, in_params = _in_clause(youtube_video_ids)
                s.execute(
                    f"""
                    SELECT *
                    FROM youtube.comment c
                    WHERE c.video_id IN {in_sql}
                      AND c.parent_comment_id IS NULL
                    ORDER BY c.created_at_ts DESC NULLS LAST, c.date_entered DESC
                    LIMIT %s
                    """,
                    (*in_params, max(1, args.youtube_videos * args.youtube_comments_per_video)),
                )
                youtube_comment_rows = s.fetchall()

            episode_rows: list[tuple[Any, ...]] = []
            if show_ids:
                in_sql, in_params = _in_clause(show_ids)
                s.execute(
                    f"""
                    SELECT *
                    FROM podcasts.episodes e
                    WHERE e.podcast_id IN {in_sql}
                    ORDER BY e.created_at_ts DESC NULLS LAST, e.date_entered DESC
                    LIMIT %s
                    """,
                    (*in_params, max(1, args.podcast_shows * args.episodes_per_show)),
                )
                episode_rows = s.fetchall()

            pr_filters: list[tuple[str, str, str]] = []
            pr_filters.extend(("reddit_submission", str(r[0]), "") for r in reddit_sub_rows)
            pr_filters.extend(("reddit_comment", str(r[0]), "") for r in reddit_comment_rows)
            pr_filters.extend(("telegram_post", str(r[0]), str(r[1])) for r in telegram_rows)
            pr_filters.extend(("youtube_video", str(r[0]), "") for r in youtube_video_rows)
            pr_filters.extend(("youtube_comment", str(r[0]), str(r[1])) for r in youtube_comment_rows)
            pr_filters.extend(("podcast_episode", str(r[0]), "") for r in episode_rows)
            pr_filters.extend(("tweet", str(r[0]), "") for r in tweet_rows)
            pr_filters.extend(("news_article", str(r[0]), "") for r in article_rows)

            post_registry_rows: list[tuple[Any, ...]] = []
            if pr_filters:
                tuples_sql = ", ".join(["(%s,%s,%s)"] * len(pr_filters))
                params: list[Any] = []
                for t in pr_filters:
                    params.extend(t)
                s.execute(
                    f"""
                    SELECT pr.*
                    FROM sm.post_registry pr
                    JOIN (VALUES {tuples_sql}) AS v(platform, key1, key2)
                      ON pr.platform = v.platform
                     AND pr.key1 = v.key1
                     AND pr.key2 = v.key2
                    """,
                    tuple(params),
                )
                post_registry_rows = s.fetchall()

            post_ids = [r[0] for r in post_registry_rows]
            post_term_hit_rows: list[tuple[Any, ...]] = []
            term_ids: set[int] = set()
            if post_ids:
                in_sql, in_params = _in_clause(post_ids)
                s.execute(
                    f"""
                    SELECT *
                    FROM matches.post_term_hit
                    WHERE post_id IN {in_sql}
                    ORDER BY matched_at DESC, id DESC
                    """,
                    in_params,
                )
                post_term_hit_rows = s.fetchall()
                term_ids = {int(r[2]) for r in post_term_hit_rows}

            vaccine_term_rows: list[tuple[Any, ...]] = []
            if term_ids:
                in_sql, in_params = _in_clause(sorted(term_ids))
                s.execute(
                    f"""
                    SELECT *
                    FROM taxonomy.vaccine_term
                    WHERE id IN {in_sql}
                    ORDER BY id
                    """,
                    in_params,
                )
                vaccine_term_rows = s.fetchall()

            copies = [
                ("taxonomy", "vaccine_term", vaccine_term_rows),
                ("sm", "reddit_submission", reddit_sub_rows),
                ("sm", "reddit_comment", reddit_comment_rows),
                ("youtube", "video", youtube_video_rows),
                ("youtube", "comment", youtube_comment_rows),
                ("sm", "telegram_post", telegram_rows),
                ("podcasts", "shows", show_rows),
                ("podcasts", "episodes", episode_rows),
                ("sm", "tweet", tweet_rows),
                ("news", "article", article_rows),
            ]

            for schema, table, rows in copies:
                src_cols = _column_names(s, schema, table)
                dst_insertable_cols = _insertable_destination_columns(d, schema, table)
                required_cols = _required_destination_columns(d, schema, table)
                cols, aligned_rows = _align_rows_to_columns(rows, src_cols, dst_insertable_cols)
                missing_required = [c for c in required_cols if c not in cols]
                if rows and missing_required:
                    raise RuntimeError(
                        f"Schema drift detected for {schema}.{table}: destination requires "
                        f"columns not available in source rows: {missing_required}"
                    )
                inserted = _insert_rows(d, schema, table, cols, aligned_rows)
                print(f"[sample] {schema}.{table}: selected={len(rows)} inserted~={inserted}", flush=True)

            # Remap source post_registry ids -> destination ids before inserting hits.
            remap = _build_post_id_remap(s, d, post_ids)
            remapped_hits: list[tuple[Any, ...]] = []
            skipped_hits = 0
            for row in post_term_hit_rows:
                src_post_id = int(row[1])
                dst_post_id = remap.get(src_post_id)
                if dst_post_id is None:
                    skipped_hits += 1
                    continue
                vals = list(row)
                vals[1] = dst_post_id
                remapped_hits.append(tuple(vals))
            if skipped_hits:
                print(f"[sample] matches.post_term_hit: skipped_unmapped={skipped_hits}", flush=True)

            src_cols = _column_names(s, "matches", "post_term_hit")
            dst_insertable_cols = _insertable_destination_columns(d, "matches", "post_term_hit")
            required_cols = _required_destination_columns(d, "matches", "post_term_hit")
            cols, aligned_rows = _align_rows_to_columns(remapped_hits, src_cols, dst_insertable_cols)
            missing_required = [c for c in required_cols if c not in cols]
            if remapped_hits and missing_required:
                raise RuntimeError(
                    "Schema drift detected for matches.post_term_hit: destination requires "
                    f"columns not available in source rows: {missing_required}"
                )
            inserted = _insert_rows(d, "matches", "post_term_hit", cols, aligned_rows)
            print(
                f"[sample] matches.post_term_hit: selected={len(post_term_hit_rows)} "
                f"remapped={len(remapped_hits)} inserted~={inserted}",
                flush=True,
            )

        dst_conn.commit()
        print("[sample] done.", flush=True)
    except Exception:
        dst_conn.rollback()
        raise
    finally:
        try:
            src_conn.close()
        except Exception:
            pass
        try:
            dst_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except KeyError as e:
        print(f"Missing env var: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
