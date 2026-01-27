from __future__ import annotations

import streamlit as st
import pandas as pd
from pandas.api.types import is_integer_dtype, is_float_dtype

from shared import (
    ensure_db_pool,
    query_table_count,
    query_df,
)


def _init_global_sidebar() -> None:
    st.sidebar.header("WMVI DB")
    st.sidebar.caption("These settings apply to all pages.")

    if "db_prefix" not in st.session_state:
        st.session_state["db_prefix"] = "DEV"

    st.session_state["db_prefix"] = st.sidebar.text_input(
        "DB prefix (for init_pool)",
        value=st.session_state["db_prefix"],
        help="Matches your db.db init_pool(prefix=...). Example: DEV / PROD.",
    ).strip() or "DEV"

    st.sidebar.divider()
    st.sidebar.caption("Landing page only (for now).")


def _fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v)


def _display_df(df: pd.DataFrame, *, pct_cols: set[str] | None = None) -> None:
    """
    Display dataframe with thousands separators WITHOUT mutating df (keeps numeric for charts).
    """
    if df is None or df.empty:
        st.caption("(no rows)")
        return

    pct_cols = pct_cols or set()

    fmt: dict[str, str] = {}
    for c in df.columns:
        if c in pct_cols:
            fmt[c] = "{:,.2f}"
            continue
        s = df[c]
        if is_integer_dtype(s.dtype):
            fmt[c] = "{:,.0f}"
        elif is_float_dtype(s.dtype):
            fmt[c] = "{:,.2f}"

    st.dataframe(
        df.style.format(fmt, na_rep=""),
        width="stretch",
    )


def _last_n_active_days_pivot(df_daily: pd.DataFrame, n_days: int = 7) -> pd.DataFrame:
    """
    df_daily columns: [day, platform, n]
    Return pivot indexed by day with platform columns, restricted to last N days where total > 0.
    """
    d = df_daily.copy()
    d["day"] = pd.to_datetime(d["day"]).dt.date
    pivot = (
        d.pivot_table(index="day", columns="platform",
                      values="n", aggfunc="sum", fill_value=0)
        .sort_index()
    )

    if pivot.empty:
        return pivot

    totals = pivot.sum(axis=1)
    active_days = list(totals[totals > 0].index)
    keep_days = active_days[-n_days:] if len(
        active_days) > n_days else active_days
    return pivot.loc[keep_days]


@st.cache_data(ttl=60, show_spinner=False)
def _load_dashboard_payload() -> dict:
    """
    Cache dashboard results briefly so refreshes are fast.
    Keep results numeric; format only at display time.
    """
    out: dict = {}

    # 1) Ingestion volume by platform (last 7 active days)
    out["ingest_daily"] = query_df(
        """
        SELECT date_trunc('day', date_entered) AS day, platform, COUNT(*)::bigint AS n
        FROM sm.posts_all
        WHERE date_entered IS NOT NULL
          AND date_entered >= now() - interval '60 days'
        GROUP BY 1, 2
        ORDER BY 1 ASC, 2 ASC;
        """
    )

    # 2) Last seen per platform + rows in last 24h
    out["last_seen"] = query_df(
        """
        SELECT
            platform,
            MAX(date_entered) AS last_seen_date_entered,
            COUNT(*) FILTER (WHERE date_entered >= now() - interval '24 hours')::bigint AS rows_last_24h
        FROM sm.posts_all
        GROUP BY 1
        ORDER BY 1;
        """
    )

    # 4) English labeling coverage + backlog
    out["en_coverage"] = query_df(
        """
        SELECT
            platform,
            COUNT(*)::bigint AS total_rows,
            COUNT(*) FILTER (WHERE is_en IS TRUE)::bigint  AS is_en_true,
            COUNT(*) FILTER (WHERE is_en IS FALSE)::bigint AS is_en_false,
            COUNT(*) FILTER (WHERE is_en IS NULL)::bigint  AS is_en_null
        FROM sm.posts_all
        GROUP BY 1
        ORDER BY 1;
        """
    )

    # 5) Scrape jobs
    out["jobs_per_day"] = query_df(
        """
        SELECT date_trunc('day', created_at) AS day, COUNT(*)::bigint AS jobs_created
        FROM scrape.job
        WHERE created_at >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )
    out["posts_linked_per_day"] = query_df(
        """
        SELECT date_trunc('day', linked_at) AS day, COUNT(*)::bigint AS posts_linked
        FROM scrape.post_scrape
        WHERE linked_at >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )

    # 6) Term matching: throughput + coverage KPIs
    out["matches_per_day"] = query_df(
        """
        SELECT date_trunc('day', matched_at) AS day, COUNT(*)::bigint AS matches
        FROM matches.post_term_hit
        WHERE matched_at >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )
    out["match_coverage_kpis"] = query_df(
        """
        WITH totals AS (
          SELECT COUNT(*)::bigint AS total_posts
          FROM sm.posts_all
        ),
        matched AS (
          SELECT COUNT(DISTINCT post_id)::bigint AS matched_posts
          FROM matches.post_term_hit
        )
        SELECT
          totals.total_posts,
          matched.matched_posts,
          CASE WHEN totals.total_posts > 0
            THEN ROUND((matched.matched_posts::numeric / totals.total_posts::numeric) * 100.0, 2)
            ELSE NULL
          END AS coverage_pct
        FROM totals, matched;
        """
    )

    # 7) Podcasts ingestion
    out["episodes_per_day"] = query_df(
        """
        SELECT date_trunc('day', date_entered) AS day, COUNT(*)::bigint AS episodes
        FROM podcasts.episodes
        WHERE date_entered >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )

    # --- Transcription health: podcasts ---
    out["podcast_transcription_health"] = query_df(
        """
        SELECT
            COUNT(*)::bigint                                       AS total,
            COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
            COUNT(*) FILTER (
                WHERE transcription_started_at IS NOT NULL
                  AND transcript IS NULL
            )::bigint AS in_progress,
            COUNT(*) FILTER (
                WHERE transcription_started_at IS NULL
            )::bigint AS not_started
        FROM podcasts.episodes;
        """
    )

    out["podcast_transcriptions_per_day"] = query_df(
        """
        SELECT
            date_trunc('day', transcript_updated_at) AS day,
            COUNT(*)::bigint AS transcriptions
        FROM podcasts.episodes
        WHERE transcript IS NOT NULL
          AND transcript_updated_at >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )

    # --- Transcription health: youtube ---
    out["youtube_transcription_health"] = query_df(
        """
        SELECT
            COUNT(*)::bigint  AS total,
            COUNT(*) FILTER (WHERE transcript IS NOT NULL)::bigint AS completed,
            COUNT(*) FILTER (
                WHERE transcription_started_at IS NOT NULL
                  AND transcript IS NULL
            )::bigint AS in_progress,
            COUNT(*) FILTER (
                WHERE transcription_started_at IS NULL
            )::bigint AS not_started
        FROM youtube.video;
        """
    )

    out["youtube_transcriptions_per_day"] = query_df(
        """
        SELECT
            date_trunc('day', transcript_updated_at) AS day,
            COUNT(*)::bigint AS transcriptions
        FROM youtube.video
        WHERE transcript IS NOT NULL
          AND transcript_updated_at >= now() - interval '60 days'
        GROUP BY 1
        ORDER BY 1 ASC;
        """
    )

    return out


def main() -> None:

    prefix = st.session_state.get("db_prefix", "DEV") or "DEV"

    st.set_page_config(page_title="WMVI Pipeline Dashboard", layout="wide")
    st.title("WMVI Pipeline Dashboard")
    st.caption(
        "End-to-end system health: ingestion, labeling, matching, and transcription "
        "(cached briefly for fast refreshes)."
    )

    _init_global_sidebar()
    # --- DB connection status (visible) ---
    with st.status("Connecting to DB…", expanded=True) as status:
        try:
            ensure_db_pool(prefix)
            status.update(label=f"DB connected (prefix={
                          prefix!r}).", state="complete")
        except Exception as e:
            status.update(label="DB connection failed.", state="error")
            st.error(f"DB pool init failed (prefix={prefix!r}): {
                     type(e).__name__}: {e}")
            st.stop()

    # --- Summary ---
    st.subheader("Summary")

    # total/approx total: using COUNT(*) for now (simple + consistent)
    sources = [
        ("sm.tweet", "sm.tweet"),
        ("sm.reddit_submission", "sm.reddit_submission"),
        ("sm.reddit_comment", "sm.reddit_comment"),
        ("sm.telegram_post", "sm.telegram_post"),
        ("youtube.video", "youtube.video"),
        ("youtube.comment", "youtube.comment"),
        ("podcasts.shows", "podcasts.shows"),
        ("podcasts.episodes", "podcasts.episodes"),
    ]

    # Lay out metrics in rows of 4 for readability
    cols = st.columns(4)
    for i, (label, table) in enumerate(sources):
        res = query_table_count(table)
        with cols[i % 4]:
            if res.ok:
                st.metric(label, _fmt_int(res.value))
            else:
                st.metric(label, "error")
                st.caption(res.error)

    st.divider()

    # --- Load dashboard payload (cached) ---
    with st.status("Loading dashboard widgets…", expanded=False) as wstatus:
        payload = _load_dashboard_payload()
        wstatus.update(label="Dashboard widgets loaded.", state="complete")

    # 1) Ingestion volume by platform (last 7 active days)
    st.subheader("1) Ingestion volume by platform (last 7 active days)")
    qr = payload["ingest_daily"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df_daily: pd.DataFrame = qr.value
        if df_daily is None or df_daily.empty:
            st.caption("(no ingestion rows)")
        else:
            pivot7 = _last_n_active_days_pivot(df_daily, n_days=7)
            if pivot7.empty:
                st.caption("(no active days)")
            else:
                st.line_chart(pivot7)
                _display_df(pivot7.reset_index())

    st.divider()

    # 2) Last seen per platform + rows in last 24h
    st.subheader("2) Last seen per platform + rows in last 24h")
    qr = payload["last_seen"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df_last: pd.DataFrame = qr.value
        if df_last is None or df_last.empty:
            st.caption("(no rows)")
        else:
            d = df_last.copy()
            # format "2025-11-25 20:36:30+00:00" -> "2025-11-25 20:36"
            d["last_seen_date_entered"] = pd.to_datetime(d["last_seen_date_entered"], utc=True).dt.strftime(
                "%Y-%m-%d %H:%M"
            )
            _display_df(d)

    st.divider()

    # 4) English labeling coverage + backlog
    st.subheader("4) English labeling coverage + backlog")
    qr = payload["en_coverage"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df_en: pd.DataFrame = qr.value
        if df_en is None or df_en.empty:
            st.caption("(no rows)")
        else:
            d = df_en.copy()
            d["labeled_rows"] = d["is_en_true"] + d["is_en_false"]
            d["coverage_pct"] = d.apply(
                lambda r: round((r["labeled_rows"] / r["total_rows"])
                                * 100.0, 2) if r["total_rows"] else None,
                axis=1,
            )
            cols = ["platform", "total_rows", "is_en_true",
                    "is_en_false", "is_en_null", "coverage_pct"]
            _display_df(d[cols], pct_cols={"coverage_pct"})

    st.divider()

    # 5) Scrape jobs: jobs created per day + posts linked per day
    st.subheader("5) Scrape jobs: jobs created per day + posts linked per day")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Jobs created per day (last 60 days)**")
        qr = payload["jobs_per_day"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            df_jobs: pd.DataFrame = qr.value
            if df_jobs is None or df_jobs.empty:
                st.caption("(no rows)")
            else:
                df = df_jobs.copy()
                df["day"] = pd.to_datetime(df["day"]).dt.date
                df = df.set_index("day")[["jobs_created"]]
                st.line_chart(df)
                _display_df(df.reset_index())

    with c2:
        st.markdown("**Posts linked per day (last 60 days)**")
        qr = payload["posts_linked_per_day"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            df_linked: pd.DataFrame = qr.value
            if df_linked is None or df_linked.empty:
                st.caption("(no rows)")
            else:
                df = df_linked.copy()
                df["day"] = pd.to_datetime(df["day"]).dt.date
                df = df.set_index("day")[["posts_linked"]]
                st.line_chart(df)
                _display_df(df.reset_index())

    st.divider()

    # 6) Term matching throughput + coverage (KPIs above)
    st.subheader("6) Term matching")

    # Coverage KPIs (big, like Summary)
    qr = payload["match_coverage_kpis"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df_cov: pd.DataFrame = qr.value
        if df_cov is None or df_cov.empty:
            st.caption("(no rows)")
        else:
            row = df_cov.iloc[0].to_dict()
            total_posts = row.get("total_posts")
            matched_posts = row.get("matched_posts")
            coverage_pct = row.get("coverage_pct")

            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("Total posts", _fmt_int(total_posts))
            with k2:
                st.metric("Matched posts", _fmt_int(matched_posts))
            with k3:
                cov_str = "" if coverage_pct is None else f"{
                    float(coverage_pct):,.2f}%"
                st.metric("Coverage %", cov_str or "(n/a)")

    st.markdown("**Term matches per day (last 60 days)**")
    qr = payload["matches_per_day"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df_m: pd.DataFrame = qr.value
        if df_m is None or df_m.empty:
            st.caption("(no rows)")
        else:
            df = df_m.copy()
            df["day"] = pd.to_datetime(df["day"]).dt.date
            df = df.set_index("day")[["matches"]]
            st.line_chart(df)
            _display_df(df.reset_index())

    st.divider()

    # 7) Podcasts ingestion: episodes per day + transcript segments per day

    st.subheader("Transcription pipeline health")

    c1, c2 = st.columns(2)

    # --- Podcasts ---
    with c1:
        st.markdown("### Podcasts")
        qr = payload["podcast_transcription_health"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            row = qr.value.iloc[0]
            a, b, c, d = st.columns(4)
            a.metric("Total", _fmt_int(row["total"]))
            b.metric("Completed", _fmt_int(row["completed"]))
            c.metric("In progress", _fmt_int(row["in_progress"]))
            d.metric("Not started", _fmt_int(row["not_started"]))

        st.markdown("### Podcast Transcriptions per day")
        qr = payload["podcast_transcriptions_per_day"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            df_yt: pd.DataFrame = qr.value
            if df_yt is None or df_yt.empty:
                st.caption("(no rows)")
            else:
                df = df_yt.copy()
                df["day"] = pd.to_datetime(df["day"]).dt.date
                df = df.set_index("day")[["transcriptions"]]
                st.line_chart(df)
                _display_df(df.reset_index())

    # --- YouTube ---
    with c2:
        st.markdown("### YouTube")
        qr = payload["youtube_transcription_health"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            row = qr.value.iloc[0]
            a, b, c, d = st.columns(4)
            a.metric("Total", _fmt_int(row["total"]))
            b.metric("Completed", _fmt_int(row["completed"]))
            c.metric("In progress", _fmt_int(row["in_progress"]))
            d.metric("Not started", _fmt_int(row["not_started"]))

        st.markdown("**Youtube transcriptions per day**")
        qr = payload["youtube_transcriptions_per_day"]
        if not qr.ok:
            st.warning(qr.error)
        else:
            df_pc: pd.DataFrame = qr.value
            if df_pc is None or df_pc.empty:
                st.caption("(no rows)")
            else:
                df = df_pc.copy()
                df["day"] = pd.to_datetime(df["day"]).dt.date
                df = df.set_index("day")[["transcriptions"]]
                st.line_chart(df)
                _display_df(df.reset_index())


if __name__ == "__main__":
    main()
