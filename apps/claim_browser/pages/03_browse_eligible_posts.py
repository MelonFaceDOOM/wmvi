
# pages/03_preview_eligible_data.py
from __future__ import annotations

import pandas as pd
import streamlit as st

from shared import ensure_db_pool, query_df


def _fmt_int(v) -> str:
    try:
        return f"{int(v):,}"
    except Exception:
        return str(v)


def _display_df(df: pd.DataFrame) -> None:
    """
    Display dataframe with thousands separators WITHOUT mutating df (keeps numeric for charts).
    """
    if df is None or df.empty:
        st.caption("(no rows)")
        return

    fmt: dict[str, str] = {}
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_integer_dtype(s):
            fmt[c] = "{:,.0f}"
        elif pd.api.types.is_float_dtype(s):
            fmt[c] = "{:,.2f}"

    st.dataframe(df.style.format(fmt, na_rep=""), width="stretch")


@st.cache_data(ttl=600, show_spinner=False)
def _load_preview_payload(days: int) -> dict:
    """
    Preview widgets based on *content time* (sm.posts_all.created_at_ts).
    We count term-hit rows, but group by the post's created_at_ts.
    """
    out: dict = {}

    # 0) KPIs for the window (based on posts_all.created_at_ts)
    out["kpis"] = query_df(
        """
        SELECT
            COUNT(*)::bigint AS total_hits,
            COUNT(DISTINCT m.post_id)::bigint AS total_posts
        FROM matches.post_term_hit m
        JOIN sm.posts_all p
          ON p.post_id = m.post_id
        WHERE p.created_at_ts IS NOT NULL
          AND p.created_at_ts >= now() - (%s || ' days')::interval;
        """,
        (int(days),),
    )

    # 1) Hits over time (by post created_at_ts)
    out["hits_over_time"] = query_df(
        """
        SELECT
            date_trunc('day', p.created_at_ts) AS day,
            COUNT(*)::bigint AS hits
        FROM matches.post_term_hit m
        JOIN sm.posts_all p
          ON p.post_id = m.post_id
        WHERE p.created_at_ts IS NOT NULL
          AND p.created_at_ts >= now() - (%s || ' days')::interval
        GROUP BY 1
        ORDER BY 1 ASC;
        """,
        (int(days),),
    )

    # 2) Hits by platform over time (by post created_at_ts)
    out["hits_by_platform_over_time"] = query_df(
        """
        SELECT
            date_trunc('day', p.created_at_ts) AS day,
            p.platform,
            COUNT(*)::bigint AS hits
        FROM matches.post_term_hit m
        JOIN sm.posts_all p
          ON p.post_id = m.post_id
        WHERE p.created_at_ts IS NOT NULL
          AND p.created_at_ts >= now() - (%s || ' days')::interval
        GROUP BY 1, 2
        ORDER BY 1 ASC, 2 ASC;
        """,
        (int(days),),
    )

    # 3) Top 10 terms by hit count (same window; window is by created_at_ts)
    out["top_terms"] = query_df(
        """
        SELECT
            t.id AS term_id,
            t.name,
            t.type,
            COUNT(*)::bigint AS hits
        FROM matches.post_term_hit m
        JOIN sm.posts_all p
          ON p.post_id = m.post_id
        JOIN taxonomy.vaccine_term t
          ON t.id = m.term_id
        WHERE p.created_at_ts IS NOT NULL
          AND p.created_at_ts >= now() - (%s || ' days')::interval
        GROUP BY 1, 2, 3
        ORDER BY hits DESC, t.name ASC
        LIMIT 10;
        """,
        (int(days),),
    )

    return out


def _pivot_platform_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """
    df columns: [day, platform, hits]
    returns pivot indexed by day with platform columns, sorted by day.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()
    d["day"] = pd.to_datetime(d["day"]).dt.date
    pivot = (
        d.pivot_table(
            index="day",
            columns="platform",
            values="hits",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )
    return pivot


def main() -> None:
    st.set_page_config(
        page_title="WMVI Claim Browser — Preview Eligible Data", layout="wide")
    st.title("Preview eligible data")
    st.caption(
        "Counts are term-hit rows (matches.post_term_hit), grouped by the post’s content time "
        "(sm.posts_all.created_at_ts)."
    )

    # ---- DB connect ----
    prefix = st.session_state.get("db_prefix", "DEV") or "DEV"
    with st.status("Connecting to DB…", expanded=False) as status:
        try:
            ensure_db_pool(prefix)
            status.update(label=f"DB connected (prefix={
                          prefix!r}).", state="complete")
        except Exception as e:
            status.update(label="DB connection failed.", state="error")
            st.error(f"DB pool init failed (prefix={prefix!r}): {
                     type(e).__name__}: {e}")
            st.stop()

    # ---- Controls ----
    with st.sidebar:
        st.header("Preview window")
        days = st.slider("Lookback (days)", min_value=7,
                         max_value=365, value=365, step=1)
        st.caption("Window is based on posts_all.created_at_ts (content time).")

    # ---- Load payload ----
    with st.status("Loading preview widgets…", expanded=False) as wstatus:
        payload = _load_preview_payload(days)
        wstatus.update(label="Preview widgets loaded.", state="complete")

    # ---- KPIs ----
    qr = payload.get("kpis")
    if not qr or not qr.ok:
        st.warning(qr.error if qr else "KPI query missing.")
    else:
        row = qr.value.iloc[0]
        k1, k2 = st.columns(2)
        with k1:
            st.metric("Total hits (window)", _fmt_int(row["total_hits"]))
        with k2:
            st.metric("Total posts (unique post_id, window)",
                      _fmt_int(row["total_posts"]))

    st.divider()
    # ---- 1) Hits over time ----
    st.subheader("1) Search-term hits over time")

    qr = payload["hits_over_time"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        df: pd.DataFrame = qr.value
        if df is None or df.empty:
            st.info("No hits found in the selected window.")
        else:
            d = df.copy()
            d["day"] = pd.to_datetime(d["day"]).dt.date
            total_hits = int(d["hits"].sum())
            st.metric("Total hits (window)", _fmt_int(total_hits))

            ts = d.set_index("day")[["hits"]]
            st.bar_chart(ts)
            _display_df(d)

    st.divider()

    # ---- 2) Hits by platform over time ----
    st.subheader("2) Hits by platform over time")

    qr = payload["hits_by_platform_over_time"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        dfp: pd.DataFrame = qr.value
        if dfp is None or dfp.empty:
            st.info("No platform hits found in the selected window.")
        else:
            pivot = _pivot_platform_timeseries(dfp)
            if pivot.empty:
                st.caption("(no rows)")
            else:
                # Different design: area emphasizes composition over time
                st.area_chart(pivot)

                totals = (
                    dfp.groupby("platform", as_index=False)["hits"]
                    .sum()
                    .sort_values(["hits", "platform"], ascending=[False, True])
                )

                c1, c2 = st.columns([2, 3])
                with c1:
                    st.markdown("**Totals (window)**")
                    _display_df(totals)
                with c2:
                    st.markdown("**Daily pivot (window)**")
                    _display_df(pivot.reset_index())

    st.divider()

    # ---- 3) Top 10 terms by hit count ----
    st.subheader("3) Top 10 terms by hit count")

    qr = payload["top_terms"]
    if not qr.ok:
        st.warning(qr.error)
    else:
        dft: pd.DataFrame = qr.value
        if dft is None or dft.empty:
            st.info("No term hits found in the selected window.")
        else:
            chart_df = dft[["name", "hits"]].copy().set_index("name")
            st.bar_chart(chart_df)
            _display_df(dft)


if __name__ == "__main__":
    main()
