"""Streamlit demo: narratives → leaves → claims (one sqlite bundle)."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import streamlit as st

from apps.claims.demo import ANTI_CUTOFF
from apps.claims.demo import db as demo_db
from apps.claims.demo import platforms as plat
from apps.claims.demo.db import DemoFilters, TRENDING_DAYS


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--bundle", type=Path, default=None)
    args, _ = ap.parse_known_args()
    if args.bundle is None:
        env = os.environ.get("WMVI_DEMO_BUNDLE")
        if env:
            args.bundle = Path(env)
    return args


def _chart_weekly(rows: list[dict], *, stacked: bool) -> None:
    if not rows:
        st.caption("No weekly counts in this filter.")
        return
    df = pd.DataFrame(rows)
    if stacked and "platform" in df.columns:
        st.area_chart(df, x="week", y="n", color="platform")
    else:
        st.line_chart(df, x="week", y="n")


def _set_page(**kwargs: str | int) -> None:
    qp = st.query_params
    for k, v in kwargs.items():
        qp[k] = str(v)


def _qp_get(name: str) -> str:
    val = st.query_params.get(name)
    if val is None:
        return ""
    if isinstance(val, list):
        return str(val[0]) if val else ""
    return str(val)


def _init_sidebar_state(groups: list[str]) -> None:
    if "demo_plats" not in st.session_state:
        raw = [g for g in _qp_get("plats").split(",") if g]
        st.session_state.demo_plats = [g for g in raw if g in groups]
    if "demo_sort" not in st.session_state:
        sort = _qp_get("sort")
        st.session_state.demo_sort = sort if sort in ("trending", "volume") else "trending"


def _set_platform_group(gid: str) -> None:
    st.session_state.demo_plats = [gid]
    st.query_params["plats"] = gid
    st.rerun()


def _card_caption(row: dict, filters: DemoFilters, extra: str = "") -> str:
    bits: list[str] = []
    n_occ = int(row.get("n_occ") or 0)
    if not filters.anti and n_occ:
        pct = round(100 * int(row.get("n_anti") or 0) / n_occ)
        bits.append(f"anti {pct}%")
    bits.append(f"{n_occ:,} occ")
    if extra:
        bits.append(extra)
    return " · ".join(bits)


def _platform_bar(
    conn,
    filters: DemoFilters,
    *,
    key_prefix: str,
    narrative_id: int | None = None,
    leaf_id: int | None = None,
    clickable: bool = True,
) -> None:
    bar_filters = DemoFilters(anti=filters.anti, platforms=(), sort=filters.sort)
    grouped = demo_db.platform_counts(
        conn, bar_filters, narrative_id=narrative_id, leaf_id=leaf_id
    )
    if not grouped:
        return
    df = pd.DataFrame({"n": [r["n"] for r in grouped]}, index=[r["label"] for r in grouped])
    st.bar_chart(df)
    if not clickable:
        return
    cols = st.columns(max(len(grouped), 1))
    for col, row in zip(cols, grouped):
        with col:
            if st.button(row["label"], key=f"{key_prefix}_{row['platform']}"):
                _set_platform_group(str(row["platform"]))


def _weekly(conn, filters: DemoFilters, selected_groups: list[str], **scope) -> None:
    stacked = len(selected_groups) != 1
    rows = demo_db.weekly_counts(conn, filters, by_platform=stacked, **scope)
    _chart_weekly(rows, stacked=stacked)


def main() -> None:
    st.set_page_config(page_title="Measles claims demo", layout="wide")
    args = _parse_args()
    bundle = args.bundle
    if bundle is None or not Path(bundle).is_file():
        st.error("Pass --bundle path/to/measles2_demo.sqlite (or WMVI_DEMO_BUNDLE).")
        return

    conn = demo_db.connect(Path(bundle))
    groups = demo_db.available_groups(conn)
    _init_sidebar_state(groups)

    anti = st.sidebar.radio(
        "Mode",
        options=(False, True),
        format_func=lambda a: "Anti (≤0.25)" if a else "All",
        index=1 if _qp_get("mode") == "anti" else 0,
    )
    st.query_params["mode"] = "anti" if anti else "all"
    st.sidebar.caption(f"Anti cutoff = {ANTI_CUTOFF} (extract alignment)")

    selected_groups = st.sidebar.multiselect(
        "Platforms",
        options=groups,
        format_func=plat.label,
        key="demo_plats",
        help="Empty = all platforms. Reddit combines comments and submissions.",
    )
    st.query_params["plats"] = ",".join(selected_groups)

    sort = st.sidebar.radio(
        "Time window",
        options=("trending", "volume"),
        format_func=lambda s: "Last 90 days" if s == "trending" else "All time",
        key="demo_sort",
    )
    st.query_params["sort"] = sort

    filters = DemoFilters(
        anti=bool(anti),
        platforms=plat.keys_for_groups(selected_groups),
        sort=sort,
    )

    page = _qp_get("page") or "home"
    if page == "leaf":
        _leaf_page(conn, filters, selected_groups)
    elif page == "narrative":
        _narrative_page(conn, filters, selected_groups)
    else:
        _home(conn, filters, selected_groups)


def _home(conn, filters: DemoFilters, selected_groups: list[str]) -> None:
    stats = demo_db.dashboard_stats(conn, filters)
    st.title("Measles / vaccine claims")
    c1, c2, c3 = st.columns(3)
    c1.metric("Posts", f"{stats['n_posts']:,}")
    c2.metric("Distinct claims", f"{stats['n_claims']:,}")
    c3.metric("Claim occurrences", f"{stats['n_occurrences']:,}")
    meta = stats.get("meta") or {}
    ts_max = demo_db.ts_max_date(conn)
    ts_min = (meta.get("ts_min") or "")[:10]
    if filters.sort == "trending" and ts_max:
        cutoff = demo_db.trending_cutoff_date(conn) or ts_min
        st.caption(
            f"{cutoff} → {ts_max}  ·  last {TRENDING_DAYS}d  ·  {meta.get('corpus', '')}"
        )
    elif ts_min:
        st.caption(f"{ts_min} → {ts_max}  ·  {meta.get('corpus', '')}")

    st.subheader("Platforms")
    _platform_bar(conn, filters, key_prefix="chip_home")

    st.subheader("Narratives")
    if filters.sort == "trending" and ts_max:
        st.caption(f"counts and rank are last {TRENDING_DAYS} days from {ts_max}")
    nars = demo_db.list_narratives(conn, filters)
    if not nars:
        st.info("No narratives with claims in this filter.")
        return
    for row in nars:
        with st.container(border=True):
            left, right = st.columns([4, 1])
            with left:
                st.markdown(f"**{row['title']}**")
                if row.get("blurb"):
                    st.caption(row["blurb"])
                extra = f"{int(row['n_leaves'])} leaves"
                st.caption(_card_caption(row, filters, extra=extra))
            with right:
                if st.button("Open", key=f"nar_{row['id']}"):
                    _set_page(page="narrative", nid=int(row["id"]))
                    st.rerun()


def _narrative_page(conn, filters: DemoFilters, selected_groups: list[str]) -> None:
    if st.button("← Dashboard"):
        _set_page(page="home")
        st.query_params.pop("nid", None)
        st.rerun()
    nid = int(_qp_get("nid") or 0)
    nar = demo_db.narrative_row(conn, nid)
    if not nar:
        st.error(f"Unknown narrative {nid}")
        return
    st.title(nar["title"])
    if nar.get("blurb"):
        st.write(nar["blurb"])

    st.subheader("Volume over time")
    _weekly(conn, filters, selected_groups, narrative_id=nid)

    st.subheader("Leaves")
    leaves = demo_db.list_leaves(conn, nid, filters)
    if not leaves:
        st.info("No leaves with claims in this filter.")
        return
    for row in leaves:
        with st.container(border=True):
            left, right = st.columns([4, 1])
            with left:
                st.markdown(f"**{row['title']}**")
                if row.get("blurb"):
                    st.caption(row["blurb"])
                st.caption(_card_caption(row, filters))
            with right:
                if st.button("Open", key=f"leaf_{row['id']}"):
                    _set_page(page="leaf", lid=int(row["id"]), nid=nid)
                    st.rerun()


def _leaf_page(conn, filters: DemoFilters, selected_groups: list[str]) -> None:
    nid = _qp_get("nid")
    if st.button("← Narrative" if nid else "← Dashboard"):
        if nid:
            _set_page(page="narrative", nid=str(nid))
        else:
            _set_page(page="home")
        st.query_params.pop("lid", None)
        st.rerun()
    lid = int(_qp_get("lid") or 0)
    leaf = demo_db.leaf_row(conn, lid)
    if not leaf:
        st.error(f"Unknown leaf {lid}")
        return
    st.title(leaf["title"])
    if leaf.get("blurb"):
        st.write(leaf["blurb"])

    st.subheader("Volume over time")
    _weekly(conn, filters, selected_groups, leaf_id=lid)

    st.subheader("Platforms")
    _platform_bar(
        conn, filters, key_prefix=f"chip_leaf_{lid}", leaf_id=lid, clickable=False
    )

    st.subheader("Member claims")
    claims = demo_db.member_claims(conn, lid, filters)
    if not claims:
        st.info("No member claims in this filter.")
        return
    for claim in claims:
        with st.expander(f"{claim['claim_text'][:160]}  ({int(claim['n_occ'])} posts)"):
            posts = demo_db.claim_posts(conn, int(claim["idx"]), filters)
            if not posts:
                st.caption("No posts in this filter.")
                continue
            for p in posts:
                bits = [plat.label(p.get("platform")), str(p.get("ts") or "")[:10]]
                if p.get("alignment") is not None:
                    bits.append(f"align={p['alignment']}")
                st.markdown(" · ".join(b for b in bits if b))
                if p.get("snippet"):
                    st.caption(p["snippet"])


if __name__ == "__main__":
    main()
