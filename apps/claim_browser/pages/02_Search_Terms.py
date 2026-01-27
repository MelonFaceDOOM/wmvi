from __future__ import annotations

import pandas as pd
import streamlit as st

from shared import ensure_db_pool, query_df


def _format_counts_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn integer-ish columns into strings with thousands separators.
    (We only use this for display tables, not charts.)
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in out.columns:
        # Try to format int/float counts nicely
        if pd.api.types.is_integer_dtype(out[c]) or pd.api.types.is_float_dtype(out[c]):
            out[c] = out[c].apply(
                lambda x: "" if pd.isna(x) else (f"{int(x):,}" if float(
                    x).is_integer() else f"{float(x):,.2f}")
            )
    return out


@st.cache_data(ttl=600, show_spinner=False)
def _load_terms() -> pd.DataFrame:
    # Expect taxonomy.vaccine_term to exist.
    qr = query_df(
        """
        SELECT id, name, type, date_entered
        FROM taxonomy.vaccine_term
        ORDER BY name ASC;
        """
    )
    if not qr.ok:
        raise RuntimeError(qr.error or "Failed to load taxonomy.vaccine_term")
    return qr.value


def main() -> None:
    st.set_page_config(
        page_title="WMVI Claim Browser — Search Terms", layout="wide")

    st.title("Search terms")

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

    # ---- Load terms ----
    try:
        terms_df = _load_terms()
    except Exception as e:
        st.error(f"Failed to load search terms: {type(e).__name__}: {e}")
        st.stop()

    st.caption(f"Loaded {len(terms_df):,} search terms.")

    # ---- Live filter + dropdown ----
    q = st.text_input(
        "Filter terms (live)",
        value=st.session_state.get("term_filter", ""),
        placeholder="type to filter…",
    )
    st.session_state["term_filter"] = q

    q_norm = q.strip().lower()
    if q_norm:
        filtered = terms_df[terms_df["name"].str.lower(
        ).str.contains(q_norm, na=False)].copy()
    else:
        filtered = terms_df

    if filtered.empty:
        st.warning("No terms match the filter.")
        st.stop()

    # Build display labels (keep it simple: name + type)
    filtered["label"] = filtered.apply(
        lambda r: f"{r['name']}  ({r['type']})", axis=1)
    options = filtered[["label", "id"]].values.tolist()

    # Persist selection by term id if possible
    prev_id = st.session_state.get("selected_term_id", None)
    labels = [lbl for (lbl, _tid) in options]
    ids = [int(_tid) for (_lbl, _tid) in options]
    default_idx = 0
    if prev_id in ids:
        default_idx = ids.index(prev_id)

    sel_label = st.selectbox("Select a term", labels, index=default_idx)
    sel_id = ids[labels.index(sel_label)]
    st.session_state["selected_term_id"] = sel_id

    term_row = terms_df[terms_df["id"] == sel_id].iloc[0]
    st.subheader(f"Selected: {term_row['name']} ({term_row['type']})")
    st.caption(f"term_id={int(term_row['id'])} • entered={
               str(term_row['date_entered'])}")

    # ---- Counts by platform ----
    with st.status("Querying match counts…", expanded=False) as s:
        qr = query_df(
            """
            SELECT
                pr.platform,
                COUNT(*)::bigint AS matches
            FROM matches.post_term_hit m
            JOIN sm.post_registry pr
              ON pr.id = m.post_id
            WHERE m.term_id = %s
            GROUP BY 1
            ORDER BY 2 DESC, 1 ASC;
            """,
            (int(sel_id),),
        )
        if not qr.ok:
            s.update(label="Query failed.", state="error")
            st.error(qr.error)
            st.stop()
        s.update(label="Done.", state="complete")

    df = qr.value
    total = int(df["matches"].sum()) if df is not None and not df.empty else 0
    st.metric("Total matches", f"{total:,}")

    if df is None or df.empty:
        st.info("No matches found for this term.")
        return

    st.dataframe(_format_counts_for_display(df), width="stretch")


if __name__ == "__main__":
    main()
