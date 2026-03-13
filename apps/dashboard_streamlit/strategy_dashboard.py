from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data/store/alpha_tracker.duckdb"

st.set_page_config(page_title="Alpha_Tracker2 Strategy Dashboard", layout="wide")

@st.cache_data(show_spinner=False)
def load_strategies() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH))
    try:
        return con.execute(
            """
            SELECT strategy_id, model_version, trade_rule, hold_n, topk, cost_bps
            FROM strategies
            ORDER BY model_version, trade_rule, hold_n, topk, cost_bps
            """
        ).df()
    finally:
        con.close()

@st.cache_data(show_spinner=False)
def load_nav(strategy_ids: list[str], start: str, end: str) -> pd.DataFrame:
    if not strategy_ids:
        return pd.DataFrame()
    con = duckdb.connect(str(DB_PATH))
    try:
        df = con.execute(
            """
            SELECT trade_date, strategy_id, version, nav, day_ret
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ?
              AND strategy_id IN (SELECT UNNEST(?))
            ORDER BY trade_date, strategy_id
            """,
            [start, end, strategy_ids],
        ).df()
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    finally:
        con.close()

@st.cache_data(show_spinner=False)
def load_eval(strategy_ids: list[str], start: str, end: str) -> pd.DataFrame:
    if not strategy_ids:
        return pd.DataFrame()
    con = duckdb.connect(str(DB_PATH))
    try:
        df = con.execute(
            """
            SELECT trade_date, strategy_id, version,
                   coverage, hit_rate, avg_ret_5d, median_ret_5d,
                   eval_n_picks, eval_n_valid
            FROM eval_5d_batch_daily
            WHERE trade_date BETWEEN ? AND ?
              AND strategy_id IN (SELECT UNNEST(?))
            ORDER BY trade_date, strategy_id
            """,
            [start, end, strategy_ids],
        ).df()
        if df.empty:
            return df
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    finally:
        con.close()

def calc_summary(nav_df: pd.DataFrame) -> pd.DataFrame:
    if nav_df.empty:
        return pd.DataFrame()
    out = []
    for sid, g in nav_df.groupby("strategy_id"):
        g = g.sort_values("trade_date")
        nav0 = float(g["nav"].iloc[0])
        nav1 = float(g["nav"].iloc[-1])
        total_ret = nav1 / nav0 - 1.0

        # max drawdown
        cum = g["nav"].astype(float)
        peak = cum.cummax()
        dd = cum / peak - 1.0
        mdd = float(dd.min())

        vol = float(g["day_ret"].astype(float).std(ddof=0)) if len(g) > 1 else 0.0
        out.append(
            {
                "strategy_id": sid,
                "start_nav": nav0,
                "end_nav": nav1,
                "total_return": total_ret,
                "max_drawdown": mdd,
                "vol_daily": vol,
                "days": len(g),
            }
        )
    return pd.DataFrame(out).sort_values("total_return", ascending=False)

st.title("Alpha_Tracker2 — Strategy Matrix Dashboard (strategy_id)")

# Sidebar controls
strategies = load_strategies()

with st.sidebar:
    st.header("Filters")

    model_versions = ["ALL"] + sorted(strategies["model_version"].unique().tolist())
    mv = st.selectbox("Model version", model_versions, index=1 if "V1" in model_versions else 0)

    df_f = strategies.copy()
    if mv != "ALL":
        df_f = df_f[df_f["model_version"] == mv]

    trade_rules = ["ALL"] + sorted(df_f["trade_rule"].unique().tolist())
    tr = st.selectbox("Trade rule", trade_rules, index=0)

    if tr != "ALL":
        df_f = df_f[df_f["trade_rule"] == tr]

    holds = ["ALL"] + sorted(df_f["hold_n"].unique().tolist())
    hn = st.selectbox("Hold days (hold_n)", holds, index=0)

    if hn != "ALL":
        df_f = df_f[df_f["hold_n"] == hn]

    topks = ["ALL"] + sorted(df_f["topk"].unique().tolist())
    tk = st.selectbox("TopK", topks, index=0)

    if tk != "ALL":
        df_f = df_f[df_f["topk"] == tk]

    cost = st.number_input("Cost bps", min_value=0, max_value=200, value=10, step=1)

    df_f = df_f[df_f["cost_bps"] == cost] if "cost_bps" in df_f.columns else df_f

    st.divider()
    st.header("Date range")
    start = st.text_input("Start (YYYY-MM-DD)", "2026-01-06")
    end = st.text_input("End (YYYY-MM-DD)", "2026-01-14")

    st.divider()
    max_pick = min(20, len(df_f))
    default_n = min(6, max_pick)
    choices = df_f["strategy_id"].tolist()
    selected = st.multiselect(
        f"Select strategies (max {max_pick})",
        choices,
        default=choices[:default_n],
    )

# Main
if not selected:
    st.warning("Select at least one strategy_id.")
    st.stop()

nav_df = load_nav(selected, start, end)
eval_df = load_eval(selected, start, end)

c1, c2 = st.columns([2, 1])

with c1:
    st.subheader("NAV curves")
    if nav_df.empty:
        st.error("No nav_daily data for selected strategies in this range.")
    else:
        pivot = nav_df.pivot_table(index="trade_date", columns="strategy_id", values="nav")
        st.line_chart(pivot)

with c2:
    st.subheader("Summary (from nav)")
    if nav_df.empty:
        st.write("—")
    else:
        summary = calc_summary(nav_df)
        st.dataframe(summary, use_container_width=True)

st.subheader("5D evaluation (per-date fwd-5d ret stored)")
if eval_df.empty:
    st.info("No eval_5d_batch_daily data for selected strategies in this range.")
else:
    # show latest row per strategy
    latest = eval_df.sort_values("trade_date").groupby("strategy_id").tail(1)
    st.dataframe(latest.sort_values("avg_ret_5d", ascending=False), use_container_width=True)

st.subheader("Raw tables")
tab1, tab2 = st.tabs(["nav_daily", "eval_5d_batch_daily"])
with tab1:
    st.dataframe(nav_df, use_container_width=True)
with tab2:
    st.dataframe(eval_df, use_container_width=True)


# & .\.venv\Scripts\python.exe -m streamlit run .\apps\dashboard_streamlit\strategy_dashboard.py