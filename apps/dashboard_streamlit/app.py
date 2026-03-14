"""
D-2: Streamlit dashboard for NAV, picks, and evaluation panels.

Data source: CSV files in config paths.out_dir (default data/out).
Run after make_dashboard so that nav_daily.csv, eval_5d_daily.csv, picks_daily.csv,
eval_summary.csv, quintile_returns.csv, ic_series.csv are present.

  streamlit run apps/dashboard_streamlit/app.py

(From repository root; or use --server.runOnSave true for development.)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st


def _find_project_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "configs" / "default.yaml").is_file():
            return parent
    return None


def _get_out_dir() -> Path:
    """Resolve data/out directory from config or default."""
    app_path = Path(__file__).resolve()
    project_root = _find_project_root(app_path)
    if project_root is not None:
        try:
            import yaml
            with (project_root / "configs" / "default.yaml").open("r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            out = cfg.get("paths") or {}
            out_dir = out.get("out_dir", "data/out")
            p = Path(out_dir)
            if not p.is_absolute():
                p = project_root / p
            return p
        except Exception:
            pass
    return Path("data/out")


def _load_csv(out_dir: Path, name: str) -> pd.DataFrame:
    path = out_dir / name
    if not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


st.set_page_config(page_title="Alpha Tracker2 Dashboard", layout="wide")
out_dir = _get_out_dir()

st.title("Alpha Tracker2 Dashboard")
st.caption(f"Data: {out_dir} (run make_dashboard first to refresh CSVs)")

# ---- NAV panel ----
st.header("NAV / 净值")
nav_df = _load_csv(out_dir, "nav_daily.csv")
if nav_df.empty:
    st.info("No nav_daily.csv or empty. Run make_dashboard (and portfolio_nav) first.")
else:
    nav_df["trade_date"] = pd.to_datetime(nav_df["trade_date"], errors="coerce")
    portfolios = sorted(nav_df["portfolio"].dropna().unique().tolist())
    sel = st.multiselect("Portfolio", options=portfolios, default=portfolios)
    n = nav_df[nav_df["portfolio"].isin(sel)] if sel else nav_df
    if not n.empty:
        st.line_chart(n.pivot(index="trade_date", columns="portfolio", values="nav"))
        st.dataframe(n.sort_values(["trade_date", "portfolio"]), use_container_width=True, hide_index=True)
    else:
        st.write("No data for selected portfolios.")

# ---- Picks panel ----
st.header("Picks / 选股")
picks_df = _load_csv(out_dir, "picks_daily.csv")
if picks_df.empty:
    st.info("No picks_daily.csv or empty. Run make_dashboard first.")
else:
    picks_df["trade_date"] = pd.to_datetime(picks_df["trade_date"], errors="coerce")
    dates = sorted(picks_df["trade_date"].dropna().dt.date.unique())
    versions = sorted(picks_df["version"].dropna().unique().tolist())
    col1, col2 = st.columns(2)
    with col1:
        date_filter = st.selectbox("Trade date", options=dates, index=len(dates) - 1 if dates else 0)
    with col2:
        version_filter = st.multiselect("Version", options=versions, default=versions)
    sub = picks_df[
        (picks_df["trade_date"].dt.date == date_filter) & (picks_df["version"].isin(version_filter))
    ]
    cols_show = [c for c in ["trade_date", "version", "ticker", "name", "rank", "score", "score_100", "picked_by"] if c in sub.columns]
    st.dataframe(sub[cols_show] if cols_show else sub, use_container_width=True, hide_index=True)

# ---- Evaluation panel ----
st.header("Evaluation / 评估")

eval_summary_df = _load_csv(out_dir, "eval_summary.csv")
quintile_df = _load_csv(out_dir, "quintile_returns.csv")
ic_df = _load_csv(out_dir, "ic_series.csv")

if not eval_summary_df.empty:
    st.subheader("Eval summary (by version)")
    st.dataframe(eval_summary_df, use_container_width=True, hide_index=True)

if not quintile_df.empty:
    st.subheader("Quintile returns (mean_fwd_ret_5d)")
    quintile_df["as_of_date"] = pd.to_datetime(quintile_df["as_of_date"], errors="coerce")
    pivot_q = quintile_df.pivot_table(
        index="quintile", columns="version", values="mean_fwd_ret_5d", aggfunc="mean"
    )
    st.bar_chart(pivot_q)
    st.dataframe(quintile_df, use_container_width=True, hide_index=True)
else:
    st.info("No quintile_returns.csv or empty. Run make_dashboard (eval_5d_batch produces it).")

if not ic_df.empty:
    st.subheader("IC series (as_of_date → IC)")
    ic_df["as_of_date"] = pd.to_datetime(ic_df["as_of_date"], errors="coerce")
    ic_pivot = ic_df.pivot(index="as_of_date", columns="version", values="ic")
    st.line_chart(ic_pivot)
    st.dataframe(ic_df, use_container_width=True, hide_index=True)
else:
    st.info("No ic_series.csv or empty. Run make_dashboard (eval_5d_batch produces it).")

# Optional: version_compare, factor_analysis
version_compare_df = _load_csv(out_dir, "version_compare.csv")
factor_df = _load_csv(out_dir, "factor_analysis.csv")
if not version_compare_df.empty:
    st.subheader("Version compare")
    st.dataframe(version_compare_df, use_container_width=True, hide_index=True)
if not factor_df.empty:
    st.subheader("Factor analysis")
    st.dataframe(factor_df, use_container_width=True, hide_index=True)
