# apps/dashboard_streamlit/app.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple

import duckdb
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


# -----------------------------
# Paths / helpers
# -----------------------------
ROOT = Path(__file__).resolve().parents[2]  # .../apps/dashboard_streamlit/app.py -> project root
OUT_DIR = ROOT / "data" / "out"
OUT_NAV_DIR = OUT_DIR / "nav"
OUT_LB_DIR = OUT_DIR / "leaderboard"
OUT_EVAL_DIR = OUT_DIR / "eval"
STORE_CANDIDATES = [
    ROOT / "data" / "store" / "alpha_tracker.duckdb",
    ROOT / "data" / "store" / "alpha_tracker2.duckdb",
    ROOT / "store" / "alpha_tracker.duckdb",
]


def _first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists():
            return p
    return None


def _try_read_csv(p: Path) -> Optional[pd.DataFrame]:
    try:
        if p.exists():
            return pd.read_csv(p)
    except Exception:
        return None
    return None


def _latest_csv_by_prefix(folder: Path, prefix: str) -> Optional[Path]:
    if not folder.exists():
        return None
    files = sorted(folder.glob(f"{prefix}*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _parse_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(s).normalize()


# -----------------------------
# Data loading
# -----------------------------
@dataclass
class DataBundle:
    leaderboard: pd.DataFrame
    nav: pd.DataFrame
    eval5d: Optional[pd.DataFrame]
    db_path: Optional[Path]


def load_leaderboard_csv() -> Optional[pd.DataFrame]:
    p = OUT_LB_DIR / "leaderboard.csv"
    return _try_read_csv(p)


def load_nav_csv_for_range(start: str, end: str) -> Optional[pd.DataFrame]:
    # preferred exact name
    p = OUT_NAV_DIR / f"nav_daily_{start}_{end}.csv"
    df = _try_read_csv(p)
    if df is not None:
        return df

    # fallback: latest nav_daily_*.csv
    latest = _latest_csv_by_prefix(OUT_NAV_DIR, "nav_daily_")
    return _try_read_csv(latest) if latest else None


def load_eval_csv_for_range(start: str, end: str) -> Optional[pd.DataFrame]:
    p = OUT_EVAL_DIR / f"eval_5d_batch_daily_{start}_{end}.csv"
    df = _try_read_csv(p)
    if df is not None:
        return df

    latest = _latest_csv_by_prefix(OUT_EVAL_DIR, "eval_5d_batch_daily_")
    return _try_read_csv(latest) if latest else None


def load_from_duckdb(db_path: Path, start: str, end: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except Exception:
        return None, None, None

    try:
        lb = con.execute(
            """
            SELECT *
            FROM (
              SELECT
                strategy_id,
                (MAX(nav) / MIN(nav) - 1.0) AS total_return,
                MIN(nav / MAX(nav) OVER (PARTITION BY strategy_id ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1.0) AS max_drawdown,
                STDDEV_SAMP(daily_ret) AS vol_daily,
                MIN(nav) AS start_nav,
                MAX(nav) AS end_nav,
                COUNT(*) AS days
              FROM (
                SELECT
                  trade_date,
                  strategy_id,
                  nav,
                  (nav / LAG(nav) OVER (PARTITION BY strategy_id ORDER BY trade_date) - 1.0) AS daily_ret
                FROM nav_daily
                WHERE trade_date BETWEEN ? AND ?
              )
              GROUP BY strategy_id
            )
            ORDER BY total_return DESC
            """,
            [start, end],
        ).df()

        nav = con.execute(
            """
            SELECT *
            FROM nav_daily
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY trade_date, strategy_id
            """,
            [start, end],
        ).df()

        ev = None
        # eval table name in your project: eval_5d_batch_daily
        try:
            ev = con.execute(
                """
                SELECT *
                FROM eval_5d_batch_daily
                WHERE trade_date BETWEEN ? AND ?
                ORDER BY trade_date, strategy_id
                """,
                [start, end],
            ).df()
        except Exception:
            ev = None

        con.close()
        return lb, nav, ev
    except Exception:
        try:
            con.close()
        except Exception:
            pass
        return None, None, None


def load_bundle(start: str, end: str) -> DataBundle:
    # CSV first (fast, matches your pipeline outputs)
    lb = load_leaderboard_csv()
    nav = load_nav_csv_for_range(start, end)
    ev = load_eval_csv_for_range(start, end)

    db_path = _first_existing(STORE_CANDIDATES)

    # fallback to duckdb if needed
    if (lb is None or nav is None) and db_path is not None:
        lb2, nav2, ev2 = load_from_duckdb(db_path, start, end)
        if lb is None and lb2 is not None:
            lb = lb2
        if nav is None and nav2 is not None:
            nav = nav2
        if ev is None and ev2 is not None:
            ev = ev2

    if lb is None:
        lb = pd.DataFrame(columns=["strategy_id"])
    if nav is None:
        nav = pd.DataFrame(columns=["trade_date", "strategy_id", "nav", "turnover", "cost"])

    # normalize columns
    if "trade_date" in nav.columns:
        nav["trade_date"] = pd.to_datetime(nav["trade_date"])
    if "strategy_id" in lb.columns:
        lb["strategy_id"] = lb["strategy_id"].astype(str)

    return DataBundle(leaderboard=lb, nav=nav, eval5d=ev, db_path=db_path)


# -----------------------------
# Analytics / plots
# -----------------------------
def nav_pivot(nav: pd.DataFrame) -> pd.DataFrame:
    needed = {"trade_date", "strategy_id", "nav"}
    if not needed.issubset(set(nav.columns)):
        return pd.DataFrame()
    p = nav.pivot_table(index="trade_date", columns="strategy_id", values="nav", aggfunc="last").sort_index()
    return p


def draw_nav_curves(piv: pd.DataFrame, strategies: List[str]) -> None:
    if piv.empty or not strategies:
        st.info("nav 数据为空或未选择策略。")
        return

    show = piv[strategies].dropna(how="all")
    st.line_chart(show)


def draw_corr_heatmap(piv: pd.DataFrame, strategies: List[str]) -> None:
    if piv.empty or len(strategies) < 2:
        st.info("相关矩阵需要至少 2 个策略且 nav 数据有效。")
        return

    show = piv[strategies].dropna(how="all")
    rets = show.pct_change().dropna(how="all")
    if rets.shape[0] < 3:
        st.info("有效交易日太少，无法稳定计算相关矩阵。")
        return

    corr = rets.corr()

    fig = plt.figure()
    ax = fig.add_subplot(111)
    im = ax.imshow(corr.values)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_yticks(range(len(corr.index)))
    ax.set_xticklabels(corr.columns, rotation=90, fontsize=8)
    ax.set_yticklabels(corr.index, fontsize=8)
    ax.set_title("Strategy Return Correlation (daily)")

    # annotate
    for i in range(corr.shape[0]):
        for j in range(corr.shape[1]):
            ax.text(j, i, f"{corr.values[i, j]:.2f}", ha="center", va="center", fontsize=7)

    fig.tight_layout()
    st.pyplot(fig)


def enrich_leaderboard_with_turnover(nav: pd.DataFrame, lb: pd.DataFrame) -> pd.DataFrame:
    # add turnover_sum / cost_sum from nav table if present
    out = lb.copy()
    if out.empty or "strategy_id" not in out.columns:
        return out

    if {"strategy_id", "turnover"}.issubset(nav.columns):
        t = nav.groupby("strategy_id", as_index=False)["turnover"].sum().rename(columns={"turnover": "turnover_sum"})
        out = out.merge(t, on="strategy_id", how="left")
    if {"strategy_id", "cost"}.issubset(nav.columns):
        c = nav.groupby("strategy_id", as_index=False)["cost"].sum().rename(columns={"cost": "cost_sum"})
        out = out.merge(c, on="strategy_id", how="left")

    return out


def make_strategy_matrix(nav: pd.DataFrame, strategies: List[str]) -> pd.DataFrame:
    """
    “策略矩阵” = 每日每策略：nav / daily_ret / turnover / cost（如果有）
    """
    if nav.empty or not strategies:
        return pd.DataFrame()

    cols = ["trade_date", "strategy_id", "nav"]
    for x in ["turnover", "cost"]:
        if x in nav.columns:
            cols.append(x)

    df = nav[cols].copy()
    df = df[df["strategy_id"].isin(strategies)]
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date

    # daily_ret computed within each strategy
    df = df.sort_values(["strategy_id", "trade_date"])
    df["daily_ret"] = df.groupby("strategy_id")["nav"].pct_change()

    # pivot
    # Multi-metric: create wide columns like NAV__<sid>, RET__<sid>, ...
    parts = []
    for metric in ["nav", "daily_ret"] + ([m for m in ["turnover", "cost"] if m in df.columns]):
        tmp = df.pivot_table(index="trade_date", columns="strategy_id", values=metric, aggfunc="last")
        tmp.columns = [f"{metric.upper()}__{c}" for c in tmp.columns]
        parts.append(tmp)

    wide = pd.concat(parts, axis=1).sort_index()
    wide.index = pd.to_datetime(wide.index)
    return wide


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Alpha_Tracker2 Strategy Dashboard", layout="wide")

st.title("Alpha_Tracker2 Strategy Dashboard")

with st.sidebar:
    st.header("Research Window")
    start = st.text_input("start (YYYY-MM-DD)", value="2025-12-01")
    end = st.text_input("end (YYYY-MM-DD)", value="2026-01-14")

    st.divider()
    st.header("Filters")
    model_prefix = st.multiselect("Model Prefix (optional)", options=["V1", "V2", "V3", "V4", "ENS"], default=["V2", "V3", "V4"])
    topn = st.number_input("Top N strategies (by total_return)", min_value=3, max_value=200, value=30, step=1)

    st.divider()
    st.caption("优先读取 data/out 下 CSV；缺失时回退读取 DuckDB。")

bundle = load_bundle(start, end)
lb = bundle.leaderboard
nav = bundle.nav
ev = bundle.eval5d

if bundle.db_path:
    st.caption(f"DB detected: {bundle.db_path}")
else:
    st.caption("DB not detected (will rely on CSV outputs in data/out).")

# sanitize leaderboard
if not lb.empty and "strategy_id" in lb.columns:
    # optional filter by model prefixes
    if model_prefix:
        mask = False
        for p in model_prefix:
            mask = mask | lb["strategy_id"].astype(str).str.startswith(p + "__")
        lb = lb[mask].copy()

    # sort + topn
    if "total_return" in lb.columns:
        lb = lb.sort_values("total_return", ascending=False).head(int(topn)).copy()
else:
    st.warning("leaderboard 数据为空：请确认 tools/export_strategy_leaderboard.py 已输出 data/out/leaderboard/leaderboard.csv")

# enrich with turnover/cost sums from nav
lb2 = enrich_leaderboard_with_turnover(nav, lb)

# Strategy selection
all_strategies = lb2["strategy_id"].astype(str).tolist() if ("strategy_id" in lb2.columns) else []
default_pick = all_strategies[:10] if len(all_strategies) >= 10 else all_strategies

selected = st.multiselect(
    "Select strategies to plot / compare",
    options=all_strategies,
    default=default_pick,
)

colA, colB = st.columns([1, 1], gap="large")

with colA:
    st.subheader("Leaderboard (filtered)")
    st.dataframe(lb2, use_container_width=True, height=520)

with colB:
    st.subheader("NAV Curves")
    piv = nav_pivot(nav)
    draw_nav_curves(piv, selected)

st.divider()

c1, c2 = st.columns([1, 1], gap="large")
with c1:
    st.subheader("Strategy Return Correlation (Heatmap)")
    piv = nav_pivot(nav)
    draw_corr_heatmap(piv, selected)

with c2:
    st.subheader("Strategy Matrix (NAV / RET / Turnover / Cost)")
    matrix = make_strategy_matrix(nav, selected)
    if matrix.empty:
        st.info("矩阵为空：请先选择策略且确保 nav_daily 有 trade_date/strategy_id/nav 列。")
    else:
        st.dataframe(matrix, use_container_width=True, height=520)

st.divider()

st.subheader("Eval 5D (optional)")
if ev is None or ev.empty:
    st.info("未检测到 eval_5d 数据（CSV 或 DuckDB）。如果你跑过 tools/eval_5d_from_nav.py，应该能在 data/out/eval/ 下看到文件。")
else:
    # basic filtering by prefix
    if "strategy_id" in ev.columns and model_prefix:
        mask = False
        for p in model_prefix:
            mask = mask | ev["strategy_id"].astype(str).str.startswith(p + "__")
        ev_show = ev[mask].copy()
    else:
        ev_show = ev.copy()
    st.dataframe(ev_show, use_container_width=True, height=420)


#  .\.venv\Scripts\python.exe -m streamlit run apps/dashboard_streamlit/app.py
