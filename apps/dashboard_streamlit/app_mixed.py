from __future__ import annotations

import os
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# hard deps: pandas/streamlit
# optional deps: duckdb, requests, matplotlib
try:
    import duckdb  # type: ignore
except Exception:
    duckdb = None

try:
    import requests  # type: ignore
except Exception:
    requests = None

try:
    import matplotlib.pyplot as plt  # type: ignore
except Exception:
    plt = None


# =========================
# Utils
# =========================
def find_project_root(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "src" / "alpha_tracker2").exists():
            return cur
        if (cur / "data" / "store" / "alpha_tracker.duckdb").exists():
            return cur
        cur = cur.parent
    return start.resolve()


def newest_csv(out_dir: Path, pattern: str) -> Optional[Path]:
    files = sorted(out_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def safe_read_csv(path: Optional[Path]) -> Optional[pd.DataFrame]:
    if path is None or (not path.exists()):
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        # some csv might be utf-8-sig
        return pd.read_csv(path, encoding="utf-8-sig")


def _to_date(s: str) -> pd.Timestamp:
    return pd.to_datetime(s).normalize()


def _fmt_pct(x: float) -> str:
    try:
        return f"{x * 100:.2f}%"
    except Exception:
        return str(x)


def run_cmd(cmd: List[str], cwd: Path) -> Tuple[int, str]:
    p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, shell=False)
    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return p.returncode, out


@dataclass
class Paths:
    root: Path
    db_path: Path
    out_nav: Path
    out_eval: Path
    out_leaderboard: Path


def resolve_paths() -> Paths:
    root = find_project_root(Path(__file__).resolve())
    db_path = root / "data" / "store" / "alpha_tracker.duckdb"
    out_nav = root / "data" / "out" / "nav"
    out_eval = root / "data" / "out" / "eval"
    out_leaderboard = root / "data" / "out" / "leaderboard"
    return Paths(root=root, db_path=db_path, out_nav=out_nav, out_eval=out_eval, out_leaderboard=out_leaderboard)


def ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col])
    return df


# =========================
# DuckDB schema adapt
# =========================
def db_ok() -> bool:
    return duckdb is not None


def db_connect(db_path: Path):
    if duckdb is None:
        raise RuntimeError("duckdb 未安装。请先 pip install duckdb")
    return duckdb.connect(str(db_path))


def table_cols(con, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    # row: (cid, name, type, notnull, dflt_value, pk)
    return [r[1] for r in rows]


def detect_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


# =========================
# Loaders (CSV first, DB fallback)
# =========================
@st.cache_data(show_spinner=False)
def load_leaderboard(paths: Paths) -> Optional[pd.DataFrame]:
    p = paths.out_leaderboard / "leaderboard.csv"
    df = safe_read_csv(p)
    if df is not None:
        return df

    # fallback: derive from nav_daily if exists in DuckDB
    if not db_ok() or (not paths.db_path.exists()):
        return None
    con = db_connect(paths.db_path)
    try:
        if "nav_daily" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
            return None
        cols = table_cols(con, "nav_daily")
        dcol = detect_col(cols, ["trade_date", "date", "asof_date"])
        scol = detect_col(cols, ["strategy_id", "strategy"])
        navcol = detect_col(cols, ["nav"])
        if not (dcol and scol and navcol):
            return None

        q = f"""
        SELECT
            {scol} AS strategy_id,
            MIN({navcol}) AS start_nav,
            MAX({navcol}) AS end_nav,
            (MAX({navcol})/NULLIF(MIN({navcol}),0)-1) AS total_return
        FROM nav_daily
        GROUP BY 1
        """
        df = con.execute(q).df()
        return df
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def load_nav(paths: Paths, start: str, end: str) -> Optional[pd.DataFrame]:
    # prefer exact-range file
    exact = paths.out_nav / f"nav_daily_{start}_{end}.csv"
    df = safe_read_csv(exact)

    # else newest
    if df is None:
        df = safe_read_csv(newest_csv(paths.out_nav, "nav_daily_*.csv"))

    if df is not None:
        # normalize schema
        if "trade_date" not in df.columns and "date" in df.columns:
            df = df.rename(columns={"date": "trade_date"})
        df = ensure_datetime(df, "trade_date")
        return df

    # DB fallback
    if not db_ok() or (not paths.db_path.exists()):
        return None

    con = db_connect(paths.db_path)
    try:
        if "nav_daily" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
            return None
        cols = table_cols(con, "nav_daily")
        dcol = detect_col(cols, ["trade_date", "date", "asof_date"])
        scol = detect_col(cols, ["strategy_id", "strategy"])
        navcol = detect_col(cols, ["nav"])
        tcol = detect_col(cols, ["turnover"])
        ccol = detect_col(cols, ["cost"])
        if not (dcol and scol and navcol):
            return None

        q = f"""
        SELECT
            {dcol} AS trade_date,
            {scol} AS strategy_id,
            {navcol} AS nav
            {"," if tcol else ""} {tcol} AS turnover
            {"," if ccol else ""} {ccol} AS cost
        FROM nav_daily
        WHERE {dcol} BETWEEN ? AND ?
        """
        df = con.execute(q, [start, end]).df()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def load_eval(paths: Paths, start: str, end: str) -> Optional[pd.DataFrame]:
    exact = paths.out_eval / f"eval_5d_batch_daily_{start}_{end}.csv"
    df = safe_read_csv(exact)
    if df is None:
        df = safe_read_csv(newest_csv(paths.out_eval, "eval_5d_batch_daily_*.csv"))
    if df is not None:
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    return None


# =========================
# Matrix: NAV/RET/Turnover/Cost wide table
# =========================
def make_nav_wide(nav_df: pd.DataFrame, strategy_ids: List[str]) -> pd.DataFrame:
    d = nav_df.copy()
    d = d[d["strategy_id"].isin(strategy_ids)].copy()
    d = d.sort_values(["trade_date", "strategy_id"])

    # daily return
    d["ret"] = d.groupby("strategy_id")["nav"].pct_change().fillna(0.0)

    # wide pivots
    wide_nav = d.pivot(index="trade_date", columns="strategy_id", values="nav")
    wide_ret = d.pivot(index="trade_date", columns="strategy_id", values="ret")

    out = pd.DataFrame(index=wide_nav.index).sort_index()
    for sid in wide_nav.columns:
        out[f"NAV__{sid}"] = wide_nav[sid]
        out[f"RET__{sid}"] = wide_ret[sid]

    if "turnover" in d.columns:
        wide_t = d.pivot(index="trade_date", columns="strategy_id", values="turnover")
        for sid in wide_t.columns:
            out[f"TURNOVER__{sid}"] = wide_t[sid]

    if "cost" in d.columns:
        wide_c = d.pivot(index="trade_date", columns="strategy_id", values="cost")
        for sid in wide_c.columns:
            out[f"COST__{sid}"] = wide_c[sid]

    out = out.reset_index().rename(columns={"trade_date": "trade_date"})
    return out


# =========================
# Holdings / Picks matrix from DuckDB
# =========================
@st.cache_data(show_spinner=False)
def load_positions_matrix_from_db(
    db_path: str,
    start: str,
    end: str,
    strategy_id: str,
    top_tickers: int = 30,
    value_mode: str = "weight",  # "weight" or "binary"
) -> Optional[pd.DataFrame]:
    """
    Return a wide matrix: index=trade_date, columns=ticker, values=weight or 1/0
    """
    if duckdb is None:
        return None
    p = Path(db_path)
    if not p.exists():
        return None

    con = db_connect(p)
    try:
        if "positions_daily" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
            return None

        cols = table_cols(con, "positions_daily")
        dcol = detect_col(cols, ["asof_date", "trade_date", "date"])
        scol = detect_col(cols, ["strategy_id"])
        tcol = detect_col(cols, ["ticker", "symbol"])
        mvcol = detect_col(cols, ["market_value", "mv"])
        cashcol = detect_col(cols, ["cash"])
        if not (dcol and scol and tcol and mvcol):
            return None

        # pull long form
        q = f"""
        SELECT
            {dcol} AS trade_date,
            {tcol} AS ticker,
            {mvcol} AS market_value
            {"," if cashcol else ""} {cashcol} AS cash
        FROM positions_daily
        WHERE {scol} = ?
          AND {dcol} BETWEEN ? AND ?
        """
        df = con.execute(q, [strategy_id, start, end]).df()
        if df.empty:
            return None
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)

        if "cash" in df.columns:
            df["cash"] = pd.to_numeric(df["cash"], errors="coerce").fillna(0.0)
        else:
            df["cash"] = 0.0

        # compute daily equity for weight
        g = df.groupby("trade_date", as_index=False).agg(
            total_mv=("market_value", "sum"),
            cash=("cash", "max"),
        )
        g["equity"] = g["total_mv"] + g["cash"]
        df = df.merge(g[["trade_date", "equity"]], on="trade_date", how="left")
        df["weight"] = np.where(df["equity"] > 0, df["market_value"] / df["equity"], 0.0)

        # select top tickers by presence count (or avg weight)
        if value_mode == "weight":
            score = df.groupby("ticker")["weight"].mean().sort_values(ascending=False)
        else:
            score = df.groupby("ticker")["market_value"].apply(lambda s: (s > 0).sum()).sort_values(ascending=False)
        keep = score.head(top_tickers).index.tolist()
        df = df[df["ticker"].isin(keep)].copy()

        val = "weight" if value_mode == "weight" else "binary"
        if value_mode == "binary":
            df["binary"] = (df["market_value"] > 0).astype(int)

        mat = df.pivot_table(index="trade_date", columns="ticker", values=val, aggfunc="max", fill_value=0.0)
        mat = mat.sort_index()
        mat.index = mat.index.astype("datetime64[ns]")
        mat = mat.reset_index()
        return mat
    finally:
        con.close()


@st.cache_data(show_spinner=False)
def load_picks_matrix_from_db(
    db_path: str,
    start: str,
    end: str,
    strategy_or_version: str,
    mode: str = "auto",  # "auto" -> strategy_id if exists else version
    top_tickers: int = 30,
) -> Optional[pd.DataFrame]:
    """
    Picks matrix:
      - If picks_daily has strategy_id: filter by strategy_id
      - Else if has version: filter by version
    Return wide matrix: trade_date x ticker (1/0)
    """
    if duckdb is None:
        return None
    p = Path(db_path)
    if not p.exists():
        return None

    con = db_connect(p)
    try:
        if "picks_daily" not in [r[0] for r in con.execute("SHOW TABLES").fetchall()]:
            return None

        cols = table_cols(con, "picks_daily")
        dcol = detect_col(cols, ["trade_date", "asof_date", "date"])
        tcol = detect_col(cols, ["ticker", "symbol"])
        sidcol = detect_col(cols, ["strategy_id"])
        vcol = detect_col(cols, ["version", "model", "model_version"])
        if not (dcol and tcol):
            return None

        use_strategy = (mode == "strategy") or (mode == "auto" and sidcol is not None)

        if use_strategy and sidcol:
            q = f"""
            SELECT {dcol} AS trade_date, {tcol} AS ticker
            FROM picks_daily
            WHERE {sidcol} = ?
              AND {dcol} BETWEEN ? AND ?
            """
            df = con.execute(q, [strategy_or_version, start, end]).df()
        elif vcol:
            q = f"""
            SELECT {dcol} AS trade_date, {tcol} AS ticker
            FROM picks_daily
            WHERE {vcol} = ?
              AND {dcol} BETWEEN ? AND ?
            """
            df = con.execute(q, [strategy_or_version, start, end]).df()
        else:
            return None

        if df.empty:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # top tickers by frequency
        top = df["ticker"].value_counts().head(top_tickers).index.tolist()
        df = df[df["ticker"].isin(top)].copy()
        df["v"] = 1

        mat = df.pivot_table(index="trade_date", columns="ticker", values="v", aggfunc="max", fill_value=0)
        mat = mat.sort_index().reset_index()
        return mat
    finally:
        con.close()


# =========================
# Plot helpers (matplotlib only to avoid extra deps)
# =========================
def plot_nav_curves(nav_df: pd.DataFrame, strategy_ids: List[str], title: str):
    if plt is None:
        st.error("缺少 matplotlib (matplotlib)。请 pip install matplotlib")
        return
    d = nav_df.copy()
    d = d[d["strategy_id"].isin(strategy_ids)].copy()
    if d.empty:
        st.info("没有可画的 NAV 数据。")
        return

    d = d.sort_values(["trade_date", "strategy_id"])
    fig = plt.figure()
    for sid, g in d.groupby("strategy_id"):
        g = g.sort_values("trade_date")
        plt.plot(g["trade_date"], g["nav"], label=sid)
    plt.title(title)
    plt.xlabel("trade_date")
    plt.ylabel("NAV")
    plt.legend(fontsize=8)
    st.pyplot(fig, clear_figure=True)


def plot_corr_heatmap(nav_df: pd.DataFrame, strategy_ids: List[str], title: str):
    if plt is None:
        st.error("缺少 matplotlib (matplotlib)。请 pip install matplotlib")
        return

    d = nav_df.copy()
    d = d[d["strategy_id"].isin(strategy_ids)].copy()
    if d.empty:
        st.info("没有可计算相关性的 NAV 数据。")
        return

    d = d.sort_values(["trade_date", "strategy_id"])
    d["ret"] = d.groupby("strategy_id")["nav"].pct_change().fillna(0.0)
    wide = d.pivot(index="trade_date", columns="strategy_id", values="ret").dropna(axis=1, how="all").fillna(0.0)

    if wide.shape[1] < 2:
        st.info("策略数量不足，无法画相关性矩阵。")
        return

    corr = wide.corr().values
    labels = list(wide.columns)

    fig = plt.figure(figsize=(6, 5))
    ax = plt.gca()
    im = ax.imshow(corr)
    ax.set_title(title)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=90, fontsize=7)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=7)

    # annotate
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j, i, f"{corr[i, j]:.2f}", ha="center", va="center", fontsize=7)

    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


def plot_matrix_heatmap(mat_df: pd.DataFrame, title: str, value_fmt: str = "float"):
    if plt is None:
        st.error("缺少 matplotlib (matplotlib)。请 pip install matplotlib")
        return
    if mat_df is None or mat_df.empty:
        st.info("矩阵为空。")
        return

    # first col is trade_date
    d = mat_df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    tickers = [c for c in d.columns if c != "trade_date"]
    if not tickers:
        st.info("没有 ticker 列可画。")
        return

    data = d[tickers].values
    fig = plt.figure(figsize=(min(12, 0.35 * len(tickers) + 3), 5))
    ax = plt.gca()
    ax.imshow(data, aspect="auto")
    ax.set_title(title)
    ax.set_yticks(range(len(d)))
    ax.set_yticklabels([dt.strftime("%Y-%m-%d") for dt in d["trade_date"]], fontsize=7)
    ax.set_xticks(range(len(tickers)))
    ax.set_xticklabels(tickers, rotation=90, fontsize=7)
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)


# =========================
# LLM summary (optional)
# =========================
def llm_summarize(prompt: str) -> Optional[str]:
    """
    Priority:
      1) Local Ollama (Ollama): http://localhost:11434
      2) OpenAI API (OpenAI API): env OPENAI_API_KEY
      3) None
    """
    # 1) Ollama
    if requests is not None:
        try:
            # You can change to your local model name
            ollama_model = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")
            r = requests.post(
                "http://localhost:11434/api/generate",
                json={"model": ollama_model, "prompt": prompt, "stream": False},
                timeout=20,
            )
            if r.status_code == 200:
                j = r.json()
                text = j.get("response")
                if text:
                    return text.strip()
        except Exception:
            pass

    # 2) OpenAI API (simple REST call, no extra deps)
    api_key = os.getenv("OPENAI_API_KEY")
    if requests is not None and api_key:
        try:
            # Uses Chat Completions style compatible endpoint; adapt if your gateway differs
            # If you use OpenAI official endpoint, set OPENAI_BASE_URL accordingly.
            base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            r = requests.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "你是量化研究助手，输出中文、结构化、可执行的分析结论。"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                },
                timeout=30,
            )
            if r.status_code == 200:
                j = r.json()
                text = j["choices"][0]["message"]["content"]
                return text.strip()
        except Exception:
            pass

    return None


def rule_based_summary(leader_df: pd.DataFrame, chosen: List[str]) -> str:
    d = leader_df[leader_df["strategy_id"].isin(chosen)].copy()
    if d.empty:
        return "当前没有可解读的策略。"

    # best by total_return
    if "total_return" in d.columns:
        d = d.sort_values("total_return", ascending=False)
    best = d.iloc[0].to_dict()

    parts = []
    parts.append(f"1）当前选中策略中，收益最高的是：{best.get('strategy_id')}，区间总收益约 {_fmt_pct(float(best.get('total_return', 0)))}。")
    if "max_drawdown" in d.columns:
        parts.append(f"2）最大回撤（max_drawdown）用于衡量区间最差下行：选中策略里最深回撤约 {_fmt_pct(float(d['max_drawdown'].min()))}。")
    parts.append("3）如果相关性热力图显示多数策略相关性接近 1，说明它们的信号或持仓高度重合；此时应优先保留“收益/回撤更优、换手更低”的那一组。")
    parts.append("4）下一步建议：用“每日持仓矩阵”检查不同策略是否实际上持仓一致；若一致，需要回到信号生成/调仓规则，拉开差异。")
    return "\n".join(parts)


# =========================
# UI
# =========================
st.set_page_config(page_title="Alpha_Tracker2 (Alpha_Tracker2) Strategy Dashboard", layout="wide")

paths = resolve_paths()

st.title("Alpha_Tracker2 策略看板（Strategy Dashboard）")
st.caption(f"DB detected (DuckDB): {paths.db_path}")

with st.sidebar:
    st.header("Research Window（研究窗口）")
    start = st.date_input("start (YYYY-MM-DD)", value=pd.to_datetime("2025-12-01").date())
    end = st.date_input("end (YYYY-MM-DD)", value=pd.to_datetime("2026-01-14").date())
    start_s = str(start)
    end_s = str(end)

    st.divider()
    st.header("Filters（筛选）")

    leader_df = load_leaderboard(paths)
    if leader_df is None or leader_df.empty:
        st.error("leaderboard 不存在/为空。先运行 tools/run_research_window.py 或 tools/run_report_bundle.py 生成 data/out。")
        st.stop()

    # normalize
    if "strategy_id" not in leader_df.columns:
        st.error("leaderboard 缺少 strategy_id 列。")
        st.stop()

    all_sids = leader_df["strategy_id"].dropna().astype(str).unique().tolist()
    all_sids = sorted(all_sids)

    # model prefix filter (V2/V3/V4)
    prefixes = sorted({sid.split("__", 1)[0] for sid in all_sids if "__" in sid})
    selected_prefix = st.multiselect("Model Prefix (optional)", prefixes, default=prefixes)

    topn = st.number_input("Top N strategies (by total_return)", min_value=5, max_value=200, value=30, step=5)

    # filtered leaderboard
    fdf = leader_df.copy()
    fdf["strategy_id"] = fdf["strategy_id"].astype(str)
    if selected_prefix:
        fdf = fdf[fdf["strategy_id"].apply(lambda x: x.split("__", 1)[0] in set(selected_prefix))].copy()

    if "total_return" in fdf.columns:
        fdf = fdf.sort_values("total_return", ascending=False)

    fdf = fdf.head(int(topn)).copy()

    default_pick = fdf["strategy_id"].head(min(8, len(fdf))).tolist()
    chosen_sids = st.multiselect("Select strategies to plot / compare", fdf["strategy_id"].tolist(), default=default_pick)

    st.divider()
    st.caption("优先读取 data/out 下 CSV；缺失时回退读取 DuckDB (DuckDB)。")

    st.divider()
    st.subheader("一键刷新（可选）")
    st.caption("如果你想从 Dashboard 内触发生成数据：")
    do_run = st.button("运行 run_research_window.py（生成/更新 data/out）", type="primary")
    if do_run:
        cmd = [
            str(paths.root / ".venv" / "Scripts" / "python.exe"),
            str(paths.root / "tools" / "run_research_window.py"),
            "--start",
            start_s,
            "--end",
            end_s,
            "--models",
            ",".join(selected_prefix) if selected_prefix else "V2,V3,V4",
            "--cost_bps",
            "10",
            "--initial_equity",
            "100000",
            "--diag_model",
            "V4",
            "--do_backfill_turnover",
            "--do_report_bundle",
        ]
        rc, out = run_cmd(cmd, cwd=paths.root)
        st.code(out)
        if rc == 0:
            st.success("运行完成。请刷新页面（R）或重新选择策略。")
        else:
            st.error("运行失败：请复制上面日志给我继续修。")

# main area loads
nav_df = load_nav(paths, start_s, end_s)
eval_df = load_eval(paths, start_s, end_s)

if nav_df is None or nav_df.empty:
    st.warning("nav 数据为空：请先生成 data/out/nav 或确保 DuckDB nav_daily 存在。")
    st.stop()

# Normalize nav schema
nav_df = nav_df.copy()
if "trade_date" not in nav_df.columns:
    st.error("nav 数据缺少 trade_date 列。")
    st.stop()
if "strategy_id" not in nav_df.columns:
    st.error("nav 数据缺少 strategy_id 列。")
    st.stop()
nav_df["strategy_id"] = nav_df["strategy_id"].astype(str)
nav_df = ensure_datetime(nav_df, "trade_date")

# Default chosen if empty
if not chosen_sids:
    chosen_sids = nav_df["strategy_id"].unique().tolist()[:5]

# layout
col_left, col_right = st.columns([1.1, 1.0], gap="large")

with col_left:
    st.subheader("Leaderboard（榜单，已筛选）")
    st.markdown(
        """
**说明（中文）**：这里展示策略在研究窗口内的核心指标：  
- **total_return**：区间总收益  
- **max_drawdown**：最大回撤（越小越好）  
- **vol_daily**：日波动率（用于衡量稳定性）  
你可以在左侧选择策略，右侧曲线和矩阵会同步更新。
        """.strip()
    )
    show_cols = [c for c in ["strategy_id", "total_return", "max_drawdown", "vol_daily", "start_nav", "end_nav", "days"] if c in fdf.columns]
    st.dataframe(fdf[show_cols], use_container_width=True, height=360)

with col_right:
    st.subheader("NAV Curves（净值曲线）")
    st.markdown(
        """
**说明（中文）**：净值曲线（NAV）用于观察不同策略在时间序列上的表现差异。  
如果多条曲线几乎重合，通常意味着：信号/持仓非常接近，需要用“持仓矩阵”进一步核查。
        """.strip()
    )
    plot_nav_curves(nav_df, chosen_sids, title=f"NAV Curves: {start_s} .. {end_s}")

st.divider()

# second row
c1, c2 = st.columns([1.0, 1.0], gap="large")

with c1:
    st.subheader("Strategy Return Correlation（策略收益相关性热力图）")
    st.markdown(
        """
**说明（中文）**：计算“日收益率”的相关系数。  
- 接近 **1**：高度同涨同跌（可能持仓/信号重合）  
- 接近 **0**：相对独立（更利于组合分散）  
        """.strip()
    )
    plot_corr_heatmap(nav_df, chosen_sids, title="Daily Return Correlation (selected strategies)")

with c2:
    st.subheader("Strategy Matrix（NAV / RET / Turnover / Cost 宽表）")
    st.markdown(
        """
**说明（中文）**：把选中策略的 NAV/RET/换手/成本拼成一张宽表，便于你做进一步分析、回归或导出到 Excel。  
- **RET__**：日收益率（由 NAV 计算）  
- **TURNOVER__/COST__**：若 nav_daily 有这两列就会展示  
        """.strip()
    )
    wide = make_nav_wide(nav_df, chosen_sids)
    st.dataframe(wide, use_container_width=True, height=360)
    csv_bytes = wide.to_csv(index=False).encode("utf-8-sig")
    st.download_button("下载 Strategy Matrix CSV", data=csv_bytes, file_name=f"strategy_matrix_{start_s}_{end_s}.csv", mime="text/csv")

st.divider()

# holdings/picks matrix (next step you asked)
st.subheader("策略矩阵（每日持仓 / 票池矩阵）— 直接读 DuckDB (DuckDB)")
st.markdown(
    """
**说明（中文）**：这是你要求的“继续推进”的关键模块：  
- **每日持仓矩阵（positions_daily）**：date × ticker，默认展示 **weight（市值/权益）**，也可切换为 0/1。  
- **每日票池矩阵（picks_daily）**：date × ticker，展示当天是否入选票池（1/0）。  
> 该模块直接从 DuckDB 表里拉数据，不依赖 data/out 的 CSV。
    """.strip()
)

db_path_input = st.text_input("DuckDB 路径（DuckDB path）", value=str(paths.db_path))

m1, m2 = st.columns([1.0, 1.0], gap="large")

with m1:
    st.markdown("### A) 每日持仓矩阵（positions_daily）")
    if db_ok():
        sid_for_pos = st.selectbox("选择策略（strategy_id）", options=chosen_sids, index=0)
        topk_tickers = st.slider("Top tickers（按平均权重/频次）", min_value=10, max_value=80, value=30, step=5)
        value_mode = st.radio("矩阵值类型", options=["weight", "binary"], index=0, horizontal=True)

        pos_mat = load_positions_matrix_from_db(
            db_path=db_path_input,
            start=start_s,
            end=end_s,
            strategy_id=sid_for_pos,
            top_tickers=int(topk_tickers),
            value_mode=value_mode,
        )

        if pos_mat is None or pos_mat.empty:
            st.info("positions_daily 无数据/或 schema 不匹配。把 PRAGMA table_info('positions_daily') 输出给我，我继续修适配。")
        else:
            st.caption("建议：若不同策略持仓矩阵几乎一致，说明策略差异主要不在调仓规则，而在信号/票池端。")
            plot_matrix_heatmap(pos_mat, title=f"Holdings Matrix ({value_mode}) — {sid_for_pos}")
            with st.expander("查看矩阵数据（表格）", expanded=False):
                st.dataframe(pos_mat, use_container_width=True, height=320)
    else:
        st.error("duckdb 未安装：pip install duckdb")

with m2:
    st.markdown("### B) 每日票池矩阵（picks_daily）")
    if db_ok():
        mode = st.radio("票池筛选维度", options=["auto", "strategy", "version"], index=0, horizontal=True)
        st.caption("auto：如果 picks_daily 有 strategy_id 就按策略，否则按 version。")

        # default: pick strategy_id; if user wants version, they can type V2/V3/V4
        default_key = chosen_sids[0] if chosen_sids else "V4"
        key = st.text_input("strategy_id 或 version（如 V2/V3/V4）", value=default_key)
        topk2 = st.slider("Top tickers（按入选频次）", min_value=10, max_value=80, value=30, step=5, key="topk_picks")

        picks_mat = load_picks_matrix_from_db(
            db_path=db_path_input,
            start=start_s,
            end=end_s,
            strategy_or_version=key.strip(),
            mode=mode,
            top_tickers=int(topk2),
        )

        if picks_mat is None or picks_mat.empty:
            st.info("picks_daily 无数据/或 schema 不匹配。把 PRAGMA table_info('picks_daily') 输出给我，我继续修适配。")
        else:
            plot_matrix_heatmap(picks_mat, title=f"Picks Matrix (1/0) — {key.strip()}")
            with st.expander("查看票池矩阵数据（表格）", expanded=False):
                st.dataframe(picks_mat, use_container_width=True, height=320)
    else:
        st.error("duckdb 未安装：pip install duckdb")

st.divider()

# Eval 5D
st.subheader("Eval 5D（可选：5日评估结果）")
st.markdown(
    """
**说明（中文）**：Eval 5D 用于衡量策略信号在未来 5 个交易日的有效性（覆盖率/命中率/平均收益等）。  
你可以结合 Leaderboard 和持仓矩阵判断：  
- **收益高但命中率低**：可能靠少数大赢家拉动  
- **命中率高但收益一般**：可能更稳健但弹性不足  
    """.strip()
)

if eval_df is None or eval_df.empty:
    st.info("eval_5d 文件不存在或为空（data/out/eval）。你已经能生成了，确保选择的日期范围对应到文件即可。")
else:
    d = eval_df.copy()
    if "strategy_id" in d.columns:
        d["strategy_id"] = d["strategy_id"].astype(str)
        d = d[d["strategy_id"].isin(chosen_sids)].copy()
    if "trade_date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["trade_date"])
        d = d.sort_values("trade_date")

    st.dataframe(d, use_container_width=True, height=260)

st.divider()

# Auto interpretation
st.subheader("自动解读（可选：LLM / 规则引擎）")
st.markdown(
    """
**说明（中文）**：  
- 如果你本机跑了 **Ollama (Ollama)**，会自动调用本地模型输出解读文字；  
- 或设置 **OpenAI API (OpenAI API)** 环境变量 `OPENAI_API_KEY`；  
- 否则使用规则引擎输出“可执行建议”。  
    """.strip()
)

with st.expander("生成解读", expanded=False):
    prompt = st.text_area(
        "Prompt（可改）",
        value=(
            "请基于以下策略指标做中文量化研究解读：\n"
            f"- 研究区间：{start_s} ~ {end_s}\n"
            f"- 选中策略：{chosen_sids}\n"
            "请输出：\n"
            "1) 哪些策略表现最优（收益/回撤/波动）\n"
            "2) 策略之间是否高度相关，如何做组合去冗余\n"
            "3) 结合换手/成本给出下一步可执行建议\n"
            "要求：中文，条目化，直接可执行。\n"
        ),
        height=160,
    )
    if st.button("生成解读文本"):
        txt = llm_summarize(prompt)
        if txt:
            st.success("LLM 解读生成成功。")
            st.write(txt)
        else:
            st.warning("未检测到 Ollama 或 OPENAI_API_KEY，使用规则引擎输出。")
            st.write(rule_based_summary(leader_df, chosen_sids))


# .\.venv\Scripts\python.exe -m streamlit run apps/dashboard_streamlit/app_mixed.py