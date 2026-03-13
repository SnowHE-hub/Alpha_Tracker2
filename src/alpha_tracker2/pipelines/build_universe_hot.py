# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import duckdb

from alpha_tracker2.modeling.plugins.universe.hot_industry import build_hot_industry_universe


def _project_root_from_here(p: Path) -> Path:
    """
    build_universe_hot.py 位于: src/alpha_tracker2/pipelines/build_universe_hot.py
    parents:
      0 pipelines
      1 alpha_tracker2
      2 src
      3 project_root
    """
    return p.resolve().parents[3]


def _resolve_db_path(project_root: Path) -> Path:
    # 兼容你当前项目常用路径
    cands = [
        project_root / "data" / "store" / "alpha_tracker.duckdb",
        project_root / "store" / "alpha_tracker.duckdb",
        project_root / "data" / "store" / "alpha_tracker2.duckdb",
    ]
    for p in cands:
        if p.exists():
            return p
    # 默认用第一候选（即使不存在也会自动创建目录）
    return cands[0]


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _date_range(start: str, end: str) -> list[str]:
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    out = []
    cur = s
    while cur <= e:
        # 只跑交易日的话你后面可以改成查 prices_daily 的交易日集合
        out.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)
    return out


def _create_tables_if_needed(con: duckdb.DuckDBPyConnection) -> None:
    # 专用 universe 表（不会污染你策略 picks）
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS universe_daily_hot (
            trade_date DATE,
            ticker VARCHAR,
            name VARCHAR,
            industry VARCHAR,
            hot_industry_rank INTEGER,
            stock_rank_in_industry INTEGER,
            amount DOUBLE,
            pct_change DOUBLE,
            stock_score DOUBLE,
            created_at TIMESTAMP DEFAULT now()
        );
        """
    )
    # 如果你希望“universe 也走 picks_daily”的方式（方便复用你现有流水线），这里也给你写入支持
    # 注意：你库里 picks_daily 已经存在且字段可能更多，但 trade_date/version/ticker/rank/score/picked_by 应该有
    # 我们只做 insert，并且用 purge 方式避免重复。
    # 这里不创建 picks_daily（你库已有），只在写入时报错再提示。



def _write_universe_to_db(
    con: duckdb.DuckDBPyConnection,
    uni: pd.DataFrame,
    purge: bool = True,
) -> None:
    """
    写入 universe_daily_hot。
    为了兼容 hot_industry 插件/数据源字段缺失，这里会自动补齐列，避免 DuckDB binder error。
    """

    # 1) 需要的列（和 universe_daily_hot 表字段一致）
    required_cols = [
        "trade_date",
        "ticker",
        "name",
        "industry",
        "hot_industry_rank",
        "stock_rank_in_industry",
        "amount",
        "pct_change",
        "stock_score",
    ]

    # 2) 补齐缺失列（关键：避免 SQL 引用不存在列时报 BinderError）
    uni = uni.copy()
    for c in required_cols:
        if c not in uni.columns:
            # name 缺失就先给空字符串；数值列先给 NA，后面再 to_numeric
            if c == "name":
                uni[c] = ""
            else:
                uni[c] = pd.NA

    # 3) 基础类型清洗（避免后续写库隐式转换问题）
    uni["trade_date"] = pd.to_datetime(uni["trade_date"]).dt.date
    uni["ticker"] = uni["ticker"].astype(str)
    uni["name"] = uni["name"].astype(str)
    uni["industry"] = uni["industry"].astype(str)

    for c in ["hot_industry_rank", "stock_rank_in_industry"]:
        uni[c] = pd.to_numeric(uni[c], errors="coerce").astype("Int64")

    for c in ["amount", "pct_change", "stock_score", "hot_score"]:
        if c in uni.columns:
            uni[c] = pd.to_numeric(uni[c], errors="coerce")

    # 如果插件没给 stock_score，但给了 hot_score，就用 hot_score 顶一下（至少可用）
    if uni["stock_score"].isna().all() and "hot_score" in uni.columns:
        uni["stock_score"] = pd.to_numeric(uni["hot_score"], errors="coerce")

    # 4) 注册并写入
    con.register("u_df", uni[required_cols])

    if purge:
        con.execute(
            """
            DELETE FROM universe_daily_hot
            WHERE trade_date = (SELECT DISTINCT trade_date FROM u_df LIMIT 1);
            """
        )

    con.execute(
        """
        INSERT INTO universe_daily_hot(
            trade_date, ticker, name, industry,
            hot_industry_rank, stock_rank_in_industry,
            amount, pct_change, stock_score
        )
        SELECT
            trade_date::DATE, ticker, name, industry,
            hot_industry_rank, stock_rank_in_industry,
            amount, pct_change, stock_score
        FROM u_df;
        """
    )
    con.unregister("u_df")


def _write_universe_to_picks_daily(
    con: duckdb.DuckDBPyConnection,
    uni: pd.DataFrame,
    version: str = "UNIVERSE_HOT",
    purge: bool = True,
) -> None:
    # 将 universe 写入 picks_daily，方便你后续 pipeline 统一用 picks_daily 做输入
    # rank 用 universe 的全局顺序；score 用 stock_score；picked_by 标记来源
    d = str(uni["trade_date"].iloc[0])
    out = uni.copy()
    out = out.reset_index(drop=True)
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
    out["version"] = version
    out["rank"] = (out.index + 1).astype(int)
    out["score"] = pd.to_numeric(out["stock_score"], errors="coerce").fillna(0.0)
    out["picked_by"] = "universe_hot_industry"

    cols = ["trade_date", "version", "ticker", "rank", "score", "picked_by"]
    con.register("p_df", out[cols])

    if purge:
        con.execute(
            """
            DELETE FROM picks_daily WHERE trade_date = ? AND version = ?;
            """,
            [d, version],
        )

    con.execute(
        """
        INSERT INTO picks_daily(trade_date, version, ticker, rank, score, picked_by)
        SELECT trade_date, version, ticker, rank, score, picked_by FROM p_df;
        """
    )
    con.unregister("p_df")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="single date YYYY-MM-DD (preferred for daily)")
    ap.add_argument("--start", help="start date YYYY-MM-DD (optional)")
    ap.add_argument("--end", help="end date YYYY-MM-DD (optional)")
    ap.add_argument("--top_industries", type=int, default=8)
    ap.add_argument("--per_industry", type=int, default=30)
    ap.add_argument("--max_universe", type=int, default=300)
    ap.add_argument("--write_picks_daily", action="store_true", help="also write to picks_daily as version=UNIVERSE_HOT")
    ap.add_argument("--purge", action="store_true", help="purge same-day rows before insert")
    ap.add_argument("--out_dir", default="data/out/universe", help="csv output folder (relative to project root)")
    args = ap.parse_args()

    if not args.date and not (args.start and args.end):
        raise SystemExit("Must provide --date OR (--start and --end).")

    project_root = _project_root_from_here(Path(__file__))
    db_path = _resolve_db_path(project_root)
    _ensure_parent(db_path)

    out_dir = project_root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.date:
        dates = [args.date]
    else:
        dates = _date_range(args.start, args.end)

    print(f"[DB] {db_path}")
    print(f"[OUT] {out_dir}")
    print(f"[CFG] top_industries={args.top_industries} per_industry={args.per_industry} max_universe={args.max_universe}")
    print(f"[DATES] {dates[0]} ~ {dates[-1]}  (n={len(dates)})")

    con = duckdb.connect(str(db_path))
    try:
        _create_tables_if_needed(con)

        for d in dates:
            print(f"\n=== [BUILD UNIVERSE HOT] trade_date={d} ===")
            uni, inds = build_hot_industry_universe(
                trade_date=d,
                top_industries=args.top_industries,
                per_industry=args.per_industry,
                max_universe=args.max_universe,
                verbose=True,
            )

            # 导出 CSV（可复现）
            uni_csv = out_dir / f"universe_hot_{d}_N{len(uni)}.csv"
            inds_csv = out_dir / f"hot_industries_{d}_top{args.top_industries}.csv"
            uni.to_csv(uni_csv, index=False, encoding="utf-8-sig")
            inds.to_csv(inds_csv, index=False, encoding="utf-8-sig")
            print(f"[OK] exported: {uni_csv}")
            print(f"[OK] exported: {inds_csv}")

            # 写 DB
            _write_universe_to_db(con, uni, purge=bool(args.purge))
            print(f"[OK] wrote DB table universe_daily_hot (purge={bool(args.purge)}) rows={len(uni)}")

            # 可选：写 picks_daily 作为 UNIVERSE_HOT
            if args.write_picks_daily:
                _write_universe_to_picks_daily(con, uni, version="UNIVERSE_HOT", purge=bool(args.purge))
                print("[OK] also wrote picks_daily as version=UNIVERSE_HOT")

    finally:
        con.close()

    print("\n[OK] build_universe_hot finished.")


if __name__ == "__main__":
    main()
