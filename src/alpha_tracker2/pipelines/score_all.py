from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple

import yaml
import pandas as pd

from alpha_tracker2.core.config import load_settings
from alpha_tracker2.core.trading_calendar import TradingCalendar
from alpha_tracker2.storage.duckdb_store import DuckDBStore
from alpha_tracker2.scoring.registry import get_scorer
from alpha_tracker2.scoring.thresholds import update_history, get_threshold, ThresholdConfig


def _delete_existing(store: DuckDBStore, trade_date, version: str) -> None:
    store.exec(
        "DELETE FROM picks_daily WHERE trade_date = ? AND version = ?;",
        (trade_date, version),
    )


def _safe_bool_series(s: pd.Series) -> pd.Series:
    """
    duckdb boolean: True/False/NULL
    pandas boolean dtype supports <NA>
    """
    try:
        return s.astype("boolean")
    except Exception:
        return s.map(
            lambda x: True if str(x).lower() in ("true", "1", "t", "yes") else False
        ).astype("boolean")


def _compute_thr_and_pass(
    out: pd.DataFrame,
    *,
    version: str,
    root: Path,
    q: float | None,
    window: int | None,
) -> pd.DataFrame:
    """
    只负责把 thr_value / pass_thr 计算出来（不做 picks 过滤，不决定 picked_by）
    - V2/V3/V4: thr_value + pass_thr
    - V1: 维持 NULL
    """
    out = out.copy()
    is_v234 = version in ("V2", "V3", "V4")
    hist_path = root / "data" / "cache" / "ab_threshold_history.json"

    # thr_value
    if "thr_value" not in out.columns:
        out["thr_value"] = None

    thr_from_reason = None
    if "reason" in out.columns and not out["reason"].isna().all():
        m = re.search(
            r"hist_thr\s*=\s*([-+]?\d+(\.\d+)?)", str(out["reason"].iloc[0])
        )
        if m:
            try:
                thr_from_reason = float(m.group(1))
            except Exception:
                thr_from_reason = None

    thr_value = None
    if is_v234:
        if thr_from_reason is not None:
            thr_value = thr_from_reason
        else:
            qq = float(q) if q is not None else 0.9
            ww = int(window) if window is not None else 60
            try:
                thr_value = float(
                    get_threshold(
                        hist_path,
                        version,
                        cfg=ThresholdConfig(q=qq, window=ww),
                    )
                )
            except Exception:
                thr_value = None

    out["thr_value"] = (thr_value if is_v234 else None)

    # pass_thr
    if "pass_thr" not in out.columns:
        out["pass_thr"] = None

    if is_v234 and thr_value is not None and "score" in out.columns:
        out["pass_thr"] = (
            pd.to_numeric(out["score"], errors="coerce").fillna(0.0) >= float(thr_value)
        )
        out["pass_thr"] = _safe_bool_series(out["pass_thr"])
    else:
        out["pass_thr"] = _safe_bool_series(
            pd.Series([pd.NA] * len(out), index=out.index)
        )

    return out


def _apply_pick_contract(
    out: pd.DataFrame,
    *,
    version: str,
    fallback_topk: int,
    q: float | None,
    window: int | None,
) -> pd.DataFrame:
    """
    Step 6-A（最小、稳定、不新增业务逻辑）：
    - V2/V3/V4：默认只保留 pass_thr == True 的行作为 picks
      - 若 0 行：fallback_topk（按 score 前 topk），并强制 pass_thr=True
      - 若 >0 行：picked_by="THRESHOLD"，且 picks 中不允许出现 pass_thr=False
    - V1：picked_by="BASELINE_RANK"，thr/pass 保持 NULL
    同时把阈值信息写进 reason，dashboard 一眼可解释。
    """
    out = out.copy()
    is_v234 = version in ("V2", "V3", "V4")

    # picked_by column
    if "picked_by" not in out.columns:
        out["picked_by"] = None

    # reason column
    if "reason" not in out.columns or out["reason"].isna().all():
        out["reason"] = f"{version} score_all normalized"
    out["reason"] = out["reason"].astype("string").fillna("")

    if not is_v234:
        out["picked_by"] = "BASELINE_RANK"
        return out

    if "pass_thr" not in out.columns:
        out["pass_thr"] = _safe_bool_series(
            pd.Series([pd.NA] * len(out), index=out.index)
        )

    pass_mask = out["pass_thr"] == True  # noqa: E712
    picked = out.loc[pass_mask].copy()

    if len(picked) == 0:
        picked = out.sort_values("score", ascending=False).head(int(fallback_topk)).copy()
        picked["picked_by"] = "FALLBACK_TOPK"
        picked["pass_thr"] = _safe_bool_series(pd.Series([True] * len(picked), index=picked.index))
        fallback_flag = "fallback_topk"
    else:
        picked["picked_by"] = "THRESHOLD"
        picked["pass_thr"] = _safe_bool_series(pd.Series([True] * len(picked), index=picked.index))
        fallback_flag = "threshold"

    qq = float(q) if q is not None else 0.9
    ww = int(window) if window is not None else 60
    thr_value = None
    if "thr_value" in picked.columns and not picked["thr_value"].isna().all():
        try:
            thr_value = float(picked["thr_value"].iloc[0])
        except Exception:
            thr_value = None

    thr_str = "None" if thr_value is None else f"{thr_value:.6f}"
    pb = str(picked["picked_by"].iloc[0])
    suffix = f"[thr={thr_str}; q={qq}; window={ww}; picked_by={pb}; mode={fallback_flag}]"
    picked["reason"] = picked["reason"].map(lambda x: f"{x} {suffix}".strip())

    return picked


def _write_picks(store: DuckDBStore, df: pd.DataFrame, trade_date, version: str) -> int:
    if df is None or df.empty:
        return 0

    df = df.copy()

    # 1) 强制写入 trade_date / version（NOT NULL）
    df["trade_date"] = trade_date
    df["version"] = version

    # 2) ticker 必须存在
    if "ticker" not in df.columns:
        raise ValueError(f"{version} scorer output must contain 'ticker' column")
    df["ticker"] = df["ticker"].astype(str)

    # 3) name 保底
    if "name" not in df.columns:
        df["name"] = None

    # 4) score 保底 + 数值化
    if "score" not in df.columns:
        df["score"] = 0.0
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)

    # 5) reason：缺失 或 全空 -> 默认；部分空也补
    if "reason" not in df.columns:
        df["reason"] = f"{version} score_all normalized"
    else:
        df["reason"] = df["reason"].astype("string").fillna(f"{version} score_all normalized")

    # 6) rank：缺失/不可转数字/任何空值 -> 按 score 降序重算
    if "rank" not in df.columns:
        need_rank = True
    else:
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
        need_rank = df["rank"].isna().any()

    if need_rank:
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)

    # 7) score_100：用于展示对齐（不改变排序逻辑）
    if "score_100" not in df.columns:
        if len(df) <= 1:
            df["score_100"] = 100.0
        else:
            tmp = df.sort_values("score", ascending=True).reset_index(drop=True)
            tmp["score_100"] = (tmp.index / (len(tmp) - 1) * 100.0).astype(float)
            df = tmp.sort_values("score", ascending=False).reset_index(drop=True)

    # 8) thr_value / pass_thr / picked_by：如果缺列，补齐（更稳）
    if "thr_value" not in df.columns:
        df["thr_value"] = None
    if "pass_thr" not in df.columns:
        df["pass_thr"] = pd.Series([pd.NA] * len(df), index=df.index).astype("boolean")
    if "picked_by" not in df.columns:
        df["picked_by"] = None

    needed = [
        "trade_date", "version", "ticker", "name",
        "rank", "score", "score_100",
        "reason", "thr_value", "pass_thr", "picked_by",
    ]

    # ========= 关键补丁：NaN/NA/NaT -> Python None -> DuckDB NULL =========
    df = df[needed].copy()
    df = df.astype(object)
    df = df.where(~pd.isna(df), None)
    # ===================================================================

    rows = df.values.tolist()

    insert_sql = """
        INSERT INTO picks_daily(
            trade_date, version, ticker, name,
            rank, score, score_100,
            reason, thr_value, pass_thr, picked_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    with store.session() as con:
        con.executemany(insert_sql, rows)

    return len(rows)


def _parse_versions(s: str | None, default: List[str]) -> List[str]:
    if not s:
        return default
    out = [x.strip().upper() for x in s.split(",") if x.strip()]
    return out or default


def _load_universe(
    store: DuckDBStore,
    trade_date,
    universe_source: str,
    limit: int | None,
) -> pd.DataFrame:
    """
    universe_source: universe_daily_hot / universe_daily / 任何包含 ticker 的表
    期望列：ticker, (optional) name
    """
    # 优先用 name（如果存在）
    sql = f"""
        SELECT
            ticker,
            CASE
                WHEN 'name' IN (SELECT column_name FROM information_schema.columns
                                WHERE table_name = '{universe_source}') THEN name
                ELSE NULL
            END AS name
        FROM {universe_source}
        WHERE trade_date = ?
        GROUP BY ticker, name
        ORDER BY ticker
    """
    rows = store.fetchall(sql, (trade_date,))
    if not rows:
        raise RuntimeError(f"No universe rows for trade_date={trade_date} in table {universe_source}. Run build_universe first.")

    uni = pd.DataFrame(rows, columns=["ticker", "name"])
    uni["ticker"] = uni["ticker"].astype(str)

    if limit and limit > 0:
        uni = uni.head(int(limit)).copy()

    return uni


def _load_features_for_universe(
    store: DuckDBStore,
    trade_date,
    tickers: List[str],
) -> pd.DataFrame:
    """
    读取 features_daily 全字段（保证 V2/V3/V4 直接用扩展列）
    """
    if not tickers:
        raise RuntimeError("Empty tickers when loading features.")

    placeholders = ",".join(["?"] * len(tickers))
    sql = f"""
        SELECT *
        FROM features_daily
        WHERE trade_date = ?
          AND ticker IN ({placeholders})
    """
    rows = store.fetchall(sql, (trade_date, *tickers))
    if not rows:
        raise RuntimeError(f"No features_daily rows for trade_date={trade_date} and given universe tickers. Run build_features/backfill_features first.")

    # 用 DESCRIBE 取列名
    with store.session() as con:
        cols = [r[0] for r in con.execute("DESCRIBE features_daily").fetchall()]

    feat = pd.DataFrame(rows, columns=cols)
    feat["ticker"] = feat["ticker"].astype(str)
    return feat


def main() -> None:
    ap = argparse.ArgumentParser(description="Score picks for multiple versions (plugins).")

    ap.add_argument("--date", type=str, default=None, help="trade_date, e.g. 2026-01-14 (default: latest trading day)")
    ap.add_argument("--limit", type=int, default=0, help="optional: limit tickers for dev (0=all)")

    # NEW (kebab-case)
    ap.add_argument("--versions", type=str, default=None, help="comma-separated versions, e.g. V1,V2,V3,V4,ENS (override config)")
    ap.add_argument("--topk", type=int, default=0, help="optional: final keep topk rows per version (0=disable)")
    ap.add_argument("--universe-source", type=str, default=None, help="universe table, e.g. universe_daily_hot (override config)")

    # 可选：覆盖阈值配置（不传就走 default.yaml）
    ap.add_argument("--thr-q", type=float, default=None, help="override q for rolling quantile threshold (V2/V3/V4)")
    ap.add_argument("--thr-window", type=int, default=None, help="override window for rolling threshold (V2/V3/V4)")
    ap.add_argument("--fallback-topk", type=int, default=0, help="override v234 fallback_topk when 0 picks after threshold (0=use config)")

    args = ap.parse_args()

    root = Path(__file__).resolve().parents[3]
    cfg = load_settings(root)

    store = DuckDBStore(
        db_path=cfg.store_db,
        schema_path=root / "src" / "alpha_tracker2" / "storage" / "schema.sql",
    )
    store.init_schema()

    cal = TradingCalendar()
    if args.date is None:
        trade_date = cal.latest_trading_day()
    else:
        trade_date = pd.to_datetime(args.date).date()

    # configs/default.yaml
    cfg_path = root / "configs" / "default.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw_cfg = yaml.safe_load(f) or {}

    scoring_cfg = (raw_cfg.get("scoring", {}) or {})

    # universe source
    universe_source = args.universe_source or scoring_cfg.get("universe_source") or "universe_daily_hot"

    # versions list
    cfg_versions = scoring_cfg.get("score_versions", ["V1"]) or ["V1"]
    versions = _parse_versions(args.versions, default=cfg_versions)

    # threshold config (for V2/V3/V4)
    v234_cfg = (scoring_cfg.get("v2_v3_v4", {}) or {})
    common_cfg = v234_cfg.get("common", v234_cfg) if isinstance(v234_cfg, dict) else {}
    common_q = args.thr_q if args.thr_q is not None else common_cfg.get("q", None)
    common_window = args.thr_window if args.thr_window is not None else common_cfg.get("window", None)

    cfg_fallback = int(common_cfg.get("topk_fallback", common_cfg.get("topk", 3)))
    fallback_topk = int(args.fallback_topk) if args.fallback_topk and args.fallback_topk > 0 else cfg_fallback

    final_topk = int(args.topk) if args.topk and args.topk > 0 else 0
    limit = int(args.limit) if args.limit and args.limit > 0 else 0

    # 1) load universe (ticker + optional name)
    uni = _load_universe(store, trade_date, universe_source, limit if limit > 0 else None)

    # 2) load full features (all cols) for those tickers
    features = _load_features_for_universe(store, trade_date, uni["ticker"].tolist())

    # 3) name 回填：优先 universe.name，其次 features 里已有 name（若存在），再否则 None
    if "name" in features.columns:
        features = features.merge(uni[["ticker", "name"]], on="ticker", how="left", suffixes=("", "_u"))
        features["name"] = features["name"].fillna(features["name_u"])
        features = features.drop(columns=["name_u"])
    else:
        features = features.merge(uni[["ticker", "name"]], on="ticker", how="left")

    # 运行
    total_written = 0
    print("[OK] score_all started.")
    print("trade_date:", trade_date)
    print("universe_source:", universe_source)
    print("versions:", versions)
    print("features_rows:", len(features))

    for v in versions:
        scorer = get_scorer(root, v)
        out = scorer.score(features=features, trade_date=trade_date)

        # ---- normalize output ----
        out = out.copy() if out is not None else pd.DataFrame()
        if out.empty:
            out = pd.DataFrame({"ticker": features["ticker"].astype(str)})

        if "ticker" not in out.columns:
            raise ValueError(f"{v} scorer output missing ticker")
        out["ticker"] = out["ticker"].astype(str)

        # name 回填
        if "name" not in out.columns:
            out = out.merge(features[["ticker", "name"]], on="ticker", how="left")
        else:
            out = out.merge(features[["ticker", "name"]], on="ticker", how="left", suffixes=("", "_f"))
            out["name"] = out["name"].fillna(out["name_f"])
            out = out.drop(columns=["name_f"])

        # score
        if "score" not in out.columns:
            out["score"] = 0.0
        out["score"] = pd.to_numeric(out["score"], errors="coerce").fillna(0.0)

        # rank
        if "rank" not in out.columns or out["rank"].isna().all():
            out = out.sort_values("score", ascending=False).reset_index(drop=True)
            out["rank"] = range(1, len(out) + 1)

        # reason
        if "reason" not in out.columns or out["reason"].isna().all():
            out["reason"] = f"{v} score_all normalized"
        # --------------------------

        # 先算阈值与 pass_thr（全量候选）
        out = _compute_thr_and_pass(
            out,
            version=v,
            root=root,
            q=common_q,
            window=common_window,
        )

        # 阈值历史更新应基于“全量候选分数分布”
        scores_for_hist = out["score"].tolist() if (out is not None and not out.empty and "score" in out.columns) else []

        # 写库前统一 picks 契约（过滤 pass_thr / fallback / picked_by 固化）
        out_to_write = _apply_pick_contract(
            out,
            version=v,
            fallback_topk=fallback_topk,
            q=common_q,
            window=common_window,
        )

        # 可选：最终写库再截断 topk（所有版本通用）
        if final_topk > 0:
            out_to_write = out_to_write.sort_values("rank", ascending=True).head(final_topk).copy()

        _delete_existing(store, trade_date, v)
        n = _write_picks(store, out_to_write, trade_date, v)

        # 更新阈值历史缓存（用全量 scores）
        hist_path = root / "data" / "cache" / "ab_threshold_history.json"
        if scores_for_hist:
            update_history(hist_path, trade_date, v, scores_for_hist)

        total_written += n
        print(f"[OK] version={v} rows_written={n}")

    print("[OK] score_all passed.")
    print("total_rows_written:", total_written)
    print("db:", cfg.store_db)


if __name__ == "__main__":
    main()
