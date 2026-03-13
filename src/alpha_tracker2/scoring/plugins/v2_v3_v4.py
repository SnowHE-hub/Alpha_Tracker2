from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import yaml

from alpha_tracker2.scoring.base import Scorer


# =========================
# Helpers (ported from four_version_compare.py)
# =========================
PRED_HORIZON_DAYS = 5
MIN_TRAIN_SAMPLES = 80
TRAIN_LOOKBACK_MAX = 260

W_TREND = 0.35
W_PROB = 0.45
W_EXPECT = 0.10
W_RISK_PENALTY = 0.10


def clamp(x: float, lo: float, hi: float) -> float:
    try:
        if np.isnan(x):
            return lo
    except Exception:
        pass
    return float(max(lo, min(hi, x)))


def score01_higher_better(x: float, worst: float, best: float) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    if best == worst:
        return 0.0
    return float(clamp((x - worst) / (best - worst), 0.0, 1.0))


def score01_lower_better(x: float, worst: float, best: float) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    if best == worst:
        return 0.0
    return float(clamp((worst - x) / (worst - best), 0.0, 1.0))


def compute_trend_score_v23(tr: Dict[str, float], rk: Dict[str, float], ex: Dict[str, float]) -> float:
    ret10 = tr.get("ret_10d", np.nan)
    ret20 = tr.get("ret_20d", np.nan)
    ma_align = tr.get("ma5_gt_ma10_gt_ma20", np.nan)
    ma2060 = tr.get("ma20_above_ma60", np.nan)
    slope20 = tr.get("ma20_slope", np.nan)

    vol = rk.get("vol_ann_60d", np.nan)
    mdd60 = abs(rk.get("mdd_60d", np.nan))
    limit_down = ex.get("limit_down_60", 0)

    s1 = score01_higher_better(ret10, worst=-0.05, best=0.12)
    s2 = score01_higher_better(ret20, worst=-0.08, best=0.20)
    s3 = 1.0 if ma_align == 1.0 else 0.3 if ma_align == 0.0 else 0.0
    s4 = 1.0 if ma2060 == 1.0 else 0.4 if ma2060 == 0.0 else 0.0
    s5 = score01_higher_better(slope20, worst=-0.003, best=0.006)

    p_vol = 1.0 - score01_lower_better(vol, worst=0.60, best=0.25)
    p_mdd = 1.0 - score01_lower_better(mdd60, worst=0.18, best=0.06)
    p_ld = clamp(float(limit_down) / 4.0, 0.0, 1.0)

    raw = 0.22 * s1 + 0.22 * s2 + 0.18 * s3 + 0.18 * s4 + 0.20 * s5
    penalty = 0.45 * p_vol + 0.40 * p_mdd + 0.15 * p_ld
    return float(clamp(raw * (1.0 - penalty), 0.0, 1.0))


def make_supervised_dataset_v23(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    d = df.copy().reset_index(drop=True)

    d["ret1"] = d["close"].pct_change(1)
    d["ret3"] = d["close"].pct_change(3)
    d["ret5"] = d["close"].pct_change(5)
    d["ret10"] = d["close"].pct_change(10)

    d["ma5"] = d["close"].rolling(5).mean()
    d["ma10"] = d["close"].rolling(10).mean()
    d["ma20"] = d["close"].rolling(20).mean()
    d["ma60"] = d["close"].rolling(60).mean()

    d["gap_ma5"] = d["close"] / d["ma5"] - 1.0
    d["gap_ma10"] = d["close"] / d["ma10"] - 1.0
    d["gap_ma20"] = d["close"] / d["ma20"] - 1.0
    d["gap_ma60"] = d["close"] / d["ma60"] - 1.0

    d["vol_10"] = d["ret1"].rolling(10).std()
    d["vol_20"] = d["ret1"].rolling(20).std()

    if "amount" in d.columns:
        d["amount_chg5"] = d["amount"].pct_change(5)
        d["amount_chg10"] = d["amount"].pct_change(10)
    else:
        d["amount_chg5"] = np.nan
        d["amount_chg10"] = np.nan

    fwd_ret = d["close"].shift(-PRED_HORIZON_DAYS) / d["close"] - 1.0
    d["y_up"] = (fwd_ret > 0).astype(int)
    d["fwd_ret"] = fwd_ret

    feat_cols = [
        "ret1", "ret3", "ret5", "ret10",
        "gap_ma5", "gap_ma10", "gap_ma20", "gap_ma60",
        "vol_10", "vol_20", "amount_chg5", "amount_chg10",
    ]

    out = d[feat_cols + ["y_up", "fwd_ret"]].replace([np.inf, -np.inf], np.nan).dropna()
    if out.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.Series(dtype=float)

    X = out[feat_cols].copy()
    y = out["y_up"].copy()
    fwd = out["fwd_ret"].copy()
    return X, y, fwd


def predict_prob_up_5d_v23(df: pd.DataFrame) -> Dict[str, float]:
    # Optional sklearn (degrade gracefully)
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.preprocessing import StandardScaler
    except Exception:
        return {"prob_lr": np.nan, "prob_gb": np.nan, "prob_ens": np.nan, "exp_ret": np.nan}

    X, y, _fwd = make_supervised_dataset_v23(df)
    if X.shape[0] < MIN_TRAIN_SAMPLES:
        return {"prob_lr": np.nan, "prob_gb": np.nan, "prob_ens": np.nan, "exp_ret": np.nan}

    if X.shape[0] > TRAIN_LOOKBACK_MAX:
        X = X.tail(TRAIN_LOOKBACK_MAX)
        y = y.tail(TRAIN_LOOKBACK_MAX)

    # last row as "latest feature row"
    X_train = X.iloc[:-1].values
    y_train = y.iloc[:-1].values
    x_last = X.iloc[-1].values.reshape(1, -1)

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X_train)
    xs_last = scaler.transform(x_last)

    try:
        lr = LogisticRegression(max_iter=800)
        lr.fit(Xs, y_train)
        prob_lr = float(lr.predict_proba(xs_last)[0, 1])
    except Exception:
        prob_lr = np.nan

    try:
        gb = GradientBoostingClassifier()
        gb.fit(X_train, y_train)
        prob_gb = float(gb.predict_proba(x_last)[0, 1])
    except Exception:
        prob_gb = np.nan

    probs = [p for p in [prob_lr, prob_gb] if not np.isnan(p)]
    prob_ens = float(np.mean(probs)) if probs else np.nan

    # exp_ret proxy = prob_ens * ret10 (ported)
    try:
        ret10 = float(df["close"].iloc[-1] / df["close"].iloc[-11] - 1.0) if len(df) >= 11 else np.nan
    except Exception:
        ret10 = np.nan
    exp_ret = float(prob_ens * ret10) if (not np.isnan(prob_ens) and not np.isnan(ret10)) else np.nan

    return {"prob_lr": prob_lr, "prob_gb": prob_gb, "prob_ens": prob_ens, "exp_ret": exp_ret}


# =========================
# Config & DB path (align to Settings.paths.store_db)
# =========================
def _find_project_root(start: Path) -> Path | None:
    for p in [start, *start.parents]:
        if (p / "configs" / "default.yaml").exists():
            return p
    return None


@lru_cache(maxsize=1)
def _load_cfg() -> dict:
    root = _find_project_root(Path(__file__).resolve())
    if root is None:
        return {}
    p = root / "configs" / "default.yaml"
    try:
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _get_db_path() -> Path:
    cfg = _load_cfg()
    store_db = ((cfg.get("paths") or {}).get("store_db")) or "data/store/alpha_tracker.duckdb"
    root = _find_project_root(Path(__file__).resolve())
    if root is None:
        return Path(store_db)
    return (root / store_db).resolve()


def _fetch_price_hist(con, ticker: str, trade_date: date, lookback_days: int = 420) -> pd.DataFrame:
    # we fetch more than 260 to survive NA dropna in feature making
    sql = """
        SELECT trade_date AS date, close, amount
        FROM prices_daily
        WHERE ticker = ?
          AND trade_date <= ?
        ORDER BY trade_date
    """
    df = con.execute(sql, [ticker, trade_date]).df()
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "close", "amount"])
    # keep tail
    if len(df) > lookback_days:
        df = df.tail(lookback_days).copy()
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    else:
        df["amount"] = np.nan
    df = df.dropna(subset=["close"])
    return df.reset_index(drop=True)


def _score_row_v23(feat_row: pd.Series, con) -> Dict[str, float]:
    # Build dicts aligned to reference code
    tr = {
        "ret_10d": float(feat_row.get("ret_10d")) if feat_row.get("ret_10d") is not None else np.nan,
        "ret_20d": float(feat_row.get("ret_20d")) if feat_row.get("ret_20d") is not None else np.nan,
        "ma5_gt_ma10_gt_ma20": float(feat_row.get("ma5_gt_ma10_gt_ma20")) if feat_row.get("ma5_gt_ma10_gt_ma20") is not None else np.nan,
        "ma20_above_ma60": float(feat_row.get("ma20_above_ma60")) if feat_row.get("ma20_above_ma60") is not None else np.nan,
        "ma20_slope": float(feat_row.get("ma20_slope")) if feat_row.get("ma20_slope") is not None else np.nan,
    }
    rk = {
        "vol_ann_60d": float(feat_row.get("vol_ann_60d")) if feat_row.get("vol_ann_60d") is not None else np.nan,
        "mdd_60d": float(feat_row.get("mdd_60d")) if feat_row.get("mdd_60d") is not None else np.nan,
    }
    ex = {
        "limit_down_60": float(feat_row.get("limit_down_60")) if feat_row.get("limit_down_60") is not None else 0.0,
    }

    v23_trend = compute_trend_score_v23(tr, rk, ex)

    vol = rk.get("vol_ann_60d", np.nan)
    mdd60 = abs(rk.get("mdd_60d", np.nan))
    ld = ex.get("limit_down_60", 0.0)

    penalty_v23 = (
        0.45 * (1.0 - score01_lower_better(vol, worst=0.60, best=0.25))
        + 0.40 * (1.0 - score01_lower_better(mdd60, worst=0.18, best=0.06))
        + 0.15 * clamp(ld / 4.0, 0.0, 1.0)
    )
    penalty_v23 = float(clamp(penalty_v23, 0.0, 1.0))

    # ML prediction (per-stock) from prices_daily
    ticker = str(feat_row.get("ticker"))
    td = feat_row.get("trade_date")
    td = td if isinstance(td, date) else pd.to_datetime(td).date()

    hist = _fetch_price_hist(con, ticker, td)
    pred = predict_prob_up_5d_v23(hist) if not hist.empty else {"prob_ens": np.nan, "exp_ret": np.nan}
    prob_ens = float(pred.get("prob_ens", np.nan))
    exp_ret = float(pred.get("exp_ret", np.nan))

    exp_score = score01_higher_better(exp_ret, worst=-0.03, best=0.10)
    prob_score = 0.50 if np.isnan(prob_ens) else float(prob_ens)

    v23_final01 = (
        W_TREND * float(v23_trend)
        + W_PROB * float(prob_score)
        + W_EXPECT * float(exp_score)
        + W_RISK_PENALTY * (1.0 - float(penalty_v23))
    )
    v23_final100 = float(clamp(v23_final01, 0.0, 1.0) * 100.0)

    return {
        "v23_trend": float(v23_trend),
        "prob_ens": float(prob_ens),
        "exp_ret": float(exp_ret),
        "penalty_v23": float(penalty_v23),
        "final01": float(v23_final01),
        "final100": float(v23_final100),
    }


# =========================
# Scorers
# =========================
@dataclass
class _CommonCfg:
    q: float = 0.80
    window: int = 60
    topk_fallback: int = 50


def _load_common_cfg() -> _CommonCfg:
    cfg = _load_cfg()
    node = (((cfg.get("scoring") or {}).get("v2_v3_v4") or {}).get("common") or {})
    return _CommonCfg(
        q=float(node.get("q", 0.80)),
        window=int(node.get("window", 60)),
        topk_fallback=int(node.get("topk_fallback", 50)),
    )


class V2TrendScorer:
    name = "V2"

    def __init__(self, hist_path: Path | None = None):
        self.cfg = _load_common_cfg()
        self.hist_path = hist_path

    def score(self, trade_date: date, features: pd.DataFrame) -> pd.DataFrame:
        import duckdb

        if features is None or features.empty:
            return pd.DataFrame(columns=["ticker", "score", "score_100", "reason"])

        df = features.copy()
        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

        db_path = _get_db_path()
        con = duckdb.connect(str(db_path))

        rows = []
        try:
            for _, r in df.iterrows():
                s = _score_row_v23(r, con)
                rows.append(
                    {
                        "ticker": r["ticker"],
                        "name": r.get("name", None),
                        "score": s["final100"],
                        "score_100": s["final100"],
                        "reason": f"V2(v23): trend={s['v23_trend']:.4f} prob={s['prob_ens']:.4f} "
                                  f"exp={s['exp_ret']:.4f} pen={s['penalty_v23']:.4f}",
                    }
                )
        finally:
            con.close()

        out = pd.DataFrame(rows)
        out["ticker"] = out["ticker"].astype(str)
        return out


class V3LowVolScorer:
    name = "V3"

    def __init__(self, hist_path: Path | None = None):
        self.cfg = _load_common_cfg()
        self.hist_path = hist_path

    def score(self, trade_date: date, features: pd.DataFrame) -> pd.DataFrame:
        # Same core as V2 in reference (A/B handled by threshold/history in system)
        import duckdb

        if features is None or features.empty:
            return pd.DataFrame(columns=["ticker", "score", "score_100", "reason"])

        df = features.copy()
        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

        db_path = _get_db_path()
        con = duckdb.connect(str(db_path))

        rows = []
        try:
            for _, r in df.iterrows():
                s = _score_row_v23(r, con)
                rows.append(
                    {
                        "ticker": r["ticker"],
                        "name": r.get("name", None),
                        "score": s["final100"],
                        "score_100": s["final100"],
                        "reason": f"V3(v23-core): trend={s['v23_trend']:.4f} prob={s['prob_ens']:.4f} "
                                  f"exp={s['exp_ret']:.4f} pen={s['penalty_v23']:.4f}",
                    }
                )
        finally:
            con.close()

        out = pd.DataFrame(rows)
        out["ticker"] = out["ticker"].astype(str)
        return out


class V4TrendMAScorer:
    name = "V4"

    def __init__(self, hist_path: Path | None = None):
        self.cfg = _load_common_cfg()
        self.hist_path = hist_path

    def score(self, trade_date: date, features: pd.DataFrame) -> pd.DataFrame:
        # Keep trend formula unified with v23 (Improvement #4), plus same prob/exp/penalty backbone
        import duckdb

        if features is None or features.empty:
            return pd.DataFrame(columns=["ticker", "score", "score_100", "reason"])

        df = features.copy()
        if "trade_date" not in df.columns:
            df["trade_date"] = trade_date

        db_path = _get_db_path()
        con = duckdb.connect(str(db_path))

        rows = []
        try:
            for _, r in df.iterrows():
                s = _score_row_v23(r, con)
                # V4 can slightly emphasize trend consistency via MA alignment if available
                ma_align = r.get("ma5_gt_ma10_gt_ma20", np.nan)
                ma_bonus = 0.02 if ma_align == 1.0 else 0.0  # tiny bonus, no distortion
                v4_final01 = clamp(s["final01"] + ma_bonus, 0.0, 1.0)
                v4_final100 = float(v4_final01 * 100.0)

                rows.append(
                    {
                        "ticker": r["ticker"],
                        "name": r.get("name", None),
                        "score": v4_final100,
                        "score_100": v4_final100,
                        "reason": f"V4(v23+MA): trend={s['v23_trend']:.4f} prob={s['prob_ens']:.4f} "
                                  f"exp={s['exp_ret']:.4f} pen={s['penalty_v23']:.4f} ma_bonus={ma_bonus:.2f}",
                    }
                )
        finally:
            con.close()

        out = pd.DataFrame(rows)
        out["ticker"] = out["ticker"].astype(str)
        return out
