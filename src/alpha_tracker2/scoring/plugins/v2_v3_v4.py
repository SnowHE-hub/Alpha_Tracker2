from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

import pandas as pd

from alpha_tracker2.scoring.base import Scorer, ensure_score_frame
from alpha_tracker2.storage.duckdb_store import DuckDBStore


@dataclass(frozen=True)
class CoreTrendRiskConfig:
    trend_weight: float = 1.0
    risk_weight: float = 1.0
    bt_weight: float = 0.0


def _fetch_features(store: DuckDBStore, trade_date: date) -> pd.DataFrame:
    """
    Fetch a richer set of features for V2–V4.

    We keep the set intentionally compact but extensible.
    """
    # Only use columns that exist in features_daily (schema has ret_1d, ret_5d, ret_10d, ret_20d; no ret_60d)
    cols = [
        "trade_date",
        "ticker",
        "ret_5d",
        "ret_20d",
        "vol_ann_60d",
        "mdd_60d",
        "ma5",
        "ma20",
        "ma60",
        "ma20_slope",
    ]
    sql = f"""
        SELECT {', '.join(cols)}
        FROM features_daily
        WHERE trade_date = ?
    """
    rows = store.fetchall(sql, [trade_date.isoformat()])
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _compute_trend_score(df: pd.DataFrame) -> pd.Series:
    """
    Simplified trend score:
      - positively related to recent returns and ma20 slope
      - bonus if ma20 > ma60 and price above ma20
    """
    ret_20 = pd.to_numeric(df.get("ret_20d"), errors="coerce")
    ret_5 = pd.to_numeric(df.get("ret_5d"), errors="coerce")
    ma20_slope = pd.to_numeric(df.get("ma20_slope"), errors="coerce")
    ma5 = pd.to_numeric(df.get("ma5"), errors="coerce")
    ma20 = pd.to_numeric(df.get("ma20"), errors="coerce")
    ma60 = pd.to_numeric(df.get("ma60"), errors="coerce")

    base = 0.5 * ret_20.fillna(0.0) + 0.3 * ret_5.fillna(0.0) + 0.2 * ma20_slope.fillna(0.0)

    bonus = 0.0
    if not ma20.isna().all() and not ma60.isna().all():
        bonus_series = (ma20 > ma60).astype(float)
        bonus = bonus_series * 0.1
    above_ma = (ma5 > ma20).astype(float) * 0.05

    return (base + bonus + above_ma).rename("trend_score")


def _compute_risk_penalty(df: pd.DataFrame) -> pd.Series:
    """
    Risk penalty based on volatility and drawdown.
    """
    vol = pd.to_numeric(df.get("vol_ann_60d"), errors="coerce")
    mdd = pd.to_numeric(df.get("mdd_60d"), errors="coerce")

    vol_z = (vol - vol.mean()) / vol.std(ddof=0) if vol.std(ddof=0) not in (0, float("nan")) else vol * 0.0
    mdd_z = (mdd - mdd.mean()) / mdd.std(ddof=0) if mdd.std(ddof=0) not in (0, float("nan")) else mdd * 0.0

    penalty = 0.7 * vol_z.fillna(0.0) + 0.3 * mdd_z.fillna(0.0)
    return penalty.rename("risk_penalty")


def _compose_score(
    trend_score: pd.Series,
    risk_penalty: pd.Series,
    cfg: CoreTrendRiskConfig,
) -> pd.Series:
    score = cfg.trend_weight * trend_score.fillna(0.0) - cfg.risk_weight * risk_penalty.fillna(0.0)
    return score.rename("score")


class _BaseTrendRiskScorer(Scorer):
    """
    Shared implementation for V2/V3/V4.
    """

    model_name: str
    cfg: CoreTrendRiskConfig

    def __init__(self, model_name: str, cfg: CoreTrendRiskConfig) -> None:
        self.model_name = model_name
        self.cfg = cfg

    def score(self, trade_date: date, store: DuckDBStore) -> pd.DataFrame:
        df = _fetch_features(store, trade_date)
        if df.empty:
            return ensure_score_frame(pd.DataFrame(columns=["ticker", "score", "reason"]))

        df = df.set_index("ticker")
        trend_score = _compute_trend_score(df)
        risk_penalty = _compute_risk_penalty(df)

        core_score = _compose_score(trend_score, risk_penalty, self.cfg)

        rows = []
        for ticker, s in core_score.items():
            payload = {
                "model": self.model_name,
                "trend_score": _safe_float(trend_score.loc[ticker]),
                "risk_penalty": _safe_float(risk_penalty.loc[ticker]),
                "trend_weight": self.cfg.trend_weight,
                "risk_weight": self.cfg.risk_weight,
                "bt_weight": self.cfg.bt_weight,
            }
            rows.append(
                {
                    "ticker": str(ticker),
                    "score": float(s) if pd.notna(s) else None,
                    "reason": json.dumps(payload, ensure_ascii=False),
                }
            )

        result = pd.DataFrame(rows)
        return ensure_score_frame(result)


class V2Scorer(_BaseTrendRiskScorer):
    """
    V2: balanced trend vs risk.
    """

    def __init__(self) -> None:
        super().__init__("V2", CoreTrendRiskConfig(trend_weight=1.0, risk_weight=0.8, bt_weight=0.0))


class V3Scorer(_BaseTrendRiskScorer):
    """
    V3: more conservative, higher risk penalty.
    """

    def __init__(self) -> None:
        super().__init__("V3", CoreTrendRiskConfig(trend_weight=1.0, risk_weight=1.3, bt_weight=0.0))


class V4Scorer(_BaseTrendRiskScorer):
    """
    V4: same core engine as V2, placeholder hook for backtest strength.
    """

    def __init__(self) -> None:
        super().__init__("V4", CoreTrendRiskConfig(trend_weight=1.0, risk_weight=0.9, bt_weight=0.3))


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return float(f)

