from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from alpha_tracker2.scoring.base import Scorer, ensure_score_frame
from alpha_tracker2.storage.duckdb_store import DuckDBStore


def load_core_config(project_root: Path, version: str) -> "CoreTrendRiskConfig":
    """
    Load trend_weight, risk_weight, bt_weight from configs/default.yaml
    scoring.v2_v3_v4.versions.<V2|V3|V4> with fallback to common then code default.
    """
    default = CoreTrendRiskConfig(trend_weight=1.0, risk_weight=1.0, bt_weight=0.0)
    cfg_path = project_root / "configs" / "default.yaml"
    if not cfg_path.is_file():
        return default
    with cfg_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    v234 = (raw.get("scoring") or {}).get("v2_v3_v4") or {}
    common = (v234.get("common") or {}) if isinstance(v234.get("common"), dict) else {}
    versions = v234.get("versions") or {}
    if not isinstance(versions, dict):
        versions = {}
    ver_key = version.upper()
    ver_cfg = versions.get(ver_key) or {}
    if not isinstance(ver_cfg, dict):
        ver_cfg = {}

    def _float(key: str, fallback: float) -> float:
        for src in (ver_cfg, common):
            v = src.get(key)
            if isinstance(v, (int, float)):
                return float(v)
        return fallback

    return CoreTrendRiskConfig(
        trend_weight=_float("trend_weight", default.trend_weight),
        risk_weight=_float("risk_weight", default.risk_weight),
        bt_weight=_float("bt_weight", default.bt_weight),
    )


def load_bt_column_weights(project_root: Path) -> dict[str, float]:
    """
    Load bt_column_weights from configs/default.yaml scoring.v2_v3_v4.bt_column_weights.
    Used by V4 for bt_score. Fallback: bt_mean 0.5, bt_winrate 0.3, bt_worst_mdd 0.2.
    """
    default = {"bt_mean": 0.5, "bt_winrate": 0.3, "bt_worst_mdd": 0.2}
    cfg_path = project_root / "configs" / "default.yaml"
    if not cfg_path.is_file():
        return default
    with cfg_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    v234 = (raw.get("scoring") or {}).get("v2_v3_v4") or {}
    w = v234.get("bt_column_weights") if isinstance(v234, dict) else None
    if not isinstance(w, dict) or not w:
        return default
    out: dict[str, float] = {}
    for k, v in w.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out if out else default


@dataclass(frozen=True)
class CoreTrendRiskConfig:
    trend_weight: float = 1.0
    risk_weight: float = 1.0
    bt_weight: float = 0.0


# Columns for V2/V3 (no bt_*). V4 uses _fetch_features_v4 which adds bt_*.
_FEATURE_COLS = [
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

# bt_* columns for V4 (schema has these; nullable).
BT_COLS = ["bt_mean", "bt_winrate", "bt_worst_mdd"]


def _fetch_features(store: DuckDBStore, trade_date: date, include_bt: bool = False) -> pd.DataFrame:
    """
    Fetch features for V2–V4. When include_bt=True (V4), also fetch bt_mean, bt_winrate, bt_worst_mdd.
    If bt_* are missing from schema, omit them (caller gets no bt columns).
    """
    cols = list(_FEATURE_COLS)
    if include_bt:
        cols = cols + BT_COLS
    sql = f"""
        SELECT {', '.join(cols)}
        FROM features_daily
        WHERE trade_date = ?
    """
    try:
        rows = store.fetchall(sql, [trade_date.isoformat()])
    except Exception:
        # Schema may not have bt_* yet; retry without
        if include_bt:
            cols = list(_FEATURE_COLS)
            sql = f"SELECT {', '.join(cols)} FROM features_daily WHERE trade_date = ?"
            rows = store.fetchall(sql, [trade_date.isoformat()])
        else:
            raise
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows, columns=cols)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _compute_trend_score(df: pd.DataFrame) -> pd.Series:
    """
    Simplified trend score:
      - positively related to recent returns and ma20 slope
      - bonus if ma20 > ma60 and price above ma20 (ma_bonus component)
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


def _compute_ma_bonus(df: pd.DataFrame) -> pd.Series:
    """Explicit ma_bonus (ma20>ma60 + ma5>ma20) for reason display; already included in trend_score."""
    ma5 = pd.to_numeric(df.get("ma5"), errors="coerce")
    ma20 = pd.to_numeric(df.get("ma20"), errors="coerce")
    ma60 = pd.to_numeric(df.get("ma60"), errors="coerce")
    bonus = pd.Series(0.0, index=df.index)
    if not ma20.isna().all() and not ma60.isna().all():
        bonus = bonus + (ma20 > ma60).astype(float) * 0.1
    bonus = bonus + (ma5 > ma20).astype(float) * 0.05
    return bonus.rename("ma_bonus")


def _compute_bt_score(df: pd.DataFrame, weights: dict[str, float]) -> pd.Series:
    """
    Weighted combination of bt_mean, bt_winrate, bt_worst_mdd.
    Uses cross-sectional z-score then weighted sum; when bt_* all NULL for a row, bt_score=0.
    """
    available = [c for c in BT_COLS if c in df.columns and c in weights]
    if not available:
        return pd.Series(0.0, index=df.index, name="bt_score")
    total_w = sum(weights.get(c, 0.0) for c in available)
    if total_w <= 0:
        return pd.Series(0.0, index=df.index, name="bt_score")
    out = pd.Series(0.0, index=df.index, name="bt_score")
    for col in available:
        s = pd.to_numeric(df[col], errors="coerce")
        mean = s.mean()
        std = s.std(ddof=0)
        if std and not pd.isna(std) and std != 0:
            z = (s - mean) / std
        else:
            z = pd.Series(0.0, index=df.index)
        w = weights.get(col, 0.0) / total_w
        out = out + w * z.fillna(0.0)
    return out


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
    bt_score: pd.Series | None = None,
) -> pd.Series:
    score = cfg.trend_weight * trend_score.fillna(0.0) - cfg.risk_weight * risk_penalty.fillna(0.0)
    if bt_score is not None and cfg.bt_weight != 0:
        score = score + cfg.bt_weight * bt_score.fillna(0.0)
    return score.rename("score")


class _BaseTrendRiskScorer(Scorer):
    """
    Shared implementation for V2/V3/V4. V4 overrides score() to add bt_* and ma_bonus.
    """

    model_name: str
    cfg: CoreTrendRiskConfig

    def __init__(self, model_name: str, cfg: CoreTrendRiskConfig) -> None:
        self.model_name = model_name
        self.cfg = cfg

    def score(self, trade_date: date, store: DuckDBStore) -> pd.DataFrame:
        df = _fetch_features(store, trade_date, include_bt=False)
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
    V2: balanced trend vs risk. Config from scoring.v2_v3_v4.versions.V2 (or common/code default).
    """

    def __init__(self, cfg: CoreTrendRiskConfig | None = None) -> None:
        super().__init__("V2", cfg or CoreTrendRiskConfig(trend_weight=1.0, risk_weight=0.8, bt_weight=0.0))


class V3Scorer(_BaseTrendRiskScorer):
    """
    V3: more conservative, higher risk penalty. Config from versions.V3 or common/code default.
    """

    def __init__(self, cfg: CoreTrendRiskConfig | None = None) -> None:
        super().__init__("V3", cfg or CoreTrendRiskConfig(trend_weight=1.0, risk_weight=1.3, bt_weight=0.0))


class V4Scorer(_BaseTrendRiskScorer):
    """
    V4: trend + risk + bt_score (from bt_mean/bt_winrate/bt_worst_mdd) + ma_bonus in trend.
    Config from versions.V4; bt_column_weights from scoring.v2_v3_v4.bt_column_weights.
    """

    def __init__(
        self,
        cfg: CoreTrendRiskConfig | None = None,
        bt_column_weights: dict[str, float] | None = None,
    ) -> None:
        super().__init__("V4", cfg or CoreTrendRiskConfig(trend_weight=1.0, risk_weight=0.9, bt_weight=0.3))
        self._bt_weights = bt_column_weights or {"bt_mean": 0.5, "bt_winrate": 0.3, "bt_worst_mdd": 0.2}

    def score(self, trade_date: date, store: DuckDBStore) -> pd.DataFrame:
        df = _fetch_features(store, trade_date, include_bt=True)
        if df.empty:
            return ensure_score_frame(pd.DataFrame(columns=["ticker", "score", "reason"]))

        df = df.set_index("ticker")
        trend_score = _compute_trend_score(df)
        risk_penalty = _compute_risk_penalty(df)
        ma_bonus = _compute_ma_bonus(df)

        bt_score = _compute_bt_score(df, self._bt_weights)
        core_score = _compose_score(trend_score, risk_penalty, self.cfg, bt_score=bt_score)

        rows = []
        for ticker, s in core_score.items():
            payload = {
                "model": "V4",
                "trend_score": _safe_float(trend_score.loc[ticker]),
                "risk_penalty": _safe_float(risk_penalty.loc[ticker]),
                "bt_score": _safe_float(bt_score.loc[ticker]),
                "ma_bonus": _safe_float(ma_bonus.loc[ticker]),
                "ma_bonus_included_in_trend_score": True,
                "trend_weight": self.cfg.trend_weight,
                "risk_weight": self.cfg.risk_weight,
                "bt_weight": self.cfg.bt_weight,
            }
            if all(c in df.columns for c in BT_COLS):
                payload["bt_mean"] = _safe_float(df.loc[ticker].get("bt_mean"))
                payload["bt_winrate"] = _safe_float(df.loc[ticker].get("bt_winrate"))
                payload["bt_worst_mdd"] = _safe_float(df.loc[ticker].get("bt_worst_mdd"))
            rows.append(
                {
                    "ticker": str(ticker),
                    "score": float(s) if pd.notna(s) else None,
                    "reason": json.dumps(payload, ensure_ascii=False),
                }
            )

        result = pd.DataFrame(rows)
        return ensure_score_frame(result)


def _safe_float(value: object) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return float(f)

