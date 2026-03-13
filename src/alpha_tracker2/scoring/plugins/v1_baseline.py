from __future__ import annotations

import json
from typing import Dict, Any

import numpy as np
import pandas as pd


def _zscore(s: pd.Series) -> pd.Series:
    """Safe z-score: ignore NaN; if std==0 -> 0."""
    x = pd.to_numeric(s, errors="coerce").astype(float)
    mu = np.nanmean(x.to_numpy())
    sd = np.nanstd(x.to_numpy())
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(np.zeros(len(x), dtype=float), index=x.index)
    return (x - mu) / sd


class V1BaselineScorer:
    """
    V1 baseline scorer (JSON reason)
    --------------------------------
    规则型打分：动量/收益越大越好，波动越小越好。
    目标：提供一个稳定、可解释、可对比的 baseline。
    """

    name = "V1"

    def score(self, trade_date, features: pd.DataFrame) -> pd.DataFrame:
        df = features.copy()

        # ---- required cols guard ----
        for col in ["ticker", "ret_1d", "mom_5d", "vol_5d"]:
            if col not in df.columns:
                raise ValueError(f"V1 requires column '{col}' in features")

        df["ticker"] = df["ticker"].astype(str)

        # name 字段（如果没有就补空，score_all 会回填）
        if "name" not in df.columns:
            df["name"] = None

        # ---- compute components (zscore) ----
        z_mom = _zscore(df["mom_5d"])          # higher better
        z_ret = _zscore(df["ret_1d"])          # higher better
        z_vol = _zscore(df["vol_5d"])          # higher worse -> invert

        w_mom, w_vol, w_ret = 0.6, 0.3, 0.1
        score = w_mom * z_mom + w_vol * (-z_vol) + w_ret * z_ret

        # 缺失值：如果原始值缺失，zscore 已经是 NaN -> 这里统一变 0（中性）
        score = pd.to_numeric(score, errors="coerce").fillna(0.0)
        df["score"] = score

        # rank：分数越高 rank 越靠前
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        df["rank"] = (np.arange(len(df)) + 1).astype(int)

        # ---- reason JSON ----
        # 为了可解释：写入原始值 + z 值 + 加权贡献
        reasons = []
        n = len(df)

        # 取前几位用于 debug（避免 JSON 过大，你也可以改成全量记录）
        for i, row in df.iterrows():
            mom = row.get("mom_5d")
            ret1 = row.get("ret_1d")
            vol = row.get("vol_5d")

            # z 值（从已算好的 series 中取）
            zm = float(z_mom.loc[row.name]) if pd.notna(z_mom.loc[row.name]) else 0.0
            zr = float(z_ret.loc[row.name]) if pd.notna(z_ret.loc[row.name]) else 0.0
            zv = float(z_vol.loc[row.name]) if pd.notna(z_vol.loc[row.name]) else 0.0

            score_components = {
                "mom_5d": {"value": None if pd.isna(mom) else float(mom), "z": zm, "weight": w_mom, "contrib": w_mom * zm},
                "vol_5d": {"value": None if pd.isna(vol) else float(vol), "z": zv, "weight": w_vol, "contrib": w_vol * (-zv)},
                "ret_1d": {"value": None if pd.isna(ret1) else float(ret1), "z": zr, "weight": w_ret, "contrib": w_ret * zr},
            }

            payload: Dict[str, Any] = {
                "universe": {"source": "features_daily", "n": int(n)},
                "features": {"asof": str(trade_date)},
                "signals": {
                    "mom_5d": score_components["mom_5d"]["value"],
                    "vol_5d": score_components["vol_5d"]["value"],
                    "ret_1d": score_components["ret_1d"]["value"],
                },
                "filters": {
                    # V1 baseline 不做硬过滤，这里留空结构，方便后续扩展
                },
                "rank_detail": {
                    "score_raw": float(row["score"]),
                    "score_components": score_components,
                    "rank": int(row["rank"]),
                    "method": "V1 zscore: 0.6*z(mom_5d) + 0.3*(-z(vol_5d)) + 0.1*z(ret_1d)",
                },
                "debug": {
                    "legacy_fields": {
                        # 如果你旧脚本有字段名，可以在这里做别名映射
                        # "trend_score": "...",
                    }
                },
            }
            reasons.append(json.dumps(payload, ensure_ascii=False))

        df["reason"] = reasons

        # picked_by（可选）：写上版本名，后续 ENS/分析更好追溯
        df["picked_by"] = "V1"

        # ---- output (score_all 会补 score_100 / thr_value / pass_thr 等) ----
        out = df[["ticker", "name", "rank", "score", "reason", "picked_by"]].copy()
        out["trade_date"] = trade_date
        return out
