# -*- coding: utf-8 -*-
"""
Hot Industry Universe plugin

Goal:
- Pick "hot" industries (6-8 by default) using HotScore = f(amount, main_inflow, pct_change)
- For each industry, select top N stocks by stock-level HotScore (amount + main_inflow + pct_change)
- Union them into a universe of 200-300 tickers (configurable)
- Export CSV + optional DB upsert

Notes:
- Data source: AkShare / Eastmoney interfaces
- This plugin is designed for fast daily testing loops (small universe) instead of full A-share.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ----------------------------
# Helpers
# ----------------------------

def _to_date(x) -> date:
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    return pd.to_datetime(x).date()


def _safe_float(s) -> float:
    try:
        if pd.isna(s):
            return np.nan
        return float(s)
    except Exception:
        return np.nan


def _zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.mean(skipna=True)
    sd = s.std(skipna=True)
    if sd == 0 or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


def _normalize_ticker(raw: str) -> Optional[str]:
    """
    Normalize A-share ticker to '000001.SZ' / '600000.SH' format.
    Accepts:
    - '000001' + market guess
    - '000001.SZ' already ok
    - Eastmoney style like '000001' with market field maybe present elsewhere
    """
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    t = str(raw).strip()
    if not t:
        return None
    if t.endswith(".SZ") or t.endswith(".SH") or t.endswith(".BJ"):
        return t
    # guess by leading digit
    if len(t) == 6 and t.isdigit():
        if t.startswith(("0", "3")):
            return f"{t}.SZ"
        if t.startswith(("6", "9")):
            return f"{t}.SH"
        if t.startswith(("4", "8")):
            return f"{t}.BJ"
        return f"{t}.SZ"
    return None


def _normalize_spot_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Eastmoney A-share spot columns to:
    ['ticker','name','amount','main_inflow','pct_change','close']
    Not all fields are guaranteed.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "name", "amount", "main_inflow", "pct_change", "close"])

    colmap_candidates = {
        "代码": "ticker",
        "股票代码": "ticker",
        "证券代码": "ticker",
        "名称": "name",
        "股票简称": "name",
        "证券简称": "name",
        "成交额": "amount",
        "成交额(元)": "amount",
        "成交额(万)": "amount",
        "主力净流入": "main_inflow",
        "主力净流入(元)": "main_inflow",
        "主力净流入(万)": "main_inflow",
        "涨跌幅": "pct_change",
        "涨跌幅(%)": "pct_change",
        "最新价": "close",
        "最新": "close",
        "收盘": "close",
    }

    out = df.copy()
    # rename if match
    rename = {}
    for c in out.columns:
        if c in colmap_candidates:
            rename[c] = colmap_candidates[c]
    out = out.rename(columns=rename)

    # ensure expected cols exist
    for c in ["ticker", "name", "amount", "main_inflow", "pct_change", "close"]:
        if c not in out.columns:
            out[c] = np.nan

    out["ticker"] = out["ticker"].map(_normalize_ticker)
    out = out.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"], keep="first")

    # numeric conversions
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out["main_inflow"] = pd.to_numeric(out["main_inflow"], errors="coerce")
    out["pct_change"] = pd.to_numeric(out["pct_change"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")

    return out[["ticker", "name", "amount", "main_inflow", "pct_change", "close"]].reset_index(drop=True)


def _normalize_industry_board_spot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Eastmoney industry board spot to:
    ['industry','amount','main_inflow','pct_change']
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["industry", "amount", "main_inflow", "pct_change"])

    colmap = {
        "板块名称": "industry",
        "名称": "industry",
        "行业": "industry",
        "成交额": "amount",
        "成交额(元)": "amount",
        "主力净流入": "main_inflow",
        "主力净流入(元)": "main_inflow",
        "涨跌幅": "pct_change",
        "涨跌幅(%)": "pct_change",
    }

    out = df.copy()
    rename = {}
    for c in out.columns:
        if c in colmap:
            rename[c] = colmap[c]
    out = out.rename(columns=rename)

    for c in ["industry", "amount", "main_inflow", "pct_change"]:
        if c not in out.columns:
            out[c] = np.nan

    out["industry"] = out["industry"].astype(str).str.strip()
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out["main_inflow"] = pd.to_numeric(out["main_inflow"], errors="coerce")
    out["pct_change"] = pd.to_numeric(out["pct_change"], errors="coerce")

    out = out.dropna(subset=["industry"]).drop_duplicates(subset=["industry"], keep="first")
    return out[["industry", "amount", "main_inflow", "pct_change"]].reset_index(drop=True)


def _hot_score(df: pd.DataFrame, w_amount=0.5, w_inflow=0.3, w_pct=0.2) -> pd.Series:
    """
    HotScore via z-scored mix (robust across scales):
      z(amount)*w_amount + z(main_inflow)*w_inflow + z(pct_change)*w_pct
    """
    z_amount = _zscore(df["amount"])
    z_inflow = _zscore(df["main_inflow"])
    z_pct = _zscore(df["pct_change"])
    return z_amount * w_amount + z_inflow * w_inflow + z_pct * w_pct


# ----------------------------
# Config / Result
# ----------------------------

@dataclass
class HotIndustryConfig:
    top_industries: int = 8
    per_industry: int = 30
    max_universe: int = 300

    # weights for industry hot score
    w_amount: float = 0.5
    w_inflow: float = 0.3
    w_pct: float = 0.2

    # stock-level hot score weights
    stock_w_amount: float = 0.5
    stock_w_inflow: float = 0.3
    stock_w_pct: float = 0.2

    # filters
    min_amount: float = 0.0  # optional: filter low liquidity
    allow_bj: bool = True    # include BJ by default

    # output
    out_dir: Optional[str] = None


# ----------------------------
# Main class
# ----------------------------

class HotIndustryUniverse:
    def __init__(self, cfg: HotIndustryConfig):
        self.cfg = cfg

    def build(self, trade_date: str | date, verbose: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns:
          universe_df columns:
            ['trade_date','ticker','name','industry','amount','main_inflow','pct_change','hot_score']
          industries_df columns:
            ['trade_date','industry','amount','main_inflow','pct_change','hot_score','rank']
        """
        d = _to_date(trade_date)

        if verbose:
            print("[FETCH] industry board spot (Eastmoney)")

        ind_spot_raw = _ak_industry_board_spot()
        ind_spot = _normalize_industry_board_spot(ind_spot_raw)

        if ind_spot.empty:
            # graceful fallback
            if verbose:
                print("[WARN] industry board spot empty; returning empty universe.")
            uni_empty = pd.DataFrame(
                columns=["trade_date", "ticker", "name", "industry", "amount", "main_inflow", "pct_change", "hot_score"]
            )
            ind_empty = pd.DataFrame(
                columns=["trade_date", "industry", "amount", "main_inflow", "pct_change", "hot_score", "rank"]
            )
            return uni_empty, ind_empty

        ind_spot["hot_score"] = _hot_score(
            ind_spot,
            w_amount=self.cfg.w_amount,
            w_inflow=self.cfg.w_inflow,
            w_pct=self.cfg.w_pct,
        )
        ind_spot = ind_spot.sort_values("hot_score", ascending=False).reset_index(drop=True)
        ind_spot["rank"] = np.arange(1, len(ind_spot) + 1)

        top_inds = ind_spot.head(int(self.cfg.top_industries)).copy()
        top_inds.insert(0, "trade_date", d)

        if verbose:
            print("\n[HOT-INDUSTRY] Top industries by HotScore")
            cols_show = ["rank", "industry", "amount", "main_inflow", "pct_change", "hot_score"]
            print(top_inds[cols_show])

        if verbose:
            print("[FETCH] A-share spot (Eastmoney)")

        spot_raw = _ak_ashare_spot()
        spot = _normalize_spot_columns(spot_raw)

        # constituents per industry
        cons_parts: List[pd.DataFrame] = []
        for ind in top_inds["industry"].tolist():
            if verbose:
                print(f"[FETCH] industry constituents: {ind}")
            cons_raw = _ak_industry_constituents(industry=ind)
            cons = _normalize_constituents(cons_raw, industry=ind)
            if cons.empty:
                continue

            # merge with spot to get amount / inflow / pct_change / close
            # IMPORTANT FIX:
            #   both cons and spot may contain 'name'. If so, pandas will suffix columns.
            #   downstream expects a single column named 'name'.
            sub = cons.merge(spot, on="ticker", how="left", suffixes=("_cons", "_spot"))
            # If both sides contain a name column, pandas will suffix them.
            # We always want a single 'name' column for downstream code.
            if "name" not in sub.columns:
                if "name_cons" in sub.columns and "name_spot" in sub.columns:
                    sub["name"] = sub["name_cons"].fillna(sub["name_spot"])
                elif "name_cons" in sub.columns:
                    sub["name"] = sub["name_cons"]
                elif "name_spot" in sub.columns:
                    sub["name"] = sub["name_spot"]

            # if spot missing for some tickers, fill zeros to keep ranking stable
            for c in ["amount", "main_inflow", "pct_change"]:
                if c not in sub.columns:
                    sub[c] = 0.0
                sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(0.0)

            sub["hot_score"] = _hot_score(
                sub,
                w_amount=self.cfg.stock_w_amount,
                w_inflow=self.cfg.stock_w_inflow,
                w_pct=self.cfg.stock_w_pct,
            )

            # optional liquidity filter
            if self.cfg.min_amount and self.cfg.min_amount > 0:
                sub = sub[sub["amount"] >= float(self.cfg.min_amount)]

            # optional BJ filter
            if not self.cfg.allow_bj:
                sub = sub[~sub["ticker"].str.endswith(".BJ")]

            # pick top N stocks for this industry
            sub = sub.sort_values("hot_score", ascending=False).head(int(self.cfg.per_industry)).copy()
            cons_parts.append(sub)

        if not cons_parts:
            if verbose:
                print("[WARN] no constituents collected; returning empty universe.")
            uni_empty = pd.DataFrame(
                columns=["trade_date", "ticker", "name", "industry", "amount", "main_inflow", "pct_change", "hot_score"]
            )
            return uni_empty, top_inds[["trade_date", "industry", "amount", "main_inflow", "pct_change", "hot_score", "rank"]]

        uni = pd.concat(cons_parts, ignore_index=True)

        # enforce final columns (this is where your KeyError previously happened)
        uni.insert(0, "trade_date", d)
        # Some APIs may still not provide name; ensure the column exists.
        if "name" not in uni.columns:
            uni["name"] = ""
        uni = uni[
            ["trade_date", "ticker", "name", "industry", "amount", "main_inflow", "pct_change", "hot_score"]
        ].copy()

        # de-dup by ticker: keep the best score if appears in multiple industries
        uni = uni.sort_values("hot_score", ascending=False).drop_duplicates(subset=["ticker"], keep="first")

        # clip to max_universe
        uni = uni.sort_values("hot_score", ascending=False).head(int(self.cfg.max_universe)).reset_index(drop=True)

        return uni, top_inds[["trade_date", "industry", "amount", "main_inflow", "pct_change", "hot_score", "rank"]]


# ----------------------------
# Normalizers for constituents
# ----------------------------

def _normalize_constituents(df: pd.DataFrame, industry: str) -> pd.DataFrame:
    """
    Normalize industry constituents to:
      ['ticker','name','industry']
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "name", "industry"])

    colmap = {
        "代码": "ticker",
        "股票代码": "ticker",
        "证券代码": "ticker",
        "名称": "name",
        "股票简称": "name",
        "证券简称": "name",
    }
    out = df.copy()
    rename = {}
    for c in out.columns:
        if c in colmap:
            rename[c] = colmap[c]
    out = out.rename(columns=rename)

    for c in ["ticker", "name"]:
        if c not in out.columns:
            out[c] = np.nan

    out["ticker"] = out["ticker"].map(_normalize_ticker)
    out = out.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
    out["name"] = out["name"].astype(str).fillna("").str.strip()
    out["industry"] = industry

    return out[["ticker", "name", "industry"]]


# ----------------------------
# Public function (entry)
# ----------------------------

def build_hot_industry_universe(
    trade_date: str | date,
    top_industries: int = 8,
    per_industry: int = 30,
    max_universe: int = 300,
    verbose: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cfg = HotIndustryConfig(
        top_industries=top_industries,
        per_industry=per_industry,
        max_universe=max_universe,
    )
    return HotIndustryUniverse(cfg).build(trade_date=trade_date, verbose=verbose)


# ----------------------------
# Data sources (AkShare / Eastmoney)
# ----------------------------

def _ak_industry_board_spot() -> pd.DataFrame:
    """
    Eastmoney industry board spot.
    Must return a DataFrame.
    """
    try:
        import akshare as ak  # type: ignore
        # commonly used api:
        # ak.stock_board_industry_name_em() or similar
        # We'll try a few candidates to be robust.
        if hasattr(ak, "stock_board_industry_name_em"):
            return ak.stock_board_industry_name_em()
        if hasattr(ak, "stock_board_industry_spot_em"):
            return ak.stock_board_industry_spot_em()
        # fallback
        return ak.stock_board_industry_name_em()
    except Exception as e:
        print(f"[WARN] _ak_industry_board_spot failed: {e}")
        return pd.DataFrame()


def _ak_ashare_spot() -> pd.DataFrame:
    """
    Eastmoney A-share spot.
    """
    try:
        import akshare as ak  # type: ignore
        if hasattr(ak, "stock_zh_a_spot_em"):
            return ak.stock_zh_a_spot_em()
        # fallback
        return ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"[WARN] _ak_ashare_spot failed: {e}")
        return pd.DataFrame()


def _ak_industry_constituents(industry: str) -> pd.DataFrame:
    """
    Eastmoney industry constituents.
    """
    try:
        import akshare as ak  # type: ignore
        # Common:
        # ak.stock_board_industry_cons_em(symbol="半导体")
        if hasattr(ak, "stock_board_industry_cons_em"):
            return ak.stock_board_industry_cons_em(symbol=industry)
        # fallback
        return ak.stock_board_industry_cons_em(symbol=industry)
    except Exception as e:
        print(f"[WARN] _ak_industry_constituents failed ({industry}): {e}")
        return pd.DataFrame()
