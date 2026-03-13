from __future__ import annotations

from datetime import date
from typing import List

import pandas as pd
import akshare as ak

from alpha_tracker2.ingestion.base import UniverseRow


class AkShareUniverseProvider:
    name = "akshare"

    def fetch_universe(self, trade_date: date) -> List[UniverseRow]:
        """
        最小实现：获取A股列表（代码+名称）
        trade_date 先不参与过滤（后续接交易日历/上市状态）
        """
        #raise RuntimeError("simulate akshare down")

        # ak.stock_info_a_code_name() 通常返回 code/name 两列
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return []

        # 兼容列名差异
        cols = {c.lower(): c for c in df.columns}
        code_col = cols.get("code") or cols.get("股票代码") or cols.get("证券代码")
        name_col = cols.get("name") or cols.get("股票简称") or cols.get("名称") or cols.get("证券简称")

        if code_col is None or name_col is None:
            raise ValueError(f"Unexpected columns from akshare: {list(df.columns)}")

        out: List[UniverseRow] = []
        for _, r in df[[code_col, name_col]].dropna().iterrows():
            code = str(r[code_col]).strip()
            name = str(r[name_col]).strip()

            # AkShare 常见是 000001 这种裸码；我们统一加后缀，便于后续行情接口一致化
            if code.isdigit() and len(code) == 6:
                if code.startswith(("0", "3")):
                    ticker = f"{code}.SZ"
                elif code.startswith("6"):
                    ticker = f"{code}.SH"
                else:
                    # 其他情况先原样
                    ticker = code
            else:
                ticker = code

            out.append(UniverseRow(ticker=ticker, name=name))

        return out
