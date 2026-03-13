# tools/register_strategies.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import yaml

from alpha_tracker2.core.strategy_id import StrategySpec, build_strategy_id

ROOT = Path(__file__).resolve().parents[1]

def load_cfg(cfg_path: Path) -> dict:
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "data" / "store" / "alpha_tracker.duckdb"))
    ap.add_argument("--config", default=str(ROOT / "configs" / "default.yaml"))
    args = ap.parse_args()

    cfg = load_cfg(Path(args.config))
    m = cfg.get("experiments", {}).get("matrix", {})
    models = m.get("models", [])
    trade_rules = m.get("trade_rules", [])
    holds = m.get("holds", [])
    topk = int(m.get("topk", 6))
    cost_bps = int(m.get("cost_bps", 10))
    lot_size = int(m.get("lot_size", 100))

    if not models or not trade_rules or not holds:
        raise ValueError("experiments.matrix missing models/trade_rules/holds")

    con = duckdb.connect(args.db)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
              strategy_id VARCHAR PRIMARY KEY,
              model_version VARCHAR,
              trade_rule VARCHAR,
              hold_n INTEGER,
              topk INTEGER,
              cost_bps INTEGER,
              lot_size INTEGER,
              params_json VARCHAR,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        inserted = 0
        for model in models:
            for tr in trade_rules:
                for h in holds:
                    spec = StrategySpec(model_version=model, trade_rule=tr, hold_n=int(h),
                                        topk=topk, cost_bps=cost_bps)
                    sid = build_strategy_id(spec)

                    params = {
                        "model_version": model,
                        "trade_rule": tr,
                        "hold_n": int(h),
                        "topk": topk,
                        "cost_bps": cost_bps,
                        "lot_size": lot_size,
                    }
                    # DuckDB has INSERT OR REPLACE; we keep it idempotent
                    con.execute("""
                        INSERT OR REPLACE INTO strategies
                        (strategy_id, model_version, trade_rule, hold_n, topk, cost_bps, lot_size, params_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [sid, model, tr, int(h), topk, cost_bps, lot_size, json.dumps(params, ensure_ascii=False)])

                    inserted += 1

        print(f"[OK] strategies upserted: {inserted}")
    finally:
        con.close()

if __name__ == "__main__":
    main()
