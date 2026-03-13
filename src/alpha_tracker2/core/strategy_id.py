# src/alpha_tracker2/core/strategy_id.py
from __future__ import annotations

import re
from dataclasses import dataclass

_ALLOWED_MODELS = {"V1", "V2", "V3", "V4", "ENS"}
_ALLOWED_TRADE_RULES = {"REB_ON_SIGNAL_CHANGE", "REB_DAILY"}

_STRAT_RE = re.compile(
    r"^(?P<model>[A-Z0-9]+)__(?P<trade_rule>[A-Z0-9_]+)__H(?P<hold_n>\d+)__TOP(?P<topk>\d+)__C(?P<cost_bps>\d+)$"
)

@dataclass(frozen=True)
class StrategySpec:
    model_version: str
    trade_rule: str
    hold_n: int
    topk: int
    cost_bps: int

def build_strategy_id(spec: StrategySpec) -> str:
    validate_spec(spec)
    return (
        f"{spec.model_version}__{spec.trade_rule}__H{spec.hold_n}__TOP{spec.topk}__C{spec.cost_bps}"
    )

def parse_strategy_id(strategy_id: str) -> StrategySpec:
    m = _STRAT_RE.match(strategy_id.strip())
    if not m:
        raise ValueError(f"Invalid strategy_id format: {strategy_id}")

    spec = StrategySpec(
        model_version=m.group("model"),
        trade_rule=m.group("trade_rule"),
        hold_n=int(m.group("hold_n")),
        topk=int(m.group("topk")),
        cost_bps=int(m.group("cost_bps")),
    )
    validate_spec(spec)
    return spec

def validate_spec(spec: StrategySpec) -> None:
    if spec.model_version not in _ALLOWED_MODELS:
        raise ValueError(f"Unsupported model_version: {spec.model_version}")
    if spec.trade_rule not in _ALLOWED_TRADE_RULES:
        raise ValueError(f"Unsupported trade_rule: {spec.trade_rule}")
    if spec.hold_n <= 0:
        raise ValueError("hold_n must be > 0")
    if spec.topk <= 0:
        raise ValueError("topk must be > 0")
    if spec.cost_bps < 0:
        raise ValueError("cost_bps must be >= 0")
