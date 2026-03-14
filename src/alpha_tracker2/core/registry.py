"""
Registry for ingestion providers (and later scoring/strategies).

This module holds only provider registration for ingestion.
Other agents will extend registry for scorers and strategies.
"""

from __future__ import annotations

from alpha_tracker2.ingestion.base import PriceProvider, UniverseProvider
from alpha_tracker2.ingestion.plugins.yahoo_price_provider import YahooPriceProvider
from alpha_tracker2.ingestion.plugins.yahoo_universe import YahooUniverseProvider

UNIVERSE_PROVIDERS: dict[str, type[UniverseProvider]] = {
    "yahoo_universe": YahooUniverseProvider,
}

PRICE_PROVIDERS: dict[str, type[PriceProvider]] = {
    "yahoo_prices": YahooPriceProvider,
}


def get_universe_provider(name: str) -> type[UniverseProvider]:
    if name not in UNIVERSE_PROVIDERS:
        raise KeyError(f"Unknown universe provider: {name!r}. Known: {list(UNIVERSE_PROVIDERS)}")
    return UNIVERSE_PROVIDERS[name]


def get_price_provider(name: str) -> type[PriceProvider]:
    if name not in PRICE_PROVIDERS:
        raise KeyError(f"Unknown price provider: {name!r}. Known: {list(PRICE_PROVIDERS)}")
    return PRICE_PROVIDERS[name]
