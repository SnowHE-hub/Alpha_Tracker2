from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Type

from alpha_tracker2.ingestion.base import UniverseProvider
from alpha_tracker2.ingestion.prices_base import PriceProvider


@dataclass
class Registry:
    universe_providers: Dict[str, Type[UniverseProvider]]
    prices_providers: Dict[str, Type[PriceProvider]]

    def get_universe_provider(self, name: str) -> Type[UniverseProvider]:
        if name not in self.universe_providers:
            raise KeyError(f"Unknown universe provider: {name}")
        return self.universe_providers[name]

    def get_prices_provider(self, name: str) -> Type[PriceProvider]:
        if name not in self.prices_providers:
            raise KeyError(f"Unknown prices provider: {name}")
        return self.prices_providers[name]


REGISTRY = Registry(universe_providers={}, prices_providers={})
