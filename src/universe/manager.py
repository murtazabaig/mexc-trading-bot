import asyncio
from typing import Dict, Any, Optional
from .filters import filter_markets, UniverseConfig
from .market_loader import load_mexc_futures_markets

class UniverseManager:
    """Manages the market universe and its periodic refresh."""
    
    def __init__(self, db_conn: Any, exchange: Any, config: Any):
        self.db_conn = db_conn
        self.exchange = exchange
        self.config = config
        self.symbols = {}
        
        # Initialize filter config
        self.filter_config = UniverseConfig(
            min_volume_usd=config.universe.min_volume_usd,
            max_spread_percent=config.universe.max_spread_percent,
            exclude_patterns=config.universe.exclude_patterns,
            exclude_symbols=config.universe.exclude_symbols,
            min_notional=config.universe.min_notional,
            min_price=config.universe.min_price,
            max_price=config.universe.max_price,
        )

    async def refresh(self):
        """Refresh the market universe."""
        try:
            # Load markets (run in executor since it's blocking)
            markets = await asyncio.get_event_loop().run_in_executor(
                None, load_mexc_futures_markets, self.exchange, 3600, False
            )
            
            # Apply filters
            self.symbols = filter_markets(markets, self.filter_config)
            return self.symbols
        except Exception as e:
            # In a real scenario, we'd use the logger here
            print(f"Failed to refresh universe: {e}")
            return self.symbols

    def get_symbols(self) -> Dict[str, Any]:
        """Return the current set of filtered symbols."""
        return self.symbols
