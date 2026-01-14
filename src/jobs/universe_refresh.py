"""Universe refresh job for MEXC futures markets."""

import asyncio
import json
from typing import Dict, Any, Optional

import ccxt

from ..logger import get_logger
from ..universe import (
    load_mexc_futures_markets,
    filter_markets,
    compute_universe_hash,
    compare_universes,
    UniverseConfig as FilterUniverseConfig
)

logger = get_logger(__name__)

# Store previous universe for comparison
_previous_universe: Dict[str, Any] = {}
_previous_hash: Optional[str] = None


async def refresh_universe(
    exchange: ccxt.Exchange,
    db_conn: Any,
    config: FilterUniverseConfig,
    logger_instance: Any = None
) -> Dict[str, Any]:
    """
    Refresh the market universe with error handling and retry logic.
    
    Args:
        exchange: Initialized ccxt exchange instance
        db_conn: Database connection for storing snapshots
        config: UniverseConfig with filter settings
        logger_instance: Optional logger instance
        
    Returns:
        Dictionary with refresh results including:
        - success: bool
        - markets_count: int
        - filtered_count: int
        - changes: dict with added/removed symbols
        - hash: str of the universe
    """
    log = logger_instance or logger
    
    global _previous_universe, _previous_hash
    
    result = {
        "success": False,
        "markets_count": 0,
        "filtered_count": 0,
        "changes": {"added": [], "removed": []},
        "hash": None,
        "error": None
    }
    
    try:
        log.info("Starting universe refresh...")
        
        # Load markets with retry logic
        max_retries = 3
        backoff = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                # Load markets (this is async via ccxt)
                markets = await asyncio.get_event_loop().run_in_executor(
                    None, load_mexc_futures_markets, exchange, 3600, False
                )
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    log.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff *= 2  # Exponential backoff
                else:
                    raise
        
        result["markets_count"] = len(markets)
        log.info(f"Loaded {len(markets)} total markets from MEXC")
        
        # Apply filters
        filtered_markets = filter_markets(markets, config)
        result["filtered_count"] = len(filtered_markets)
        log.info(f"Filtered to {len(filtered_markets)} markets")
        
        # Compute hash
        universe_hash = compute_universe_hash(filtered_markets)
        result["hash"] = universe_hash
        
        # Compare with previous universe
        changes = compare_universes(_previous_universe, filtered_markets)
        result["changes"] = changes
        
        if changes["added"]:
            log.info(f"Added {len(changes['added'])} markets: {', '.join(changes['added'][:10])}{'...' if len(changes['added']) > 10 else ''}")
        
        if changes["removed"]:
            log.info(f"Removed {len(changes['removed'])} markets: {', '.join(changes['removed'][:10])}{'...' if len(changes['removed']) > 10 else ''}")
        
        if not changes["added"] and not changes["removed"]:
            log.info("No changes in universe")
        
        # Store universe snapshot in database
        if db_conn:
            try:
                snapshot_data = {
                    "universe_hash": universe_hash,
                    "markets_count": len(filtered_markets),
                    "symbols": sorted(filtered_markets.keys()),
                    "config": {
                        "min_volume_usd": config.min_volume_usd,
                        "max_spread_percent": config.max_spread_percent,
                        "min_notional": config.min_notional,
                        "exclude_patterns": config.exclude_patterns,
                    },
                    "changes": changes
                }
                
                # Use insert_params_snapshot to store
                from ..database import insert_params_snapshot
                snapshot_id = insert_params_snapshot(db_conn, snapshot_data)
                log.info(f"Stored universe snapshot with ID: {snapshot_id}")
                
            except Exception as e:
                log.error(f"Failed to store universe snapshot: {e}")
                # Don't fail the whole job if snapshot storage fails
        
        # Update previous universe
        _previous_universe = filtered_markets
        _previous_hash = universe_hash
        
        result["success"] = True
        log.success("Universe refresh completed successfully")
        
    except ccxt.NetworkError as e:
        result["error"] = f"Network error: {e}"
        log.error(f"Network error during universe refresh: {e}")
    except ccxt.ExchangeError as e:
        result["error"] = f"Exchange error: {e}"
        log.error(f"Exchange error during universe refresh: {e}")
    except Exception as e:
        result["error"] = f"Unexpected error: {e}"
        log.exception(f"Unexpected error during universe refresh: {e}")
    
    return result


def get_current_universe() -> Dict[str, Any]:
    """
    Get the current universe state.
    
    Returns:
        Dictionary of current universe markets
    """
    global _previous_universe
    return _previous_universe.copy()


def get_universe_stats() -> Dict[str, Any]:
    """
    Get statistics about the current universe.
    
    Returns:
        Dictionary with universe statistics
    """
    global _previous_universe, _previous_hash
    
    return {
        "count": len(_previous_universe),
        "hash": _previous_hash,
        "sample_symbols": list(_previous_universe.keys())[:10] if _previous_universe else []
    }
