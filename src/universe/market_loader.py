"""Market loader for MEXC futures markets."""

import time
import hashlib
from typing import Dict, Optional
from datetime import datetime, timedelta

import ccxt

from ..logger import get_logger

logger = get_logger(__name__)


# Simple in-memory cache
_market_cache: Dict[str, tuple] = {}  # {key: (data, timestamp)}


def load_mexc_futures_markets(
    exchange: ccxt.Exchange,
    cache_ttl: int = 3600,
    force_refresh: bool = False
) -> Dict[str, Dict]:
    """
    Fetch all USDT-M perpetual symbols from MEXC via ccxt.
    
    Args:
        exchange: Initialized ccxt exchange instance
        cache_ttl: Cache time-to-live in seconds (default: 1 hour)
        force_refresh: Force refresh even if cache is valid
        
    Returns:
        Dictionary keyed by symbol with market metadata
        
    Raises:
        ccxt.NetworkError: If API request fails after retries
        ccxt.ExchangeError: If exchange returns error response
    """
    cache_key = "mexc_futures_markets"
    current_time = time.time()
    
    # Check cache
    if not force_refresh and cache_key in _market_cache:
        cached_data, cached_time = _market_cache[cache_key]
        if current_time - cached_time < cache_ttl:
            logger.info(f"Using cached markets (age: {int(current_time - cached_time)}s)")
            return cached_data
    
    try:
        logger.info("Fetching MEXC futures markets...")
        markets = exchange.load_markets()
        
        # Filter for USDT-M perpetual futures
        filtered_markets = {}
        usdt_perp_count = 0
        
        for symbol, market in markets.items():
            # Check if it's a USDT-M perpetual contract
            if (market.get('type') == 'swap' and 
                market.get('settle') == 'USDT' and
                market.get('linear', False) and
                market.get('active', True)):
                
                # Ensure symbol ends with USDT
                if symbol.endswith('USDT'):
                    filtered_markets[symbol] = market
                    usdt_perp_count += 1
        
        logger.info(f"Loaded {len(filtered_markets)} USDT-M perpetual markets")
        
        # Store in cache
        _market_cache[cache_key] = (filtered_markets, current_time)
        
        return filtered_markets
        
    except ccxt.NetworkError as e:
        logger.error(f"Network error loading markets: {e}")
        # Try to return cached data as fallback
        if cache_key in _market_cache:
            logger.warning("Using stale cached data due to network error")
            return _market_cache[cache_key][0]
        raise
    except ccxt.ExchangeError as e:
        logger.error(f"Exchange error loading markets: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading markets: {e}")
        raise


def clear_market_cache():
    """Clear the in-memory market cache."""
    global _market_cache
    _market_cache.clear()
    logger.info("Market cache cleared")


def get_market_summary(markets: Dict[str, Dict]) -> Dict[str, any]:
    """
    Generate a summary of the markets dictionary.
    
    Args:
        markets: Dictionary of markets
        
    Returns:
        Summary statistics
    """
    if not markets:
        return {
            "total": 0,
            "min_notional": None,
            "max_notional": None,
            "avg_notional": None
        }
    
    notionals = []
    for market in markets.values():
        if market.get('limits', {}).get('cost', {}).get('min'):
            notional = market['limits']['cost']['min']
            notionals.append(notional)
    
    return {
        "total": len(markets),
        "symbols": list(markets.keys())[:10],  # First 10 as sample
        "has_volume_data": any(m.get('info', {}).get('volume') for m in markets.values())
    }


def compute_universe_hash(markets: Dict[str, Dict]) -> str:
    """
    Compute a hash of the universe for comparison.
    
    Args:
        markets: Dictionary of markets
        
    Returns:
        SHA256 hash of the universe
    """
    # Sort symbols to ensure consistent hashing
    sorted_symbols = sorted(markets.keys())
    symbols_str = ",".join(sorted_symbols)
    return hashlib.sha256(symbols_str.encode()).hexdigest()
