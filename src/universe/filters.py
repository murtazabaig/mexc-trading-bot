"""Market universe filters for MEXC futures."""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from ..logger import get_logger

logger = get_logger(__name__)


@dataclass
class UniverseConfig:
    """Configuration for market universe filtering."""
    
    # Volume filter
    min_volume_usd: float = 1_000_000  # 24h volume in USDT
    
    # Spread filter (max bid-ask spread percentage)
    max_spread_percent: float = 0.05  # 0.05%
    
    # Exclusion patterns (stablecoins, leverage tokens, etc.)
    exclude_patterns: List[str] = field(default_factory=lambda: [
        "BUSD",       # BUSD stablecoin
        "UPUSDT",     # Bull leverage tokens (e.g., BTCUP)
        "DOWNUSDT",   # Bear leverage tokens (e.g., BTCDOWN)
        "BEAR",       # Bear tokens
        "BULL",       # Bull tokens
        "3L$",        # 3x leverage long (e.g., BTC3L)
        "3S$",        # 3x leverage short (e.g., BTC3S)
        "5L$",        # 5x leverage long (e.g., BTC5L)
        "5S$",        # 5x leverage short (e.g., BTC5S)
    ])
    
    # Explicit symbol exclusions (including stablecoins)
    exclude_symbols: List[str] = field(default_factory=lambda: [
        "USDTUSDT"   # USDT perpetual against itself
    ])
    
    # Minimum order size
    min_notional: float = 10  # Min order size in USDT
    
    # Minimum price
    min_price: float = 0.0001
    
    # Maximum price (optional, None = no limit)
    max_price: float = None


def is_above_min_volume(market: Dict, min_usd: float) -> Tuple[bool, str]:
    """
    Check if market has sufficient 24h volume.
    
    Args:
        market: Market dictionary from ccxt
        min_usd: Minimum volume in USDT
        
    Returns:
        Tuple of (passes, reason)
    """
    # Try multiple sources for volume data
    volume = None
    
    # Check info field first (exchange-specific data)
    if market.get('info'):
        info = market['info']
        # Try different field names across exchanges
        volume = (info.get('vol24h') or 
                 info.get('volumeUsd') or 
                 info.get('quoteVolume') or
                 info.get('takerVol') or
                 info.get('makerVol'))
        
        if volume:
            try:
                volume = float(volume)
            except (ValueError, TypeError):
                volume = None
    
    # Check if volume data is available
    if volume is None:
        # Can't determine volume - be conservative and include it
        logger.debug(f"No volume data for {market.get('symbol')}, including anyway")
        return True, "No volume data"
    
    if volume >= min_usd:
        return True, ""
    else:
        return False, f"Volume {volume:,.0f} USDT < min {min_usd:,.0f} USDT"


def is_below_max_spread(market: Dict, max_spread: float) -> Tuple[bool, str]:
    """
    Check if market spread is acceptable.
    
    Args:
        market: Market dictionary from ccxt
        max_spread: Maximum allowed spread percentage
        
    Returns:
        Tuple of (passes, reason)
    """
    try:
        # Get current price and spread info
        bid = market.get('info', {}).get('bidPrice') or market.get('bid')
        ask = market.get('info', {}).get('askPrice') or market.get('ask')
        
        if bid is None or ask is None:
            # No spread data available - include conservatively
            return True, "No spread data"
        
        bid = float(bid)
        ask = float(ask)
        
        if bid <= 0 or ask <= 0:
            return True, "Invalid price data"
        
        spread_percent = ((ask - bid) / bid) * 100
        
        if spread_percent <= max_spread:
            return True, ""
        else:
            return False, f"Spread {spread_percent:.4f}% > max {max_spread}%"
            
    except (ValueError, TypeError, ZeroDivisionError) as e:
        logger.debug(f"Error calculating spread for {market.get('symbol')}: {e}")
        return True, "Spread calculation error"


def is_not_excluded(symbol: str, exclude_patterns: List[str], exclude_symbols: List[str]) -> Tuple[bool, str]:
    """
    Check if symbol is not in exclusion list.
    
    Args:
        symbol: Trading symbol
        exclude_patterns: List of regex patterns to exclude
        exclude_symbols: List of exact symbols to exclude
        
    Returns:
        Tuple of (passes, reason)
    """
    # Check exact exclusions first
    if symbol in exclude_symbols:
        return False, f"Explicitly excluded: {symbol}"
    
    # Check pattern exclusions
    for pattern in exclude_patterns:
        if re.search(pattern, symbol):
            return False, f"Matches exclusion pattern: {pattern}"
    
    return True, ""


def meets_notional_requirement(market: Dict, min_notional: float) -> Tuple[bool, str]:
    """
    Check if market's minimum order size is acceptable.
    
    Args:
        market: Market dictionary from ccxt
        min_notional: Maximum allowed minimum order size in USDT
        
    Returns:
        Tuple of (passes, reason)
    """
    try:
        limits = market.get('limits', {})
        cost_limit = limits.get('cost', {})
        min_cost = cost_limit.get('min')
        
        if min_cost is None:
            # No limit data - include conservatively
            return True, "No notional limit data"
        
        min_cost = float(min_cost)
        
        # Market passes if its minimum order size is not too high
        # i.e., min_cost must be <= our configured maximum minimum
        if min_cost <= min_notional:
            return True, ""
        else:
            return False, f"Min order size {min_cost} USDT exceeds maximum allowed {min_notional} USDT"
            
    except (ValueError, TypeError) as e:
        logger.debug(f"Error checking notional for {market.get('symbol')}: {e}")
        return True, "Notional check error"


def meets_price_range(market: Dict, min_price: float, max_price: float = None) -> Tuple[bool, str]:
    """
    Check if market price is within acceptable range.
    
    Args:
        market: Market dictionary from ccxt
        min_price: Minimum price
        max_price: Maximum price (None = no limit)
        
    Returns:
        Tuple of (passes, reason)
    """
    try:
        # Try to get last price
        last = market.get('info', {}).get('lastPrice') or market.get('last')
        
        if last is None:
            # No price data - include conservatively
            return True, "No price data"
        
        last = float(last)
        
        if last < min_price:
            return False, f"Price {last} < min {min_price}"
        
        if max_price is not None and last > max_price:
            return False, f"Price {last} > max {max_price}"
        
        return True, ""
        
    except (ValueError, TypeError) as e:
        logger.debug(f"Error checking price for {market.get('symbol')}: {e}")
        return True, "Price check error"


def filter_markets(
    markets: Dict[str, Dict],
    config: UniverseConfig
) -> Dict[str, Dict]:
    """
    Apply all filters to markets and return filtered dict.
    
    Args:
        markets: Dictionary of markets (symbol -> market dict)
        config: UniverseConfig with filter settings
        
    Returns:
        Filtered dictionary of markets
    """
    if not markets:
        logger.warning("No markets to filter")
        return {}
    
    logger.info(f"Filtering {len(markets)} markets with config: {config}")
    
    filtered_markets = {}
    exclusion_stats = {
        "volume": 0,
        "spread": 0,
        "excluded": 0,
        "notional": 0,
        "price": 0,
        "other": 0
    }
    
    for symbol, market in markets.items():
        # Apply each filter
        passes, reason = is_above_min_volume(market, config.min_volume_usd)
        if not passes:
            exclusion_stats["volume"] += 1
            logger.debug(f"Excluded {symbol}: {reason}")
            continue
        
        passes, reason = is_below_max_spread(market, config.max_spread_percent)
        if not passes:
            exclusion_stats["spread"] += 1
            logger.debug(f"Excluded {symbol}: {reason}")
            continue
        
        passes, reason = is_not_excluded(symbol, config.exclude_patterns, config.exclude_symbols)
        if not passes:
            exclusion_stats["excluded"] += 1
            logger.debug(f"Excluded {symbol}: {reason}")
            continue
        
        passes, reason = meets_notional_requirement(market, config.min_notional)
        if not passes:
            exclusion_stats["notional"] += 1
            logger.debug(f"Excluded {symbol}: {reason}")
            continue
        
        passes, reason = meets_price_range(market, config.min_price, config.max_price)
        if not passes:
            exclusion_stats["price"] += 1
            logger.debug(f"Excluded {symbol}: {reason}")
            continue
        
        # All filters passed
        filtered_markets[symbol] = market
    
    # Log summary
    total_excluded = sum(exclusion_stats.values())
    logger.info(f"Filtered to {len(filtered_markets)} markets (excluded {total_excluded}: {exclusion_stats})")
    
    return filtered_markets


def compare_universes(
    old_universe: Dict[str, Dict],
    new_universe: Dict[str, Dict]
) -> Dict[str, List[str]]:
    """
    Compare two universes and find differences.
    
    Args:
        old_universe: Previous universe
        new_universe: New universe
        
    Returns:
        Dictionary with 'added' and 'removed' symbol lists
    """
    old_symbols = set(old_universe.keys())
    new_symbols = set(new_universe.keys())
    
    added = sorted(new_symbols - old_symbols)
    removed = sorted(old_symbols - new_symbols)
    
    return {
        "added": added,
        "removed": removed
    }
