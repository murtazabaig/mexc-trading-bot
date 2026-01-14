"""Helper functions for technical indicators."""

from typing import List


def sma(closes: List[float], period: int) -> float:
    """
    Calculate Simple Moving Average.
    
    Args:
        closes: List of closing prices (oldest first)
        period: MA period
        
    Returns:
        Latest SMA value
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(closes) < period:
        raise ValueError(f"Not enough data points for SMA. Need {period}, got {len(closes)}")
    
    if period <= 0:
        raise ValueError("Period must be positive")
    
    # Calculate SMA for the last period
    period_closes = closes[-period:]
    sma_value = sum(period_closes) / period
    
    return sma_value


def atr_smoothed_variant(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """
    Calculate ATR using EMA smoothing variant.
    
    This is a more responsive version of ATR that uses EMA instead of SMA
    for smoothing the True Range values.
    
    Args:
        highs: List of high prices (oldest first)
        lows: List of low prices (oldest first)
        closes: List of close prices (oldest first)
        period: ATR period (default 14)
        
    Returns:
        Latest ATR value using EMA smoothing
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    from .core import true_range
    
    n = len(highs)
    if n != len(lows) or n != len(closes):
        raise ValueError("Highs, lows, and closes must have same length")
    
    if n < period + 1:
        raise ValueError(f"Not enough data points for ATR. Need {period + 1}, got {n}")
    
    # Calculate True Range values
    tr_values = []
    for i in range(1, n):
        tr = true_range(highs[i], lows[i], closes[i - 1])
        tr_values.append(tr)
    
    if len(tr_values) < period:
        raise ValueError(f"Not enough True Range values for ATR calculation")
    
    # Calculate ATR using EMA smoothing
    alpha = 2.0 / (period + 1)
    
    # Initialize with SMA for first value
    atr_value = sum(tr_values[-period:]) / period
    
    # Apply EMA formula for subsequent values (if we had more data)
    for tr in tr_values[-period+1:]:
        atr_value = alpha * tr + (1 - alpha) * atr_value
    
    return max(0.0, atr_value)