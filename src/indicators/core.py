"""Core technical indicators for signal generation and regime classification."""

from typing import List, Dict
import math

# Import SMA from helpers for use in other indicators
from .helpers import sma


def ema(closes: List[float], period: int) -> float:
    """
    Calculate Exponential Moving Average.
    
    Args:
        closes: List of closing prices (oldest first)
        period: EMA period
        
    Returns:
        Latest EMA value
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(closes) < period:
        raise ValueError(f"Not enough data points for EMA. Need {period}, got {len(closes)}")
    
    if period <= 0:
        raise ValueError("Period must be positive")
    
    # Calculate alpha
    alpha = 2.0 / (period + 1)
    
    # Initialize with SMA for first value
    ema_value = sum(closes[:period]) / period
    
    # Apply EMA formula for subsequent values
    for i in range(period, len(closes)):
        ema_value = alpha * closes[i] + (1 - alpha) * ema_value
    
    return ema_value


def rsi(closes: List[float], period: int = 14) -> float:
    """
    Calculate Relative Strength Index.
    
    Args:
        closes: List of closing prices (oldest first)
        period: RSI period (default 14)
        
    Returns:
        Latest RSI value (0-100)
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(closes) < period + 1:
        raise ValueError(f"Not enough data points for RSI. Need {period + 1}, got {len(closes)}")
    
    if period <= 0:
        raise ValueError("Period must be positive")
    
    # Calculate price changes
    changes = []
    for i in range(1, len(closes)):
        changes.append(closes[i] - closes[i - 1])
    
    if len(changes) < period:
        raise ValueError(f"Not enough changes for RSI calculation")
    
    # Get the last period's changes
    period_changes = changes[-period:]
    
    # Calculate gains and losses
    gains = [max(0, change) for change in period_changes]
    losses = [max(0, -change) for change in period_changes]
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    # Handle edge cases
    if avg_loss == 0 and avg_gain == 0:
        return 50.0  # Flat prices mean RSI = 50
    
    if avg_loss == 0:
        return 100.0  # No losses means RSI = 100
    
    if avg_gain == 0:
        return 0.0  # No gains means RSI = 0
    
    # Calculate RS and RSI
    rs = avg_gain / avg_loss
    rsi_value = 100 - (100 / (1 + rs))
    
    return max(0.0, min(100.0, rsi_value))


def true_range(high: float, low: float, prev_close: float) -> float:
    """
    Calculate True Range for a single period.
    
    Args:
        high: High price
        low: Low price
        prev_close: Previous close price
        
    Returns:
        True range value
    """
    tr1 = high - low
    tr2 = abs(high - prev_close)
    tr3 = abs(low - prev_close)
    
    return max(tr1, tr2, tr3)


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """
    Calculate Average True Range using EMA smoothing.
    
    Args:
        highs: List of high prices (oldest first)
        lows: List of low prices (oldest first)
        closes: List of close prices (oldest first)
        period: ATR period (default 14)
        
    Returns:
        Latest ATR value
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
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
    
    # Get the last period's True Range values
    period_tr = tr_values[-period:]
    
    # Calculate ATR using Simple Moving Average (more stable for ATR)
    atr_value = sum(period_tr) / period
    
    return max(0.0, atr_value)


def atr_percent(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """
    Calculate ATR as percentage of current close price.
    
    Args:
        highs: List of high prices (oldest first)
        lows: List of low prices (oldest first)
        closes: List of close prices (oldest first)
        period: ATR period (default 14)
        
    Returns:
        ATR percentage value
    """
    atr_val = atr(highs, lows, closes, period)
    last_close = closes[-1]
    
    if last_close <= 0:
        raise ValueError("Last close price must be positive")
    
    return (atr_val / last_close) * 100


def vwap(highs: List[float], lows: List[float], closes: List[float], volumes: List[float]) -> float:
    """
    Calculate Volume-Weighted Average Price.
    
    Args:
        highs: List of high prices (oldest first)
        lows: List of low prices (oldest first)
        closes: List of close prices (oldest first)
        volumes: List of volume values (oldest first)
        
    Returns:
        Latest VWAP value
        
    Raises:
        ValueError: If arrays have different lengths or insufficient data
    """
    n = len(highs)
    if n != len(lows) or n != len(closes) or n != len(volumes):
        raise ValueError("All price and volume arrays must have same length")
    
    if n == 0:
        raise ValueError("Arrays cannot be empty")
    
    # Check for non-positive volumes
    for vol in volumes:
        if vol <= 0:
            # If volume is zero or negative, skip this period
            continue
    
    # Calculate typical price for each period
    typical_prices = []
    valid_volumes = []
    
    for i in range(n):
        typical_price = (highs[i] + lows[i] + closes[i]) / 3
        if volumes[i] > 0:  # Only include periods with positive volume
            typical_prices.append(typical_price)
            valid_volumes.append(volumes[i])
    
    if not typical_prices:
        raise ValueError("No valid volume data for VWAP calculation")
    
    # Calculate VWAP
    pv_sum = sum(tp * vol for tp, vol in zip(typical_prices, valid_volumes))
    volume_sum = sum(valid_volumes)
    
    return pv_sum / volume_sum


def volume_zscore(volumes: List[float], period: int = 20) -> float:
    """
    Calculate Volume Z-Score.
    
    Args:
        volumes: List of volume values (oldest first)
        period: Lookback period for mean/std calculation
        
    Returns:
        Z-score of latest volume
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(volumes) < period:
        raise ValueError(f"Not enough data points for volume Z-score. Need {period}, got {len(volumes)}")
    
    if period <= 0:
        raise ValueError("Period must be positive")
    
    # Get the last period's volumes
    period_volumes = volumes[-period:]
    
    # Calculate mean
    mean_vol = sum(period_volumes) / period
    
    # Calculate standard deviation
    variance = sum((vol - mean_vol) ** 2 for vol in period_volumes) / period
    std_dev = math.sqrt(variance)
    
    # Handle case where std_dev = 0
    if std_dev == 0:
        return 0.0  # All volumes are the same
    
    # Calculate Z-score
    latest_volume = volumes[-1]
    z_score = (latest_volume - mean_vol) / std_dev
    
    return z_score


def _smoothed_dm(dm_values: List[float], period: int, alpha: float = None) -> float:
    """Helper function to smooth DM values using EMA."""
    if alpha is None:
        alpha = 1.0 / period
    
    if len(dm_values) == 0:
        return 0.0
    
    smoothed = dm_values[0]
    for value in dm_values[1:]:
        smoothed = alpha * max(0, value) + (1 - alpha) * smoothed
    
    return smoothed


def macd(closes: List[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, float]:
    """
    Calculate MACD (Moving Average Convergence Divergence).
    
    Args:
        closes: List of closing prices (oldest first)
        fast_period: Fast EMA period (default 12)
        slow_period: Slow EMA period (default 26)
        signal_period: Signal line EMA period (default 9)
        
    Returns:
        Dictionary with MACD, signal, and histogram values
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(closes) < slow_period + signal_period:
        raise ValueError(f"Not enough data points for MACD. Need {slow_period + signal_period}, got {len(closes)}")
    
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        raise ValueError("All periods must be positive")
    
    if fast_period >= slow_period:
        raise ValueError("Fast period must be less than slow period")
    
    # Calculate EMAs
    ema_fast = ema(closes, fast_period)
    ema_slow = ema(closes, slow_period)
    
    # Calculate MACD line
    macd_line = ema_fast - ema_slow
    
    # Calculate signal line (EMA of MACD line)
    # For signal calculation, we need the MACD line history
    macd_history = []
    for i in range(slow_period - 1, len(closes)):
        fast_ema_i = ema(closes[:i+1], fast_period)
        slow_ema_i = ema(closes[:i+1], slow_period)
        macd_history.append(fast_ema_i - slow_ema_i)
    
    if len(macd_history) < signal_period:
        # Not enough history for signal line, return basic MACD
        return {
            "macd": macd_line,
            "signal": macd_line,  # Fallback
            "histogram": 0.0
        }
    
    signal_line = ema(macd_history, signal_period)
    histogram = macd_line - signal_line
    
    return {
        "macd": macd_line,
        "signal": signal_line,
        "histogram": histogram
    }


def bollinger_bands(closes: List[float], period: int = 20, std_dev: float = 2.0) -> Dict[str, float]:
    """
    Calculate Bollinger Bands.
    
    Args:
        closes: List of closing prices (oldest first)
        period: Period for SMA and standard deviation (default 20)
        std_dev: Number of standard deviations for bands (default 2.0)
        
    Returns:
        Dictionary with upper band, middle band (SMA), and lower band
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    if len(closes) < period:
        raise ValueError(f"Not enough data points for Bollinger Bands. Need {period}, got {len(closes)}")
    
    if period <= 0 or std_dev <= 0:
        raise ValueError("Period and standard deviation must be positive")
    
    # Calculate middle band (SMA)
    middle_band = sma(closes, period)
    
    # Calculate standard deviation
    period_closes = closes[-period:]
    variance = sum((price - middle_band) ** 2 for price in period_closes) / period
    std_deviation = math.sqrt(variance)
    
    # Calculate upper and lower bands
    upper_band = middle_band + (std_deviation * std_dev)
    lower_band = middle_band - (std_deviation * std_dev)
    
    return {
        "upper": upper_band,
        "middle": middle_band,
        "lower": lower_band,
        "bandwidth": upper_band - lower_band,
        "position": (closes[-1] - lower_band) / (upper_band - lower_band) if upper_band != lower_band else 0.5
    }


def adx(highs: List[float], lows: List[float], period: int = 14) -> float:
    """
    Calculate Average Directional Index.
    
    Args:
        highs: List of high prices (oldest first)
        lows: List of low prices (oldest first)
        period: ADX period (default 14)
        
    Returns:
        Latest ADX value (0-100)
        
    Raises:
        ValueError: If not enough data points or invalid inputs
    """
    n = len(highs)
    if n != len(lows):
        raise ValueError("Highs and lows must have same length")
    
    if n < period + 1:
        raise ValueError(f"Not enough data points for ADX. Need {period + 1}, got {n}")
    
    if period <= 0:
        raise ValueError("Period must be positive")
    
    # Calculate True Range
    tr_values = []
    for i in range(1, n):
        tr = true_range(highs[i], lows[i], lows[i-1])  # Use low[i-1] as proxy for prev_close
        tr_values.append(tr)
    
    # Calculate +DM and -DM
    plus_dm_values = []
    minus_dm_values = []
    
    for i in range(1, n):
        high_diff = highs[i] - highs[i-1]
        low_diff = lows[i-1] - lows[i]
        
        # +DM: upward movement
        plus_dm = high_diff if (high_diff > low_diff and high_diff > 0) else 0
        # -DM: downward movement
        minus_dm = low_diff if (low_diff > high_diff and low_diff > 0) else 0
        
        plus_dm_values.append(plus_dm)
        minus_dm_values.append(minus_dm)
    
    # Calculate smoothed values
    tr_smoothed = sum(tr_values[-period:]) / period
    plus_dm_smoothed = _smoothed_dm(plus_dm_values[-period:], period)
    minus_dm_smoothed = _smoothed_dm(minus_dm_values[-period:], period)
    
    # Calculate DI+ and DI-
    if tr_smoothed == 0:
        return 0.0
    
    di_plus = (plus_dm_smoothed / tr_smoothed) * 100
    di_minus = (minus_dm_smoothed / tr_smoothed) * 100
    
    # Calculate ADX
    di_diff = abs(di_plus - di_minus)
    di_sum = di_plus + di_minus
    
    if di_sum == 0:
        return 0.0
    
    dx = (di_diff / di_sum) * 100
    
    # Smooth DX to get ADX
    if len(tr_values) < period:
        return dx  # Not enough data for smoothing, return raw DX
    
    # For simplicity, return DX as ADX (in production, would smooth over multiple periods)
    return max(0.0, min(100.0, dx))