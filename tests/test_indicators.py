"""Unit tests for technical indicators."""

import json
import math
import pytest
from pathlib import Path

from src.indicators import (
    ema,
    rsi,
    atr,
    atr_percent,
    vwap,
    volume_zscore,
    adx,
    true_range,
    sma,
    atr_smoothed_variant
)


class TestEMA:
    """Test EMA (Exponential Moving Average) indicator."""
    
    def test_ema_basic(self):
        """Test basic EMA calculation."""
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = ema(closes, 2)
        expected = 4.5  # Manually calculated
        assert abs(result - expected) < 0.01
    
    def test_ema_insufficient_data(self):
        """Test EMA with insufficient data."""
        closes = [1.0, 2.0]
        with pytest.raises(ValueError):
            ema(closes, 5)
    
    def test_ema_constant_prices(self):
        """Test EMA with constant prices."""
        closes = [5.0] * 10
        result = ema(closes, 5)
        assert result == 5.0
    
    def test_ema_single_period(self):
        """Test EMA with period = 1."""
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = ema(closes, 1)
        assert result == 5.0  # Should equal last close


class TestRSI:
    """Test RSI (Relative Strength Index) indicator."""
    
    def test_rsi_basic(self):
        """Test basic RSI calculation."""
        # Create data that should give RSI around 50
        closes = [100.0] * 15
        result = rsi(closes, 14)
        assert abs(result - 50.0) < 1.0  # Should be close to 50 for flat prices
    
    def test_rsi_rising(self):
        """Test RSI with rising prices."""
        closes = [100.0 + i for i in range(20)]  # Rising from 100 to 119
        result = rsi(closes, 14)
        assert result > 50.0  # Should be bullish
    
    def test_rsi_falling(self):
        """Test RSI with falling prices."""
        closes = [120.0 - i for i in range(20)]  # Falling from 120 to 101
        result = rsi(closes, 14)
        assert result < 50.0  # Should be bearish
    
    def test_rsi_all_gains(self):
        """Test RSI with only gains."""
        closes = [100.0 + i * 0.1 for i in range(20)]  # Strictly increasing
        result = rsi(closes, 14)
        assert result > 80.0  # Should be overbought
    
    def test_rsi_all_losses(self):
        """Test RSI with only losses."""
        closes = [100.0 - i * 0.1 for i in range(20)]  # Strictly decreasing
        result = rsi(closes, 14)
        assert result < 20.0  # Should be oversold
    
    def test_rsi_insufficient_data(self):
        """Test RSI with insufficient data."""
        closes = [100.0] * 10
        with pytest.raises(ValueError):
            rsi(closes, 14)  # Need 15 points for 14-period RSI
    
    def test_rsi_boundary_values(self):
        """Test RSI returns values in valid range."""
        # Test with extreme cases
        closes = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0, 
                 200.0, 210.0, 220.0, 230.0, 240.0, 250.0, 260.0, 270.0, 280.0, 290.0]
        result = rsi(closes, 14)
        assert 0.0 <= result <= 100.0


class TestATR:
    """Test ATR (Average True Range) indicator."""
    
    def test_tr_basic(self):
        """Test basic True Range calculation."""
        tr = true_range(105.0, 95.0, 100.0)
        assert tr == 10.0  # High - Low
        
        tr = true_range(105.0, 103.0, 100.0)
        assert tr == 5.0  # High - Prev Close
        
        tr = true_range(98.0, 95.0, 100.0)
        assert tr == 5.0  # Low - Prev Close
    
    def test_atr_basic(self):
        """Test basic ATR calculation."""
        highs = [102.0, 103.0, 104.0]
        lows = [98.0, 99.0, 100.0]
        closes = [100.0, 101.0, 102.0]
        
        result = atr(highs, lows, closes, 2)
        assert result > 0.0
    
    def test_atr_insufficient_data(self):
        """Test ATR with insufficient data."""
        highs = [102.0]
        lows = [98.0]
        closes = [100.0]
        
        with pytest.raises(ValueError):
            atr(highs, lows, closes, 5)
    
    def test_atr_constant_prices(self):
        """Test ATR with constant prices."""
        highs = [100.0] * 10
        lows = [99.0] * 10
        closes = [99.5] * 10
        
        result = atr(highs, lows, closes, 5)
        assert result == 1.0  # High - Low = 1.0
    
    def test_atr_percent_basic(self):
        """Test ATR percentage calculation."""
        highs = [102.0, 103.0, 104.0]
        lows = [98.0, 99.0, 100.0]
        closes = [100.0, 101.0, 102.0]
        
        result = atr_percent(highs, lows, closes, 2)
        assert result > 0.0
        assert isinstance(result, float)


class TestVWAP:
    """Test VWAP (Volume-Weighted Average Price) indicator."""
    
    def test_vwap_basic(self):
        """Test basic VWAP calculation."""
        highs = [100.0, 101.0, 102.0]
        lows = [99.0, 100.0, 101.0]
        closes = [99.5, 100.5, 101.5]
        volumes = [1000.0, 2000.0, 1500.0]
        
        result = vwap(highs, lows, closes, volumes)
        assert result > 0.0
        
        # Should be weighted average of typical prices
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
        # VWAP should be between min and max of typical prices
        assert min(typical_prices) <= result <= max(typical_prices)
    
    def test_vwap_different_lengths(self):
        """Test VWAP with mismatched array lengths."""
        highs = [100.0, 101.0]
        lows = [99.0, 100.0]
        closes = [99.5, 100.5]
        volumes = [1000.0]  # Wrong length
        
        with pytest.raises(ValueError):
            vwap(highs, lows, closes, volumes)
    
    def test_vwap_zero_volume(self):
        """Test VWAP with zero volumes."""
        highs = [100.0, 101.0]
        lows = [99.0, 100.0]
        closes = [99.5, 100.5]
        volumes = [0.0, 1000.0]
        
        result = vwap(highs, lows, closes, volumes)
        assert result == 100.5  # Should ignore zero volume periods


class TestVolumeZScore:
    """Test Volume Z-Score indicator."""
    
    def test_volume_zscore_basic(self):
        """Test basic volume Z-score calculation."""
        volumes = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0,
                  200.0, 210.0, 220.0, 230.0, 240.0, 250.0, 260.0, 270.0, 280.0, 300.0]
        
        result = volume_zscore(volumes, 20)
        assert isinstance(result, float)
        # Latest volume (300) should be significantly above mean
        assert result > 0.0
    
    def test_volume_zscore_insufficient_data(self):
        """Test volume Z-score with insufficient data."""
        volumes = [100.0, 110.0, 120.0]
        
        with pytest.raises(ValueError):
            volume_zscore(volumes, 10)
    
    def test_volume_zscore_constant_volume(self):
        """Test volume Z-score with constant volumes."""
        volumes = [100.0] * 20
        
        result = volume_zscore(volumes, 20)
        assert result == 0.0  # Z-score should be 0 when all volumes are same
    
    def test_volume_zscore_extreme_values(self):
        """Test volume Z-score with extreme outlier."""
        volumes = [100.0] * 19 + [1000.0]  # Last volume is 10x normal
        
        result = volume_zscore(volumes, 20)
        assert result > 2.0  # Should be significant outlier


class TestADX:
    """Test ADX (Average Directional Index) indicator."""
    
    def test_adx_basic(self):
        """Test basic ADX calculation."""
        highs = [100.0, 101.0, 102.0, 103.0, 104.0]
        lows = [99.0, 100.0, 101.0, 102.0, 103.0]
        
        result = adx(highs, lows, 3)
        assert 0.0 <= result <= 100.0
    
    def test_adx_insufficient_data(self):
        """Test ADX with insufficient data."""
        highs = [100.0, 101.0]
        lows = [99.0, 100.0]
        
        with pytest.raises(ValueError):
            adx(highs, lows, 5)
    
    def test_adx_constant_prices(self):
        """Test ADX with flat prices."""
        highs = [100.0] * 10
        lows = [99.0] * 10
        
        result = adx(highs, lows, 5)
        assert result == 0.0  # No directional movement
    
    def test_adx_strong_trend(self):
        """Test ADX with strong uptrend."""
        highs = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0]
        lows = [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0]
        
        result = adx(highs, lows, 5)
        assert result > 0.0  # Should show directional movement


class TestSMA:
    """Test Simple Moving Average helper."""
    
    def test_sma_basic(self):
        """Test basic SMA calculation."""
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = sma(closes, 3)
        expected = (3.0 + 4.0 + 5.0) / 3  # Last 3 values
        assert result == expected
    
    def test_sma_insufficient_data(self):
        """Test SMA with insufficient data."""
        closes = [1.0, 2.0]
        with pytest.raises(ValueError):
            sma(closes, 5)


class TestATRVariants:
    """Test ATR smoothing variants."""
    
    def test_atr_smoothed_variant_basic(self):
        """Test ATR with EMA smoothing."""
        highs = [102.0, 103.0, 104.0]
        lows = [98.0, 99.0, 100.0]
        closes = [100.0, 101.0, 102.0]
        
        result = atr_smoothed_variant(highs, lows, closes, 2)
        assert result > 0.0


class TestRealData:
    """Test indicators with realistic OHLCV data."""
    
    def load_sample_data(self):
        """Load sample OHLCV data from fixture."""
        fixture_path = Path(__file__).parent / "fixtures" / "sample_ohlcv.json"
        with open(fixture_path, 'r') as f:
            data = json.load(f)
        
        ohlcv = data["data"]
        highs = [candle["high"] for candle in ohlcv]
        lows = [candle["low"] for candle in ohlcv]
        closes = [candle["close"] for candle in ohlcv]
        volumes = [candle["volume"] for candle in ohlcv]
        
        return highs, lows, closes, volumes
    
    def test_all_indicators_with_real_data(self):
        """Test all indicators with realistic data."""
        highs, lows, closes, volumes = self.load_sample_data()
        
        # Test each indicator
        ema_val = ema(closes, 14)
        assert isinstance(ema_val, float)
        
        rsi_val = rsi(closes, 14)
        assert 0.0 <= rsi_val <= 100.0
        
        atr_val = atr(highs, lows, closes, 14)
        assert atr_val > 0.0
        
        atr_pct_val = atr_percent(highs, lows, closes, 14)
        assert atr_pct_val > 0.0
        
        vwap_val = vwap(highs, lows, closes, volumes)
        assert vwap_val > 0.0
        
        vol_zscore = volume_zscore(volumes, 20)
        assert isinstance(vol_zscore, float)
        
        adx_val = adx(highs, lows, 14)
        assert 0.0 <= adx_val <= 100.0
    
    def test_indicator_consistency(self):
        """Test indicator consistency with flat prices."""
        # Use flat price data
        highs = [100.0] * 25
        lows = [99.0] * 25
        closes = [99.5] * 25
        volumes = [1000.0] * 25
        
        # RSI should be 50 for flat prices
        rsi_val = rsi(closes, 14)
        assert abs(rsi_val - 50.0) < 1.0
        
        # ATR should be 1.0 (high - low)
        atr_val = atr(highs, lows, closes, 14)
        assert atr_val == 1.0
        
        # Volume Z-score should be 0
        vol_zscore = volume_zscore(volumes, 20)
        assert abs(vol_zscore) < 0.01
        
        # ADX should be 0 for no trend
        adx_val = adx(highs, lows, 14)
        assert adx_val == 0.0