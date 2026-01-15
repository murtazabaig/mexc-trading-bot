"""Tests for warning detector functionality."""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, MagicMock
import numpy as np

from src.warnings.detector import WarningDetector


@pytest.fixture
def mock_exchange():
    """Create a mock MEXC exchange."""
    exchange = Mock()
    exchange.fetch_ohlcv = AsyncMock()
    return exchange


@pytest.fixture
def mock_db_conn():
    """Create a mock database connection."""
    return Mock()


@pytest.fixture
def mock_config():
    """Create a mock configuration."""
    return {
        'btc_shock_threshold_warning': 0.05,
        'btc_shock_threshold_critical': 0.08,
        'breadth_collapse_threshold_warning': 0.40,
        'breadth_collapse_threshold_critical': 0.50,
        'correlation_spike_threshold_warning': 0.30,
        'correlation_spike_threshold_critical': 0.50
    }


@pytest.fixture
def mock_universe():
    """Create a mock market universe."""
    return {
        'BTC/USDT:USDT': {'symbol': 'BTC/USDT:USDT'},
        'ETH/USDT:USDT': {'symbol': 'ETH/USDT:USDT'},
        'SOL/USDT:USDT': {'symbol': 'SOL/USDT:USDT'},
        'ADA/USDT:USDT': {'symbol': 'ADA/USDT:USDT'},
        'DOT/USDT:USDT': {'symbol': 'DOT/USDT:USDT'}
    }


@pytest.fixture
def warning_detector(mock_exchange, mock_db_conn, mock_config, mock_universe):
    """Create a WarningDetector instance."""
    detector = WarningDetector(
        exchange=mock_exchange,
        db_conn=mock_db_conn,
        config=mock_config,
        universe=mock_universe
    )
    return detector


class TestBTCShockDetection:
    """Test BTC shock detection functionality."""
    
    async def test_btc_shock_warning_threshold(self, warning_detector, mock_exchange):
        """Test BTC shock detection at warning threshold (5%)."""
        # Mock BTC data with 5.1% increase
        mock_exchange.fetch_ohlcv.return_value = [
            [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],  # Previous candle
            [1710003600000, 50200.0, 52800.0, 50100.0, 52750.0, 150.0]   # Current candle (+5.1%)
        ]
        
        warning = await warning_detector.detect_btc_shock()
        
        assert warning is not None
        assert warning['type'] == 'BTC_SHOCK'
        assert warning['severity'] == 'WARNING'
        assert abs(warning['price_change_pct'] - 0.0509) < 0.001  # ~5.09% change
        assert warning['direction'] == 'up'
    
    async def test_btc_shock_critical_threshold(self, warning_detector, mock_exchange):
        """Test BTC shock detection at critical threshold (8%)."""
        # Mock BTC data with 8.5% decrease
        mock_exchange.fetch_ohlcv.return_value = [
            [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],  # Previous candle
            [1710003600000, 50200.0, 50300.0, 45800.0, 45900.0, 200.0]   # Current candle (-8.5%)
        ]
        
        warning = await warning_detector.detect_btc_shock()
        
        assert warning is not None
        assert warning['type'] == 'BTC_SHOCK'
        assert warning['severity'] == 'CRITICAL'
        assert abs(warning['price_change_pct'] - 0.0856) < 0.001  # ~8.56% change
        assert warning['direction'] == 'down'
    
    async def test_btc_shock_no_warning(self, warning_detector, mock_exchange):
        """Test BTC shock detection below threshold."""
        # Mock BTC data with 2% increase (below threshold)
        mock_exchange.fetch_ohlcv.return_value = [
            [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],  # Previous candle
            [1710003600000, 50200.0, 51200.0, 50100.0, 51000.0, 120.0]   # Current candle (+2%)
        ]
        
        warning = await warning_detector.detect_btc_shock()
        
        assert warning is None


class TestBreadthCollapseDetection:
    """Test breadth collapse detection functionality."""
    
    async def test_breadth_collapse_warning_threshold(self, warning_detector, mock_exchange, mock_universe):
        """Test breadth collapse detection at warning threshold (40%)."""
        # Mock BTC direction as bullish
        mock_exchange.fetch_ohlcv.side_effect = [
            # BTC data (bullish)
            [
                [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],
                [1710003600000, 50200.0, 51200.0, 50100.0, 51000.0, 150.0]
            ],
            # ETH data (bearish - against trend)
            [
                [1710000000000, 3000.0, 3050.0, 2990.0, 3020.0, 50.0],
                [1710003600000, 3020.0, 3030.0, 2950.0, 2960.0, 60.0]
            ],
            # SOL data (bearish - against trend)
            [
                [1710000000000, 100.0, 105.0, 99.0, 102.0, 20.0],
                [1710003600000, 102.0, 103.0, 98.0, 99.0, 25.0]
            ],
            # ADA data (bullish - with trend)
            [
                [1710000000000, 0.5, 0.52, 0.49, 0.51, 10.0],
                [1710003600000, 0.51, 0.53, 0.50, 0.52, 12.0]
            ],
            # DOT data (bullish - with trend)
            [
                [1710000000000, 8.0, 8.2, 7.9, 8.1, 5.0],
                [1710003600000, 8.1, 8.3, 8.0, 8.2, 6.0]
            ]
        ]
        
        warning = await warning_detector.detect_breadth_collapse(list(mock_universe.keys()))
        
        assert warning is not None
        assert warning['type'] == 'BREADTH_COLLAPSE'
        assert warning['severity'] == 'WARNING'
        assert warning['bullish_count'] == 2  # ADA, DOT
        assert warning['bearish_count'] == 2  # ETH, SOL
        assert abs(warning['pct_against_trend'] - 0.5) < 0.01  # 50% against trend
    
    async def test_breadth_collapse_critical_threshold(self, warning_detector, mock_exchange, mock_universe):
        """Test breadth collapse detection at critical threshold (50%)."""
        # Mock BTC direction as bearish
        mock_exchange.fetch_ohlcv.side_effect = [
            # BTC data (bearish)
            [
                [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],
                [1710003600000, 50200.0, 50300.0, 48000.0, 48200.0, 200.0]
            ],
            # ETH data (bullish - against trend)
            [
                [1710000000000, 3000.0, 3050.0, 2990.0, 3020.0, 50.0],
                [1710003600000, 3020.0, 3100.0, 3010.0, 3080.0, 60.0]
            ],
            # SOL data (bullish - against trend)
            [
                [1710000000000, 100.0, 105.0, 99.0, 102.0, 20.0],
                [1710003600000, 102.0, 108.0, 101.0, 106.0, 25.0]
            ],
            # ADA data (bullish - against trend)
            [
                [1710000000000, 0.5, 0.52, 0.49, 0.51, 10.0],
                [1710003600000, 0.51, 0.54, 0.50, 0.53, 12.0]
            ],
            # DOT data (bearish - with trend)
            [
                [1710000000000, 8.0, 8.2, 7.9, 8.1, 5.0],
                [1710003600000, 8.1, 8.2, 7.8, 7.9, 6.0]
            ]
        ]
        
        warning = await warning_detector.detect_breadth_collapse(list(mock_universe.keys()))
        
        assert warning is not None
        assert warning['type'] == 'BREADTH_COLLAPSE'
        assert warning['severity'] == 'CRITICAL'
        assert warning['bullish_count'] == 3  # ETH, SOL, ADA
        assert warning['bearish_count'] == 1  # DOT
        assert abs(warning['pct_against_trend'] - 0.75) < 0.01  # 75% against trend
    
    async def test_breadth_collapse_no_warning(self, warning_detector, mock_exchange, mock_universe):
        """Test breadth collapse detection below threshold."""
        # Mock BTC direction as bullish
        mock_exchange.fetch_ohlcv.side_effect = [
            # BTC data (bullish)
            [
                [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],
                [1710003600000, 50200.0, 51200.0, 50100.0, 51000.0, 150.0]
            ],
            # ETH data (bullish - with trend)
            [
                [1710000000000, 3000.0, 3050.0, 2990.0, 3020.0, 50.0],
                [1710003600000, 3020.0, 3100.0, 3010.0, 3080.0, 60.0]
            ],
            # SOL data (bullish - with trend)
            [
                [1710000000000, 100.0, 105.0, 99.0, 102.0, 20.0],
                [1710003600000, 102.0, 108.0, 101.0, 106.0, 25.0]
            ],
            # ADA data (bullish - with trend)
            [
                [1710000000000, 0.5, 0.52, 0.49, 0.51, 10.0],
                [1710003600000, 0.51, 0.54, 0.50, 0.53, 12.0]
            ],
            # DOT data (bearish - against trend)
            [
                [1710000000000, 8.0, 8.2, 7.9, 8.1, 5.0],
                [1710003600000, 8.1, 8.2, 7.8, 7.9, 6.0]
            ]
        ]
        
        warning = await warning_detector.detect_breadth_collapse(list(mock_universe.keys()))
        
        assert warning is None  # Only 20% against trend, below threshold


class TestCorrelationSpikeDetection:
    """Test correlation spike detection functionality."""
    
    async def test_correlation_spike_warning_threshold(self, warning_detector, mock_exchange):
        """Test correlation spike detection at warning threshold (30%)."""
        # Mock BTC prices
        btc_prices = list(range(50000, 50048, 10))  # 48 hours of BTC prices
        
        # Mock symbol prices with correlation change
        # First 24 hours: high correlation with BTC
        symbol_prices_1 = [price * 0.02 + np.random.normal(0, 5) for price in btc_prices[:24]]
        # Last 24 hours: low correlation with BTC (spike)
        symbol_prices_2 = [price * 0.001 + np.random.normal(0, 50) for price in btc_prices[24:]]
        symbol_prices = symbol_prices_1 + symbol_prices_2
        
        mock_exchange.fetch_ohlcv.side_effect = [
            # BTC data (48 candles)
            [[i*1000, p, p+100, p-100, p, 10] for i, p in enumerate(btc_prices)],
            # Symbol data (48 candles)
            [[i*1000, p, p+5, p-5, p, 1] for i, p in enumerate(symbol_prices)]
        ]
        
        warnings = await warning_detector.detect_correlation_spike(['ETH/USDT:USDT'])
        
        # Should detect correlation spike
        assert len(warnings) == 1
        warning = warnings[0]
        assert warning['type'] == 'CORRELATION_SPIKE'
        assert warning['severity'] in ['WARNING', 'CRITICAL']
        assert warning['symbol'] == 'ETH/USDT:USDT'
        assert warning['correlation_change_pct'] > 0.30  # >30% change
    
    async def test_correlation_spike_no_warning(self, warning_detector, mock_exchange):
        """Test correlation spike detection below threshold."""
        # Mock BTC prices
        btc_prices = list(range(50000, 50048, 10))  # 48 hours of BTC prices
        
        # Mock symbol prices with stable correlation
        symbol_prices = [price * 0.02 + np.random.normal(0, 5) for price in btc_prices]
        
        mock_exchange.fetch_ohlcv.side_effect = [
            # BTC data (48 candles)
            [[i*1000, p, p+100, p-100, p, 10] for i, p in enumerate(btc_prices)],
            # Symbol data (48 candles)
            [[i*1000, p, p+5, p-5, p, 1] for i, p in enumerate(symbol_prices)]
        ]
        
        warnings = await warning_detector.detect_correlation_spike(['ETH/USDT:USDT'])
        
        # Should not detect correlation spike (stable correlation)
        assert len(warnings) == 0


class TestWarningHandling:
    """Test warning handling functionality."""
    
    async def test_warning_storage(self, warning_detector, mock_db_conn):
        """Test warning storage in database."""
        # Create a test warning
        test_warning = {
            'type': 'BTC_SHOCK',
            'severity': 'WARNING',
            'price_change_pct': 0.055,
            'direction': 'up',
            'current_price': 52750.0,
            'previous_price': 50200.0,
            'timestamp': datetime.utcnow().isoformat(),
            'message': 'BTC price up by 5.08% in 1 hour',
            'triggered_value': 0.055,
            'threshold': 0.05,
            'action_taken': 'MONITORING'
        }
        
        # Mock the database insert
        mock_db_conn.execute.return_value.lastrowid = 123
        
        # Test warning storage
        warning_id = await warning_detector._store_warning_in_database(test_warning)
        
        assert warning_id == 123
        mock_db_conn.execute.assert_called()
    
    async def test_warning_telegram_integration(self, warning_detector, mock_db_conn):
        """Test Telegram integration for warnings."""
        # Create a test warning
        test_warning = {
            'type': 'BTC_SHOCK',
            'severity': 'CRITICAL',
            'price_change_pct': 0.085,
            'direction': 'down',
            'current_price': 45900.0,
            'previous_price': 50200.0,
            'timestamp': datetime.utcnow().isoformat(),
            'message': 'BTC price down by 8.56% in 1 hour',
            'triggered_value': 0.085,
            'threshold': 0.08,
            'action_taken': 'MONITORING'
        }
        
        # Mock Telegram bot
        mock_telegram_bot = Mock()
        mock_telegram_bot.send_warning = AsyncMock(return_value=True)
        warning_detector.set_telegram_bot(mock_telegram_bot)
        
        # Mock database insert
        mock_db_conn.execute.return_value.lastrowid = 124
        
        # Test warning handling
        await warning_detector._handle_warning(test_warning)
        
        # Verify database storage was called
        mock_db_conn.execute.assert_called()
        
        # Verify Telegram notification was sent
        mock_telegram_bot.send_warning.assert_called_once()


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    async def test_empty_universe(self, warning_detector, mock_exchange):
        """Test warning detection with empty universe."""
        # Set empty universe
        warning_detector.universe = {}
        
        # Should handle gracefully
        await warning_detector._check_all_warnings()
        
        # No warnings should be generated
        assert warning_detector.stats['warnings_generated'] == 0
    
    async def test_api_error_handling(self, warning_detector, mock_exchange):
        """Test API error handling."""
        # Mock API error
        mock_exchange.fetch_ohlcv.side_effect = Exception("API Error")
        
        # Should handle API errors gracefully
        warning = await warning_detector.detect_btc_shock()
        
        assert warning is None
        assert warning_detector.stats['errors_count'] > 0
    
    async def test_insufficient_data(self, warning_detector, mock_exchange):
        """Test handling of insufficient data."""
        # Mock insufficient data
        mock_exchange.fetch_ohlcv.return_value = [
            [1710003600000, 50200.0, 51200.0, 50100.0, 51000.0, 150.0]  # Only 1 candle
        ]
        
        # Should handle insufficient data gracefully
        warning = await warning_detector.detect_btc_shock()
        
        assert warning is None