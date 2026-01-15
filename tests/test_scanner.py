"""Tests for scanner job functionality."""

import pytest
import asyncio
import sqlite3
import tempfile
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
import json

from src.jobs.scanner import ScannerJob, OHLCVCache, create_scanner_job
from src.database import init_db, create_schema, query_recent_signals
from src.regime import RegimeClassifier
from src.scoring import ScoringEngine


class TestOHLCVCache:
    """Test OHLCV cache functionality."""
    
    def test_cache_initialization(self):
        """Test cache initialization."""
        cache = OHLCVCache(max_size=50)
        assert cache.max_size == 50
        assert len(cache.data) == 0
        assert len(cache.timestamps) == 0
    
    def test_add_and_retrieve_data(self):
        """Test adding and retrieving OHLCV data."""
        cache = OHLCVCache()
        
        # Test data
        test_data = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3],
            [1641002400000, 47600, 48000, 47500, 47900, 1420.7]
        ]
        
        # Add data
        cache.add_data("BTCUSDT", test_data)
        
        # Verify data was stored
        assert "BTCUSDT" in cache.data
        assert len(cache.data["BTCUSDT"]) == 3
        
        # Get arrays
        ohlcv_arrays = cache.get_ohlcv_arrays("BTCUSDT")
        assert ohlcv_arrays is not None
        assert len(ohlcv_arrays["closes"]) == 3
        assert ohlcv_arrays["closes"] == [47200, 47600, 47900]
    
    def test_get_latest_price(self):
        """Test getting latest price."""
        cache = OHLCVCache()
        test_data = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3]
        ]
        
        cache.add_data("BTCUSDT", test_data)
        latest_price = cache.get_latest_price("BTCUSDT")
        
        assert latest_price == 47600
    
    def test_fresh_data_check(self):
        """Test fresh data checking."""
        cache = OHLCVCache()
        
        # Old data (more than 120 minutes ago)
        old_data = [
            [(datetime.utcnow() - timedelta(hours=3)).timestamp() * 1000, 47000, 47500, 46800, 47200, 1250.5]
        ]
        cache.add_data("OLD", old_data)
        
        # Fresh data
        fresh_data = [
            [datetime.utcnow().timestamp() * 1000, 47000, 47500, 46800, 47200, 1250.5]
        ]
        cache.add_data("FRESH", fresh_data)
        
        assert not cache.has_fresh_data("OLD")
        assert cache.has_fresh_data("FRESH")


class TestScannerJob:
    """Test ScannerJob functionality."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        conn = init_db(db_path)
        create_schema(conn)
        
        yield conn
        
        conn.close()
        import os
        os.unlink(db_path)
    
    @pytest.fixture
    def mock_exchange(self):
        """Create mock MEXC exchange."""
        exchange = Mock()
        exchange.fetch_ohlcv = AsyncMock()
        return exchange
    
    @pytest.fixture
    def test_universe(self):
        """Create test market universe."""
        return {
            "BTCUSDT": {"symbol": "BTC/USDT", "active": True},
            "ETHUSDT": {"symbol": "ETH/USDT", "active": True},
            "ADAUSDT": {"symbol": "ADA/USDT", "active": True}
        }
    
    @pytest.fixture
    def test_config(self):
        """Create test configuration."""
        return {
            "scanner": {
                "min_score": 7.0,
                "max_score": 10.0
            }
        }
    
    @pytest.fixture
    def scanner_job(self, mock_exchange, temp_db, test_config, test_universe):
        """Create scanner job instance for testing."""
        job = ScannerJob(
            exchange=mock_exchange,
            db_conn=temp_db,
            config=test_config,
            universe=test_universe
        )
        return job
    
    def test_scanner_initialization(self, scanner_job, test_universe):
        """Test scanner job initialization."""
        assert scanner_job.exchange is not None
        assert scanner_job.db_conn is not None
        assert scanner_job.universe == test_universe
        assert scanner_job.regime_classifier is not None
        assert scanner_job.scoring_engine is not None
        assert not scanner_job.running
        assert scanner_job.stats['start_time'] is None
    
    def test_ohlcv_data_processing(self, scanner_job):
        """Test OHLCV data processing."""
        # Mock OHLCV data
        ohlcv_data = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3],
            [1641002400000, 47600, 48000, 47500, 47900, 1420.7]
        ]
        
        # Add to cache
        scanner_job.cache.add_data("BTCUSDT", ohlcv_data)
        
        # Get processed data
        processed = scanner_job.cache.get_ohlcv_arrays("BTCUSDT")
        
        assert processed is not None
        assert len(processed["closes"]) == 3
        assert processed["closes"] == [47200, 47600, 47900]
        assert len(processed["highs"]) == 3
        assert len(processed["lows"]) == 3
        assert len(processed["volumes"]) == 3
    
    @pytest.mark.asyncio
    async def test_fetch_ohlcv_data_success(self, scanner_job, mock_exchange):
        """Test successful OHLCV data fetching."""
        # Mock successful API response
        mock_ohlcv = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3],
        ]
        mock_exchange.fetch_ohlcv.return_value = mock_ohlcv
        
        result = await scanner_job._fetch_ohlcv_data("BTCUSDT")
        
        assert result is not None
        assert len(result) == 2
        mock_exchange.fetch_ohlcv.assert_called_once_with("BTCUSDT", "1h", limit=100)
    
    @pytest.mark.asyncio
    async def test_fetch_ohlcv_data_network_error(self, scanner_job, mock_exchange):
        """Test OHLCV data fetching with network error."""
        import ccxt
        mock_exchange.fetch_ohlcv.side_effect = ccxt.NetworkError("Connection failed")
        
        result = await scanner_job._fetch_ohlcv_data("BTCUSDT")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_fetch_ohlcv_data_rate_limit(self, scanner_job, mock_exchange):
        """Test OHLCV data fetching with rate limit error."""
        import ccxt
        mock_exchange.fetch_ohlcv.side_effect = ccxt.RateLimitExceeded("Rate limit exceeded")
        
        result = await scanner_job._fetch_ohlcv_data("BTCUSDT")
        
        assert result is None
    
    def test_calculate_indicators_success(self, scanner_job):
        """Test successful indicator calculation."""
        # Create test OHLCV data with enough candles
        ohlcv_data = {
            "closes": [47000 + i * 100 for i in range(50)],  # 50 candles
            "highs": [47050 + i * 100 for i in range(50)],
            "lows": [46950 + i * 100 for i in range(50)],
            "volumes": [1000 + i * 10 for i in range(50)]
        }
        
        indicators = scanner_job._calculate_indicators(ohlcv_data)
        
        assert indicators is not None
        assert "rsi" in indicators
        assert "ema" in indicators
        assert "macd" in indicators
        assert "bollinger_bands" in indicators
        assert "atr" in indicators
        assert "atr_percent" in indicators
        assert "vwap" in indicators
        assert "volume_zscore" in indicators
        assert "adx" in indicators
        
        # Check values are reasonable
        assert 0 <= indicators["rsi"]["value"] <= 100
        assert indicators["ema"]["20"] > 0
        assert indicators["ema"]["50"] > 0
        assert indicators["atr"]["14"] >= 0
        assert indicators["atr_percent"]["14"]["14"] >= 0
    
    def test_calculate_indicators_insufficient_data(self, scanner_job):
        """Test indicator calculation with insufficient data."""
        ohlcv_data = {
            "closes": [47000, 47100, 47200],  # Only 3 candles
            "highs": [47050, 47150, 47250],
            "lows": [46950, 47050, 47150],
            "volumes": [1000, 1100, 1200]
        }
        
        indicators = scanner_job._calculate_indicators(ohlcv_data)
        
        assert indicators is None  # Should return None for insufficient data
    
    @pytest.mark.asyncio
    async def test_process_symbol_success(self, scanner_job, mock_exchange):
        """Test successful symbol processing."""
        # Mock OHLCV data
        mock_ohlcv = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3],
        ] * 25  # 50 candles total
        mock_exchange.fetch_ohlcv.return_value = mock_ohlcv
        
        result = await scanner_job._process_symbol("BTCUSDT")
        
        assert result is not None
        assert "symbol" in result
        assert result["symbol"] == "BTCUSDT"
    
    @pytest.mark.asyncio
    async def test_process_symbol_api_error(self, scanner_job, mock_exchange):
        """Test symbol processing with API error."""
        mock_exchange.fetch_ohlcv.side_effect = Exception("API error")
        
        result = await scanner_job._process_symbol("BTCUSDT")
        
        assert result is not None
        assert "error" in result
        assert result["symbol"] == "BTCUSDT"
    
    @pytest.mark.asyncio
    async def test_create_signal_record_success(self, scanner_job, temp_db):
        """Test successful signal record creation."""
        # Update scanner job to use test database
        scanner_job.db_conn = temp_db
        
        # Mock data
        ohlcv_data = {
            "closes": [47000 + i * 100 for i in range(50)],
            "highs": [47050 + i * 100 for i in range(50)],
            "lows": [46950 + i * 100 for i in range(50)],
            "volumes": [1000 + i * 10 for i in range(50)]
        }
        
        indicators = scanner_job._calculate_indicators(ohlcv_data)
        regime = scanner_job.regime_classifier.classify_regime("BTCUSDT", ohlcv_data, indicators)
        score_result = scanner_job.scoring_engine.score_signal("BTCUSDT", ohlcv_data, indicators, regime)
        
        # Create signal with high score to trigger insertion
        score_result["meets_threshold"] = True
        score_result["score"] = 8.5
        score_result["signal_direction"] = "LONG"
        
        signal_id = await scanner_job._create_signal_record("BTCUSDT", ohlcv_data, indicators, regime, score_result)
        
        assert signal_id is not None
        assert isinstance(signal_id, int)
        assert signal_id > 0
        
        # Verify signal was inserted
        signals = query_recent_signals(temp_db, limit=1)
        assert len(signals) > 0
        assert signals[0]["symbol"] == "BTCUSDT"
    
    def test_get_stats(self, scanner_job):
        """Test statistics retrieval."""
        scanner_job.stats.update({
            'last_scan_time': datetime.utcnow(),
            'symbols_scanned': 100,
            'signals_created': 5,
            'errors_count': 2,
            'api_calls_made': 150,
            'start_time': datetime.utcnow() - timedelta(hours=1)
        })
        
        # Add some cache data
        scanner_job.cache.data = {"BTCUSDT": [{"close": 47000}]}
        
        stats = scanner_job.get_stats()
        
        assert stats["running"] == False
        assert stats["uptime_hours"] is not None
        assert stats["symbols_in_universe"] == 3
        assert stats["total_symbols_scanned"] == 100
        assert stats["total_signals_created"] == 5
        assert stats["total_errors"] == 2
        assert stats["total_api_calls"] == 150
        assert stats["cached_symbols"] == 1
    
    @pytest.mark.asyncio
    async def test_start_and_stop_scanning(self, scanner_job):
        """Test starting and stopping scanner."""
        # Create mock scheduler
        mock_scheduler = Mock()
        scanner_job.set_scheduler(mock_scheduler)
        
        # Test start scanning
        await scanner_job.start_scanning()
        
        assert scanner_job.running == True
        assert scanner_job.stats['start_time'] is not None
        
        # Test stop scanning
        await scanner_job.stop_scanning()
        
        assert scanner_job.running == False


class TestScannerIntegration:
    """Integration tests for scanner with real components."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        conn = init_db(db_path)
        create_schema(conn)
        
        yield conn
        
        conn.close()
        import os
        os.unlink(db_path)
    
    @pytest.fixture
    def mock_exchange(self):
        """Create mock MEXC exchange with realistic data."""
        exchange = Mock()
        
        def mock_fetch_ohlcv(symbol, timeframe, limit):
            # Generate realistic OHLCV data
            base_price = 47000 if "BTC" in symbol else (3000 if "ETH" in symbol else 1.0)
            data = []
            
            for i in range(limit):
                timestamp = 1640995200000 + (i * 3600000)  # 1h intervals
                price = base_price + (i * 10) + (i % 5) * 5  # Slight upward trend
                high = price + 50
                low = price - 50
                open_price = price - 25
                volume = 1000 + i * 10
                
                data.append([timestamp, open_price, high, low, price, volume])
            
            return data
        
        exchange.fetch_ohlcv = AsyncMock(side_effect=mock_fetch_ohlcv)
        return exchange
    
    @pytest.mark.asyncio
    async def test_full_scanning_workflow(self, mock_exchange, temp_db):
        """Test complete scanning workflow."""
        test_universe = {
            "BTCUSDT": {"symbol": "BTC/USDT", "active": True},
            "ETHUSDT": {"symbol": "ETH/USDT", "active": True}
        }
        
        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        
        # Create scanner
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)
        
        # Start scanning
        await scanner.start_scanning()
        
        assert scanner.running == True
        
        # Wait for initial scan to complete
        await asyncio.sleep(2)
        
        # Check stats
        stats = scanner.get_stats()
        assert stats["total_symbols_scanned"] >= 0
        assert stats["total_api_calls"] > 0
        
        # Stop scanning
        await scanner.stop_scanning()
        assert scanner.running == False


if __name__ == "__main__":
    pytest.main([__file__])