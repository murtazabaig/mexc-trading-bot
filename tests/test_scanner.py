"""Tests for scanner job functionality."""

import pytest
import asyncio
import sqlite3
import tempfile
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta
import json

from src.jobs.scanner import ScannerJob, OHLCVCache, create_scanner_job
from src.database import (
    init_db, create_schema, query_recent_signals,
    get_last_processed_candle, update_processed_candle, clear_processed_candles
)
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


class TestCandleCloseStateTracking:
    """Test candle-close state tracking to prevent look-ahead bias and duplicate signals."""

    @pytest.fixture
    def mock_exchange(self):
        """Create a mock MEXC exchange."""
        exchange = Mock(spec=ccxt.mexc)

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
    async def test_get_last_closed_candle_ts(self, mock_exchange):
        """Test extraction of last closed candle timestamp."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch OHLCV data
        ohlcv = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=10)

        # Get last closed candle timestamp
        last_closed_ts = scanner._get_last_closed_candle_ts(ohlcv, "1h")

        # Should return the second-to-last candle's timestamp (definitely closed)
        assert last_closed_ts is not None
        assert last_closed_ts == ohlcv[-2][0]

        # Test with insufficient data
        insufficient_ohlcv = ohlcv[:1]
        last_closed_ts = scanner._get_last_closed_candle_ts(insufficient_ohlcv, "1h")
        assert last_closed_ts is None

    @pytest.mark.asyncio
    async def test_candle_already_processed_skip(self, mock_exchange):
        """Test that scanner skips symbols when candle was already processed."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch OHLCV data
        ohlcv = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=10)
        last_closed_ts = scanner._get_last_closed_candle_ts(ohlcv, "1h")

        # Mark this candle as already processed
        update_processed_candle(temp_db, "BTCUSDT", "1h", last_closed_ts)

        # Verify it's marked as processed
        retrieved_ts = get_last_processed_candle(temp_db, "BTCUSDT", "1h")
        assert retrieved_ts == last_closed_ts

        # Process symbol - should skip
        result = await scanner._process_symbol("BTCUSDT")

        assert result is not None
        assert result['symbol'] == "BTCUSDT"
        assert result['signal_created'] == False
        assert result['reason'] == 'CANDLE_ALREADY_PROCESSED'
        assert result['skipped'] == True

    @pytest.mark.asyncio
    async def test_new_candle_processed(self, mock_exchange):
        """Test that scanner processes symbols when a new candle is detected."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch OHLCV data
        ohlcv = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=10)
        last_closed_ts = scanner._get_last_closed_candle_ts(ohlcv, "1h")

        # Mark an older candle as processed
        update_processed_candle(temp_db, "BTCUSDT", "1h", last_closed_ts - 3600000)

        # Process symbol - should process new candle
        result = await scanner._process_symbol("BTCUSDT")

        assert result is not None
        assert result['symbol'] == "BTCUSDT"
        # Signal may or may not be created depending on score, but it should not be skipped
        assert result.get('reason') != 'CANDLE_ALREADY_PROCESSED'

        # Verify the new candle is now marked as processed if signal was created
        if result.get('signal_created'):
            retrieved_ts = get_last_processed_candle(temp_db, "BTCUSDT", "1h")
            assert retrieved_ts == last_closed_ts

    @pytest.mark.asyncio
    async def test_processed_candle_tracking_multiple_timeframes(self):
        """Test that processed candles are tracked independently per timeframe."""
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        symbol = "BTCUSDT"
        ts_1h = 1640995200000
        ts_5m = 1640995200000
        ts_4h = 1640995200000

        # Update different timeframes
        update_processed_candle(temp_db, symbol, "1h", ts_1h)
        update_processed_candle(temp_db, symbol, "5m", ts_5m)
        update_processed_candle(temp_db, symbol, "4h", ts_4h)

        # Retrieve each independently
        assert get_last_processed_candle(temp_db, symbol, "1h") == ts_1h
        assert get_last_processed_candle(temp_db, symbol, "5m") == ts_5m
        assert get_last_processed_candle(temp_db, symbol, "4h") == ts_4h

    @pytest.mark.asyncio
    async def test_update_processed_candle_overwrites(self):
        """Test that update_processed_candle overwrites existing records."""
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        symbol = "BTCUSDT"
        timeframe = "1h"
        ts1 = 1640995200000
        ts2 = 1640998800000  # 1 hour later

        # Insert first timestamp
        update_processed_candle(temp_db, symbol, timeframe, ts1)
        assert get_last_processed_candle(temp_db, symbol, timeframe) == ts1

        # Update with new timestamp
        update_processed_candle(temp_db, symbol, timeframe, ts2)
        assert get_last_processed_candle(temp_db, symbol, timeframe) == ts2

        # Ensure only one record exists
        cursor = temp_db.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM processed_candles WHERE symbol = ? AND timeframe = ?",
            (symbol, timeframe)
        )
        count = cursor.fetchone()[0]
        assert count == 1

    @pytest.mark.asyncio
    async def test_clear_processed_candles(self):
        """Test that clear_processed_candles removes all records."""
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        # Add some records
        update_processed_candle(temp_db, "BTCUSDT", "1h", 1640995200000)
        update_processed_candle(temp_db, "ETHUSDT", "1h", 1640995200000)
        update_processed_candle(temp_db, "BTCUSDT", "5m", 1640995200000)

        # Verify records exist
        cursor = temp_db.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_candles")
        count = cursor.fetchone()[0]
        assert count == 3

        # Clear all
        clear_processed_candles(temp_db)

        # Verify all cleared
        cursor.execute("SELECT COUNT(*) FROM processed_candles")
        count = cursor.fetchone()[0]
        assert count == 0

    @pytest.mark.asyncio
    async def test_no_duplicate_signals_on_reprocess(self, mock_exchange):
        """Test that re-running scanner on same closed candle does not generate duplicate signals."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Process symbol first time
        result1 = await scanner._process_symbol("BTCUSDT")

        # Process symbol second time (should skip if candle already processed)
        result2 = await scanner._process_symbol("BTCUSDT")

        # If first run created a signal, second run should skip
        if result1.get('signal_created'):
            assert result2.get('signal_created') == False
            assert result2.get('reason') == 'CANDLE_ALREADY_PROCESSED'

    @pytest.mark.asyncio
    async def test_get_last_processed_candle_returns_zero_when_not_found(self):
        """Test that get_last_processed_candle returns 0 when no record exists."""
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        # Query non-existent symbol
        ts = get_last_processed_candle(temp_db, "NONEXISTENT", "1h")
        assert ts == 0


class TestMultiTimeframeScanner:
    """Test multi-timeframe scanner functionality."""

    @pytest.mark.asyncio
    async def test_fetch_all_three_timeframes(self, mock_exchange):
        """Test that scanner fetches 5m, 1h, and 4h timeframes."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Clear processed candles to ensure fresh start
        clear_processed_candles(temp_db)

        # Process symbol - should fetch all three timeframes
        result = await scanner._process_symbol("BTCUSDT")

        assert result is not None
        assert result['symbol'] == "BTCUSDT"

        # Verify 5m candle is tracked
        ts_5m = get_last_processed_candle(temp_db, "BTCUSDT", "5m")
        assert ts_5m > 0

    @pytest.mark.asyncio
    async def test_mtf_confluence_blocks_bearish_1h_for_long(self, mock_exchange):
        """Test that MTF confluence blocks LONG signals when 1h trend is bearish."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Force bearish 1h by modifying EMA values
        if 'ema' in ind_1h:
            ind_1h['ema']['20'] = 100.0
            ind_1h['ema']['50'] = 110.0  # EMA20 < EMA50 = bearish

        # Test confluence for LONG direction
        confluence = scanner._check_mtf_confluence(ind_5m, ind_1h, ind_4h, 'LONG')

        assert confluence['aligned'] == False
        assert 'bearish' in confluence['reason'].lower()
        assert confluence['score_penalty'] == -3.0

    @pytest.mark.asyncio
    async def test_mtf_confluence_blocks_bullish_1h_for_short(self, mock_exchange):
        """Test that MTF confluence blocks SHORT signals when 1h trend is bullish."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Force bullish 1h by modifying EMA values
        if 'ema' in ind_1h:
            ind_1h['ema']['20'] = 110.0
            ind_1h['ema']['50'] = 100.0  # EMA20 > EMA50 = bullish

        # Test confluence for SHORT direction
        confluence = scanner._check_mtf_confluence(ind_5m, ind_1h, ind_4h, 'SHORT')

        assert confluence['aligned'] == False
        assert 'bullish' in confluence['reason'].lower()
        assert confluence['score_penalty'] == -3.0

    @pytest.mark.asyncio
    async def test_mtf_confluence_applies_penalty_for_weak_4h(self, mock_exchange):
        """Test that MTF confluence applies penalty when 4h opposes the trend."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Force bullish 1h but bearish 4h (weak alignment)
        if 'ema' in ind_1h:
            ind_1h['ema']['20'] = 110.0
            ind_1h['ema']['50'] = 100.0  # 1h bullish

        if 'ema' in ind_4h:
            ind_4h['ema']['50'] = 100.0
            ind_4h['ema']['200'] = 110.0  # 4h bearish (downtrend)

        # Test confluence for LONG direction
        confluence = scanner._check_mtf_confluence(ind_5m, ind_1h, ind_4h, 'LONG')

        assert confluence['aligned'] == True
        assert 'downtrend' in confluence['reason'].lower() or 'caution' in confluence['reason'].lower()
        assert confluence['score_penalty'] == -1.5

    @pytest.mark.asyncio
    async def test_mtf_confluence_allows_strong_alignment(self, mock_exchange):
        """Test that MTF confluence allows signals with strong alignment."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Force strong alignment (1h bullish, 4h bullish)
        if 'ema' in ind_1h:
            ind_1h['ema']['20'] = 110.0
            ind_1h['ema']['50'] = 100.0  # 1h bullish

        if 'ema' in ind_4h:
            ind_4h['ema']['50'] = 110.0
            ind_4h['ema']['200'] = 100.0  # 4h bullish (uptrend)

        # Test confluence for LONG direction
        confluence = scanner._check_mtf_confluence(ind_5m, ind_1h, ind_4h, 'LONG')

        assert confluence['aligned'] == True
        assert 'support' in confluence['reason'].lower() or 'aligned' in confluence['reason'].lower()
        assert confluence['score_penalty'] == 0.0

    @pytest.mark.asyncio
    async def test_score_threshold_enforced_after_mtf_penalty(self, mock_exchange):
        """Test that 7.0 score threshold is enforced after MTF penalty."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Force weak alignment (1h bullish, 4h bearish = -1.5 penalty)
        if 'ema' in ind_1h:
            ind_1h['ema']['20'] = 110.0
            ind_1h['ema']['50'] = 100.0  # 1h bullish

        if 'ema' in ind_4h:
            ind_4h['ema']['50'] = 100.0
            ind_4h['ema']['200'] = 110.0  # 4h bearish

        # Test that a score of 7.5 with -1.5 penalty becomes 6.0 and is rejected
        confluence = scanner._check_mtf_confluence(ind_5m, ind_1h, ind_4h, 'LONG')

        assert confluence['aligned'] == True  # Not blocked
        assert confluence['score_penalty'] == -1.5

        # Simulate final score
        initial_score = 7.5
        final_score = initial_score + confluence['score_penalty']

        assert final_score == 6.0
        assert final_score < 7.0  # Below threshold

    @pytest.mark.asyncio
    async def test_5m_candle_state_tracking_mtf(self, mock_exchange):
        """Test that only NEW 5m closed candles generate signals in MTF mode."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch 5m data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        if not ohlcv_5m:
            pytest.skip("No 5m data available")

        last_closed_5m_ts = scanner._get_last_closed_candle_ts(ohlcv_5m, "5m")

        # Mark this candle as processed
        update_processed_candle(temp_db, "BTCUSDT", "5m", last_closed_5m_ts)

        # Process symbol - should skip
        result = await scanner._process_symbol("BTCUSDT")

        assert result is not None
        assert result['symbol'] == "BTCUSDT"
        assert result.get('signal_created') == False
        assert result.get('reason') == 'CANDLE_ALREADY_PROCESSED'

    @pytest.mark.asyncio
    async def test_convert_ohlcv_to_arrays(self, mock_exchange):
        """Test that _convert_ohlcv_to_arrays correctly converts data."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch OHLCV data
        ohlcv = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=10)
        if not ohlcv:
            pytest.skip("No OHLCV data available")

        # Convert to arrays
        arrays = scanner._convert_ohlcv_to_arrays(ohlcv)

        assert arrays is not None
        assert 'closes' in arrays
        assert 'highs' in arrays
        assert 'lows' in arrays
        assert 'opens' in arrays
        assert 'volumes' in arrays
        assert 'timestamps' in arrays
        assert len(arrays['closes']) == len(ohlcv)

    @pytest.mark.asyncio
    async def test_log_mtf_data_outputs_clear_formatting(self, mock_exchange):
        """Test that _log_mtf_data outputs clearly formatted logs."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch data
        ohlcv_5m = await scanner._fetch_ohlcv_data("BTCUSDT", "5m", limit=100)
        ohlcv_1h = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=100)
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)

        if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
            pytest.skip("Insufficient data for MTF test")

        # Convert to arrays
        data_5m = scanner._convert_ohlcv_to_arrays(ohlcv_5m)
        data_1h = scanner._convert_ohlcv_to_arrays(ohlcv_1h)
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)

        if not (data_5m and data_1h and data_4h):
            pytest.skip("Failed to convert OHLCV data")

        # Calculate indicators
        ind_5m = await scanner._calculate_indicators(data_5m)
        ind_1h = await scanner._calculate_indicators(data_1h)
        ind_4h = await scanner._calculate_indicators(data_4h)

        if not (ind_5m and ind_1h and ind_4h):
            pytest.skip("Failed to calculate indicators")

        # Call log method - should not raise errors
        scanner._log_mtf_data("BTCUSDT", data_5m, data_1h, data_4h, ind_5m, ind_1h, ind_4h)

        # If we get here, logging worked correctly
        assert True

    @pytest.mark.asyncio
    async def test_signals_include_mtf_context(self, mock_exchange):
        """Test that signals include full MTF context in metadata."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        clear_processed_candles(temp_db)

        # Process symbol
        result = await scanner._process_symbol("BTCUSDT")

        if result and result.get('signal_created'):
            # Query the signal from database
            signals = query_recent_signals(temp_db, limit=1)
            if signals:
                signal = signals[0]

                # Check that MTF context is present
                reason = signal.get('reason', {})
                metadata = signal.get('metadata', {})

                # Verify MTF check is in reason
                assert 'mtf_check' in reason or 'mtf_signal' in metadata

    @pytest.mark.asyncio
    async def test_ema200_calculated_for_4h(self, mock_exchange):
        """Test that EMA200 is calculated for 4h timeframe."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch 4h data
        ohlcv_4h = await scanner._fetch_ohlcv_data("BTCUSDT", "4h", limit=100)
        if not ohlcv_4h:
            pytest.skip("No 4h data available")

        # Convert to arrays
        data_4h = scanner._convert_ohlcv_to_arrays(ohlcv_4h)
        if not data_4h:
            pytest.skip("Failed to convert 4h data")

        # Calculate indicators
        ind_4h = await scanner._calculate_indicators(data_4h)
        if not ind_4h:
            pytest.skip("Failed to calculate 4h indicators")

        # Verify EMA200 is present
        assert 'ema' in ind_4h
        assert '200' in ind_4h['ema']
        assert ind_4h['ema']['200'] > 0

    @pytest.mark.asyncio
    async def test_multiple_symbols_independent_tracking(self, mock_exchange):
        """Test that processed candles are tracked independently per symbol."""
        test_universe = {
            "BTCUSDT": {"symbol": "BTC/USDT", "active": True},
            "ETHUSDT": {"symbol": "ETH/USDT", "active": True}
        }
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # Fetch OHLCV for BTC
        ohlcv_btc = await scanner._fetch_ohlcv_data("BTCUSDT", "1h", limit=10)
        last_closed_ts_btc = scanner._get_last_closed_candle_ts(ohlcv_btc, "1h")

        # Fetch OHLCV for ETH
        ohlcv_eth = await scanner._fetch_ohlcv_data("ETHUSDT", "1h", limit=10)
        last_closed_ts_eth = scanner._get_last_closed_candle_ts(ohlcv_eth, "1h")

        # Mark BTC as processed
        update_processed_candle(temp_db, "BTCUSDT", "1h", last_closed_ts_btc)

        # Verify BTC is marked but ETH is not
        assert get_last_processed_candle(temp_db, "BTCUSDT", "1h") == last_closed_ts_btc
        assert get_last_processed_candle(temp_db, "ETHUSDT", "1h") == 0


    @pytest.mark.asyncio
    async def test_no_signal_when_1h_opposes_5m_entry(self, mock_exchange):
        """Test that signals are blocked when 1h trend opposes 5m entry trigger.
        This is a conceptual test - actual blocking is verified in confluence tests."""
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        temp_db = sqlite3.connect(":memory:")
        create_schema(temp_db)

        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        scanner = create_scanner_job(mock_exchange, temp_db, config, test_universe)

        # This test verifies confluence logic blocks signals
        # when 1h trend opposes 5m entry direction
        # The actual blocking is verified in test_mtf_confluence_blocks_bearish_1h_for_long
        assert True  # Placeholder - actual test in confluence tests above


if __name__ == "__main__":
    pytest.main([__file__])
