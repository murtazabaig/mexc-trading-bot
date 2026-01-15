#!/usr/bin/env python3
"""Simple validation script for scanner job functionality."""

import sys
import os
import asyncio
import tempfile
import sqlite3
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from jobs.scanner import ScannerJob, OHLCVCache
    from database import init_db, create_schema
    from indicators import rsi, ema, macd, bollinger_bands, atr_percent
    from regime import RegimeClassifier
    from scoring import ScoringEngine
    print("✓ All imports successful")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)


def test_ohlcv_cache():
    """Test OHLCV cache functionality."""
    print("\nTesting OHLCV Cache...")
    
    cache = OHLCVCache(max_size=50)
    
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
    
    print("✓ OHLCV cache tests passed")


def test_indicators():
    """Test technical indicators."""
    print("\nTesting Technical Indicators...")
    
    # Create test data
    closes = [47000 + i * 100 for i in range(50)]
    highs = [47050 + i * 100 for i in range(50)]
    lows = [46950 + i * 100 for i in range(50)]
    volumes = [1000 + i * 10 for i in range(50)]
    
    # Test RSI
    rsi_value = rsi(closes, 14)
    assert 0 <= rsi_value <= 100
    print(f"  RSI(14): {rsi_value:.2f}")
    
    # Test EMA
    ema_20 = ema(closes, 20)
    ema_50 = ema(closes, 50)
    assert ema_20 > 0 and ema_50 > 0
    print(f"  EMA(20): {ema_20:.2f}")
    print(f"  EMA(50): {ema_50:.2f}")
    
    # Test MACD
    macd_data = macd(closes, 12, 26, 9)
    assert "macd" in macd_data
    print(f"  MACD: {macd_data['macd']:.2f}")
    
    # Test Bollinger Bands
    bb_data = bollinger_bands(closes, 20, 2.0)
    assert "upper" in bb_data
    print(f"  BB Upper: {bb_data['upper']:.2f}")
    
    # Test ATR%
    atr_pct = atr_percent(highs, lows, closes, 14)
    assert atr_pct >= 0
    print(f"  ATR%: {atr_pct:.2f}%")
    
    print("✓ Technical indicators tests passed")


def test_regime_classifier():
    """Test regime classification."""
    print("\nTesting Regime Classifier...")
    
    classifier = RegimeClassifier()
    
    # Create test data
    ohlcv_data = {
        'closes': [47000 + i * 100 for i in range(50)],
        'highs': [47050 + i * 100 for i in range(50)],
        'lows': [46950 + i * 100 for i in range(50)],
        'volumes': [1000 + i * 10 for i in range(50)]
    }
    
    indicators = {
        'rsi': {'value': 45.0},
        'ema': {'20': 47200, '50': 47000},
        'atr_percent': {'14': 3.5},
        'adx': {'14': 25.0}
    }
    
    regime = classifier.classify_regime("BTCUSDT", ohlcv_data, indicators)
    
    assert regime is not None
    assert "regime" in regime
    assert "confidence" in regime
    print(f"  Regime: {regime['regime']}")
    print(f"  Confidence: {regime['confidence']:.2f}")
    
    print("✓ Regime classifier tests passed")


def test_scoring_engine():
    """Test signal scoring."""
    print("\nTesting Scoring Engine...")
    
    scoring_engine = ScoringEngine()
    
    # Create test data
    ohlcv_data = {
        'closes': [47000 + i * 100 for i in range(50)],
        'highs': [47050 + i * 100 for i in range(50)],
        'lows': [46950 + i * 100 for i in range(50)],
        'volumes': [1000 + i * 10 for i in range(50)]
    }
    
    indicators = {
        'rsi': {'value': 35.0},
        'ema': {'20': 47200, '50': 47000},
        'macd': {'macd': 0.5, 'signal': 0.3, 'histogram': 0.2},
        'bollinger_bands': {'position': 0.3, 'upper': 48000, 'lower': 46500},
        'atr_percent': {'14': 3.5},
        'adx': {'14': 25.0},
        'volume_zscore': {'20': 1.5}
    }
    
    regime = {
        'regime': 'BULLISH_NORMAL_NEUTRAL',
        'trend': 'BULLISH',
        'volatility': 'NORMAL',
        'momentum': 'NEUTRAL',
        'confidence': 0.7
    }
    
    score_result = scoring_engine.score_signal("BTCUSDT", ohlcv_data, indicators, regime)
    
    assert score_result is not None
    assert "score" in score_result
    assert "confidence" in score_result
    assert "reasons" in score_result
    print(f"  Score: {score_result['score']:.1f}/{score_result['max_score']}")
    print(f"  Direction: {score_result['signal_direction']}")
    print(f"  Meets threshold: {score_result['meets_threshold']}")
    print(f"  Reasons: {len(score_result['reasons'])} factors")
    
    print("✓ Scoring engine tests passed")


async def test_scanner_job_creation():
    """Test scanner job creation and basic functionality."""
    print("\nTesting Scanner Job Creation...")
    
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        conn = init_db(db_path)
        create_schema(conn)
        
        # Mock exchange
        class MockExchange:
            def __init__(self):
                pass
            
            async def fetch_ohlcv(self, symbol, timeframe, limit=100):
                # Return mock OHLCV data
                return [
                    [1640995200000 + i * 3600000, 47000 + i * 10, 47050 + i * 10, 46950 + i * 10, 47025 + i * 10, 1000 + i * 10]
                    for i in range(50)
                ]
        
        exchange = MockExchange()
        
        test_universe = {
            "BTCUSDT": {"symbol": "BTC/USDT", "active": True},
            "ETHUSDT": {"symbol": "ETH/USDT", "active": True}
        }
        
        config = {"scanner": {"min_score": 7.0, "max_score": 10.0}}
        
        # Create scanner job
        scanner = ScannerJob(
            exchange=exchange,
            db_conn=conn,
            config=config,
            universe=test_universe
        )
        
        assert scanner is not None
        assert len(scanner.universe) == 2
        assert scanner.regime_classifier is not None
        assert scanner.scoring_engine is not None
        
        print("  ✓ Scanner job created successfully")
        
        # Test OHLCV fetching
        ohlcv_data = await scanner._fetch_ohlcv_data("BTCUSDT")
        assert ohlcv_data is not None
        assert len(ohlcv_data) == 50
        print("  ✓ OHLCV data fetching works")
        
        # Test indicator calculation
        cache_data = {
            "closes": [47000 + i * 100 for i in range(50)],
            "highs": [47050 + i * 100 for i in range(50)],
            "lows": [46950 + i * 100 for i in range(50)],
            "volumes": [1000 + i * 10 for i in range(50)]
        }
        
        indicators = scanner._calculate_indicators(cache_data)
        assert indicators is not None
        assert len(indicators) >= 5  # Should have several indicators
        print("  ✓ Indicator calculation works")
        
        conn.close()
        
        print("✓ Scanner job tests passed")
        
    finally:
        # Cleanup
        try:
            os.unlink(db_path)
        except:
            pass


async def test_full_scan_simulation():
    """Test a simplified full scan workflow."""
    print("\nTesting Full Scan Simulation...")
    
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    
    try:
        conn = init_db(db_path)
        create_schema(conn)
        
        class MockExchange:
            async def fetch_ohlcv(self, symbol, timeframe, limit=100):
                # Generate realistic data
                base_price = 47000 if "BTC" in symbol else 3000
                data = []
                for i in range(50):
                    timestamp = 1640995200000 + (i * 3600000)
                    price = base_price + (i * 10)
                    data.append([timestamp, price - 25, price + 25, price - 25, price, 1000])
                return data
        
        exchange = MockExchange()
        test_universe = {"BTCUSDT": {"symbol": "BTC/USDT", "active": True}}
        
        scanner = ScannerJob(exchange, conn, {"scanner": {}}, test_universe)
        
        # Test full symbol processing
        result = await scanner._process_symbol("BTCUSDT")
        assert result is not None
        assert "symbol" in result
        print(f"  Processed symbol: {result['symbol']}")
        print(f"  Signal created: {result.get('signal_created', False)}")
        
        if result.get('signal_created'):
            print(f"  Score: {result.get('score', 'N/A')}")
            print(f"  Confidence: {result.get('confidence', 'N/A')}")
        
        conn.close()
        print("✓ Full scan simulation passed")
        
    finally:
        try:
            os.unlink(db_path)
        except:
            pass


def main():
    """Run all validation tests."""
    print("MEXC Futures Signal Bot - Scanner Job Validation")
    print("=" * 50)
    
    try:
        # Run synchronous tests
        test_ohlcv_cache()
        test_indicators()
        test_regime_classifier()
        test_scoring_engine()
        
        # Run async tests
        print("\nRunning async tests...")
        asyncio.run(test_scanner_job_creation())
        asyncio.run(test_full_scan_simulation())
        
        print("\n" + "=" * 50)
        print("✓ ALL TESTS PASSED!")
        print("Scanner job implementation is working correctly.")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())