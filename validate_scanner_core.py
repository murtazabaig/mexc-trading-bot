#!/usr/bin/env python3
"""Simple validation script for scanner job core functionality without external deps."""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_core_indicators():
    """Test core indicator calculations without external dependencies."""
    print("\nTesting Core Technical Indicators...")
    
    # Import indicators
    try:
        from indicators import rsi, ema, atr, atr_percent, vwap, volume_zscore, adx, sma
        from indicators.core import macd, bollinger_bands
        print("✓ All indicators imported successfully")
    except ImportError as e:
        print(f"✗ Indicator import error: {e}")
        return False
    
    # Create test data
    closes = [47000 + i * 100 for i in range(50)]
    highs = [47050 + i * 100 for i in range(50)]
    lows = [46950 + i * 100 for i in range(50)]
    volumes = [1000 + i * 10 for i in range(50)]
    
    try:
        # Test RSI
        rsi_value = rsi(closes, 14)
        assert 0 <= rsi_value <= 100, f"RSI out of range: {rsi_value}"
        print(f"  ✓ RSI(14): {rsi_value:.2f}")
        
        # Test EMA
        ema_20 = ema(closes, 20)
        ema_50 = ema(closes, 50)
        assert ema_20 > 0 and ema_50 > 0, f"EMA values invalid: {ema_20}, {ema_50}"
        print(f"  ✓ EMA(20): {ema_20:.2f}")
        print(f"  ✓ EMA(50): {ema_50:.2f}")
        
        # Test SMA
        sma_20 = sma(closes, 20)
        assert sma_20 > 0, f"SMA invalid: {sma_20}"
        print(f"  ✓ SMA(20): {sma_20:.2f}")
        
        # Test MACD
        macd_data = macd(closes, 12, 26, 9)
        assert isinstance(macd_data, dict), "MACD should return dict"
        assert "macd" in macd_data, "MACD should have macd key"
        print(f"  ✓ MACD: {macd_data['macd']:.4f}")
        print(f"  ✓ MACD Signal: {macd_data['signal']:.4f}")
        print(f"  ✓ MACD Histogram: {macd_data['histogram']:.4f}")
        
        # Test Bollinger Bands
        bb_data = bollinger_bands(closes, 20, 2.0)
        assert isinstance(bb_data, dict), "BB should return dict"
        assert "upper" in bb_data and "lower" in bb_data, "BB should have upper/lower"
        print(f"  ✓ BB Upper: {bb_data['upper']:.2f}")
        print(f"  ✓ BB Lower: {bb_data['lower']:.2f}")
        print(f"  ✓ BB Position: {bb_data['position']:.3f}")
        
        # Test ATR
        atr_14 = atr(highs, lows, closes, 14)
        atr_pct = atr_percent(highs, lows, closes, 14)
        assert atr_14 >= 0, f"ATR invalid: {atr_14}"
        assert atr_pct >= 0, f"ATR% invalid: {atr_pct}"
        print(f"  ✓ ATR(14): {atr_14:.2f}")
        print(f"  ✓ ATR%: {atr_pct:.2f}%")
        
        # Test VWAP
        vwap_val = vwap(highs, lows, closes, volumes)
        assert vwap_val > 0, f"VWAP invalid: {vwap_val}"
        print(f"  ✓ VWAP: {vwap_val:.2f}")
        
        # Test Volume Z-Score
        vol_zscore = volume_zscore(volumes, 20)
        assert -10 <= vol_zscore <= 10, f"Volume Z-score out of range: {vol_zscore}"
        print(f"  ✓ Volume Z-Score: {vol_zscore:.2f}")
        
        # Test ADX
        adx_14 = adx(highs, lows, 14)
        assert 0 <= adx_14 <= 100, f"ADX out of range: {adx_14}"
        print(f"  ✓ ADX(14): {adx_14:.2f}")
        
        print("✓ All technical indicators working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Indicator calculation error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_regime_classifier():
    """Test regime classification logic."""
    print("\nTesting Regime Classification...")
    
    try:
        from regime import RegimeClassifier
        print("✓ Regime classifier imported successfully")
    except ImportError as e:
        print(f"✗ Regime import error: {e}")
        return False
    
    try:
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
        
        assert regime is not None, "Regime should not be None"
        assert "regime" in regime, "Regime should have regime key"
        assert "confidence" in regime, "Regime should have confidence"
        assert "trend" in regime, "Regime should have trend"
        assert "volatility" in regime, "Regime should have volatility"
        assert "momentum" in regime, "Regime should have momentum"
        
        print(f"  ✓ Regime: {regime['regime']}")
        print(f"  ✓ Trend: {regime['trend']}")
        print(f"  ✓ Volatility: {regime['volatility']}")
        print(f"  ✓ Momentum: {regime['momentum']}")
        print(f"  ✓ Confidence: {regime['confidence']:.2f}")
        
        print("✓ Regime classification working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Regime classification error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_scoring_engine():
    """Test signal scoring logic."""
    print("\nTesting Signal Scoring Engine...")
    
    try:
        from scoring import ScoringEngine
        print("✓ Scoring engine imported successfully")
    except ImportError as e:
        print(f"✗ Scoring import error: {e}")
        return False
    
    try:
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
        
        assert score_result is not None, "Score result should not be None"
        assert "score" in score_result, "Score result should have score"
        assert "confidence" in score_result, "Score result should have confidence"
        assert "reasons" in score_result, "Score result should have reasons"
        assert "signal_direction" in score_result, "Score result should have signal_direction"
        assert "meets_threshold" in score_result, "Score result should have meets_threshold"
        
        print(f"  ✓ Score: {score_result['score']:.1f}/{score_result['max_score']}")
        print(f"  ✓ Confidence: {score_result['confidence']:.2f}")
        print(f"  ✓ Direction: {score_result['signal_direction']}")
        print(f"  ✓ Meets threshold: {score_result['meets_threshold']}")
        print(f"  ✓ Reasons: {len(score_result['reasons'])} factors")
        
        if score_result['reasons']:
            for i, reason in enumerate(score_result['reasons'][:3]):  # Show first 3 reasons
                print(f"    - {reason}")
        
        print("✓ Signal scoring working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Signal scoring error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_scanner_classes():
    """Test scanner job classes structure."""
    print("\nTesting Scanner Job Classes...")
    
    try:
        from jobs.scanner import OHLCVCache, ScannerJob
        print("✓ Scanner classes imported successfully")
    except ImportError as e:
        print(f"✗ Scanner import error: {e}")
        return False
    
    try:
        # Test OHLCV Cache
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
        assert "BTCUSDT" in cache.data, "BTCUSDT should be in cache"
        assert len(cache.data["BTCUSDT"]) == 3, "Should have 3 candles"
        
        # Get arrays
        ohlcv_arrays = cache.get_ohlcv_arrays("BTCUSDT")
        assert ohlcv_arrays is not None, "Should return arrays"
        assert len(ohlcv_arrays["closes"]) == 3, "Should have 3 closes"
        assert ohlcv_arrays["closes"] == [47200, 47600, 47900], "Close prices should match"
        
        # Test latest price
        latest_price = cache.get_latest_price("BTCUSDT")
        assert latest_price == 47900, f"Latest price should be 47900, got {latest_price}"
        
        print(f"  ✓ OHLCV Cache: {len(cache.data)} symbols cached")
        print(f"  ✓ Latest price for BTCUSDT: {latest_price}")
        
        # Test ScannerJob class structure
        # We can't create a full instance without external dependencies,
        # but we can test the class exists and has expected methods
        
        expected_methods = ['start_scanning', 'stop_scanning', 'get_stats', 
                          '_scan_all_symbols', '_process_symbol', '_fetch_ohlcv_data',
                          '_calculate_indicators', '_create_signal_record']
        
        for method in expected_methods:
            assert hasattr(ScannerJob, method), f"ScannerJob should have {method} method"
        
        print(f"  ✓ ScannerJob class structure validated")
        print(f"  ✓ All {len(expected_methods)} required methods present")
        
        print("✓ Scanner job classes working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Scanner classes error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_database_integration():
    """Test database schema and operations."""
    print("\nTesting Database Integration...")
    
    try:
        from database import init_db, create_schema, insert_signal
        import tempfile
        import sqlite3
        print("✓ Database modules imported successfully")
    except ImportError as e:
        print(f"✗ Database import error: {e}")
        return False
    
    try:
        # Create temporary database
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        
        conn = init_db(db_path)
        create_schema(conn)
        
        # Test signal insertion
        test_signal = {
            "symbol": "BTCUSDT",
            "timeframe": "1h",
            "side": "LONG",
            "confidence": 0.85,
            "regime": "TRENDING",
            "entry_price": 50000.0,
            "stop_loss": 48000.0,
            "tp1": 52000.0,
            "reason": {"confluence": ["RSI Oversold", "Support Touch"]},
            "metadata": {"test": True}
        }
        
        signal_id = insert_signal(conn, test_signal)
        assert signal_id is not None, "Signal ID should be returned"
        assert isinstance(signal_id, int), "Signal ID should be integer"
        assert signal_id > 0, "Signal ID should be positive"
        
        print(f"  ✓ Signal inserted with ID: {signal_id}")
        
        conn.close()
        
        # Cleanup
        import os
        os.unlink(db_path)
        
        print("✓ Database integration working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Database integration error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all validation tests."""
    print("MEXC Futures Signal Bot - Scanner Job Core Validation")
    print("=" * 55)
    print("Testing core functionality without external dependencies...")
    
    tests = [
        ("Technical Indicators", test_core_indicators),
        ("Regime Classification", test_regime_classifier),
        ("Signal Scoring", test_scoring_engine),
        ("Scanner Classes", test_scanner_classes),
        ("Database Integration", test_database_integration)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            if result:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ {test_name} test crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 55)
    print(f"VALIDATION RESULTS: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("✓ ALL CORE TESTS PASSED!")
        print("Scanner job implementation is working correctly.")
        print("\nKey Features Validated:")
        print("  • Technical indicators (RSI, EMA, MACD, Bollinger Bands, ATR)")
        print("  • Regime classification system")
        print("  • Signal scoring engine")
        print("  • OHLCV data caching")
        print("  • Database signal insertion")
        print("  • Scanner job class structure")
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())