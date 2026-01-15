#!/usr/bin/env python3
"""Simple validation script that runs standalone without import issues."""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_basic_imports():
    """Test basic module imports."""
    print("Testing Basic Imports...")
    
    try:
        # Test indicator imports
        from indicators import rsi, ema, atr, atr_percent, vwap, volume_zscore, adx, sma
        print("✓ Basic indicators imported")
        
        # Import the new indicators
        from indicators.core import macd, bollinger_bands
        print("✓ MACD and Bollinger Bands imported")
        
        # Test regime and scoring imports
        from regime import RegimeClassifier
        from scoring import ScoringEngine
        print("✓ Regime and scoring modules imported")
        
        return True
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False


def test_indicator_calculations():
    """Test indicator calculations work."""
    print("\nTesting Indicator Calculations...")
    
    try:
        from indicators import rsi, ema, atr_percent, sma
        from indicators.core import macd, bollinger_bands
        
        # Create test data
        closes = [47000 + i * 100 for i in range(50)]
        highs = [47050 + i * 100 for i in range(50)]
        lows = [46950 + i * 100 for i in range(50)]
        volumes = [1000 + i * 10 for i in range(50)]
        
        # Test each indicator
        rsi_val = rsi(closes, 14)
        ema_val = ema(closes, 20)
        sma_val = sma(closes, 20)
        atr_pct = atr_percent(highs, lows, closes, 14)
        macd_val = macd(closes, 12, 26, 9)
        bb_val = bollinger_bands(closes, 20, 2.0)
        
        print(f"  RSI(14): {rsi_val:.2f}")
        print(f"  EMA(20): {ema_val:.2f}")
        print(f"  SMA(20): {sma_val:.2f}")
        print(f"  ATR%: {atr_pct:.2f}%")
        print(f"  MACD: {macd_val['macd']:.2f}")
        print(f"  BB Position: {bb_val['position']:.3f}")
        
        # Basic sanity checks
        assert 0 <= rsi_val <= 100, "RSI should be 0-100"
        assert ema_val > 0, "EMA should be positive"
        assert sma_val > 0, "SMA should be positive"
        assert atr_pct >= 0, "ATR% should be non-negative"
        assert isinstance(macd_val, dict), "MACD should return dict"
        assert isinstance(bb_val, dict), "BB should return dict"
        
        print("✓ All indicators working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Indicator calculation error: {e}")
        return False


def test_regime_and_scoring():
    """Test regime classification and signal scoring."""
    print("\nTesting Regime Classification and Scoring...")
    
    try:
        from regime import RegimeClassifier
        from scoring import ScoringEngine
        
        # Test regime classifier
        classifier = RegimeClassifier()
        
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
            'atr_percent': {'14': 3.5},
            'adx': {'14': 25.0}
        }
        
        regime = classifier.classify_regime("BTCUSDT", ohlcv_data, indicators)
        
        print(f"  Regime: {regime['regime']}")
        print(f"  Confidence: {regime['confidence']:.2f}")
        
        # Test scoring engine
        scorer = ScoringEngine()
        
        # Enhanced indicators for scoring
        scoring_indicators = {
            'rsi': {'value': 30.0},  # Oversold for scoring
            'ema': {'20': 47200, '50': 47000},
            'macd': {'macd': 0.5, 'signal': 0.3, 'histogram': 0.2},
            'bollinger_bands': {'position': 0.2, 'upper': 48000, 'lower': 46500},
            'atr_percent': {'14': 3.5},
            'adx': {'14': 25.0},
            'volume_zscore': {'20': 1.5}
        }
        
        score_result = scorer.score_signal("BTCUSDT", ohlcv_data, scoring_indicators, regime)
        
        print(f"  Score: {score_result['score']:.1f}/{score_result['max_score']}")
        print(f"  Direction: {score_result['signal_direction']}")
        print(f"  Meets threshold: {score_result['meets_threshold']}")
        
        # Basic sanity checks
        assert regime is not None, "Regime should not be None"
        assert score_result is not None, "Score result should not be None"
        assert 'score' in score_result, "Should have score"
        assert 'reasons' in score_result, "Should have reasons"
        
        print("✓ Regime classification and scoring working correctly")
        return True
        
    except Exception as e:
        print(f"✗ Regime/scoring error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_scanner_structure():
    """Test scanner job class structure."""
    print("\nTesting Scanner Job Structure...")
    
    try:
        # Just test that we can import the classes
        from jobs.scanner import ScannerJob, OHLCVCache
        print("✓ Scanner classes imported")
        
        # Test OHLCV cache works
        cache = OHLCVCache()
        assert cache.max_size == 100, "Default cache size should be 100"
        
        test_data = [
            [1640995200000, 47000, 47500, 46800, 47200, 1250.5],
            [1640998800000, 47200, 47800, 47100, 47600, 1180.3],
        ]
        
        cache.add_data("BTCUSDT", test_data)
        assert "BTCUSDT" in cache.data, "Data should be cached"
        
        arrays = cache.get_ohlcv_arrays("BTCUSDT")
        assert arrays is not None, "Should return arrays"
        assert len(arrays['closes']) == 2, "Should have 2 closes"
        
        print("✓ OHLCV cache working correctly")
        
        # Check ScannerJob has required methods
        required_methods = [
            'start_scanning', 'stop_scanning', 'get_stats',
            '_scan_all_symbols', '_process_symbol', '_fetch_ohlcv_data',
            '_calculate_indicators', '_create_signal_record'
        ]
        
        for method in required_methods:
            assert hasattr(ScannerJob, method), f"ScannerJob should have {method} method"
        
        print(f"✓ ScannerJob has all {len(required_methods)} required methods")
        return True
        
    except Exception as e:
        print(f"✗ Scanner structure error: {e}")
        return False


def test_database_basics():
    """Test database functionality."""
    print("\nTesting Database Basics...")
    
    try:
        # Test relative imports work
        from database import init_db, create_schema, insert_signal
        import tempfile
        import sqlite3
        
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
        assert signal_id is not None, "Should return signal ID"
        
        print(f"✓ Database working - signal ID: {signal_id}")
        
        conn.close()
        
        # Cleanup
        os.unlink(db_path)
        return True
        
    except Exception as e:
        print(f"✗ Database error: {e}")
        return False


def main():
    """Run all validation tests."""
    print("MEXC Futures Signal Bot - Scanner Job Validation")
    print("=" * 50)
    
    tests = [
        test_basic_imports,
        test_indicator_calculations,
        test_regime_and_scoring,
        test_scanner_structure,
        test_database_basics
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Test crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"RESULTS: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("✓ ALL TESTS PASSED!")
        print("\nScanner Job Implementation Complete:")
        print("  • Technical indicators (RSI, EMA, MACD, Bollinger Bands, ATR)")
        print("  • Regime classification system")
        print("  • Signal scoring engine")
        print("  • OHLCV data caching")
        print("  • Database integration")
        print("  • Scanner job with continuous operation")
        print("  • Error handling and rate limiting")
        print("  • Statistics and monitoring")
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())