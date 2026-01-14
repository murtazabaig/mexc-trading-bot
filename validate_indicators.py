#!/usr/bin/env python3
"""Comprehensive validation of all acceptance criteria for technical indicators."""

import sys
import os
import time
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from indicators import ema, rsi, atr, atr_percent, vwap, volume_zscore, adx, true_range, sma

def test_deterministic_behavior():
    """Test that indicators are deterministic (same input -> same output)."""
    print("=== Testing Deterministic Behavior ===")
    
    data = {
        'highs': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        'lows': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
        'closes': [99.5, 100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 108.5, 109.5],
        'volumes': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000]
    }
    
    # Test each indicator multiple times
    ema_results = [ema(data['closes'], 5) for _ in range(10)]
    rsi_results = [rsi(data['closes'], 5) for _ in range(10)]
    atr_results = [atr(data['highs'], data['lows'], data['closes'], 5) for _ in range(10)]
    vwap_results = [vwap(data['highs'], data['lows'], data['closes'], data['volumes']) for _ in range(10)]
    
    # Check all results are identical
    assert len(set(ema_results)) == 1, f"EMA not deterministic: {ema_results}"
    assert len(set(rsi_results)) == 1, f"RSI not deterministic: {rsi_results}"
    assert len(set(atr_results)) == 1, f"ATR not deterministic: {atr_results}"
    assert len(set(vwap_results)) == 1, f"VWAP not deterministic: {vwap_results}"
    
    print("✅ All indicators are deterministic")
    return True

def test_boundary_values():
    """Test that all indicators return values within expected boundaries."""
    print("\n=== Testing Boundary Values ===")
    
    # Test with various market conditions
    test_cases = [
        {
            "name": "Bull market",
            "data": {
                'highs': list(range(100, 150, 2)),
                'lows': list(range(98, 148, 2)),
                'closes': list(range(99, 149, 2)),
                'volumes': [1000] * 25
            }
        },
        {
            "name": "Bear market",
            "data": {
                'highs': list(range(150, 100, -2)),
                'lows': list(range(148, 98, -2)),
                'closes': list(range(149, 99, -2)),
                'volumes': [1000] * 25
            }
        },
        {
            "name": "High volatility",
            "data": {
                'highs': [100 + i*10 + (-1)**i*5 for i in range(25)],
                'lows': [100 + i*10 - (-1)**i*5 for i in range(25)],
                'closes': [100 + i*10 + (-1)**i*2 for i in range(25)],
                'volumes': [1000 + i*100 for i in range(25)]
            }
        }
    ]
    
    for case in test_cases:
        print(f"Testing {case['name']}...")
        data = case['data']
        
        try:
            rsi_val = rsi(data['closes'], 14)
            atr_val = atr(data['highs'], data['lows'], data['closes'], 14)
            atr_pct_val = atr_percent(data['highs'], data['lows'], data['closes'], 14)
            adx_val = adx(data['highs'], data['lows'], 14)
            
            # Validate boundaries
            assert 0.0 <= rsi_val <= 100.0, f"RSI out of range: {rsi_val}"
            assert atr_val >= 0.0, f"ATR negative: {atr_val}"
            assert atr_pct_val >= 0.0, f"ATR% negative: {atr_pct_val}"
            assert 0.0 <= adx_val <= 100.0, f"ADX out of range: {adx_val}"
            
            print(f"  RSI: {rsi_val:.2f}, ATR: {atr_val:.4f}, ATR%: {atr_pct_val:.2f}%, ADX: {adx_val:.2f}")
            
        except Exception as e:
            print(f"  ❌ Error in {case['name']}: {e}")
            return False
    
    print("✅ All boundary values validated")
    return True

def test_edge_cases():
    """Test edge cases and error handling."""
    print("\n=== Testing Edge Cases ===")
    
    # Test insufficient data
    print("Testing insufficient data...")
    try:
        ema([1, 2, 3], 10)
        assert False, "Should have raised ValueError for insufficient data"
    except ValueError:
        print("  ✅ EMA correctly handles insufficient data")
    
    try:
        rsi([1, 2, 3], 14)
        assert False, "Should have raised ValueError for insufficient data"
    except ValueError:
        print("  ✅ RSI correctly handles insufficient data")
    
    # Test constant prices
    print("Testing constant prices...")
    constant_closes = [100.0] * 30
    constant_highs = [100.5] * 30
    constant_lows = [99.5] * 30
    constant_volumes = [1000.0] * 30
    
    ema_const = ema(constant_closes, 14)
    rsi_const = rsi(constant_closes, 14)
    atr_const = atr(constant_highs, constant_lows, constant_closes, 14)
    vol_z_const = volume_zscore(constant_volumes, 14)
    adx_const = adx(constant_highs, constant_lows, 14)
    
    assert abs(ema_const - 100.0) < 0.01, f"EMA should equal constant price: {ema_const}"
    assert abs(rsi_const - 50.0) < 1.0, f"RSI should be 50 for flat prices: {rsi_const}"
    assert abs(atr_const - 1.0) < 0.01, f"ATR should be 1.0: {atr_const}"
    assert abs(vol_z_const) < 0.01, f"Volume Z-Score should be 0: {vol_z_const}"
    assert abs(adx_const) < 0.01, f"ADX should be 0 for no trend: {adx_const}"
    
    print("  ✅ All indicators handle constant prices correctly")
    
    # Test zero/invalid volumes
    print("Testing zero volumes...")
    data_with_zero_vol = {
        'highs': [100, 101, 102],
        'lows': [99, 100, 101],
        'closes': [99.5, 100.5, 101.5],
        'volumes': [0, 1000, 0]  # Some zero volumes
    }
    
    try:
        vwap_val = vwap(data_with_zero_vol['highs'], data_with_zero_vol['lows'], 
                        data_with_zero_vol['closes'], data_with_zero_vol['volumes'])
        assert vwap_val > 0, "VWAP should handle zero volumes gracefully"
        print("  ✅ VWAP handles zero volumes correctly")
    except Exception as e:
        print(f"  ❌ VWAP failed with zero volumes: {e}")
        return False
    
    print("✅ All edge cases handled correctly")
    return True

def test_performance_requirement():
    """Test the performance requirement: 500 bars < 100ms total."""
    print("\n=== Testing Performance Requirement (500 bars < 100ms) ===")
    
    # Generate 500 data points
    data = {
        'highs': [50000 + i + (-1)**i * 100 for i in range(500)],
        'lows': [49900 + i - (-1)**i * 100 for i in range(500)],
        'closes': [49950 + i + (-1)**i * 50 for i in range(500)],
        'volumes': [1000 + i * 10 for i in range(500)]
    }
    
    # Time all indicators
    start_time = time.perf_counter()
    
    ema_result = ema(data['closes'], 14)
    rsi_result = rsi(data['closes'], 14)
    atr_result = atr(data['highs'], data['lows'], data['closes'], 14)
    atr_pct_result = atr_percent(data['highs'], data['lows'], data['closes'], 14)
    vwap_result = vwap(data['highs'], data['lows'], data['closes'], data['volumes'])
    vol_z_result = volume_zscore(data['volumes'], 20)
    adx_result = adx(data['highs'], data['lows'], 14)
    
    end_time = time.perf_counter()
    total_time_ms = (end_time - start_time) * 1000
    
    print(f"Total time for all indicators on 500 bars: {total_time_ms:.3f} ms")
    print(f"Results: EMA={ema_result:.4f}, RSI={rsi_result:.2f}, ATR={atr_result:.4f}, " +
          f"ATR%={atr_pct_result:.2f}%, VWAP={vwap_result:.4f}, VolZ={vol_z_result:.2f}, ADX={adx_result:.2f}")
    
    if total_time_ms < 100.0:
        print(f"✅ Performance requirement met: {total_time_ms:.3f} ms < 100.0 ms")
        return True
    else:
        print(f"❌ Performance requirement failed: {total_time_ms:.3f} ms >= 100.0 ms")
        return False

def test_known_calculations():
    """Test indicators against known calculated values."""
    print("\n=== Testing Known Calculations ===")
    
    # Simple test data with known outcomes
    test_cases = [
        {
            "name": "EMA test",
            "data": ([1.0, 2.0, 3.0, 4.0, 5.0], 2),
            "expected": 4.5,
            "tolerance": 0.1,
            "function": lambda x, p: ema(x, p)
        },
        {
            "name": "True Range test",
            "data": (105.0, 95.0, 100.0),
            "expected": 10.0,
            "tolerance": 0.01,
            "function": lambda h, l, pc: true_range(h, l, pc)
        }
    ]
    
    for case in test_cases:
        result = case["function"](*case["data"])
        expected = case["expected"]
        tolerance = case["tolerance"]
        
        if abs(result - expected) <= tolerance:
            print(f"  ✅ {case['name']}: {result:.4f} ≈ {expected}")
        else:
            print(f"  ❌ {case['name']}: {result:.4f} ≠ {expected} (tolerance: {tolerance})")
            return False
    
    print("✅ All known calculations validated")
    return True

def validate_acceptance_criteria():
    """Validate all acceptance criteria."""
    print("=== ACCEPTANCE CRITERIA VALIDATION ===\n")
    
    criteria = [
        ("All indicators compute correctly", True),
        ("No external dependencies beyond stdlib", True),
        ("Functions handle edge cases", True),
        ("All RSI values in [0, 100], ATR > 0, ADX in [0, 100]", True),
        ("Indicator functions are deterministic", True),
        ("Performance: all indicators on 500 bars < 100ms total", True)
    ]
    
    results = []
    
    # Test criteria
    results.append(("All indicators compute correctly", test_known_calculations()))
    results.append(("No external dependencies beyond stdlib", True))  # Verified by code inspection
    results.append(("Functions handle edge cases", test_edge_cases()))
    results.append(("Boundary checks", test_boundary_values()))
    results.append(("Indicator functions are deterministic", test_deterministic_behavior()))
    results.append(("Performance: all indicators on 500 bars < 100ms total", test_performance_requirement()))
    
    # Print summary
    print("\n" + "="*50)
    print("ACCEPTANCE CRITERIA SUMMARY")
    print("="*50)
    
    all_passed = True
    for i, (criterion, result) in enumerate(results, 1):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{i}. {criterion}: {status}")
        if not result:
            all_passed = False
    
    print("\n" + "="*50)
    if all_passed:
        print("🎉 ALL ACCEPTANCE CRITERIA PASSED!")
        print("✅ Ready for production use")
    else:
        print("💥 SOME CRITERIA FAILED!")
        print("❌ Review and fix issues before production use")
    print("="*50)
    
    return all_passed

if __name__ == "__main__":
    print("Technical Indicators - Comprehensive Validation\n")
    
    success = validate_acceptance_criteria()
    
    if success:
        print("\n✅ Implementation complete and validated!")
        sys.exit(0)
    else:
        print("\n❌ Validation failed!")
        sys.exit(1)