#!/usr/bin/env python3
"""Performance test for technical indicators."""

import sys
import os
import time
import random
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from indicators import ema, rsi, atr, atr_percent, vwap, volume_zscore, adx

def generate_test_data(n_points=500):
    """Generate realistic test data with trends and volatility."""
    data = {
        'highs': [],
        'lows': [],
        'closes': [],
        'volumes': []
    }
    
    # Start with a base price
    base_price = 50000.0
    volume_base = 1000.0
    
    for i in range(n_points):
        # Add some trend and noise
        trend = i * 0.1  # Slight uptrend
        noise = random.uniform(-0.02, 0.02)  # 2% volatility
        close = base_price + trend + (base_price * noise)
        
        # High and low around close
        high = close * (1 + random.uniform(0, 0.01))
        low = close * (1 - random.uniform(0, 0.01))
        
        # Volume with some variation
        volume = volume_base * (1 + random.uniform(-0.5, 0.5))
        
        data['highs'].append(high)
        data['lows'].append(low)
        data['closes'].append(close)
        data['volumes'].append(max(1.0, volume))  # Ensure positive volume
    
    return data

def benchmark_indicators():
    """Benchmark all indicators with 500 data points."""
    print("=== Performance Test: 500 Data Points ===")
    
    # Generate test data
    data = generate_test_data(500)
    n = len(data['closes'])
    
    print(f"Generated {n} data points")
    print(f"Price range: {min(data['closes']):.2f} - {max(data['closes']):.2f}")
    print(f"Volume range: {min(data['volumes']):.0f} - {max(data['volumes']):.0f}")
    
    # Benchmark each indicator
    indicators = [
        ('EMA', lambda: ema(data['closes'], 14)),
        ('RSI', lambda: rsi(data['closes'], 14)),
        ('ATR', lambda: atr(data['highs'], data['lows'], data['closes'], 14)),
        ('ATR%', lambda: atr_percent(data['highs'], data['lows'], data['closes'], 14)),
        ('VWAP', lambda: vwap(data['highs'], data['lows'], data['closes'], data['volumes'])),
        ('Volume Z-Score', lambda: volume_zscore(data['volumes'], 20)),
        ('ADX', lambda: adx(data['highs'], data['lows'], 14))
    ]
    
    total_time = 0
    results = {}
    
    for name, func in indicators:
        # Warm up
        func()
        
        # Benchmark
        iterations = 100
        start_time = time.perf_counter()
        
        for _ in range(iterations):
            result = func()
        
        end_time = time.perf_counter()
        elapsed = (end_time - start_time) * 1000  # Convert to milliseconds
        avg_time = elapsed / iterations
        
        results[name] = {
            'time_ms': avg_time,
            'value': result
        }
        total_time += avg_time
        
        print(f"  {name:15}: {avg_time:.3f} ms (value: {result:.4f})")
    
    print(f"\nTotal time: {total_time:.3f} ms")
    
    if total_time < 100.0:
        print("✅ PASSED: All indicators completed under 100ms")
        return True
    else:
        print("❌ FAILED: Total time exceeds 100ms")
        return False

def test_edge_cases():
    """Test edge cases and boundary conditions."""
    print("\n=== Edge Case Tests ===")
    
    # Test with minimal data
    print("Testing with minimal data...")
    minimal_data = {
        'highs': [100, 101, 102, 103, 104, 105],
        'lows': [99, 100, 101, 102, 103, 104],
        'closes': [99.5, 100.5, 101.5, 102.5, 103.5, 104.5],
        'volumes': [100, 120, 110, 130, 115, 125]
    }
    
    try:
        ema_val = ema(minimal_data['closes'], 3)
        print(f"  EMA(3) with minimal data: {ema_val:.4f}")
        
        rsi_val = rsi(minimal_data['closes'], 3)
        print(f"  RSI(3) with minimal data: {rsi_val:.2f}")
        
        atr_val = atr(minimal_data['highs'], minimal_data['lows'], minimal_data['closes'], 3)
        print(f"  ATR(3) with minimal data: {atr_val:.4f}")
        
    except Exception as e:
        print(f"  Error with minimal data: {e}")
        return False
    
    # Test with constant prices
    print("Testing with constant prices...")
    constant_data = {
        'highs': [100] * 20,
        'lows': [99] * 20,
        'closes': [99.5] * 20,
        'volumes': [100] * 20
    }
    
    try:
        ema_const = ema(constant_data['closes'], 10)
        rsi_const = rsi(constant_data['closes'], 10)
        atr_const = atr(constant_data['highs'], constant_data['lows'], constant_data['closes'], 10)
        vol_z_const = volume_zscore(constant_data['volumes'], 10)
        adx_const = adx(constant_data['highs'], constant_data['lows'], 10)
        
        print(f"  EMA with constant prices: {ema_const:.4f}")
        print(f"  RSI with constant prices: {rsi_const:.2f}")
        print(f"  ATR with constant prices: {atr_const:.4f}")
        print(f"  Volume Z-Score with constant volume: {vol_z_const:.4f}")
        print(f"  ADX with constant prices: {adx_const:.4f}")
        
        # Validate expected values
        assert abs(ema_const - 99.5) < 0.01, "EMA should equal constant price"
        assert abs(rsi_const - 50.0) < 1.0, "RSI should be 50 for flat prices"
        assert abs(atr_const - 1.0) < 0.01, "ATR should be 1.0 (high - low)"
        assert abs(vol_z_const) < 0.01, "Volume Z-Score should be 0 for constant volume"
        assert abs(adx_const) < 0.01, "ADX should be 0 for no trend"
        
    except Exception as e:
        print(f"  Error with constant data: {e}")
        return False
    
    print("✅ All edge case tests passed!")
    return True

if __name__ == "__main__":
    print("Technical Indicators Performance Test\n")
    
    success = True
    success &= benchmark_indicators()
    success &= test_edge_cases()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)