#!/usr/bin/env python3
"""Simple test script for technical indicators."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from indicators import ema, rsi, atr, atr_percent, vwap, volume_zscore, adx

def test_indicators():
    """Test all indicators with sample data."""
    print("=== Technical Indicators Test ===")
    
    # Sample datasets for testing
    test_datasets = [
        {
            "name": "BTC trending up",
            "highs": [47000, 47500, 47800, 48200, 48500, 48800, 49100, 49400, 49700, 50000],
            "lows": [46500, 47100, 47400, 47800, 48100, 48400, 48700, 49000, 49300, 49600],
            "closes": [47200, 47650, 48100, 47950, 48420, 48600, 48750, 49050, 49200, 49380],
            "volumes": [1250, 1180, 1420, 980, 1350, 1100, 1200, 1450, 1380, 1150]
        },
        {
            "name": "ETH sideways movement", 
            "highs": [3000, 3010, 3005, 3015, 3008, 3012, 3006, 3014, 3009, 3011],
            "lows": [2990, 2995, 2992, 2998, 2994, 2996, 2993, 2997, 2995, 2997],
            "closes": [2995, 3002, 2998, 3006, 3001, 3008, 2999, 3009, 3003, 3005],
            "volumes": [5000, 5200, 4800, 5500, 5100, 5300, 4900, 5400, 5200, 5100]
        },
        {
            "name": "High volatility ADA",
            "highs": [0.5, 0.52, 0.48, 0.55, 0.47, 0.58, 0.45, 0.60, 0.44, 0.62],
            "lows": [0.48, 0.49, 0.46, 0.50, 0.45, 0.52, 0.43, 0.54, 0.42, 0.56],
            "closes": [0.49, 0.51, 0.47, 0.53, 0.46, 0.56, 0.44, 0.58, 0.43, 0.60],
            "volumes": [100000, 120000, 95000, 110000, 90000, 130000, 85000, 125000, 80000, 115000]
        }
    ]
    
    # Extended datasets for indicators that need more data
    extended_datasets = []
    for dataset in test_datasets:
        extended = dataset.copy()
        # Extend data for 14-period indicators
        extended["highs"] = dataset["highs"] * 2  # 20 data points
        extended["lows"] = dataset["lows"] * 2
        extended["closes"] = dataset["closes"] * 2
        extended["volumes"] = dataset["volumes"] * 2
        extended_datasets.append(extended)
    
    for i, dataset in enumerate(extended_datasets, 1):
        print(f"\nDataset {i}: {dataset['name']}")
        print("-" * 40)
        
        try:
            # Calculate all indicators
            ema_14 = ema(dataset['closes'], 14)
            rsi_14 = rsi(dataset['closes'], 14)
            atr_14 = atr(dataset['highs'], dataset['lows'], dataset['closes'], 14)
            atr_pct_14 = atr_percent(dataset['highs'], dataset['lows'], dataset['closes'], 14)
            vwap_val = vwap(dataset['highs'], dataset['lows'], dataset['closes'], dataset['volumes'])
            vol_zscore = volume_zscore(dataset['volumes'], 20)
            adx_14 = adx(dataset['highs'], dataset['lows'], 14)
            
            print(f"  EMA(14):     {ema_14:.4f}")
            print(f"  RSI(14):     {rsi_14:.2f}")
            print(f"  ATR(14):     {atr_14:.4f}")
            print(f"  ATR%:        {atr_pct_14:.2f}%")
            print(f"  VWAP:        {vwap_val:.4f}")
            print(f"  Vol Z-Score: {vol_zscore:.2f}")
            print(f"  ADX(14):     {adx_14:.2f}")
            
            # Validate ranges
            assert 0.0 <= rsi_14 <= 100.0, f"RSI out of range: {rsi_14}"
            assert atr_14 >= 0.0, f"ATR negative: {atr_14}"
            assert atr_pct_14 >= 0.0, f"ATR% negative: {atr_pct_14}"
            assert 0.0 <= adx_14 <= 100.0, f"ADX out of range: {adx_14}"
            
        except Exception as e:
            print(f"  ERROR: {e}")
            return False
    
    print("\n=== All indicators tested successfully! ===")
    return True

if __name__ == "__main__":
    success = test_indicators()
    sys.exit(0 if success else 1)