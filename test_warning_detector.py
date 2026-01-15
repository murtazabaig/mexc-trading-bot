#!/usr/bin/env python3
"""Simple test script for warning detector functionality."""

import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.warnings.detector import WarningDetector


class MockExchange:
    """Mock MEXC exchange for testing."""
    
    def __init__(self):
        self.fetch_ohlcv_calls = 0
        
    def fetch_ohlcv(self, symbol, timeframe='1h', limit=100):
        """Mock OHLCV data fetch."""
        self.fetch_ohlcv_calls += 1
        
        if 'BTC' in symbol:
            # BTC data with 6% increase (should trigger WARNING)
            return [
                [1710000000000, 50000.0, 50500.0, 49900.0, 50200.0, 100.0],  # Previous candle
                [1710003600000, 50200.0, 53300.0, 50100.0, 53200.0, 150.0]   # Current candle (+6%)
            ]
        elif 'ETH' in symbol:
            # ETH data with 2% decrease
            return [
                [1710000000000, 3000.0, 3050.0, 2990.0, 3020.0, 50.0],
                [1710003600000, 3020.0, 3030.0, 2950.0, 2960.0, 60.0]
            ]
        elif 'SOL' in symbol:
            # SOL data with 1% increase
            return [
                [1710000000000, 100.0, 105.0, 99.0, 102.0, 20.0],
                [1710003600000, 102.0, 104.0, 101.0, 103.0, 25.0]
            ]
        else:
            # Default data with small changes
            return [
                [1710000000000, 10.0, 10.5, 9.9, 10.2, 5.0],
                [1710003600000, 10.2, 10.4, 10.1, 10.3, 6.0]
            ]


class MockDBConnection:
    """Mock database connection for testing."""
    
    def __init__(self):
        self.warnings_inserted = []
        
    def execute(self, query, params=None):
        """Mock execute method."""
        mock_cursor = MockCursor(self)
        return mock_cursor
    
    def cursor(self):
        """Mock cursor method."""
        return MockCursor(self)
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class MockCursor:
    """Mock database cursor."""
    
    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.lastrowid = len(db_conn.warnings_inserted) + 1
        
    def execute(self, query, params):
        """Mock execute method."""
        if 'INSERT INTO warnings' in query:
            warning_data = {
                'severity': params[0],
                'warning_type': params[1],
                'message': params[2],
                'triggered_value': params[3],
                'threshold': params[4],
                'action_taken': params[5],
                'metadata': params[6]
            }
            self.db_conn.warnings_inserted.append(warning_data)
            self.lastrowid = len(self.db_conn.warnings_inserted)
        
    def fetchone(self):
        """Mock fetchone method."""
        return None


async def test_warning_detector():
    """Test the warning detector with mock data."""
    print("🧪 Testing Warning Detector...")
    
    # Create mock components
    exchange = MockExchange()
    db_conn = MockDBConnection()
    
    # Configuration
    config = {
        'btc_shock_threshold_warning': 0.05,
        'btc_shock_threshold_critical': 0.08,
        'breadth_collapse_threshold_warning': 0.40,
        'breadth_collapse_threshold_critical': 0.50,
        'correlation_spike_threshold_warning': 0.30,
        'correlation_spike_threshold_critical': 0.50
    }
    
    # Market universe
    universe = {
        'BTC/USDT:USDT': {'symbol': 'BTC/USDT:USDT'},
        'ETH/USDT:USDT': {'symbol': 'ETH/USDT:USDT'},
        'SOL/USDT:USDT': {'symbol': 'SOL/USDT:USDT'}
    }
    
    # Create warning detector
    detector = WarningDetector(
        exchange=exchange,
        db_conn=db_conn,
        config=config,
        universe=universe
    )
    
    print("✅ WarningDetector initialized")
    
    # Test BTC shock detection
    print("\n🔍 Testing BTC Shock Detection...")
    btc_warning = await detector.detect_btc_shock()
    
    if btc_warning:
        print(f"✅ BTC Shock Detected: {btc_warning['severity']}")
        print(f"   Price Change: {btc_warning['price_change_pct']:.2%}")
        print(f"   Direction: {btc_warning['direction']}")
        print(f"   Current Price: ${btc_warning['current_price']:,.2f}")
        print(f"   Previous Price: ${btc_warning['previous_price']:,.2f}")
    else:
        print("❌ No BTC shock detected")
    
    # Test breadth collapse detection
    print("\n🔍 Testing Breadth Collapse Detection...")
    symbols = list(universe.keys())
    breadth_warning = await detector.detect_breadth_collapse(symbols)
    
    if breadth_warning:
        print(f"✅ Breadth Collapse Detected: {breadth_warning['severity']}")
        print(f"   Bullish: {breadth_warning['bullish_count']}")
        print(f"   Bearish: {breadth_warning['bearish_count']}")
        print(f"   Against Trend: {breadth_warning['pct_against_trend']:.1%}")
    else:
        print("❌ No breadth collapse detected")
    
    # Test correlation spike detection
    print("\n🔍 Testing Correlation Spike Detection...")
    correlation_warnings = await detector.detect_correlation_spike(symbols)
    
    if correlation_warnings:
        print(f"✅ Found {len(correlation_warnings)} correlation spike(s)")
        for warning in correlation_warnings:
            print(f"   Symbol: {warning['symbol']}")
            print(f"   Severity: {warning['severity']}")
            print(f"   Correlation Change: {warning['correlation_change_pct']:.2%}")
    else:
        print("❌ No correlation spikes detected")
    
    # Test warning storage
    print("\n💾 Testing Warning Storage...")
    if btc_warning:
        warning_id = await detector._store_warning_in_database(btc_warning)
        print(f"✅ Warning stored with ID: {warning_id}")
        print(f"   Total warnings in DB: {len(db_conn.warnings_inserted)}")
    
    # Summary
    print(f"\n📊 Test Summary:")
    print(f"   API Calls Made: {exchange.fetch_ohlcv_calls}")
    print(f"   Warnings Generated: {detector.stats['warnings_generated']}")
    print(f"   Errors: {detector.stats['errors_count']}")
    
    print("\n🎉 Warning Detector Test Completed!")


if __name__ == "__main__":
    # Run the test
    asyncio.run(test_warning_detector())