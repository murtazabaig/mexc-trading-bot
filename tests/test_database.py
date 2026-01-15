import unittest
import sqlite3
import os
from src.database import (
    init_db, create_schema, insert_signal, insert_warning,
    insert_params_snapshot, query_recent_signals, query_active_warnings,
    transaction, get_last_processed_candle, update_processed_candle,
    clear_processed_candles
)

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Use in-memory database for testing
        self.conn = init_db(":memory:")
        create_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_schema_creation(self):
        cursor = self.conn.cursor()
        # Check if tables exist
        tables = ["signals", "warnings", "params_snapshot", "paper_positions", "processed_candles", "heartbeats"]
        for table in tables:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            self.assertIsNotNone(cursor.fetchone(), f"Table {table} should exist")

    def test_insert_and_query_signal(self):
        signal_data = {
            "symbol": "ETHUSDT",
            "timeframe": "4h",
            "side": "SHORT",
            "confidence": 0.75,
            "regime": "RANGING",
            "entry_price": 2500.0,
            "reason": {"indicator": "RSI Overbought"},
            "metadata": {"source": "test"}
        }
        signal_id = insert_signal(self.conn, signal_data)
        self.assertGreater(signal_id, 0)

        results = query_recent_signals(self.conn, symbol="ETHUSDT")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["symbol"], "ETHUSDT")
        self.assertEqual(results[0]["side"], "SHORT")
        self.assertEqual(results[0]["reason"], {"indicator": "RSI Overbought"})

    def test_insert_and_query_warning(self):
        warning_data = {
            "severity": "CRITICAL",
            "warning_type": "LIQUIDITY_DROP",
            "message": "Liquidity dropped below threshold",
            "triggered_value": 1000.0,
            "threshold": 5000.0,
            "action_taken": "NONE",
            "metadata": {"depth": "shallow"}
        }
        warning_id = insert_warning(self.conn, warning_data)
        self.assertGreater(warning_id, 0)

        results = query_active_warnings(self.conn, hours=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["warning_type"], "LIQUIDITY_DROP")
        self.assertEqual(results[0]["severity"], "CRITICAL")

    def test_params_snapshot_deduplication(self):
        config = {"param1": "value1", "param2": 2}
        id1 = insert_params_snapshot(self.conn, config)
        id2 = insert_params_snapshot(self.conn, config)
        
        self.assertEqual(id1, id2, "Should return same ID for same config (deduplication)")
        
        config2 = {"param1": "value1", "param2": 3}
        id3 = insert_params_snapshot(self.conn, config2)
        self.assertNotEqual(id1, id3, "Should return different ID for different config")

    def test_transaction_rollback(self):
        # Test rollback on error
        try:
            with transaction(self.conn):
                # Valid insert
                insert_warning(self.conn, {"severity": "INFO", "message": "Test"})
                # Invalid insert (missing required column symbol in signals)
                self.conn.execute("INSERT INTO signals (symbol) VALUES (NULL)")
        except Exception:
            pass
        
        # Check that no warning was inserted due to rollback
        results = query_active_warnings(self.conn)
        self.assertEqual(len(results), 0, "Transaction should have rolled back all changes")

    def test_get_last_processed_candle_not_found(self):
        """Test get_last_processed_candle returns 0 when not found."""
        ts = get_last_processed_candle(self.conn, "NONEXISTENT", "1h")
        self.assertEqual(ts, 0, "Should return 0 for non-existent record")

    def test_update_and_get_processed_candle(self):
        """Test update_processed_candle and get_last_processed_candle."""
        symbol = "BTCUSDT"
        timeframe = "1h"
        ts = 1640995200000

        # Update processed candle
        update_processed_candle(self.conn, symbol, timeframe, ts)

        # Retrieve it
        retrieved_ts = get_last_processed_candle(self.conn, symbol, timeframe)
        self.assertEqual(retrieved_ts, ts, "Should retrieve the same timestamp")

    def test_update_processed_candle_overwrites(self):
        """Test that update_processed_candle overwrites existing record."""
        symbol = "BTCUSDT"
        timeframe = "1h"
        ts1 = 1640995200000
        ts2 = 1640998800000  # 1 hour later

        # Insert first timestamp
        update_processed_candle(self.conn, symbol, timeframe, ts1)
        self.assertEqual(get_last_processed_candle(self.conn, symbol, timeframe), ts1)

        # Update with new timestamp
        update_processed_candle(self.conn, symbol, timeframe, ts2)
        self.assertEqual(get_last_processed_candle(self.conn, symbol, timeframe), ts2)

        # Verify only one record exists
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM processed_candles WHERE symbol = ? AND timeframe = ?",
            (symbol, timeframe)
        )
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1, "Should have only one record after update")

    def test_processed_candles_unique_constraint(self):
        """Test that (symbol, timeframe) is unique."""
        symbol = "BTCUSDT"
        timeframe = "1h"
        ts1 = 1640995200000
        ts2 = 1640998800000

        # Insert first record
        update_processed_candle(self.conn, symbol, timeframe, ts1)

        # Update with same symbol/timeframe
        update_processed_candle(self.conn, symbol, timeframe, ts2)

        # Verify only one record exists
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM processed_candles WHERE symbol = ? AND timeframe = ?",
            (symbol, timeframe)
        )
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1, "Unique constraint should prevent duplicates")

    def test_processed_candles_multiple_symbols(self):
        """Test tracking processed candles for multiple symbols."""
        update_processed_candle(self.conn, "BTCUSDT", "1h", 1640995200000)
        update_processed_candle(self.conn, "ETHUSDT", "1h", 1640995200000)
        update_processed_candle(self.conn, "BTCUSDT", "5m", 1640995200000)

        self.assertEqual(get_last_processed_candle(self.conn, "BTCUSDT", "1h"), 1640995200000)
        self.assertEqual(get_last_processed_candle(self.conn, "ETHUSDT", "1h"), 1640995200000)
        self.assertEqual(get_last_processed_candle(self.conn, "BTCUSDT", "5m"), 1640995200000)

    def test_clear_processed_candles(self):
        """Test clear_processed_candles removes all records."""
        # Add some records
        update_processed_candle(self.conn, "BTCUSDT", "1h", 1640995200000)
        update_processed_candle(self.conn, "ETHUSDT", "1h", 1640995200000)
        update_processed_candle(self.conn, "BTCUSDT", "5m", 1640995200000)

        # Verify records exist
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_candles")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 3, "Should have 3 records before clearing")

        # Clear all
        clear_processed_candles(self.conn)

        # Verify all cleared
        cursor.execute("SELECT COUNT(*) FROM processed_candles")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0, "Should have 0 records after clearing")

if __name__ == "__main__":
    unittest.main()
