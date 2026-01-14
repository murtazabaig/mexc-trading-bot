import unittest
import sqlite3
import os
from src.database import (
    init_db, create_schema, insert_signal, insert_warning, 
    insert_params_snapshot, query_recent_signals, query_active_warnings,
    transaction
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
        tables = ["signals", "warnings", "params_snapshot", "paper_positions"]
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

if __name__ == "__main__":
    unittest.main()
