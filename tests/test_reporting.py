import pytest
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from src.reporting.summarizer import DailySummary, ReportGenerator
from src.reporting.formatters import format_daily_summary, format_summary_csv
from src.database import (
    init_db, create_schema, insert_signal, insert_warning, 
    query_signals_by_date, query_warnings_by_date, 
    query_closed_positions_by_date, query_uptime,
    record_heartbeat, transaction
)

@pytest.fixture
def db_conn():
    conn = init_db(":memory:")
    create_schema(conn)
    return conn

@pytest.mark.asyncio
async def test_daily_summary_calculation(db_conn):
    date = "2025-01-15"
    
    # Insert some signals
    with transaction(db_conn):
        db_conn.execute(
            "INSERT INTO signals (timestamp, symbol, timeframe, side, confidence, regime) VALUES (?, ?, ?, ?, ?, ?)",
            (f"{date} 10:00:00", "BTCUSDT", "1h", "LONG", 0.8, "TREND_UP")
        )
        db_conn.execute(
            "INSERT INTO signals (timestamp, symbol, timeframe, side, confidence, regime) VALUES (?, ?, ?, ?, ?, ?)",
            (f"{date} 11:00:00", "ETHUSDT", "1h", "SHORT", 0.6, "RANGE")
        )
        
        # Insert some warnings
        db_conn.execute(
            "INSERT INTO warnings (timestamp, severity, warning_type, message) VALUES (?, ?, ?, ?)",
            (f"{date} 12:00:00", "WARNING", "PRICE_SHOCK", "ETH dropped")
        )
        db_conn.execute(
            "INSERT INTO warnings (timestamp, severity, warning_type, message) VALUES (?, ?, ?, ?)",
            (f"{date} 13:00:00", "CRITICAL", "API_ERROR", "MEXC down")
        )
        
        # Insert closed positions
        db_conn.execute(
            "INSERT INTO paper_positions (signal_id, status, exit_time, pnl) VALUES (?, ?, ?, ?)",
            (1, "CLOSED", f"{date} 15:00:00", 1.5)
        )
        db_conn.execute(
            "INSERT INTO paper_positions (signal_id, status, exit_time, pnl) VALUES (?, ?, ?, ?)",
            (2, "CLOSED", f"{date} 16:00:00", -0.5)
        )
        
        # Insert heartbeats (60 for 1 hour)
        for i in range(60):
            db_conn.execute(
                "INSERT INTO heartbeats (timestamp) VALUES (?)",
                (f"{date} 10:{i:02d}:00",)
            )

    generator = ReportGenerator()
    summary = await generator.generate_daily_summary(db_conn, date, 100)
    
    assert summary.date == date
    assert summary.total_signals == 2
    assert summary.signals_by_type["LONG"] == 1
    assert summary.signals_by_type["SHORT"] == 1
    assert summary.avg_confidence == 0.7
    assert summary.warnings_triggered == 2
    assert summary.warnings_by_severity["WARNING"] == 1
    assert summary.warnings_by_severity["CRITICAL"] == 1
    assert "MEXC down" in summary.critical_events
    assert summary.paper_positions_closed == 2
    assert summary.paper_profit_loss_r == 1.0
    assert summary.win_rate == 50.0
    assert summary.uptime_hours == 1.0
    assert len(summary.top_signals) == 2
    assert summary.metadata["universe_size"] == 100

def test_format_daily_summary():
    summary = DailySummary(
        date="2025-01-15",
        total_signals=10,
        signals_by_type={"LONG": 6, "SHORT": 4},
        signals_by_regime={"TREND_UP": 5, "RANGE": 5},
        avg_confidence=0.75,
        warnings_triggered=1,
        warnings_by_severity={"WARNING": 1},
        paper_positions_closed=2,
        paper_profit_loss_r=1.5,
        win_rate=50.0,
        max_drawdown_r=0.5,
        uptime_hours=24.0,
        top_signals=[{"symbol": "BTC", "timeframe": "1h", "side": "LONG", "confidence": 0.85}],
        metadata={"universe_size": 100}
    )
    
    formatted = format_daily_summary(summary)
    assert "Daily Summary - 2025-01-15" in formatted
    assert "Total: 10" in formatted
    assert "LONG: 6 | SHORT: 4" in formatted
    assert "Avg Confidence: 75%" in formatted
    assert "PnL: +1.50R" in formatted
    assert "Uptime: 24.0h" in formatted
    assert "BTC 1h LONG (85%)" in formatted

def test_format_summary_csv():
    summary = DailySummary(
        date="2025-01-15",
        total_signals=10,
        avg_confidence=0.75,
        warnings_triggered=1,
        paper_profit_loss_r=1.5,
        win_rate=50.0,
        uptime_hours=24.0
    )
    
    csv_row = format_summary_csv(summary)
    assert csv_row == "2025-01-15,10,0.7500,1,1.5000,50.00,24.00"

@pytest.mark.asyncio
async def test_edge_cases_empty(db_conn):
    date = "2025-01-16"
        
    generator = ReportGenerator()
    summary = await generator.generate_daily_summary(db_conn, date, 0)
    
    assert summary.date == date
    assert summary.total_signals == 0
    assert summary.paper_positions_closed == 0
    assert summary.paper_profit_loss_r == 0.0
    assert summary.win_rate == 0.0
    assert summary.uptime_hours == 0.0
