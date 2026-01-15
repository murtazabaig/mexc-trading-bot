"""Database management for MEXC Futures Signal Bot."""

import sqlite3
import json
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager

from .logger import get_logger

logger = get_logger(__name__)

def init_db(db_path: str = "data/signals.db") -> sqlite3.Connection:
    """Initialize connection to the SQLite database."""
    path = Path(db_path)
    if db_path != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    )
    conn.row_factory = sqlite3.Row
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

@contextmanager
def transaction(conn: sqlite3.Connection):
    """Context manager for safe transaction handling."""
    try:
        with conn:
            yield conn
    except Exception as e:
        logger.error(f"Transaction failed: {e}")
        raise

def create_schema(conn: sqlite3.Connection):
    """Create tables on startup if they do not exist."""
    with transaction(conn):
        cursor = conn.cursor()
        
        # signals table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            symbol TEXT NOT NULL,
            timeframe TEXT,
            side TEXT,  -- LONG/SHORT
            confidence REAL,
            regime TEXT,
            entry_price REAL,
            entry_band_min REAL,
            entry_band_max REAL,
            stop_loss REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            trailing_start_tp REAL,
            trailing_amount REAL,
            time_stop_bars INTEGER,
            reason TEXT,  -- JSON blob describing confluence
            metadata JSON  -- Additional context
        );
        """)
        
        # warnings table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            severity TEXT,  -- INFO/WARNING/CRITICAL
            warning_type TEXT,  -- BTC_SHOCK, BREADTH_COLLAPSE, CORRELATION_SPIKE, etc.
            message TEXT,
            triggered_value REAL,
            threshold REAL,
            action_taken TEXT,  -- e.g., PAUSED_SIGNALS
            metadata JSON
        );
        """)
        
        # params_snapshot table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS params_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            config_hash TEXT UNIQUE,
            config_json JSON  -- Full config at this snapshot
        );
        """)
        
        # paper_positions table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT,  -- OPEN/CLOSED
            side TEXT,    -- LONG/SHORT
            size REAL,
            entry_price REAL,
            entry_time DATETIME,
            exit_price REAL,
            exit_time DATETIME,
            stop_loss REAL,
            take_profit REAL,
            pnl REAL,
            pnl_percent REAL,
            pnl_r REAL,
            duration_hours REAL,
            max_drawdown REAL,
            exit_reason TEXT,
            metadata JSON,
            FOREIGN KEY(signal_id) REFERENCES signals(id)
        );
        """)

        # heartbeats table for uptime tracking
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS heartbeats (
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # processed_candles table for tracking closed candles to prevent look-ahead bias
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            last_closed_ts INTEGER NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timeframe)
        );
        """)

    logger.info("Database schema verified/created.")

def insert_signal(conn: sqlite3.Connection, signal_dict: Dict[str, Any]) -> int:
    """Insert a new signal into the database."""
    query = """
    INSERT INTO signals (
        symbol, timeframe, side, confidence, regime, entry_price,
        entry_band_min, entry_band_max, stop_loss, tp1, tp2, tp3,
        trailing_start_tp, trailing_amount, time_stop_bars, reason, metadata
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Prepare JSON fields
    reason = json.dumps(signal_dict.get('reason', {}))
    metadata = json.dumps(signal_dict.get('metadata', {}))
    
    params = (
        signal_dict.get('symbol'),
        signal_dict.get('timeframe'),
        signal_dict.get('side'),
        signal_dict.get('confidence'),
        signal_dict.get('regime'),
        signal_dict.get('entry_price'),
        signal_dict.get('entry_band_min'),
        signal_dict.get('entry_band_max'),
        signal_dict.get('stop_loss'),
        signal_dict.get('tp1'),
        signal_dict.get('tp2'),
        signal_dict.get('tp3'),
        signal_dict.get('trailing_start_tp'),
        signal_dict.get('trailing_amount'),
        signal_dict.get('time_stop_bars'),
        reason,
        metadata
    )
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.lastrowid

def insert_warning(conn: sqlite3.Connection, warning_dict: Dict[str, Any]) -> int:
    """Insert a new warning into the database."""
    query = """
    INSERT INTO warnings (
        severity, warning_type, message, triggered_value, threshold, action_taken, metadata
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    metadata = json.dumps(warning_dict.get('metadata', {}))
    
    params = (
        warning_dict.get('severity'),
        warning_dict.get('warning_type'),
        warning_dict.get('message'),
        warning_dict.get('triggered_value'),
        warning_dict.get('threshold'),
        warning_dict.get('action_taken'),
        metadata
    )
    
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor.lastrowid

def insert_params_snapshot(conn: sqlite3.Connection, config_dict: Dict[str, Any]) -> int:
    """Insert a configuration snapshot if it has changed."""
    config_json = json.dumps(config_dict, sort_keys=True)
    config_hash = hashlib.sha256(config_json.encode()).hexdigest()
    
    query = """
    INSERT OR IGNORE INTO params_snapshot (config_hash, config_json)
    VALUES (?, ?)
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (config_hash, config_json))
    if cursor.rowcount == 0:
        # Already exists, fetch the id
        cursor.execute("SELECT id FROM params_snapshot WHERE config_hash = ?", (config_hash,))
        result = cursor.fetchone()
        return result['id'] if result else -1
    return cursor.lastrowid

def query_recent_signals(conn: sqlite3.Connection, limit: int = 10, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Query recent signals from the database."""
    if symbol:
        query = "SELECT * FROM signals WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?"
        params = (symbol, limit)
    else:
        query = "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?"
        params = (limit,)
        
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    
    results = []
    for row in rows:
        d = dict(row)
        d['reason'] = json.loads(d['reason']) if d.get('reason') else {}
        d['metadata'] = json.loads(d['metadata']) if d.get('metadata') else {}
        results.append(d)
    return results

def query_active_warnings(conn: sqlite3.Connection, hours: int = 24) -> List[Dict[str, Any]]:
    """Query active warnings within the last N hours."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
    query = "SELECT * FROM warnings WHERE timestamp >= ? ORDER BY timestamp DESC"
    
    cursor = conn.execute(query, (since,))
    rows = cursor.fetchall()
    
    results = []
    for row in rows:
        d = dict(row)
        d['metadata'] = json.loads(d['metadata']) if d.get('metadata') else {}
        results.append(d)
    return results

def query_signals_by_date(conn: sqlite3.Connection, date: str) -> List[Dict[str, Any]]:
    """Query all signals for a specific date (YYYY-MM-DD)."""
    query = "SELECT * FROM signals WHERE date(timestamp) = ? ORDER BY timestamp ASC"
    cursor = conn.execute(query, (date,))
    rows = cursor.fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d['reason'] = json.loads(d['reason']) if d.get('reason') else {}
        d['metadata'] = json.loads(d['metadata']) if d.get('metadata') else {}
        results.append(d)
    return results

def query_warnings_by_date(conn: sqlite3.Connection, date: str) -> List[Dict[str, Any]]:
    """Query all warnings for a specific date (YYYY-MM-DD)."""
    query = "SELECT * FROM warnings WHERE date(timestamp) = ? ORDER BY timestamp ASC"
    cursor = conn.execute(query, (date,))
    rows = cursor.fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d['metadata'] = json.loads(d['metadata']) if d.get('metadata') else {}
        results.append(d)
    return results

def query_closed_positions_by_date(conn: sqlite3.Connection, date: str) -> List[Dict[str, Any]]:
    """Query all closed positions for a specific date (YYYY-MM-DD)."""
    query = "SELECT * FROM paper_positions WHERE status = 'CLOSED' AND date(exit_time) = ? ORDER BY exit_time ASC"
    cursor = conn.execute(query, (date,))
    rows = cursor.fetchall()
    return [dict(row) for row in rows]

def query_uptime(conn: sqlite3.Connection, date: str) -> float:
    """Calculate total uptime in hours for a specific date (YYYY-MM-DD)."""
    query = "SELECT count(*) as count FROM heartbeats WHERE date(timestamp) = ?"
    cursor = conn.execute(query, (date,))
    result = cursor.fetchone()
    if result:
        # Assuming heartbeat every minute
        return result['count'] / 60.0
    return 0.0

def record_heartbeat(conn: sqlite3.Connection):
    """Record a heartbeat to track uptime."""
    with transaction(conn):
        conn.execute("INSERT INTO heartbeats DEFAULT VALUES")


def get_last_processed_candle(conn: sqlite3.Connection, symbol: str, timeframe: str) -> int:
    """Return the last processed candle timestamp for symbol/timeframe.

    Args:
        conn: Database connection
        symbol: Trading symbol
        timeframe: Timeframe (e.g., '1h', '5m', '4h')

    Returns:
        Last processed candle timestamp (ms) or 0 if not found
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT last_closed_ts FROM processed_candles WHERE symbol = ? AND timeframe = ?",
        (symbol, timeframe)
    )
    row = cursor.fetchone()
    return row[0] if row else 0


def update_processed_candle(conn: sqlite3.Connection, symbol: str, timeframe: str, ts: int):
    """Update the last processed candle timestamp.

    Args:
        conn: Database connection
        symbol: Trading symbol
        timeframe: Timeframe (e.g., '1h', '5m', '4h')
        ts: Timestamp of the closed candle in milliseconds
    """
    with transaction(conn):
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO processed_candles (symbol, timeframe, last_closed_ts)
               VALUES (?, ?, ?)
               ON CONFLICT(symbol, timeframe) DO UPDATE SET
               last_closed_ts = ?, processed_at = CURRENT_TIMESTAMP""",
            (symbol, timeframe, ts, ts)
        )


def clear_processed_candles(conn: sqlite3.Connection):
    """Clear all processed candles table - useful for testing or restart.

    Args:
        conn: Database connection
    """
    with transaction(conn):
        conn.execute("DELETE FROM processed_candles")
    logger.info("Processed candles table cleared")
