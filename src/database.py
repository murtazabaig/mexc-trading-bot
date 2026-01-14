"""Database management for MEXC Futures Signal Bot."""

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from contextlib import contextmanager

from .logger import get_logger

logger = get_logger()


class DatabaseManager:
    """Manages SQLite database connections and schema operations."""
    
    def __init__(self, db_path: Path):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[sqlite3.Connection] = None
        
        logger.info(f"Database initialized at: {self.db_path.resolve()}")
    
    def initialize_schema(self) -> None:
        """Initialize database schema (creates all tables if they don't exist)."""
        schema_statements = self._get_schema_statements()
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Enable foreign keys
                cursor.execute("PRAGMA foreign_keys = ON")
                
                for statement in schema_statements:
                    cursor.execute(statement)
                
                # Add indexes for performance
                self._create_indexes(cursor)
                
                conn.commit()
                logger.success("Database schema initialized successfully")
                
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize database schema: {e}")
            raise
    
    def _get_schema_statements(self) -> List[str]:
        """Get all database schema creation statements."""
        return [
            # Signals table - stores all generated trading signals
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                signal_type TEXT NOT NULL CHECK (signal_type IN ('LONG', 'SHORT')),
                entry_price REAL NOT NULL,
                stop_loss REAL,
                take_profit REAL,
                confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
                strength REAL CHECK (strength >= 0 AND strength <= 1),
                indicators TEXT DEFAULT '[]',  -- JSON array of indicator values
                regime TEXT,  -- Market regime (trending, ranging, volatile)
                volume_24h REAL,
                spread_percent REAL,
                atr_percent REAL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME,
                status TEXT DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'TRIGGERED', 'EXPIRED', 'CANCELLED')),
                notes TEXT,
                
                -- Indexing optimization
                UNIQUE(symbol, timeframe, timestamp, signal_type)
            )
            """,
            
            # Signal executions - track when signals were triggered/executed
            """
            CREATE TABLE IF NOT EXISTS signal_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                execution_type TEXT NOT NULL CHECK (execution_type IN ('PAPER', 'LIVE')),
                position_id INTEGER,  -- References position in paper_positions or live_positions
                entry_price REAL NOT NULL,
                filled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED')),
                
                FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE
            )
            """,
            
            # Paper trading positions
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                position_type TEXT NOT NULL CHECK (position_type IN ('LONG', 'SHORT')),
                entry_price REAL NOT NULL,
                position_size_usdt REAL NOT NULL,
                leverage REAL DEFAULT 1.0,
                stop_loss REAL,
                take_profit REAL,
                open_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                close_at DATETIME,
                close_price REAL,
                pnl_usdt REAL DEFAULT 0.0,
                pnl_percent REAL DEFAULT 0.0,
                status TEXT DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'CLOSED')),
                close_reason TEXT,  -- TP_HIT, SL_HIT, MANUAL, EXPIRED
                
                FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE SET NULL
            )
            """,
            
            # Warnings and alerts table
            """
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                warning_type TEXT NOT NULL,
                severity TEXT NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
                message TEXT NOT NULL,
                context TEXT,  -- JSON string with additional context
                notified BOOLEAN DEFAULT FALSE,
                acknowledged BOOLEAN DEFAULT FALSE,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """,
            
            # System parameters and configuration
            """
            CREATE TABLE IF NOT EXISTS system_parameters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                param_key TEXT UNIQUE NOT NULL,
                param_value TEXT NOT NULL,
                description TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_by TEXT DEFAULT 'system'
            )
            """,
            
            # Market data snapshots
            """
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                snapshot_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL,
                quote_volume REAL,
                number_of_trades INTEGER,
                snapshot_data TEXT NOT NULL,  -- Full OHLCV and indicators as JSON
                
                UNIQUE(symbol, timeframe, snapshot_timestamp)
            )
            """,
            
            # Signal statistics and performance tracking
            """
            CREATE TABLE IF NOT EXISTS signal_statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timeframe TEXT NOT NULL,
                date DATE NOT NULL,
                total_signals INTEGER DEFAULT 0,
                long_signals INTEGER DEFAULT 0,
                short_signals INTEGER DEFAULT 0,
                triggered_signals INTEGER DEFAULT 0,
                profitable_signals INTEGER DEFAULT 0,
                avg_roi_percent REAL DEFAULT 0.0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(timeframe, date)
            )
            """
        ]
    
    def _create_indexes(self, cursor: sqlite3.Cursor) -> None:
        """Create performance-optimized indexes."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_signals_timeframe ON signals(timeframe)",
            "CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)",
            "CREATE INDEX IF NOT EXISTS idx_signals_symbol_tf_time ON signals(symbol, timeframe, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_signals_expires ON signals(expires_at) WHERE status = 'ACTIVE'",
            
            "CREATE INDEX IF NOT EXISTS idx_executions_signal_id ON signal_executions(signal_id)",
            "CREATE INDEX IF NOT EXISTS idx_executions_status ON signal_executions(status)",
            
            "CREATE INDEX IF NOT EXISTS idx_positions_symbol ON paper_positions(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_positions_status ON paper_positions(status)",
            "CREATE INDEX IF NOT EXISTS idx_positions_open_at ON paper_positions(open_at)",
            
            "CREATE INDEX IF NOT EXISTS idx_warnings_type ON warnings(warning_type)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_severity ON warnings(severity)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_timestamp ON warnings(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_notified ON warnings(notified) WHERE notified = FALSE",
            
            "CREATE INDEX IF NOT EXISTS idx_snapshots_symbol_tf ON market_snapshots(symbol, timeframe)",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON market_snapshots(snapshot_timestamp)",
            
            "CREATE INDEX IF NOT EXISTS idx_stats_timeframe_date ON signal_statistics(timeframe, date)",
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    @contextmanager
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection with automatic commit/rollback."""
        
        if self._connection is None or not self._connection:
            self._connection = sqlite3.connect(
                self.db_path,
                timeout=30.0,  # 30-second timeout
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            )
            
            self._connection.row_factory = sqlite3.Row
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging for better concurrency
            self._connection.execute("PRAGMA synchronous = NORMAL")  # Balance between safety and performance
        
        try:
            yield self._connection
        except sqlite3.Error:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics and health information."""
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Table counts
                cursor.execute("SELECT COUNT(*) FROM signals")
                stats["total_signals"] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM paper_positions")
                stats["total_positions"] = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM warnings")
                stats["total_warnings"] = cursor.fetchone()[0]
                
                # Recent activity (last 24 hours)
                cursor.execute(
                    "SELECT COUNT(*) FROM signals WHERE timestamp >= datetime('now', '-1 day')"
                )
                stats["signals_24h"] = cursor.fetchone()[0]
                
                # Database size
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
                stats["database_size_mb"] = round((page_count * page_size) / (1024 * 1024), 2)
                
                return stats
        except sqlite3.Error as e:
            logger.error(f"Failed to get database statistics: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Tuple[Any, ...]]:
        """Execute a query and return results."""
        
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(query, params)
                results = cursor.fetchall()
                conn.commit()
                return results
        except sqlite3.Error as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute a query with multiple parameter sets."""
        
        try:
            with self.get_connection() as conn:
                cursor = conn.executemany(query, params_list)
                rowcount = cursor.rowcount
                conn.commit()
                return rowcount
        except sqlite3.Error as e:
            logger.error(f"Batch query execution failed: {e}")
            raise
    
    def close(self) -> None:
        """Close the database connection."""
        
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Database connection closed")


def create_database_manager(db_path: Optional[Path] = None) -> DatabaseManager:
    """Create and initialize a database manager."""
    
    if db_path is None:
        db_path = Path("data/signals.db")
    
    db_manager = DatabaseManager(db_path)
    db_manager.initialize_schema()
    
    return db_manager