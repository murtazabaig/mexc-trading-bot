"""Portfolio manager for enforcing trading risk controls and constraints."""

import sqlite3
import json
import numpy as np
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from ..logger import get_logger
from ..database import transaction, insert_signal

logger = get_logger(__name__)

class PortfolioManager:
    """
    Enforces real trading risk controls and constraints:
    - Max alerts per day
    - Max correlation between active positions
    - Cooldown between same-symbol signals
    - Daily loss limit in R units
    """
    
    def __init__(self, config: Any, db_conn: sqlite3.Connection, exchange: Any = None):
        """
        Initialize PortfolioManager.
        
        Args:
            config: Main configuration object
            db_conn: SQLite database connection
            exchange: CCXT exchange instance (optional, required for correlation checks)
        """
        self.config = config
        self.db_conn = db_conn
        self.exchange = exchange
        self.portfolio_config = config.portfolio
        self.trading_config = config.trading
        
        # State
        self.active_positions = []  # List of dicts
        self.daily_pnl_r = 0.0
        self.signals_today_count = 0
        self.last_reset_date = datetime.now(timezone.utc).date()
        
        self._load_state()

    def _load_state(self):
        """Load active positions and today's stats from database."""
        try:
            # 1. Load active positions
            cursor = self.db_conn.execute(
                """
                SELECT p.*, s.symbol 
                FROM paper_positions p
                JOIN signals s ON p.signal_id = s.id
                WHERE p.status = 'OPEN'
                """
            )
            rows = cursor.fetchall()
            self.active_positions = [dict(row) for row in rows]
            
            # 2. Load today's stats
            today = datetime.now(timezone.utc).date().isoformat()
            
            # Signal count (only approved ones)
            # We look for signals where metadata does not contain status: REJECTED
            cursor = self.db_conn.execute(
                "SELECT count(*) FROM signals WHERE date(timestamp) = ?",
                (today,)
            )
            # Note: We actually want to count APPROVED signals.
            # Since we store decision in metadata, we have to filter.
            
            cursor = self.db_conn.execute(
                "SELECT metadata FROM signals WHERE date(timestamp) = ?",
                (today,)
            )
            rows = cursor.fetchall()
            self.signals_today_count = 0
            for row in rows:
                meta = json.loads(row['metadata']) if row['metadata'] else {}
                if meta.get('status') == 'APPROVED':
                    self.signals_today_count += 1
            
            # Daily P&L in R units
            cursor = self.db_conn.execute(
                """
                SELECT p.*, s.side, s.entry_price as signal_entry, s.stop_loss 
                FROM paper_positions p
                JOIN signals s ON p.signal_id = s.id
                WHERE p.status = 'CLOSED' AND date(p.exit_time) = ?
                """,
                (today,)
            )
            closed_rows = cursor.fetchall()
            self.daily_pnl_r = 0.0
            for row in closed_rows:
                self.daily_pnl_r += self._calculate_r_pnl(dict(row))
                
            self.last_reset_date = datetime.now(timezone.utc).date()
            logger.info(
                f"PortfolioManager state loaded: {len(self.active_positions)} active positions, "
                f"{self.signals_today_count} signals today, {self.daily_pnl_r:.2f}R daily P&L"
            )
        except Exception as e:
            logger.error(f"Error loading PortfolioManager state: {e}")

    def _calculate_r_pnl(self, row: Dict) -> float:
        """Calculate realized P&L in R units for a closed position."""
        try:
            side = row.get('side', 'LONG')
            entry = row.get('entry_price') or row.get('signal_entry')
            exit = row.get('exit_price')
            stop_loss = row.get('stop_loss')
            
            if entry is None or exit is None or stop_loss is None:
                return 0.0
                
            if side == 'LONG':
                risk = entry - stop_loss
                if risk <= 0: return 0.0
                return (exit - entry) / risk
            else: # SHORT
                risk = stop_loss - entry
                if risk <= 0: return 0.0
                return (entry - exit) / risk
        except Exception as e:
            logger.error(f"Error calculating R P&L: {e}")
            return 0.0

    def _check_day_boundary(self):
        """Reset daily counters if UTC midnight has passed."""
        current_date = datetime.now(timezone.utc).date()
        if current_date > self.last_reset_date:
            logger.info(f"Day boundary crossed. Resetting daily counters. New date: {current_date}")
            self.signals_today_count = 0
            self.daily_pnl_r = 0.0
            self.last_reset_date = current_date

    async def add_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a new signal against all risk constraints.
        
        Returns:
            Decision dictionary with status, reason, etc.
        """
        self._check_day_boundary()
        
        # 1. Max Alerts Per Day
        if self.signals_today_count >= self.portfolio_config.max_alerts_per_day:
            reason = f"Max alerts per day ({self.portfolio_config.max_alerts_per_day}) reached"
            return self._reject(signal, reason, "MAX_ALERTS_REACHED")

        # 2. Cooldown Period
        cooldown_violation, last_time = self._check_cooldown(signal['symbol'])
        if cooldown_violation:
            reason = f"Symbol {signal['symbol']} is in cooldown. Last signal: {last_time}"
            return self._reject(signal, reason, "COOLDOWN_VIOLATION")

        # 3. Daily Loss Limit
        if self.daily_pnl_r <= -self.portfolio_config.daily_loss_limit_r:
             reason = f"Daily loss limit ({self.portfolio_config.daily_loss_limit_r}R) reached. Current P&L: {self.daily_pnl_r:.2f}R"
             return self._reject(signal, reason, "DAILY_LOSS_LIMIT_REACHED")

        # 4. Correlation Gating
        correlation_too_high, avg_corr, corr_matrix = await self._check_correlation(signal)
        if correlation_too_high:
            reason = f"Average correlation ({avg_corr:.2f}) with active positions exceeds threshold ({self.portfolio_config.max_correlation})"
            return self._reject(signal, reason, "HIGH_CORRELATION", metadata={"avg_correlation": avg_corr, "correlation_matrix": corr_matrix})

        # APPROVED if all passed
        return self._approve(signal, metadata={"avg_correlation": avg_corr, "correlation_matrix": corr_matrix})

    def _reject(self, signal: Dict[str, Any], reason: str, violation_type: str, metadata: Dict = None) -> Dict[str, Any]:
        """Record and return a REJECTED decision."""
        logger.warning(f"Signal REJECTED for {signal['symbol']}: {reason}")
        decision = {
            "status": "REJECTED",
            "reason": reason,
            "constraint_violations": [violation_type],
            "confidence_score": signal.get("confidence", 0)
        }
        self._record_signal_history(signal, decision, metadata)
        return decision

    def _approve(self, signal: Dict[str, Any], metadata: Dict = None) -> Dict[str, Any]:
        """Record and return an APPROVED decision."""
        logger.info(f"Signal APPROVED for {signal['symbol']}")
        self.signals_today_count += 1
        decision = {
            "status": "APPROVED",
            "reason": "All risk constraints passed",
            "constraint_violations": [],
            "confidence_score": signal.get("confidence", 0)
        }
        self._record_signal_history(signal, decision, metadata)
        return decision

    def _record_signal_history(self, signal: Dict[str, Any], decision: Dict[str, Any], metadata: Dict = None):
        """Persist signal and decision metadata to the database."""
        signal_to_save = signal.copy()
        sig_metadata = signal_to_save.get('metadata', {}).copy()
        sig_metadata.update(decision)
        if metadata:
            sig_metadata.update(metadata)
        signal_to_save['metadata'] = sig_metadata
        
        try:
            with transaction(self.db_conn):
                signal_id = insert_signal(self.db_conn, signal_to_save)
                logger.debug(f"Signal recorded in DB with ID: {signal_id}")
                return signal_id
        except Exception as e:
            logger.error(f"Error recording signal history: {e}")
            return None

    def _check_cooldown(self, symbol: str) -> Tuple[bool, Optional[str]]:
        """Check if symbol is within its cooldown period."""
        cooldown_mins = self.portfolio_config.cooldown_minutes
        if cooldown_mins <= 0:
            return False, None
            
        cursor = self.db_conn.execute(
            """
            SELECT timestamp FROM signals 
            WHERE symbol = ? 
            ORDER BY timestamp DESC LIMIT 1
            """,
            (symbol,)
        )
        row = cursor.fetchone()
        if row:
            ts_str = row['timestamp']
            if isinstance(ts_str, str):
                # Handle potential space instead of T
                ts_str = ts_str.replace(' ', 'T')
                last_time = datetime.fromisoformat(ts_str)
            else:
                last_time = ts_str
                
            if last_time.tzinfo is None:
                last_time = last_time.replace(tzinfo=timezone.utc)
                
            elapsed = (datetime.now(timezone.utc) - last_time).total_seconds() / 60
            if elapsed < cooldown_mins:
                return True, last_time.isoformat()
        
        return False, None

    async def _check_correlation(self, signal: Dict[str, Any]) -> Tuple[bool, float, Dict[str, float]]:
        """Calculate correlation between new signal and active positions."""
        if not self.active_positions:
            return False, 0.0, {}
            
        if not self.exchange:
            logger.warning("No exchange provided to PortfolioManager, skipping correlation check")
            return False, 0.0, {}

        symbol = signal['symbol']
        active_symbols = list(set([p['symbol'] for p in self.active_positions]))
        
        # Remove current symbol if it's already in active positions (shouldn't happen with cooldown)
        if symbol in active_symbols:
            active_symbols.remove(symbol)
            
        if not active_symbols:
            return False, 0.0, {}

        try:
            limit = 25  # For 24 hourly returns
            timeframe = '1h'
            
            # Fetch OHLCV in parallel
            tasks = []
            symbols_to_fetch = [symbol] + active_symbols
            for sym in symbols_to_fetch:
                tasks.append(self._fetch_ohlcv(sym, timeframe, limit))
                
            ohlcv_results = await asyncio.gather(*tasks)
            
            price_series = {}
            for sym, ohlcv in zip(symbols_to_fetch, ohlcv_results):
                if ohlcv and len(ohlcv) >= 5: # Need at least some data
                    price_series[sym] = [candle[4] for candle in ohlcv]
            
            if symbol not in price_series:
                logger.warning(f"Could not fetch enough price data for {symbol}")
                return False, 0.0, {}
                
            correlations = {}
            total_corr = 0.0
            count = 0
            
            new_prices = price_series[symbol]
            for sym in active_symbols:
                if sym in price_series:
                    corr = self._calculate_correlation(new_prices, price_series[sym])
                    correlations[sym] = corr
                    total_corr += corr
                    count += 1
            
            if count == 0:
                return False, 0.0, {}
                
            avg_corr = total_corr / count
            return avg_corr >= self.portfolio_config.max_correlation, avg_corr, correlations
            
        except Exception as e:
            logger.error(f"Error in correlation check: {e}")
            return False, 0.0, {}

    async def _fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> List:
        """Fetch OHLCV data with error handling."""
        try:
            return await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception as e:
            logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return []

    def _calculate_correlation(self, series1: List[float], series2: List[float]) -> float:
        """Calculate Pearson correlation of log returns."""
        try:
            min_len = min(len(series1), len(series2))
            if min_len < 3: return 0.0
            
            s1 = np.array(series1[-min_len:])
            s2 = np.array(series2[-min_len:])
            
            # Log returns
            r1 = np.diff(np.log(s1))
            r2 = np.diff(np.log(s2))
            
            if len(r1) < 2: return 0.0
            
            correlation = np.corrcoef(r1, r2)[0, 1]
            return float(correlation) if not np.isnan(correlation) else 0.0
        except Exception as e:
            logger.debug(f"Correlation calculation error: {e}")
            return 0.0

    def update_state(self):
        """Manually trigger a state reload from database."""
        self._load_state()

    def open_position(self, signal_id: int, entry_price: float, size: float):
        """
        Record a new open position.
        
        Args:
            signal_id: ID of the signal that triggered this position
            entry_price: Actual entry price
            size: Position size
        """
        try:
            with transaction(self.db_conn):
                self.db_conn.execute(
                    """
                    INSERT INTO paper_positions (signal_id, status, entry_price, entry_time, size)
                    VALUES (?, 'OPEN', ?, ?, ?)
                    """,
                    (signal_id, entry_price, datetime.now(timezone.utc), size)
                )
            self._load_state()
            logger.info(f"Position opened for signal {signal_id} at {entry_price}")
        except Exception as e:
            logger.error(f"Error opening position: {e}")

    def close_position(self, signal_id: int, exit_price: float, exit_reason: str = None):
        """
        Record a closed position and update realized P&L.
        
        Args:
            signal_id: ID of the signal
            exit_price: Actual exit price
            exit_reason: Reason for exit (TP, SL, Time-stop, etc.)
        """
        try:
            # Get entry details and side
            cursor = self.db_conn.execute(
                """
                SELECT p.entry_price, p.size, s.side 
                FROM paper_positions p
                JOIN signals s ON p.signal_id = s.id
                WHERE p.signal_id = ? AND p.status = 'OPEN'
                """,
                (signal_id,)
            )
            row = cursor.fetchone()
            if not row:
                logger.warning(f"No open position found for signal {signal_id}")
                return
                
            entry_price = row['entry_price']
            size = row['size']
            side = row['side']
            
            # Calculate P&L
            if side == 'LONG':
                pnl = (exit_price - entry_price) * size
                pnl_percent = (exit_price - entry_price) / entry_price * 100
            else: # SHORT
                pnl = (entry_price - exit_price) * size
                pnl_percent = (entry_price - exit_price) / entry_price * 100
                
            with transaction(self.db_conn):
                self.db_conn.execute(
                    """
                    UPDATE paper_positions 
                    SET status = 'CLOSED', exit_price = ?, exit_time = ?, pnl = ?, pnl_percent = ?, exit_reason = ?
                    WHERE signal_id = ? AND status = 'OPEN'
                    """,
                    (exit_price, datetime.now(timezone.utc), pnl, pnl_percent, exit_reason, signal_id)
                )
            
            self._load_state()
            logger.info(f"Position closed for signal {signal_id} at {exit_price}. P&L: {pnl:.2f} ({pnl_percent:.2f}%)")
        except Exception as e:
            logger.error(f"Error closing position: {e}")
