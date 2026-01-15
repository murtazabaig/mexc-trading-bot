"""Paper trading engine for strategy evaluation."""

import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import sqlite3

from ..logger import get_logger
from ..database import transaction

logger = get_logger(__name__)

class PaperTrader:
    """
    Handles paper trading logic, tracking positions and calculating P&L in R units.
    """
    def __init__(self, config: Dict[str, Any], db_conn: sqlite3.Connection):
        self.config = config
        self.db_conn = db_conn
        self.trading_config = config.get('trading', {})
        
        # In-memory cache for open positions to avoid frequent DB hits for price updates
        self.open_positions: Dict[str, Dict[str, Any]] = self._load_open_positions()

    def _load_open_positions(self) -> Dict[str, Dict[str, Any]]:
        """Load all open positions from the database into memory."""
        query = "SELECT * FROM paper_positions WHERE status = 'OPEN'"
        cursor = self.db_conn.execute(query)
        rows = cursor.fetchall()
        
        positions = {}
        for row in rows:
            pos = dict(row)
            if pos.get('metadata'):
                pos['metadata'] = json.loads(pos['metadata'])
            positions[pos['symbol']] = pos
        return positions

    def open_position(self, signal: Dict[str, Any], timestamp: Optional[datetime] = None) -> Optional[int]:
        """
        Open a new paper position based on a signal.
        """
        symbol = signal.get('symbol')
        if symbol in self.open_positions:
            logger.warning(f"Position already open for {symbol}. Skipping.")
            return None
        
        # Check max concurrent positions
        max_pos = self.trading_config.get('max_concurrent_positions', 5)
        if len(self.open_positions) >= max_pos:
            logger.warning(f"Max concurrent positions ({max_pos}) reached. Cannot open {symbol}.")
            return None

        side = signal.get('side')  # LONG/SHORT
        entry_price = signal.get('entry_price')
        stop_loss = signal.get('stop_loss')
        # Default take profit if not provided
        take_profit = signal.get('tp1') or signal.get('take_profit')
        
        if not entry_price or not stop_loss:
            logger.error(f"Missing entry_price or stop_loss for {symbol}")
            return None

        # Calculate risk in price units
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit == 0:
            logger.error(f"Stop loss equals entry price for {symbol}")
            return None

        # Position size in R units. Here we default to 1R per trade as per requirement
        # size = position_value / risk (in R units, default 1R per trade)
        size_r = 1.0 
        
        if not timestamp:
            entry_time = datetime.now(timezone.utc)
        else:
            entry_time = timestamp
        
        query = """
        INSERT INTO paper_positions (
            signal_id, symbol, status, side, size, entry_price, entry_time, 
            stop_loss, take_profit, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        metadata = {
            'max_price': entry_price,
            'min_price': entry_price,
            'risk_per_unit': risk_per_unit,
            'fees_paid_r': 0.0 # Will track fees in R units
        }
        
        # Apply entry fee (taker fee usually for market orders)
        entry_fee_pct = 0.001 # 0.1% taker fee
        entry_fee_r = (entry_price * entry_fee_pct) / risk_per_unit
        metadata['fees_paid_r'] += entry_fee_r

        params = (
            signal.get('id'),
            symbol,
            'OPEN',
            side,
            size_r,
            entry_price,
            entry_time,
            stop_loss,
            take_profit,
            json.dumps(metadata)
        )
        
        try:
            with transaction(self.db_conn):
                cursor = self.db_conn.cursor()
                cursor.execute(query, params)
                pos_id = cursor.lastrowid
                
                # Update memory cache
                new_pos = {
                    'id': pos_id,
                    'signal_id': signal.get('id'),
                    'symbol': symbol,
                    'status': 'OPEN',
                    'side': side,
                    'size': size_r,
                    'entry_price': entry_price,
                    'entry_time': entry_time,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'metadata': metadata
                }
                self.open_positions[symbol] = new_pos
                
                logger.info(f"Opened {side} position for {symbol} at {entry_price}")
                return pos_id
        except Exception as e:
            logger.error(f"Failed to open position for {symbol}: {e}")
            return None

    def close_position(self, symbol: str, exit_price: float, reason: str, exit_time: Optional[datetime] = None) -> bool:
        """
        Close an open paper position.
        """
        if symbol not in self.open_positions:
            logger.warning(f"No open position found for {symbol} to close.")
            return False
        
        pos = self.open_positions[symbol]
        entry_price = pos['entry_price']
        side = pos['side']
        size_r = pos['size']
        entry_time = pos['entry_time']
        if isinstance(entry_time, str):
            entry_time = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
        
        if not exit_time:
            exit_time = datetime.now(timezone.utc)
        
        # Calculate P&L in R units
        # Exit P&L = (exit_price - entry_price) / risk * direction (long/short adjustment)
        risk_per_unit = pos['metadata'].get('risk_per_unit')
        if not risk_per_unit:
             risk_per_unit = abs(entry_price - pos['stop_loss'])
        
        # Apply exit fee
        exit_fee_pct = 0.001 # 0.1% taker fee
        exit_fee_r = (exit_price * exit_fee_pct) / risk_per_unit
        pos['metadata']['fees_paid_r'] += exit_fee_r
        
        direction = 1 if side.upper() == 'LONG' else -1
        pnl_r = ((exit_price - entry_price) / risk_per_unit) * direction * size_r
        
        # Deduct total fees from P&L R
        pnl_r -= pos['metadata']['fees_paid_r']
        
        pnl_percent = ((exit_price - entry_price) / entry_price) * direction * 100
        # Also adjust percent for fees
        pnl_percent -= (pos['metadata']['fees_paid_r'] * risk_per_unit / entry_price) * 100
        
        # Duration
        duration = exit_time - entry_time
        duration_hours = duration.total_seconds() / 3600
        
        # Max Drawdown for this position
        max_drawdown = pos['metadata'].get('max_drawdown', 0.0)

        query = """
        UPDATE paper_positions SET
            status = 'CLOSED',
            exit_price = ?,
            exit_time = ?,
            pnl_percent = ?,
            pnl_r = ?,
            duration_hours = ?,
            max_drawdown = ?,
            exit_reason = ?,
            metadata = ?
        WHERE id = ?
        """
        
        pos['metadata']['exit_reason'] = reason
        params = (
            exit_price,
            exit_time,
            pnl_percent,
            pnl_r,
            duration_hours,
            max_drawdown,
            reason,
            json.dumps(pos['metadata']),
            pos['id']
        )
        
        try:
            with transaction(self.db_conn):
                self.db_conn.execute(query, params)
                del self.open_positions[symbol]
                logger.info(f"Closed {side} position for {symbol} at {exit_price}. P&L: {pnl_r:.2f}R ({pnl_percent:.2f}%)")
                return True
        except Exception as e:
            logger.error(f"Failed to close position for {symbol}: {e}")
            return False

    def update_prices(self, current_prices: Dict[str, float]):
        """
        Update mark-to-market and check SL/TP for open positions.
        """
        for symbol, current_price in current_prices.items():
            if symbol in self.open_positions:
                pos = self.open_positions[symbol]
                
                # Update max/min price for drawdown/run-up calculation
                metadata = pos['metadata']
                if current_price > metadata.get('max_price', 0):
                    metadata['max_price'] = current_price
                if current_price < metadata.get('min_price', float('inf')):
                    metadata['min_price'] = current_price
                
                # Calculate current drawdown
                # For LONG: drawdown is (max_price - current_price) / max_price
                # For SHORT: drawdown is (current_price - min_price) / min_price
                if pos['side'].upper() == 'LONG':
                    dd = (metadata['max_price'] - current_price) / metadata['max_price'] * 100
                else:
                    dd = (current_price - metadata['min_price']) / metadata['min_price'] * 100
                
                metadata['max_drawdown'] = max(metadata.get('max_drawdown', 0), dd)
                
                # Check SL
                if pos['side'].upper() == 'LONG' and current_price <= pos['stop_loss']:
                    self.close_position(symbol, pos['stop_loss'], 'STOP_LOSS')
                elif pos['side'].upper() == 'SHORT' and current_price >= pos['stop_loss']:
                    self.close_position(symbol, pos['stop_loss'], 'STOP_LOSS')
                
                # Check TP
                elif pos['take_profit']:
                    if pos['side'].upper() == 'LONG' and current_price >= pos['take_profit']:
                        self.close_position(symbol, pos['take_profit'], 'TAKE_PROFIT')
                    elif pos['side'].upper() == 'SHORT' and current_price <= pos['take_profit']:
                        self.close_position(symbol, pos['take_profit'], 'TAKE_PROFIT')

    def get_portfolio_stats(self) -> Dict[str, Any]:
        """
        Return portfolio performance metrics.
        """
        query = "SELECT pnl_r, pnl_percent FROM paper_positions WHERE status = 'CLOSED'"
        cursor = self.db_conn.execute(query)
        rows = cursor.fetchall()
        
        closed_pnls_r = [row['pnl_r'] for row in rows]
        total_pnl_r = sum(closed_pnls_r)
        
        # Simple drawdown calculation on closed equity
        equity_curve = [0.0]
        current_equity = 0.0
        max_equity = 0.0
        max_dd = 0.0
        
        for pnl in closed_pnls_r:
            current_equity += pnl
            equity_curve.append(current_equity)
            max_equity = max(max_equity, current_equity)
            dd = max_equity - current_equity
            max_dd = max(max_dd, dd)
            
        return {
            'total_pnl_r': total_pnl_r,
            'max_drawdown_r': max_dd,
            'active_positions_count': len(self.open_positions),
            'closed_positions_count': len(closed_pnls_r),
            'win_rate': (len([p for p in closed_pnls_r if p > 0]) / len(closed_pnls_r) * 100) if closed_pnls_r else 0
        }
