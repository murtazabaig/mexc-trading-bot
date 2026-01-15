"""Walk-forward backtest system for strategy evaluation."""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
import sqlite3

from ..logger import get_logger
from ..trading.paper_trader import PaperTrader
from ..jobs.scanner import OHLCVCache
from ..regime import RegimeClassifier
from ..scoring import ScoringEngine

logger = get_logger(__name__)

class BacktestEngine:
    """
    Handles historical simulation and walk-forward testing.
    """
    def __init__(self, config: Dict[str, Any], db_conn: sqlite3.Connection, exchange: Any = None):
        self.config = config
        self.db_conn = db_conn
        self.exchange = exchange
        self.paper_trader = PaperTrader(config, db_conn)
        self.logger = logger
        
        # Components from scanner
        self.regime_classifier = RegimeClassifier()
        self.scoring_engine = ScoringEngine()
        
        self.regime_classifier.set_logger(self.logger)
        self.scoring_engine.set_logger(self.logger)
        
        # Realistic fees and slippage
        self.maker_fee = 0.0005  # 0.05%
        self.taker_fee = 0.001   # 0.1%
        self.entry_slippage = 0.001  # 0.1%
        self.exit_slippage = 0.0015  # 0.15%

    async def run_backtest(self, symbol: str, timeframe: str, ohlcv_data: List[List[float]], 
                          start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Run backtest on a single period.
        """
        self.logger.info(f"Starting backtest for {symbol} on {timeframe}")
        
        # Reset paper trader for this run or use a separate one? 
        # For backtest, we might want a fresh paper trader instance if we want to isolate results.
        # But here we want to track results in the DB. Maybe we use a dedicated backtest_id.
        
        # Filter data by time if provided
        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        if start_time:
            df = df[df['datetime'] >= start_time]
        if end_time:
            df = df[df['datetime'] <= end_time]
            
        if df.empty:
            self.logger.warning(f"No data for {symbol} in specified range")
            return {'error': 'No data'}

        # Simulation loop
        # We need at least 50 candles before we can start generating signals (as per scanner)
        for i in range(50, len(df)):
            current_row = df.iloc[i]
            current_time = current_row['datetime']
            current_close = current_row['close']
            
            # 1. Update PaperTrader with current prices (SL/TP check)
            # Use current high/low for more realistic SL/TP hitting
            current_prices = {symbol: current_close}
            # Note: PaperTrader.update_prices currently only takes a single price.
            # For backtest, it's better to check if high/low hit SL/TP.
            self._check_sl_tp_backtest(symbol, current_row)
            
            # 2. Prepare data for scanner
            history = df.iloc[max(0, i-100):i+1] # Include current candle
            processed_data = {
                'timestamps': history['timestamp'].tolist(),
                'opens': history['open'].tolist(),
                'highs': history['high'].tolist(),
                'lows': history['low'].tolist(),
                'closes': history['close'].tolist(),
                'volumes': history['volume'].tolist()
            }
            
            # 3. Run Strategy (Scan -> Classify -> Score)
            # This mimics ScannerJob._process_symbol
            from ..indicators import rsi, ema, atr, atr_percent, macd, bollinger_bands, vwap, volume_zscore, adx
            
            # Calculate indicators (Need to mock this or call the internal functions)
            indicators = self._calculate_indicators(processed_data)
            
            # Classify regime
            regime = self.regime_classifier.classify_regime(symbol, processed_data, indicators)
            
            # Score signal
            score_result = self.scoring_engine.score_signal(symbol, processed_data, indicators, regime)
            
            # 4. Filter & Trade
            if score_result.get('meets_threshold', False) and score_result.get('score', 0) >= 7.0:
                # Apply slippage to entry price
                side = score_result.get('side', 'LONG')
                raw_entry = current_close
                if side == 'LONG':
                    entry_price = raw_entry * (1 + self.entry_slippage)
                else:
                    entry_price = raw_entry * (1 - self.entry_slippage)
                
                # Prepare signal for PaperTrader
                signal_data = {
                    'symbol': symbol,
                    'side': side,
                    'entry_price': entry_price,
                    'stop_loss': score_result.get('stop_loss'),
                    'tp1': score_result.get('tp1'),
                    'id': int(current_row['timestamp'])
                }
                
                # Check if we should open
                if symbol not in self.paper_trader.open_positions:
                    self.paper_trader.open_position(signal_data, timestamp=current_time)

        return self.generate_performance_report()

    def _check_sl_tp_backtest(self, symbol: str, current_row: pd.Series):
        """Check SL/TP against High/Low of the current candle."""
        if symbol not in self.paper_trader.open_positions:
            return
            
        pos = self.paper_trader.open_positions[symbol]
        high = current_row['high']
        low = current_row['low']
        current_time = current_row['datetime']
        
        sl = pos['stop_loss']
        tp = pos['take_profit']
        side = pos['side']
        
        if side == 'LONG':
            # SL hit
            if low <= sl:
                # Apply slippage to exit
                exit_price = sl * (1 - self.exit_slippage)
                self.paper_trader.close_position(symbol, exit_price, 'STOP_LOSS', exit_time=current_time)
            # TP hit
            elif tp and high >= tp:
                exit_price = tp * (1 - self.exit_slippage)
                self.paper_trader.close_position(symbol, exit_price, 'TAKE_PROFIT', exit_time=current_time)
        else: # SHORT
            # SL hit
            if high >= sl:
                exit_price = sl * (1 + self.exit_slippage)
                self.paper_trader.close_position(symbol, exit_price, 'STOP_LOSS', exit_time=current_time)
            # TP hit
            elif tp and low <= tp:
                exit_price = tp * (1 + self.exit_slippage)
                self.paper_trader.close_position(symbol, exit_price, 'TAKE_PROFIT', exit_time=current_time)

    def _calculate_indicators(self, data: Dict[str, List[float]]) -> Dict[str, Any]:
        """Calculate indicators for backtest."""
        from ..indicators import rsi, ema, atr, atr_percent, macd, bollinger_bands, vwap, volume_zscore, adx
        
        closes = data['closes']
        highs = data['highs']
        lows = data['lows']
        volumes = data['volumes']
        
        try:
            return {
                'rsi': rsi(closes, 14),
                'ema_20': ema(closes, 20),
                'ema_50': ema(closes, 50),
                'ema_200': ema(closes, 200),
                'atr': atr(highs, lows, closes, 14),
                'atr_pct': atr_percent(highs, lows, closes, 14),
                'adx': adx(highs, lows, 14),
                'volume_zscore': volume_zscore(volumes, 20),
                'vwap': vwap(highs, lows, closes, volumes)
            }
        except Exception as e:
            logger.error(f"Indicator calculation error: {e}")
            return {}

    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate full backtest performance metrics."""
        stats = self.paper_trader.get_portfolio_stats()
        
        # Additional metrics
        query = "SELECT * FROM paper_positions WHERE status = 'CLOSED'"
        cursor = self.db_conn.execute(query)
        trades = [dict(row) for row in cursor.fetchall()]
        
        if not trades:
            return {"status": "No trades executed"}
            
        pnls_r = [t['pnl_r'] for t in trades]
        durations = [t['duration_hours'] for t in trades]
        
        win_rate = len([p for p in pnls_r if p > 0]) / len(pnls_r)
        profit_factor = sum([p for p in pnls_r if p > 0]) / abs(sum([p for p in pnls_r if p < 0])) if any(p < 0 for p in pnls_r) else float('inf')
        sharpe = np.mean(pnls_r) / np.std(pnls_r) * np.sqrt(365) if len(pnls_r) > 1 and np.std(pnls_r) > 0 else 0
        
        report = {
            'total_return_r': sum(pnls_r),
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'sharpe_ratio': sharpe,
            'max_drawdown_r': stats['max_drawdown_r'],
            'num_trades': len(trades),
            'avg_duration_hours': np.mean(durations),
            'r_multiple_distribution': pnls_r,
            'equity_curve': self._generate_equity_curve(trades)
        }
        
        return report

    def _generate_equity_curve(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate equity curve data points."""
        curve = []
        balance = 0.0
        for t in sorted(trades, key=lambda x: x['exit_time']):
            balance += t['pnl_r']
            curve.append({
                'timestamp': t['exit_time'],
                'balance': balance
            })
        return curve

    async def run_walk_forward(self, symbol: str, timeframe: str, ohlcv_data: List[List[float]], 
                              train_days: int = 90, test_days: int = 30) -> Dict[str, Any]:
        """
        Run walk-forward analysis.
        """
        df = pd.DataFrame(ohlcv_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        start_date = df['datetime'].min()
        end_date = df['datetime'].max()
        
        results = []
        current_train_start = start_date
        
        while current_train_start + timedelta(days=train_days + test_days) <= end_date:
            train_end = current_train_start + timedelta(days=train_days)
            test_end = train_end + timedelta(days=test_days)
            
            logger.info(f"Walk-forward window: Train {current_train_start.date()} to {train_end.date()}, Test {train_end.date()} to {test_end.date()}")
            
            # 1. Train period (In this rule-based bot, we just log it or could optimize parameters)
            # train_results = await self.run_backtest(symbol, timeframe, ohlcv_data, current_train_start, train_end)
            
            # 2. Test period (Out-of-sample)
            test_results = await self.run_backtest(symbol, timeframe, ohlcv_data, train_end, test_end)
            
            results.append({
                'window': f"{train_end.date()} - {test_end.date()}",
                'performance': test_results
            })
            
            # Move window forward
            current_train_start += timedelta(days=test_days)
            
        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'walk_forward_results': results
        }

    def export_results_json(self, results: Dict[str, Any], filepath: str):
        """Export backtest results to JSON."""
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=4, default=str)
        logger.info(f"Results exported to {filepath}")
