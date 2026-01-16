"""Warning detector for identifying market anomalies and risk conditions in real-time."""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import math
import numpy as np
import pandas as pd

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..database import insert_warning, transaction
from ..logger import get_logger

logger = get_logger(__name__)


class WarningDetector:
    """Detects market anomalies and risk conditions in real-time."""
    
    def __init__(self, exchange: ccxt.mexc, db_conn, config: Dict[str, Any], 
                 universe: Dict[str, Any], pause_state: Any = None):
        """Initialize warning detector.
        
        Args:
            exchange: MEXC ccxt exchange instance
            db_conn: Database connection
            config: Configuration dictionary
            universe: Market universe dictionary
            pause_state: Pause state singleton
        """
        self.exchange = exchange
        self.db_conn = db_conn
        self.config = config
        self.universe = universe
        self.pause_state = pause_state
        
        # Set logger
        self.logger = logger
        
        # Statistics tracking
        self.stats = {
            'last_check_time': None,
            'warnings_generated': 0,
            'errors_count': 0,
            'api_calls_made': 0,
            'start_time': None
        }
        
        # Control flags
        self.running = False
        self.scheduler = None
        
        # Data storage for detection
        self.btc_price_history = []  # List of (timestamp, price) tuples
        self.symbol_correlation_data = {}  # {symbol: {'correlations': [], 'prices': []}}
        self.symbol_direction_cache = {}  # {symbol: direction} for breadth calculation
        
        # Thresholds
        self.btc_shock_threshold_warning = 0.05  # 5%
        self.btc_shock_threshold_critical = 0.08  # 8%
        self.breadth_collapse_threshold_warning = 0.40  # 40%
        self.breadth_collapse_threshold_critical = 0.50  # 50%
        self.correlation_spike_threshold_warning = 0.30  # 30%
        self.correlation_spike_threshold_critical = 0.50  # 50%
        
    def set_scheduler(self, scheduler: AsyncIOScheduler):
        """Set APScheduler instance.
        
        Args:
            scheduler: APScheduler instance
        """
        self.scheduler = scheduler
    
    async def start_detection(self):
        """Start the continuous warning detection process."""
        if self.running:
            self.logger.warning("Warning detector is already running")
            return
        
        self.running = True
        self.stats['start_time'] = datetime.utcnow()
        
        self.logger.info("Starting continuous warning detector...")
        
        # Schedule the detection job every 5 minutes (synchronized with scanner)
        if self.scheduler:
            self.scheduler.add_job(
                self.run_detection,
                'interval',
                minutes=5,
                id='warning_detector',
                name='Warning Detector',
                replace_existing=True
            )
            self.logger.info("Warning detector job scheduled to run every 5 minutes")
        
        # Run initial check
        await self.run_detection()
    
    async def stop_detection(self):
        """Stop the continuous warning detection process."""
        if not self.running:
            self.logger.warning("Warning detector is not running")
            return
        
        self.running = False
        
        if self.scheduler:
            self.scheduler.remove_job('warning_detector')
        
        self.logger.info("Warning detector stopped")
    
    async def run_detection(self):
        """Main warning detection function - checks all warning conditions."""
        check_start = time.time()
        self.stats['last_check_time'] = datetime.utcnow()
        
        self.logger.info("Starting warning detection check...")
        
        # Reset counters for this check
        warnings_generated = 0
        errors_count = 0
        
        if not self.universe:
            self.logger.warning("No symbols in universe - skipping warning check")
            return
        
        # Get symbol list
        symbols = list(self.universe.keys())
        total_symbols = len(symbols)
        
        self.logger.info(f"Checking warnings across {total_symbols} symbols...")
        
        try:
            # 1. Check BTC shock
            btc_warning = await self.detect_btc_shock()
            if btc_warning:
                await self._handle_warning(btc_warning)
                warnings_generated += 1
            
            # 2. Check breadth collapse
            breadth_warning = await self.detect_breadth_collapse(symbols)
            if breadth_warning:
                await self._handle_warning(breadth_warning)
                warnings_generated += 1
            
            # 3. Check correlation spikes
            correlation_warnings = await self.detect_correlation_spike(symbols)
            for warning in correlation_warnings:
                await self._handle_warning(warning)
                warnings_generated += 1
                
        except Exception as e:
            self.logger.error(f"Error during warning detection: {e}")
            errors_count += 1
        
        # Update statistics
        self.stats['warnings_generated'] += warnings_generated
        self.stats['errors_count'] += errors_count
        
        check_duration = time.time() - check_start
        
        self.logger.info(
            f"Warning check completed in {check_duration:.1f}s: "
            f"{warnings_generated} warnings generated, {errors_count} errors"
        )
    
    async def detect_btc_shock(self) -> Optional[Dict[str, Any]]:
        """Detect BTC price shock - sudden >5% move within 1 hour.
        
        Returns:
            Warning dictionary or None if no shock detected
        """
        try:
            # Fetch BTC 1h candles
            btc_symbol = 'BTC/USDT:USDT'  # MEXC futures format
            ohlcv_data = await self._fetch_ohlcv_data(btc_symbol, limit=2)
            
            if not ohlcv_data or len(ohlcv_data) < 2:
                self.logger.warning("Insufficient BTC OHLCV data for shock detection")
                return None
            
            # Get current and previous candle
            current_candle = ohlcv_data[-1]
            previous_candle = ohlcv_data[-2]
            
            current_close = current_candle[4]
            previous_close = previous_candle[4]
            
            # Calculate price change percentage
            price_change = (current_close - previous_close) / previous_close
            price_change_pct = abs(price_change)
            
            # Store in history
            self.btc_price_history.append((current_candle[0], current_close))
            if len(self.btc_price_history) > 100:  # Keep last 100 entries
                self.btc_price_history.pop(0)
            
            # Determine direction
            direction = 'up' if price_change > 0 else 'down'
            
            # Check thresholds
            severity = None
            if price_change_pct > self.btc_shock_threshold_critical:
                severity = 'CRITICAL'
            elif price_change_pct > self.btc_shock_threshold_warning:
                severity = 'WARNING'
            else:
                return None
            
            # Create warning
            warning = {
                'type': 'BTC_SHOCK',
                'severity': severity,
                'price_change_pct': price_change_pct,
                'direction': direction,
                'current_price': current_close,
                'previous_price': previous_close,
                'timestamp': datetime.utcnow().isoformat(),
                'message': f'BTC price {direction} by {price_change_pct:.2%} in 1 hour',
                'triggered_value': price_change_pct,
                'threshold': self.btc_shock_threshold_warning if severity == 'WARNING' else self.btc_shock_threshold_critical,
                'action_taken': 'MONITORING'
            }
            
            self.logger.warning(f"BTC Shock detected: {direction} {price_change_pct:.2%} - {severity}")
            return warning
            
        except Exception as e:
            self.logger.error(f"Error detecting BTC shock: {e}")
            return None
    
    async def detect_breadth_collapse(self, symbols: List[str]) -> Optional[Dict[str, Any]]:
        """Detect breadth collapse - >40% of symbols moving against market direction.
        
        Args:
            symbols: List of symbols to analyze
            
        Returns:
            Warning dictionary or None if no collapse detected
        """
        try:
            if not symbols:
                return None
            
            # Get BTC direction as market direction
            btc_direction = await self._get_btc_direction()
            if btc_direction is None:
                return None
            
            # Analyze symbol directions
            bullish_count = 0
            bearish_count = 0
            neutral_count = 0
            
            # Process symbols in batches to avoid overwhelming the API
            batch_size = 20
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                batch_tasks = []
                for symbol in batch:
                    task = asyncio.create_task(self._get_symbol_direction(symbol))
                    batch_tasks.append(task)
                
                # Wait for batch completion
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Process results
                for symbol, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        self.logger.debug(f"Error getting direction for {symbol}: {result}")
                        continue
                    
                    if result == 'bullish':
                        bullish_count += 1
                    elif result == 'bearish':
                        bearish_count += 1
                    else:
                        neutral_count += 1
                
                # Brief pause between batches
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.5)
            
            # Calculate percentages
            total_directional = bullish_count + bearish_count
            if total_directional == 0:
                return None
            
            # Determine what percentage is moving against BTC direction
            if btc_direction == 'bullish':
                pct_against_trend = bearish_count / total_directional
                symbols_against = bearish_count
            else:  # bearish
                pct_against_trend = bullish_count / total_directional
                symbols_against = bullish_count
            
            # Check thresholds
            severity = None
            if pct_against_trend > self.breadth_collapse_threshold_critical:
                severity = 'CRITICAL'
            elif pct_against_trend > self.breadth_collapse_threshold_warning:
                severity = 'WARNING'
            else:
                return None
            
            # Create warning
            warning = {
                'type': 'BREADTH_COLLAPSE',
                'severity': severity,
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'neutral_count': neutral_count,
                'pct_against_trend': pct_against_trend,
                'symbols_against_trend': symbols_against,
                'btc_direction': btc_direction,
                'timestamp': datetime.utcnow().isoformat(),
                'message': f'{pct_against_trend:.1%} of symbols moving against BTC trend',
                'triggered_value': pct_against_trend,
                'threshold': self.breadth_collapse_threshold_warning if severity == 'WARNING' else self.breadth_collapse_threshold_critical,
                'action_taken': 'MONITORING'
            }
            
            self.logger.warning(f"Breadth collapse detected: {pct_against_trend:.1%} against trend - {severity}")
            return warning
            
        except Exception as e:
            self.logger.error(f"Error detecting breadth collapse: {e}")
            return None
    
    async def detect_correlation_spike(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """Detect correlation spikes - symbol correlation with BTC changes >30% in 1 hour.
        
        Args:
            symbols: List of symbols to analyze
            
        Returns:
            List of warning dictionaries for symbols with correlation spikes
        """
        warnings = []
        
        try:
            if not symbols:
                return warnings
            
            # Get BTC price data
            btc_prices = await self._get_btc_prices()
            if not btc_prices or len(btc_prices) < 2:
                return warnings
            
            # Process symbols in batches
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                batch_tasks = []
                for symbol in batch:
                    task = asyncio.create_task(self._check_symbol_correlation_spike(symbol, btc_prices))
                    batch_tasks.append(task)
                
                # Wait for batch completion
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                # Process results
                for symbol, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        self.logger.debug(f"Error checking correlation for {symbol}: {result}")
                        continue
                    
                    if result:
                        warnings.append(result)
                
                # Brief pause between batches
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.5)
            
            return warnings
            
        except Exception as e:
            self.logger.error(f"Error detecting correlation spikes: {e}")
            return warnings
    
    async def _check_symbol_correlation_spike(self, symbol: str, btc_prices: List[float]) -> Optional[Dict[str, Any]]:
        """Check if a symbol has a correlation spike with BTC.
        
        Args:
            symbol: Trading symbol
            btc_prices: List of BTC prices
            
        Returns:
            Warning dictionary or None if no spike detected
        """
        try:
            # Skip BTC itself
            if 'BTC' in symbol:
                return None
            
            # Get symbol price data
            symbol_prices = await self._get_symbol_prices(symbol)
            if not symbol_prices or len(symbol_prices) < 2:
                return None
            
            # Calculate current correlation
            current_corr = self._calculate_correlation(btc_prices[-24:], symbol_prices[-24:])
            
            # Get previous correlation (if available)
            if len(btc_prices) >= 48 and len(symbol_prices) >= 48:
                previous_corr = self._calculate_correlation(btc_prices[-48:-24], symbol_prices[-48:-24])
            else:
                previous_corr = 0.0  # No previous data
            
            # Calculate correlation change
            correlation_change = abs(current_corr - previous_corr)
            
            # Check thresholds
            severity = None
            if correlation_change > self.correlation_spike_threshold_critical:
                severity = 'CRITICAL'
            elif correlation_change > self.correlation_spike_threshold_warning:
                severity = 'WARNING'
            else:
                return None
            
            # Create warning
            warning = {
                'type': 'CORRELATION_SPIKE',
                'severity': 'WARNING',  # Change from CRITICAL to WARNING
                'symbol': symbol,
                'correlation_change_pct': correlation_change,
                'previous_correlation': previous_corr,
                'current_correlation': current_corr,
                'timestamp': datetime.utcnow().isoformat(),
                'message': f'{symbol} correlation with BTC changed by {correlation_change:.2%}',
                'triggered_value': correlation_change,
                'threshold': self.correlation_spike_threshold_warning if severity == 'WARNING' else self.correlation_spike_threshold_critical,
                'action_taken': 'MONITORING'
            }
            
            self.logger.warning(f"Correlation spike detected for {symbol}: {correlation_change:.2%} change - {severity}")
            return warning
            
        except Exception as e:
            self.logger.error(f"Error checking correlation spike for {symbol}: {e}")
            return None
    
    def _calculate_correlation(self, series1: List[float], series2: List[float]) -> float:
        """Calculate Pearson correlation between two series.
        
        Args:
            series1: First price series
            series2: Second price series
            
        Returns:
            Correlation coefficient or 0 if calculation fails
        """
        try:
            if len(series1) != len(series2) or len(series1) < 2:
                return 0.0
            
            # Convert to numpy arrays
            arr1 = np.array(series1)
            arr2 = np.array(series2)
            
            # Calculate returns for correlation
            returns1 = np.diff(arr1) / arr1[:-1]
            returns2 = np.diff(arr2) / arr2[:-1]
            
            # Calculate Pearson correlation
            correlation = np.corrcoef(returns1, returns2)[0, 1]
            
            return float(correlation) if not np.isnan(correlation) else 0.0
            
        except Exception as e:
            self.logger.debug(f"Error calculating correlation: {e}")
            return 0.0
    
    async def _get_btc_direction(self) -> Optional[str]:
        """Get BTC direction (bullish/bearish) based on 1h price change.
        
        Returns:
            'bullish', 'bearish', or None if cannot determine
        """
        try:
            btc_symbol = 'BTC/USDT:USDT'
            ohlcv_data = await self._fetch_ohlcv_data(btc_symbol, limit=2)
            
            if not ohlcv_data or len(ohlcv_data) < 2:
                return None
            
            current_close = ohlcv_data[-1][4]
            previous_close = ohlcv_data[-2][4]
            
            price_change = (current_close - previous_close) / previous_close
            
            if price_change > 0.005:  # >0.5% up
                return 'bullish'
            elif price_change < -0.005:  # <0.5% down
                return 'bearish'
            else:
                return None  # neutral
                
        except Exception as e:
            self.logger.error(f"Error getting BTC direction: {e}")
            return None
    
    async def _get_symbol_direction(self, symbol: str) -> Optional[str]:
        """Get symbol direction (bullish/bearish) based on 1h price change.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            'bullish', 'bearish', or None if cannot determine
        """
        try:
            # Use cached direction if available and recent
            if symbol in self.symbol_direction_cache:
                return self.symbol_direction_cache[symbol]
            
            ohlcv_data = await self._fetch_ohlcv_data(symbol, limit=2)
            
            if not ohlcv_data or len(ohlcv_data) < 2:
                return None
            
            current_close = ohlcv_data[-1][4]
            previous_close = ohlcv_data[-2][4]
            
            price_change = (current_close - previous_close) / previous_close
            
            if price_change > 0.005:  # >0.5% up
                direction = 'bullish'
            elif price_change < -0.005:  # <0.5% down
                direction = 'bearish'
            else:
                direction = None  # neutral
            
            # Cache the result
            self.symbol_direction_cache[symbol] = direction
            
            return direction
                
        except Exception as e:
            self.logger.debug(f"Error getting direction for {symbol}: {e}")
            return None
    
    async def _get_btc_prices(self) -> List[float]:
        """Get BTC price history.
        
        Returns:
            List of BTC prices (most recent last)
        """
        try:
            btc_symbol = 'BTC/USDT:USDT'
            ohlcv_data = await self._fetch_ohlcv_data(btc_symbol, limit=48)  # 48 hours of data
            
            if not ohlcv_data:
                return []
            
            # Extract closing prices
            prices = [float(candle[4]) for candle in ohlcv_data]
            return prices
            
        except Exception as e:
            self.logger.error(f"Error getting BTC prices: {e}")
            return []
    
    async def _get_symbol_prices(self, symbol: str) -> List[float]:
        """Get symbol price history.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of symbol prices (most recent last)
        """
        try:
            ohlcv_data = await self._fetch_ohlcv_data(symbol, limit=48)  # 48 hours of data
            
            if not ohlcv_data:
                return []
            
            # Extract closing prices
            prices = [float(candle[4]) for candle in ohlcv_data]
            return prices
            
        except Exception as e:
            self.logger.debug(f"Error getting prices for {symbol}: {e}")
            return []
    
    async def _fetch_ohlcv_data(self, symbol: str, limit: int = 100) -> Optional[List[List[float]]]:
        """Fetch OHLCV data from MEXC API.
        
        Args:
            symbol: Trading symbol
            limit: Number of candles to fetch
            
        Returns:
            OHLCV data in ccxt format or None
        """
        try:
            self.stats['api_calls_made'] += 1
            
            # Fetch candles at 1h timeframe
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.exchange.fetch_ohlcv(symbol, '1h', limit=limit)
            )
            
            if not ohlcv or len(ohlcv) < 2:
                self.logger.debug(f"Insufficient OHLCV data for {symbol}: {len(ohlcv) if ohlcv else 0} candles")
                return None
            
            return ohlcv
            
        except ccxt.NetworkError as e:
            self.logger.warning(f"Network error fetching {symbol}: {e}")
            await asyncio.sleep(1)
            return None
        except ccxt.RateLimitExceeded:
            self.logger.warning(f"Rate limit exceeded for {symbol}")
            await asyncio.sleep(5)
            return None
        except Exception as e:
            self.logger.debug(f"Error fetching OHLCV for {symbol}: {e}")
            return None
    
    async def _handle_warning(self, warning: Dict[str, Any]):
        """Handle a detected warning - store in database and send to Telegram.
        
        Args:
            warning: Warning dictionary
        """
        try:
            # Check for critical severity and pause if necessary
            if warning.get('severity') == 'CRITICAL' and self.pause_state:
                reason = f"CRITICAL_WARNING: {warning['type']} - {warning['message']}"
                self.pause_state.pause(reason)
                warning['action_taken'] = 'PAUSED_SIGNALS'
                self.logger.warning(f"System PAUSED due to critical warning: {reason}")

            # Store in database
            warning_id = await self._store_warning_in_database(warning)
            
            # Send to Telegram if available
            if hasattr(self, 'telegram_bot') and self.telegram_bot:
                await self.telegram_bot.send_warning(warning)
            
            self.logger.info(f"Warning handled: {warning['type']} - {warning['severity']}")
            
        except Exception as e:
            self.logger.error(f"Error handling warning: {e}")
    
    async def _store_warning_in_database(self, warning: Dict[str, Any]) -> Optional[int]:
        """Store warning in database.
        
        Args:
            warning: Warning dictionary
            
        Returns:
            Warning ID if stored successfully
        """
        try:
            # Prepare warning data for database
            db_warning = {
                'severity': warning['severity'],
                'warning_type': warning['type'],
                'message': warning['message'],
                'triggered_value': warning.get('triggered_value', 0),
                'threshold': warning.get('threshold', 0),
                'action_taken': warning.get('action_taken', 'MONITORING'),
                'metadata': {
                    'timestamp': warning.get('timestamp'),
                    'details': warning
                }
            }
            
            with transaction(self.db_conn):
                warning_id = insert_warning(self.db_conn, db_warning)
            
            self.logger.info(f"Warning stored in database with ID: {warning_id}")
            return warning_id
            
        except Exception as e:
            self.logger.error(f"Error storing warning in database: {e}")
            return None
    
    def set_telegram_bot(self, telegram_bot):
        """Set Telegram bot instance for sending warnings.
        
        Args:
            telegram_bot: Telegram bot instance
        """
        self.telegram_bot = telegram_bot
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics.
        
        Returns:
            Dictionary with current statistics
        """
        return self.stats.copy()