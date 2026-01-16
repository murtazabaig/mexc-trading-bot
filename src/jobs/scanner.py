"""Continuous scanner job for fetching OHLCV data and generating trading signals."""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import math

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..database import insert_signal, transaction, get_last_processed_candle, update_processed_candle
from ..indicators import rsi, ema, atr, atr_percent, macd, bollinger_bands, vwap, volume_zscore, adx
from ..regime import RegimeClassifier
from ..scoring import ScoringEngine
from ..logger import get_logger
from ..trading.paper_trader import PaperTrader

logger = get_logger(__name__)


class OHLCVCache:
    """In-memory cache for OHLCV data."""
    
    def __init__(self, max_size: int = 100):
        """Initialize OHLCV cache.
        
        Args:
            max_size: Maximum number of candles to store per symbol
        """
        self.max_size = max_size
        self.data = {}
        self.timestamps = {}
    
    def add_data(self, symbol: str, ohlcv_data: List[List[float]]):
        """Add OHLCV data for a symbol.
        
        Args:
            symbol: Trading symbol
            ohlcv_data: OHLCV data in ccxt format (timestamp, open, high, low, close, volume)
        """
        if symbol not in self.data:
            self.data[symbol] = []
            self.timestamps[symbol] = []
        
        # Process incoming data
        processed_data = []
        for candle in ohlcv_data:
            if len(candle) >= 6:
                processed_data.append({
                    'timestamp': candle[0],
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5])
                })
        
        # Combine with existing data and remove duplicates
        existing_data = self.data.get(symbol, [])
        all_data = existing_data + processed_data
        
        # Sort by timestamp and remove duplicates
        unique_data = []
        seen_timestamps = set()
        
        for candle in sorted(all_data, key=lambda x: x['timestamp']):
            if candle['timestamp'] not in seen_timestamps:
                unique_data.append(candle)
                seen_timestamps.add(candle['timestamp'])
        
        # Keep only the most recent data up to max_size
        self.data[symbol] = unique_data[-self.max_size:] if len(unique_data) > self.max_size else unique_data
        
        # Update timestamps
        if self.data[symbol]:
            self.timestamps[symbol] = max(candle['timestamp'] for candle in self.data[symbol])
    
    def get_ohlcv_arrays(self, symbol: str) -> Optional[Dict[str, List[float]]]:
        """Get OHLCV data as arrays for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with arrays or None if no data
        """
        if symbol not in self.data or not self.data[symbol]:
            return None
        
        data = self.data[symbol]
        
        return {
            'timestamps': [candle['timestamp'] for candle in data],
            'opens': [candle['open'] for candle in data],
            'highs': [candle['high'] for candle in data],
            'lows': [candle['low'] for candle in data],
            'closes': [candle['close'] for candle in data],
            'volumes': [candle['volume'] for candle in data]
        }
    
    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get latest close price for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Latest close price or None
        """
        if symbol not in self.data or not self.data[symbol]:
            return None
        
        return self.data[symbol][-1]['close']
    
    def has_fresh_data(self, symbol: str, max_age_minutes: int = 120) -> bool:
        """Check if data for symbol is fresh enough.
        
        Args:
            symbol: Trading symbol
            max_age_minutes: Maximum age in minutes
            
        Returns:
            True if data is fresh
        """
        if symbol not in self.timestamps:
            return False
        
        latest_timestamp = self.timestamps[symbol]
        cutoff_time = int((datetime.utcnow() - timedelta(minutes=max_age_minutes)).timestamp() * 1000)
        
        return latest_timestamp > cutoff_time
    
    def clear_symbol(self, symbol: str):
        """Clear data for a symbol.
        
        Args:
            symbol: Trading symbol
        """
        self.data.pop(symbol, None)
        self.timestamps.pop(symbol, None)
    
    def clear_all(self):
        """Clear all cached data."""
        self.data.clear()
        self.timestamps.clear()


class ScannerJob:
    """Continuous scanner job for generating trading signals."""
    
    def __init__(self, exchange: ccxt.mexc, db_conn, config: Dict[str, Any], 
                 universe: Dict[str, Any], portfolio_manager: Any = None,
                 pause_state: Any = None):
        """Initialize scanner job.
        
        Args:
            exchange: MEXC ccxt exchange instance
            db_conn: Database connection
            config: Scanner configuration
            universe: Market universe dictionary
            portfolio_manager: Portfolio manager instance
            pause_state: Pause state singleton
        """
        self.exchange = exchange
        self.db_conn = db_conn
        self.config = config
        self.universe = universe
        self.portfolio_manager = portfolio_manager
        self.pause_state = pause_state
        
        # Initialize components
        self.cache = OHLCVCache(max_size=100)
        self.regime_classifier = RegimeClassifier()
        self.scoring_engine = ScoringEngine()
        self.paper_trader = PaperTrader(config, db_conn)
        
        # Set logger
        self.logger = logger
        self.regime_classifier.set_logger(self.logger)
        self.scoring_engine.set_logger(self.logger)
        
        # Statistics tracking
        self.stats = {
            'last_scan_time': None,
            'symbols_scanned': 0,
            'signals_created': 0,
            'errors_count': 0,
            'api_calls_made': 0,
            'start_time': None
        }
        
        # Control flags
        self.running = False
        self.scheduler = None
    
    def set_scheduler(self, scheduler: AsyncIOScheduler):
        """Set APScheduler instance.
        
        Args:
            scheduler: APScheduler instance
        """
        self.scheduler = scheduler
    
    async def start_scanning(self):
        """Start the continuous scanning process."""
        if self.running:
            self.logger.warning("Scanner is already running")
            return
        
        self.running = True
        self.stats['start_time'] = datetime.utcnow()
        
        self.logger.info("Starting continuous market scanner...")
        
        # Schedule the scanner job every 5 minutes
        if self.scheduler:
            self.scheduler.add_job(
                self.run_scan,
                'interval',
                minutes=5,
                id='market_scanner',
                name='Market Scanner',
                replace_existing=True
            )
            self.logger.info("Scanner job scheduled to run every 5 minutes")
        
        # Run initial scan
        await self.run_scan()
    
    async def stop_scanning(self):
        """Stop the continuous scanning process."""
        if not self.running:
            self.logger.warning("Scanner is not running")
            return
        
        self.running = False
        
        if self.scheduler:
            self.scheduler.remove_job('market_scanner')
        
        self.logger.info("Market scanner stopped")
    
    async def run_scan(self):
        """Main scanning function - processes all symbols in universe."""
        # Check BEFORE processing, not during loop
        if not self.running:
            self.logger.info("Scan skipped: scanner disabled")
            return

        if self.pause_state and self.pause_state.is_paused():
            self.logger.info(f"Scan skipped: {self.pause_state.reason()}")
            return

        scan_start = time.time()
        self.stats['last_scan_time'] = datetime.utcnow()

        self.logger.info("Starting market scan...")

        # Update paper trader prices with last known prices from cache
        current_prices = {}
        for symbol in self.universe.keys():
            price = self.cache.get_latest_price(symbol)
            if price:
                current_prices[symbol] = price

        if current_prices:
            self.paper_trader.update_prices(current_prices)

        # Reset counters for this scan
        symbols_scanned = 0
        signals_created = 0
        errors_count = 0

        if not self.universe:
            self.logger.warning("No symbols in universe - skipping scan")
            return

        # Get symbol list
        symbols = list(self.universe.keys())
        total_symbols = len(symbols)

        self.logger.info(f"Scanning {total_symbols} symbols...")

        # Process symbols in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(0, total_symbols, batch_size):
            
            batch = symbols[i:i + batch_size]
            
            # Process batch
            batch_tasks = []
            for symbol in batch:
                task = asyncio.create_task(self._process_symbol(symbol))
                batch_tasks.append(task)
            
            # Wait for batch completion
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for symbol, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    self.logger.error(f"Error processing {symbol}: {result}")
                    errors_count += 1
                elif result and result.get('signal_created'):
                    signals_created += 1
                    self.logger.info(f"Signal created for {symbol}: score {result['score']:.1f}")
            
            symbols_scanned += len(batch)
            
            # Brief pause between batches to respect rate limits
            if i + batch_size < total_symbols:
                await asyncio.sleep(1)
        
        # Update statistics
        self.stats['symbols_scanned'] += symbols_scanned
        self.stats['signals_created'] += signals_created
        self.stats['errors_count'] += errors_count
        
        scan_duration = time.time() - scan_start
        
        self.logger.info(
            f"Scan completed in {scan_duration:.1f}s: "
            f"{symbols_scanned} symbols scanned, {signals_created} signals created, {errors_count} errors"
        )
    
    async def _process_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Process a single symbol - fetch MTF data, calculate indicators, generate signals.

        Args:
            symbol: Trading symbol to process

        Returns:
            Dictionary with processing results or None
        """
        try:
            # Skip if symbol not in universe
            if symbol not in self.universe:
                return None

            # Fetch OHLCV data from MEXC API for all three timeframes
            ohlcv_5m = await self._fetch_ohlcv_data(symbol, '5m', limit=100)
            ohlcv_1h = await self._fetch_ohlcv_data(symbol, '1h', limit=100)
            ohlcv_4h = await self._fetch_ohlcv_data(symbol, '4h', limit=100)

            if not (ohlcv_5m and ohlcv_1h and ohlcv_4h):
                self.logger.warning(f"{symbol}: insufficient data for MTF scan, skipping")
                return None

            # Get last closed candle timestamp for 5m timeframe (entry trigger)
            last_closed_5m_ts = self._get_last_closed_candle_ts(ohlcv_5m, '5m')
            if last_closed_5m_ts is None:
                return None

            # Check if this 5m candle has already been processed
            last_processed_ts = get_last_processed_candle(self.db_conn, symbol, '5m')

            if last_processed_ts >= last_closed_5m_ts:
                self.logger.debug(f"{symbol}: 5m candle already processed, skipping")
                return {
                    'symbol': symbol,
                    'signal_created': False,
                    'reason': 'CANDLE_ALREADY_PROCESSED',
                    'skipped': True
                }

            # Only proceed if this is a NEW closed 5m candle
            self.logger.info(f"{symbol}: processing NEW 5m closed candle at {last_closed_5m_ts}")

            # Convert OHLCV data to array format for all timeframes
            data_5m = self._convert_ohlcv_to_arrays(ohlcv_5m)
            data_1h = self._convert_ohlcv_to_arrays(ohlcv_1h)
            data_4h = self._convert_ohlcv_to_arrays(ohlcv_4h)

            if not data_5m or not data_1h or not data_4h:
                self.logger.warning(f"{symbol}: failed to convert OHLCV data, skipping")
                return None

            # Add 5m data to cache (for paper trader)
            self.cache.add_data(symbol, ohlcv_5m)

            # Check minimum data requirements
            if len(data_5m['closes']) < 50 or len(data_1h['closes']) < 50 or len(data_4h['closes']) < 50:
                self.logger.debug(f"{symbol}: insufficient data points, skipping")
                return None

            # Compute indicators for all three timeframes
            ind_5m = await self._calculate_indicators(data_5m)
            ind_1h = await self._calculate_indicators(data_1h)
            ind_4h = await self._calculate_indicators(data_4h)

            if not (ind_5m and ind_1h and ind_4h):
                self.logger.warning(f"{symbol}: failed to calculate indicators, skipping")
                return None

            # Classify regime (using 5m data for entry context)
            regime = self.regime_classifier.classify_regime(symbol, data_5m, ind_5m)

            # Log MTF data clearly
            self._log_mtf_data(symbol, data_5m, data_1h, data_4h, ind_5m, ind_1h, ind_4h)

            # Score the signal using MTF confluence
            signal = self._score_signal_mtf(symbol, data_5m, data_1h, data_4h, ind_5m, ind_1h, ind_4h, regime)

            if not signal:
                return {
                    'symbol': symbol,
                    'signal_created': False,
                    'reason': 'NO_SIGNAL'
                }

            # Double check pause state before generating signal
            if self.pause_state and self.pause_state.is_paused():
                self.logger.info(f"Signal generation paused: {self.pause_state.reason()}")
                return {'symbol': symbol, 'signal_created': False, 'reason': 'PAUSED'}

            # Prepare signal data with MTF context
            signal_data = self._prepare_signal_data_mtf(symbol, data_5m, ind_5m, ind_1h, ind_4h, regime, signal)

            # Use portfolio manager if available
            if self.portfolio_manager:
                decision = await self.portfolio_manager.add_signal(signal_data)

                if decision.get('status') == 'APPROVED':
                    # Signal is approved and already inserted by portfolio manager
                    self.logger.info(f"Signal approved by portfolio for {symbol}")

                    # Open paper position
                    if 'signal_id' in decision:
                        signal_data['id'] = decision['signal_id']
                    self.paper_trader.open_position(signal_data)

                    # Mark this 5m candle as processed
                    update_processed_candle(self.db_conn, symbol, '5m', last_closed_5m_ts)

                    return {
                        'symbol': symbol,
                        'signal_created': True,
                        'score': signal['score'],
                        'decision': decision,
                        'candle_ts': last_closed_5m_ts
                    }
                else:
                    self.logger.info(f"Signal rejected by portfolio for {symbol}: {decision.get('reason')}")
                    return {
                        'symbol': symbol,
                        'signal_created': False,
                        'score': signal['score'],
                        'reason': decision.get('reason')
                    }

            # Fallback to direct insertion if no portfolio manager
            signal_id = await self._create_signal_record_mtf(symbol, data_5m, ind_5m, ind_1h, ind_4h, regime, signal)

            # Mark this 5m candle as processed
            update_processed_candle(self.db_conn, symbol, '5m', last_closed_5m_ts)

            return {
                'symbol': symbol,
                'signal_created': True,
                'signal_id': signal_id,
                'score': signal['score'],
                'confidence': signal['confidence'],
                'reasons': signal['reasons'],
                'candle_ts': last_closed_5m_ts
            }

        except Exception as e:
            self.logger.error(f"Error processing symbol {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}
    
    async def _fetch_ohlcv_data(self, symbol: str, timeframe: str = '1h', limit: int = 100) -> Optional[List[List[float]]]:
        """Fetch OHLCV data from MEXC API.

        Args:
            symbol: Trading symbol
            timeframe: Timeframe to fetch (e.g., '1h', '5m', '4h')
            limit: Number of candles to fetch

        Returns:
            OHLCV data in ccxt format or None
        """
        try:
            self.stats['api_calls_made'] += 1

            # Fetch candles at specified timeframe
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            )

            if not ohlcv or len(ohlcv) < 20:
                self.logger.warning(f"Insufficient OHLCV data for {symbol} {timeframe}: {len(ohlcv) if ohlcv else 0} candles")
                return None

            return ohlcv

        except ccxt.NetworkError as e:
            self.logger.warning(f"Network error fetching {symbol}: {e}")
            await asyncio.sleep(1)  # Brief retry delay
            return None
        except ccxt.RateLimitExceeded:
            self.logger.warning(f"Rate limit exceeded for {symbol}")
            await asyncio.sleep(5)  # Longer delay for rate limits
            return None
        except Exception as e:
            self.logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return None

    def _get_last_closed_candle_ts(self, ohlcv: List[List[float]], timeframe: str) -> Optional[int]:
        """Extract the last closed candle timestamp from OHLCV.

        CCXT returns: [..., [open_time, open, high, low, close, volume]]
        The last candle in the array is the most recent.

        For 1h/5m/4h timeframes, the candle is considered "closed"
        when the next candle begins (i.e., all data for that candle is final).

        Since we're fetching after the fact, the last N-1 candle is definitely closed.
        The Nth (current) candle is still forming.

        Return the timestamp of the last CLOSED candle.

        Args:
            ohlcv: OHLCV data from CCXT
            timeframe: Timeframe string (for logging)

        Returns:
            Timestamp (ms) of the last closed candle or None if insufficient data
        """
        if len(ohlcv) < 2:
            self.logger.debug(f"Insufficient OHLCV data for {timeframe}: need at least 2 candles")
            return None

        # Return the second-to-last candle's timestamp (definitely closed)
        return int(ohlcv[-2][0])

    def _convert_ohlcv_to_arrays(self, ohlcv: List[List[float]]) -> Optional[Dict[str, List[float]]]:
        """Convert OHLCV data from CCXT format to array format.

        Args:
            ohlcv: OHLCV data in ccxt format (timestamp, open, high, low, close, volume)

        Returns:
            Dictionary with arrays or None if insufficient data
        """
        if not ohlcv or len(ohlcv) < 2:
            return None

        try:
            return {
                'timestamps': [candle[0] for candle in ohlcv],
                'opens': [float(candle[1]) for candle in ohlcv],
                'highs': [float(candle[2]) for candle in ohlcv],
                'lows': [float(candle[3]) for candle in ohlcv],
                'closes': [float(candle[4]) for candle in ohlcv],
                'volumes': [float(candle[5]) for candle in ohlcv]
            }
        except Exception as e:
            self.logger.error(f"Error converting OHLCV data: {e}")
            return None

    def _log_mtf_data(self, symbol: str, data_5m: Dict, data_1h: Dict, data_4h: Dict,
                     ind_5m: Dict, ind_1h: Dict, ind_4h: Dict):
        """Log MTF data clearly for debugging.

        Args:
            symbol: Trading symbol
            data_5m: 5m OHLCV arrays
            data_1h: 1h OHLCV arrays
            data_4h: 4h OHLCV arrays
            ind_5m: 5m indicators
            ind_1h: 1h indicators
            ind_4h: 4h indicators
        """
        try:
            # Extract 5m data
            rsi_5m = ind_5m.get('rsi', {}).get('value', 0)
            ema20_5m = ind_5m.get('ema', {}).get('20', 0)
            ema50_5m = ind_5m.get('ema', {}).get('50', 0)
            vol_zscore_5m = ind_5m.get('volume_zscore', {}).get('20', 0)

            # Extract 1h data
            ema20_1h = ind_1h.get('ema', {}).get('20', 0)
            ema50_1h = ind_1h.get('ema', {}).get('50', 0)
            ema200_1h = ind_1h.get('ema', {}).get('200', 0)
            macd_1h = ind_1h.get('macd', {})
            macd_hist_1h = macd_1h.get('histogram', [0])[-1] if macd_1h else 0

            # Extract 4h data
            ema50_4h = ind_4h.get('ema', {}).get('50', 0)
            ema200_4h = ind_4h.get('ema', {}).get('200', 0)
            price_5m = data_5m['closes'][-1]
            price_1h = data_1h['closes'][-1]
            price_4h = data_4h['closes'][-1]

            # Determine trends
            trend_5m = "BULLISH" if ema20_5m > ema50_5m else "BEARISH"
            trend_1h = "UP" if ema20_1h > ema50_1h else "DOWN"
            trend_4h = "UP" if ema50_4h > ema200_4h else "DOWN"

            # Build log message
            self.logger.info(f"SCANNING: symbol={symbol}")
            self.logger.info(f"  5m: RSI={rsi_5m:.1f}, EMA20={ema20_5m:.2f} { '>' if ema20_5m > ema50_5m else '<' } EMA50={ema50_5m:.2f} ({trend_5m}), vol_zscore={vol_zscore_5m:.1f}")
            self.logger.info(f"  1h: EMA20={ema20_1h:.2f} { '>' if ema20_1h > ema50_1h else '<' } EMA50={ema50_1h:.2f} (trend {trend_1h}), MACD_hist={macd_hist_1h:.4f}")
            self.logger.info(f"  4h: EMA50={ema50_4h:.2f} { '>' if ema50_4h > ema200_4h else '<' } EMA200={ema200_4h:.2f} ({trend_4h}), price={price_4h:.2f}")

        except Exception as e:
            self.logger.warning(f"Error logging MTF data: {e}")

    def _check_mtf_confluence(self, ind_5m: Dict, ind_1h: Dict, ind_4h: Dict,
                             direction: str) -> Dict[str, Any]:
        """Check if timeframes align for the proposed direction.

        Args:
            ind_5m: 5m indicators
            ind_1h: 1h indicators
            ind_4h: 4h indicators
            direction: Proposed signal direction (LONG/SHORT)

        Returns:
            Dictionary with:
                - aligned: bool
                - reason: str
                - score_penalty: float (0 to -3.0)

        Rules for LONG:
        - 1h trend must be bullish (EMA20 > EMA50, or MACD > signal)
        - 4h must not be strongly bearish (allow some neutrality but no strong sell)
        - 5m entry trigger must exist

        Rules for SHORT:
        - 1h trend must be bearish (EMA20 < EMA50, or MACD < signal)
        - 4h must not be strongly bullish
        - 5m entry trigger must exist

        Penalties applied if alignment weak but not blocked:
        - 1h weaker than expected: -1.0 score
        - 4h slightly opposed: -1.5 score
        """
        if direction == 'LONG':
            # Check 1h trend
            ema_20_1h = ind_1h.get('ema', {}).get('20', 0)
            ema_50_1h = ind_1h.get('ema', {}).get('50', 0)
            macd_1h = ind_1h.get('macd', {})

            trend_bullish = (ema_20_1h > ema_50_1h) or (macd_1h.get('histogram', [0])[-1] > 0)

            if not trend_bullish:
                return {
                    'aligned': False,
                    'reason': f"1h trend bearish (EMA20={ema_20_1h:.2f} < EMA50={ema_50_1h:.2f})",
                    'score_penalty': -3.0
                }

            # Check 4h macro context
            ema_50_4h = ind_4h.get('ema', {}).get('50', 0)
            ema_200_4h = ind_4h.get('ema', {}).get('200', 0)

            if ema_50_4h < ema_200_4h:
                return {
                    'aligned': True,
                    'reason': "1h bullish + 4h in downtrend (macro caution)",
                    'score_penalty': -1.5
                }

            return {
                'aligned': True,
                'reason': "1h bullish + 4h support structure",
                'score_penalty': 0.0
            }

        elif direction == 'SHORT':
            ema_20_1h = ind_1h.get('ema', {}).get('20', 0)
            ema_50_1h = ind_1h.get('ema', {}).get('50', 0)
            macd_1h = ind_1h.get('macd', {})

            trend_bearish = (ema_20_1h < ema_50_1h) or (macd_1h.get('histogram', [0])[-1] < 0)

            if not trend_bearish:
                return {
                    'aligned': False,
                    'reason': f"1h trend bullish (EMA20={ema_20_1h:.2f} > EMA50={ema_50_1h:.2f})",
                    'score_penalty': -3.0
                }

            ema_50_4h = ind_4h.get('ema', {}).get('50', 0)
            ema_200_4h = ind_4h.get('ema', {}).get('200', 0)

            if ema_50_4h > ema_200_4h:
                return {
                    'aligned': True,
                    'reason': "1h bearish + 4h in uptrend (macro caution)",
                    'score_penalty': -1.5
                }

            return {
                'aligned': True,
                'reason': "1h bearish + 4h resistance structure",
                'score_penalty': 0.0
            }

        return {
            'aligned': True,
            'reason': 'NEUTRAL direction, no MTF check',
            'score_penalty': 0.0
        }

    def _score_signal_mtf(self, symbol: str, data_5m: Dict, data_1h: Dict, data_4h: Dict,
                          ind_5m: Dict, ind_1h: Dict, ind_4h: Dict,
                          regime: Dict) -> Optional[Dict[str, Any]]:
        """Score signal with MTF confluence.

        Args:
            symbol: Trading symbol
            data_5m: 5m OHLCV arrays
            data_1h: 1h OHLCV arrays
            data_4h: 4h OHLCV arrays
            ind_5m: 5m indicators
            ind_1h: 1h indicators
            ind_4h: 4h indicators
            regime: Regime classification

        Returns:
            Signal dict or None if no signal
        """
        # Score the signal using existing engine on 5m indicators
        score_result = self.scoring_engine.score_signal(symbol, data_5m, ind_5m, regime)

        if not score_result or score_result.get('score', 0) < 7.0:
            return None

        # Check MTF confluence
        direction = score_result['signal_direction']
        confluence = self._check_mtf_confluence(ind_5m, ind_1h, ind_4h, direction)

        if not confluence['aligned']:
            self.logger.info(f"{symbol}: MTF confluence check BLOCKED: {confluence['reason']}")
            return None

        # Apply MTF penalty to score
        final_score = score_result['score'] + confluence['score_penalty']

        if final_score < 7.0:
            self.logger.info(f"{symbol}: score after MTF penalty {final_score:.1f} < 7.0, rejected")
            return None

        # Build final signal
        signal = {
            'symbol': symbol,
            'direction': direction,
            'score': final_score,
            'entry_price': score_result['entry_price'],
            'stop_loss': score_result['stop_loss'],
            'take_profit': score_result['take_profit'],
            'reasons': score_result['reasons'] + [f"MTF: {confluence['reason']}"],
            'confidence': final_score / 10.0,
            'mtf_check': confluence,
            'indicators_5m': ind_5m,
            'indicators_1h': ind_1h,
            'indicators_4h': ind_4h,
        }

        self.logger.info(f"{symbol}: HIGH CONFIDENCE SIGNAL (MTF-aligned)")
        self.logger.info(f"  Direction: {direction}")
        self.logger.info(f"  Score: {final_score:.1f}/10")
        self.logger.info(f"  MTF: {confluence['reason']}")
        self.logger.info(f"  Entry: {signal['entry_price']:.2f}, SL: {signal['stop_loss']:.2f}, TP: {signal['take_profit']:.2f}")

        return signal
    
    async def _calculate_indicators(self, ohlcv_data: Dict[str, List[float]]) -> Optional[Dict[str, Any]]:
        """Calculate all technical indicators.
        
        Args:
            ohlcv_data: OHLCV data with arrays
            
        Returns:
            Dictionary with calculated indicators
        """
        try:
            closes = ohlcv_data['closes']
            highs = ohlcv_data['highs']
            lows = ohlcv_data['lows']
            volumes = ohlcv_data['volumes']
            
            if len(closes) < 50:
                return None
            
            indicators = {}
            
            # RSI(14)
            try:
                rsi_14 = rsi(closes, 14)
                indicators['rsi'] = {'value': rsi_14}
            except Exception as e:
                self.logger.debug(f"RSI calculation failed: {e}")
            
            # EMA(20, 50, 200)
            try:
                ema_20 = ema(closes, 20)
                ema_50 = ema(closes, 50)
                ema_200 = ema(closes, 200) if len(closes) >= 200 else ema_50
                indicators['ema'] = {'20': ema_20, '50': ema_50, '200': ema_200}
            except Exception as e:
                self.logger.debug(f"EMA calculation failed: {e}")
            
            # MACD
            try:
                macd_data = macd(closes, 12, 26, 9)
                indicators['macd'] = macd_data
            except Exception as e:
                self.logger.debug(f"MACD calculation failed: {e}")
            
            # Bollinger Bands
            try:
                bb_data = bollinger_bands(closes, 20, 2.0)
                indicators['bollinger_bands'] = bb_data
            except Exception as e:
                self.logger.debug(f"Bollinger Bands calculation failed: {e}")
            
            # ATR
            try:
                atr_14 = atr(highs, lows, closes, 14)
                atr_pct_14 = atr_percent(highs, lows, closes, 14)
                indicators['atr'] = {'14': atr_14}
                indicators['atr_percent'] = {'14': atr_pct_14}
            except Exception as e:
                self.logger.debug(f"ATR calculation failed: {e}")
            
            # Volume indicators
            try:
                vwap_val = vwap(highs, lows, closes, volumes)
                vol_zscore = volume_zscore(volumes, 20)
                indicators['vwap'] = vwap_val
                indicators['volume_zscore'] = {'20': vol_zscore}
            except Exception as e:
                self.logger.debug(f"Volume indicator calculation failed: {e}")
            
            # ADX
            try:
                adx_14 = adx(highs, lows, 14)
                indicators['adx'] = {'14': adx_14}
            except Exception as e:
                self.logger.debug(f"ADX calculation failed: {e}")
            
            return indicators
            
        except Exception as e:
            self.logger.error(f"Error calculating indicators: {e}")
            return None
    
    async def _create_signal_record(self, symbol: str, ohlcv_data: Dict[str, List[float]], 
                                  indicators: Dict[str, Any], regime: Dict[str, Any], 
                                  score_result: Dict[str, Any]) -> Optional[int]:
        """Create a signal record in the database.
        
        Args:
            symbol: Trading symbol
            ohlcv_data: OHLCV data
            indicators: Calculated indicators
            regime: Regime classification
            score_result: Scoring results
            
        Returns:
            Signal ID if created successfully
        """
        try:
            signal_data = self._prepare_signal_data(symbol, ohlcv_data, indicators, regime, score_result)
            
            # Insert into database
            signal_id = await asyncio.get_event_loop().run_in_executor(
                None, insert_signal, self.db_conn, signal_data
            )
            
            self.logger.info(
                f"Signal inserted: {symbol} {signal_data['side']} "
                f"(confidence: {signal_data['confidence']:.2f}, score: {score_result['score']:.1f})"
            )
            
            return signal_id
            
        except Exception as e:
            self.logger.error(f"Error creating signal record for {symbol}: {e}")
            return None
    
    def _prepare_signal_data(self, symbol: str, ohlcv_data: Dict[str, List[float]],
                              indicators: Dict[str, Any], regime: Dict[str, Any],
                              score_result: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare signal data dictionary.

        Args:
            symbol: Trading symbol
            ohlcv_data: OHLCV data
            indicators: Calculated indicators
            regime: Regime classification
            score_result: Scoring results

        Returns:
            Signal data dictionary
        """
        return {
            'symbol': symbol,
            'timeframe': '1h',
            'side': score_result.get('signal_direction', 'NEUTRAL'),
            'confidence': score_result.get('confidence', 0.0),
            'regime': regime.get('regime', 'UNKNOWN'),
            'entry_price': score_result.get('entry_price', 0.0),
            'stop_loss': score_result.get('stop_loss', 0.0),
            'tp1': score_result.get('take_profit', 0.0),
            'tp2': 0.0,
            'tp3': 0.0,
            'reason': {
                'score_components': score_result.get('components', {}),
                'reasons': score_result.get('reasons', []),
                'regime_confidence': regime.get('confidence', 0.0),
                'indicators': indicators
            },
            'metadata': {
                'scan_timestamp': datetime.utcnow().isoformat(),
                'regime_data': regime,
                'score_data': score_result
            }
        }

    def _prepare_signal_data_mtf(self, symbol: str, data_5m: Dict, ind_5m: Dict,
                                   ind_1h: Dict, ind_4h: Dict, regime: Dict,
                                   signal: Dict) -> Dict[str, Any]:
        """Prepare signal data with MTF context.

        Args:
            symbol: Trading symbol
            data_5m: 5m OHLCV arrays
            ind_5m: 5m indicators
            ind_1h: 1h indicators
            ind_4h: 4h indicators
            regime: Regime classification
            signal: MTF signal dict

        Returns:
            Signal data dictionary
        """
        return {
            'symbol': symbol,
            'timeframe': '5m',
            'side': signal['direction'],
            'confidence': signal['confidence'],
            'regime': regime.get('regime', 'UNKNOWN'),
            'entry_price': signal['entry_price'],
            'stop_loss': signal['stop_loss'],
            'tp1': signal['take_profit'],
            'tp2': 0.0,
            'tp3': 0.0,
            'reason': {
                'score': signal['score'],
                'reasons': signal['reasons'],
                'regime_confidence': regime.get('confidence', 0.0),
                'mtf_check': signal['mtf_check'],
                'indicators_5m': ind_5m,
                'indicators_1h': ind_1h,
                'indicators_4h': ind_4h
            },
            'metadata': {
                'scan_timestamp': datetime.utcnow().isoformat(),
                'regime_data': regime,
                'mtf_signal': signal
            }
        }

    async def _create_signal_record_mtf(self, symbol: str, data_5m: Dict, ind_5m: Dict,
                                        ind_1h: Dict, ind_4h: Dict, regime: Dict,
                                        signal: Dict) -> Optional[int]:
        """Create a signal record in the database with MTF context.

        Args:
            symbol: Trading symbol
            data_5m: 5m OHLCV arrays
            ind_5m: 5m indicators
            ind_1h: 1h indicators
            ind_4h: 4h indicators
            regime: Regime classification
            signal: MTF signal dict

        Returns:
            Signal ID if created successfully
        """
        try:
            signal_data = self._prepare_signal_data_mtf(symbol, data_5m, ind_5m, ind_1h, ind_4h, regime, signal)

            # Insert into database
            signal_id = await asyncio.get_event_loop().run_in_executor(
                None, insert_signal, self.db_conn, signal_data
            )

            self.logger.info(
                f"Signal inserted: {symbol} {signal_data['side']} "
                f"(confidence: {signal_data['confidence']:.2f}, score: {signal_data['metadata']['mtf_signal']['score']:.1f})"
            )

            return signal_id

        except Exception as e:
            self.logger.error(f"Error creating signal record for {symbol}: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scanner statistics.
        
        Returns:
            Dictionary with scanner statistics
        """
        uptime = None
        if self.stats.get('start_time'):
            uptime = (datetime.utcnow() - self.stats['start_time']).total_seconds() / 3600
        
        return {
            'running': self.running,
            'uptime_hours': uptime,
            'symbols_in_universe': len(self.universe),
            'last_scan_time': self.stats['last_scan_time'].isoformat() if self.stats['last_scan_time'] else None,
            'total_symbols_scanned': self.stats['symbols_scanned'],
            'total_signals_created': self.stats['signals_created'],
            'total_errors': self.stats['errors_count'],
            'total_api_calls': self.stats['api_calls_made'],
            'cached_symbols': len(self.cache.data),
            'cache_stats': {
                symbol: len(data) for symbol, data in self.cache.data.items()
            }
        }


def create_scanner_job(exchange: ccxt.mexc, db_conn, config: Dict[str, Any], 
                      universe: Dict[str, Any], portfolio_manager: Any = None,
                      pause_state: Any = None) -> ScannerJob:
    """Create and configure a scanner job instance.
    
    Args:
        exchange: MEXC ccxt exchange instance
        db_conn: Database connection
        config: Scanner configuration
        universe: Market universe dictionary
        portfolio_manager: Portfolio manager instance
        pause_state: Pause state singleton
        
    Returns:
        Configured ScannerJob instance
    """
    scanner_config = config.get('scanner', {})
    
    scanner = ScannerJob(
        exchange=exchange,
        db_conn=db_conn,
        config=scanner_config,
        universe=universe,
        portfolio_manager=portfolio_manager,
        pause_state=pause_state
    )
    
    logger.info(f"Scanner job created for {len(universe)} symbols")
    
    return scanner