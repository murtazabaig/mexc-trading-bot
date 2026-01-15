"""Continuous scanner job for fetching OHLCV data and generating trading signals."""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import math

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..database import insert_signal, transaction
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
                 universe: Dict[str, Any], portfolio_manager: Any = None):
        """Initialize scanner job.
        
        Args:
            exchange: MEXC ccxt exchange instance
            db_conn: Database connection
            config: Scanner configuration
            universe: Market universe dictionary
            portfolio_manager: Portfolio manager instance
        """
        self.exchange = exchange
        self.db_conn = db_conn
        self.config = config
        self.universe = universe
        self.portfolio_manager = portfolio_manager
        
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
                self._scan_all_symbols,
                'interval',
                minutes=5,
                id='market_scanner',
                name='Market Scanner',
                replace_existing=True
            )
            self.logger.info("Scanner job scheduled to run every 5 minutes")
        
        # Run initial scan
        await self._scan_all_symbols()
    
    async def stop_scanning(self):
        """Stop the continuous scanning process."""
        if not self.running:
            self.logger.warning("Scanner is not running")
            return
        
        self.running = False
        
        if self.scheduler:
            self.scheduler.remove_job('market_scanner')
        
        self.logger.info("Market scanner stopped")
    
    async def _scan_all_symbols(self):
        """Main scanning function - processes all symbols in universe."""
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
            if not self.running:
                break
            
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
        """Process a single symbol - fetch data, calculate indicators, generate signals.
        
        Args:
            symbol: Trading symbol to process
            
        Returns:
            Dictionary with processing results or None
        """
        try:
            # Skip if symbol not in universe
            if symbol not in self.universe:
                return None
            
            # Fetch OHLCV data from MEXC API
            ohlcv_data = await self._fetch_ohlcv_data(symbol)
            if not ohlcv_data:
                return None
            
            # Add to cache
            self.cache.add_data(symbol, ohlcv_data)
            
            # Get processed data
            processed_data = self.cache.get_ohlcv_arrays(symbol)
            if not processed_data or len(processed_data['closes']) < 50:
                return None  # Need sufficient data
            
            # Calculate technical indicators
            indicators = await self._calculate_indicators(processed_data)
            if not indicators:
                return None
            
            # Classify regime
            regime = self.regime_classifier.classify_regime(symbol, processed_data, indicators)
            
            # Score the signal
            score_result = self.scoring_engine.score_signal(symbol, processed_data, indicators, regime)
            
            # Create signal if score meets threshold
            if score_result.get('meets_threshold', False) and score_result.get('score', 0) >= 7.0:
                # Prepare signal data
                signal_data = self._prepare_signal_data(symbol, processed_data, indicators, regime, score_result)
                
                # Use portfolio manager if available
                if self.portfolio_manager:
                    decision = await self.portfolio_manager.add_signal(signal_data)
                    
                    if decision.get('status') == 'APPROVED':
                        # Open paper position
                        if 'signal_id' in decision:
                            signal_data['id'] = decision['signal_id']
                        self.paper_trader.open_position(signal_data)
                        
                        return {
                            'symbol': symbol,
                            'signal_created': True,
                            'score': score_result['score'],
                            'decision': decision
                        }
                    else:
                        return {
                            'symbol': symbol,
                            'signal_created': False,
                            'score': score_result['score'],
                            'reason': decision.get('reason')
                        }
                
                # Fallback to direct insertion if no portfolio manager
                signal_id = await self._create_signal_record(symbol, processed_data, indicators, regime, score_result)
                
                return {
                    'symbol': symbol,
                    'signal_created': True,
                    'signal_id': signal_id,
                    'score': score_result['score'],
                    'confidence': score_result['confidence'],
                    'reasons': score_result['reasons']
                }
            
            return {
                'symbol': symbol,
                'signal_created': False,
                'score': score_result['score']
            }
            
        except Exception as e:
            self.logger.error(f"Error processing symbol {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}
    
    async def _fetch_ohlcv_data(self, symbol: str) -> Optional[List[List[float]]]:
        """Fetch OHLCV data from MEXC API.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            OHLCV data in ccxt format or None
        """
        try:
            self.stats['api_calls_made'] += 1
            
            # Fetch 100 candles at 1h timeframe
            ohlcv = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.exchange.fetch_ohlcv(symbol, '1h', limit=100)
            )
            
            if not ohlcv or len(ohlcv) < 20:
                self.logger.warning(f"Insufficient OHLCV data for {symbol}: {len(ohlcv) if ohlcv else 0} candles")
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
            
            # EMA(20, 50)
            try:
                ema_20 = ema(closes, 20)
                ema_50 = ema(closes, 50)
                indicators['ema'] = {'20': ema_20, '50': ema_50}
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
                      universe: Dict[str, Any], portfolio_manager: Any = None) -> ScannerJob:
    """Create and configure a scanner job instance.
    
    Args:
        exchange: MEXC ccxt exchange instance
        db_conn: Database connection
        config: Scanner configuration
        universe: Market universe dictionary
        portfolio_manager: Portfolio manager instance
        
    Returns:
        Configured ScannerJob instance
    """
    scanner_config = config.get('scanner', {})
    
    scanner = ScannerJob(
        exchange=exchange,
        db_conn=db_conn,
        config=scanner_config,
        universe=universe,
        portfolio_manager=portfolio_manager
    )
    
    logger.info(f"Scanner job created for {len(universe)} symbols")
    
    return scanner