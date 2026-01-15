"""Signal scoring engine for evaluating trading opportunities."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json


class ScoringEngine:
    """Engine for scoring trading signals based on technical indicators and regime."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize scoring engine.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.min_score = self.config.get('min_score', 7.0)
        self.max_score = self.config.get('max_score', 10.0)
        self.logger = None
    
    def set_logger(self, logger):
        """Set logger instance."""
        self.logger = logger
    
    def score_signal(self, symbol: str, ohlcv_data: Dict[str, List[float]], 
                    indicators: Dict[str, Any], regime: Dict[str, Any]) -> Dict[str, Any]:
        """Score a trading signal for a symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            ohlcv_data: OHLCV data with keys 'highs', 'lows', 'closes', 'volumes'
            indicators: Calculated technical indicators
            regime: Regime classification results
            
        Returns:
            Dictionary with signal scoring results
        """
        try:
            if not ohlcv_data or not indicators or not regime:
                return self._default_score(symbol)
            
            closes = ohlcv_data.get('closes', [])
            highs = ohlcv_data.get('highs', [])
            lows = ohlcv_data.get('lows', [])
            
            if len(closes) < 20:  # Need minimum data
                return self._default_score(symbol)
            
            # Extract indicators
            rsi_14 = indicators.get('rsi', {}).get('value', 50.0)
            ema_20 = indicators.get('ema', {}).get('20', closes[-1])
            ema_50 = indicators.get('ema', {}).get('50', closes[-1])
            macd_data = indicators.get('macd', {})
            bb_data = indicators.get('bollinger_bands', {})
            atr_pct = indicators.get('atr_percent', {}).get('14', 0.0)
            adx = indicators.get('adx', {}).get('14', 0.0)
            volume_zscore = indicators.get('volume_zscore', {}).get('20', 0.0)
            
            # Calculate score components
            scores = {}
            reasons = []
            
            # 1. RSI Scoring (0-2 points)
            rsi_score = self._score_rsi(rsi_14)
            scores['rsi'] = rsi_score
            
            if rsi_score > 0:
                if rsi_14 < 30:
                    reasons.append(f"RSI oversold ({rsi_14:.1f})")
                elif rsi_14 > 70:
                    reasons.append(f"RSI overbought ({rsi_14:.1f})")
            
            # 2. EMA Alignment Scoring (0-2 points)
            ema_score = self._score_ema_alignment(closes[-1], ema_20, ema_50)
            scores['ema_alignment'] = ema_score
            
            if ema_score > 0:
                price_vs_ema20 = (closes[-1] - ema_20) / ema_20 * 100
                if price_vs_ema20 > 0:
                    reasons.append(f"Price above EMA20 (+{price_vs_ema20:.1f}%)")
                else:
                    reasons.append(f"Price below EMA20 ({price_vs_ema20:.1f}%)")
            
            # 3. MACD Scoring (0-2 points)
            macd_score = self._score_macd(macd_data)
            scores['macd'] = macd_score
            
            if macd_score > 0 and macd_data:
                macd_line = macd_data.get('macd', 0)
                signal_line = macd_data.get('signal', 0)
                histogram = macd_data.get('histogram', 0)
                
                if macd_line > signal_line and histogram > 0:
                    reasons.append("MACD bullish crossover")
                elif macd_line < signal_line and histogram < 0:
                    reasons.append("MACD bearish crossover")
            
            # 4. Bollinger Bands Scoring (0-2 points)
            bb_score = self._score_bollinger_bands(closes[-1], bb_data)
            scores['bollinger_bands'] = bb_score
            
            if bb_score > 0 and bb_data:
                bb_position = bb_data.get('position', 0.5)
                if bb_position < 0.2:
                    reasons.append("Price near lower Bollinger Band")
                elif bb_position > 0.8:
                    reasons.append("Price near upper Bollinger Band")
            
            # 5. Volume Scoring (0-1 point)
            volume_score = self._score_volume(volume_zscore)
            scores['volume'] = volume_score
            
            if volume_score > 0:
                reasons.append(f"High volume (Z-score: {volume_zscore:.2f})")
            
            # 6. Volatility Scoring (0-1 point)
            volatility_score = self._score_volatility(atr_pct)
            scores['volatility'] = volatility_score
            
            if volatility_score > 0:
                reasons.append(f"Moderate volatility ({atr_pct:.1f}%)")
            
            # Calculate total score
            total_score = sum(scores.values())
            
            # Determine signal direction
            signal_direction = self._determine_signal_direction(closes, ema_20, ema_50, rsi_14, macd_data)
            
            # Generate entry and exit prices
            entry_price, stop_loss, take_profit = self._calculate_price_levels(
                closes[-1], atr_pct, signal_direction, bb_data
            )
            
            result = {
                "symbol": symbol,
                "score": total_score,
                "max_score": self.max_score,
                "signal_direction": signal_direction,
                "confidence": total_score / self.max_score,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "components": scores,
                "reasons": reasons,
                "timestamp": datetime.utcnow().isoformat(),
                "meets_threshold": total_score >= self.min_score
            }
            
            if self.logger:
                self.logger.debug(f"Score calculated for {symbol}: {total_score:.1f}/{self.max_score} ({signal_direction})")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scoring signal for {symbol}: {e}")
            return self._default_score(symbol)
    
    def _score_rsi(self, rsi: float) -> float:
        """Score RSI indicator (0-2 points)."""
        if rsi < 25:
            return 2.0  # Strong oversold
        elif rsi < 35:
            return 1.5  # Moderate oversold
        elif rsi < 45:
            return 0.5  # Slight oversold
        elif rsi > 75:
            return 2.0  # Strong overbought
        elif rsi > 65:
            return 1.5  # Moderate overbought
        elif rsi > 55:
            return 0.5  # Slight overbought
        else:
            return 0.0  # Neutral RSI
    
    def _score_ema_alignment(self, current_price: float, ema_20: float, ema_50: float) -> float:
        """Score EMA alignment (0-2 points)."""
        if ema_20 <= 0 or ema_50 <= 0:
            return 0.0
        
        price_vs_ema20 = (current_price - ema_20) / ema_20 * 100
        ema20_vs_ema50 = (ema_20 - ema_50) / ema_50 * 100
        
        # Bullish alignment
        if price_vs_ema20 > 1 and ema20_vs_ema50 > 0.5:
            return 2.0
        elif price_vs_ema20 > 0 and ema20_vs_ema50 > 0:
            return 1.0
        
        # Bearish alignment
        elif price_vs_ema20 < -1 and ema20_vs_ema50 < -0.5:
            return 2.0
        elif price_vs_ema20 < 0 and ema20_vs_ema50 < 0:
            return 1.0
        
        return 0.0
    
    def _score_macd(self, macd_data: Dict[str, float]) -> float:
        """Score MACD indicator (0-2 points)."""
        if not macd_data:
            return 0.0
        
        macd_line = macd_data.get('macd', 0)
        signal_line = macd_data.get('signal', 0)
        histogram = macd_data.get('histogram', 0)
        
        # Strong bullish signal
        if macd_line > signal_line and histogram > 0 and abs(histogram) > 0.01:
            return 2.0
        # Moderate bullish signal
        elif macd_line > signal_line and histogram > 0:
            return 1.0
        # Strong bearish signal
        elif macd_line < signal_line and histogram < 0 and abs(histogram) > 0.01:
            return 2.0
        # Moderate bearish signal
        elif macd_line < signal_line and histogram < 0:
            return 1.0
        
        return 0.0
    
    def _score_bollinger_bands(self, current_price: float, bb_data: Dict[str, float]) -> float:
        """Score Bollinger Bands position (0-2 points)."""
        if not bb_data:
            return 0.0
        
        position = bb_data.get('position', 0.5)
        upper_band = bb_data.get('upper', current_price)
        lower_band = bb_data.get('lower', current_price)
        
        # Near lower band (oversold condition)
        if position < 0.1:
            return 2.0
        elif position < 0.2:
            return 1.5
        # Near upper band (overbought condition)
        elif position > 0.9:
            return 2.0
        elif position > 0.8:
            return 1.5
        # Middle range
        elif 0.4 <= position <= 0.6:
            return 0.5
        
        return 0.0
    
    def _score_volume(self, volume_zscore: float) -> float:
        """Score volume indicator (0-1 point)."""
        if volume_zscore > 2.0:
            return 1.0  # Very high volume
        elif volume_zscore > 1.5:
            return 0.5  # High volume
        
        return 0.0
    
    def _score_volatility(self, atr_percent: float) -> float:
        """Score volatility conditions (0-1 point)."""
        # Prefer moderate volatility for trading
        if 2.0 <= atr_percent <= 8.0:
            return 1.0  # Optimal volatility
        elif 1.0 <= atr_percent <= 12.0:
            return 0.5  # Acceptable volatility
        
        return 0.0
    
    def _determine_signal_direction(self, closes: List[float], ema_20: float, 
                                  ema_50: float, rsi: float, macd_data: Dict[str, float]) -> str:
        """Determine signal direction based on indicators."""
        bullish_signals = 0
        bearish_signals = 0
        
        # EMA alignment
        if closes[-1] > ema_20 > ema_50:
            bullish_signals += 1
        elif closes[-1] < ema_20 < ema_50:
            bearish_signals += 1
        
        # RSI
        if rsi < 40:
            bullish_signals += 1
        elif rsi > 60:
            bearish_signals += 1
        
        # MACD
        if macd_data and macd_data.get('macd', 0) > macd_data.get('signal', 0):
            bullish_signals += 1
        elif macd_data and macd_data.get('macd', 0) < macd_data.get('signal', 0):
            bearish_signals += 1
        
        if bullish_signals > bearish_signals:
            return "LONG"
        elif bearish_signals > bullish_signals:
            return "SHORT"
        else:
            return "NEUTRAL"
    
    def _calculate_price_levels(self, current_price: float, atr_percent: float, 
                              direction: str, bb_data: Dict[str, float]) -> tuple:
        """Calculate entry, stop loss, and take profit levels."""
        if direction == "NEUTRAL":
            return current_price, current_price, current_price
        
        atr_value = current_price * (atr_percent / 100)
        
        if direction == "LONG":
            entry_price = current_price
            stop_loss = current_price - (atr_value * 1.5)
            take_profit = current_price + (atr_value * 3.0)
        else:  # SHORT
            entry_price = current_price
            stop_loss = current_price + (atr_value * 1.5)
            take_profit = current_price - (atr_value * 3.0)
        
        return entry_price, stop_loss, take_profit
    
    def _default_score(self, symbol: str) -> Dict[str, Any]:
        """Return default score for failed calculations."""
        return {
            "symbol": symbol,
            "score": 0.0,
            "max_score": self.max_score,
            "signal_direction": "NEUTRAL",
            "confidence": 0.0,
            "entry_price": 0.0,
            "stop_loss": 0.0,
            "take_profit": 0.0,
            "components": {},
            "reasons": [],
            "timestamp": datetime.utcnow().isoformat(),
            "meets_threshold": False
        }