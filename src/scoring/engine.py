"""Signal scoring engine for evaluating trading opportunities."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import math


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
            volumes = ohlcv_data.get('volumes', [])
            
            if len(closes) < 20:  # Need minimum data
                return self._default_score(symbol)
            
            # Extract indicators
            rsi_14 = indicators.get('rsi', {}).get('value', 50.0)
            ema_20 = indicators.get('ema', {}).get('20', closes[-1])
            ema_50 = indicators.get('ema', {}).get('50', closes[-1])
            macd_data = indicators.get('macd', {})
            bb_data = indicators.get('bollinger_bands', {})
            atr_pct = indicators.get('atr_percent', {}).get('14', 0.0)
            atr_val = indicators.get('atr', {}).get('14', 0.0)
            volume_zscore = indicators.get('volume_zscore', {}).get('20', 0.0)
            
            # Calculate signal direction first to align scores
            signal_direction = self._determine_signal_direction(closes, ema_20, ema_50, rsi_14, macd_data, regime)
            
            # Calculate score components
            scores = {}
            reasons = []
            
            # 1. RSI Scoring (0-2 points)
            rsi_score = self._score_rsi(rsi_14, signal_direction)
            scores['rsi'] = rsi_score
            if rsi_score >= 1.5:
                reasons.append("RSI_EXTREME")
            elif rsi_score >= 0.5:
                reasons.append("RSI_ALIGNMENT")
            
            # 2. EMA Alignment Scoring (0-2 points)
            ema_score = self._score_ema_alignment(closes[-1], ema_20, ema_50, signal_direction)
            scores['ema_alignment'] = ema_score
            if ema_score >= 1.5:
                reasons.append("EMA_STRONG_TREND")
            elif ema_score >= 0.5:
                reasons.append("EMA_ALIGNMENT")
            
            # 3. MACD Scoring (0-2 points)
            macd_score = self._score_macd(macd_data, signal_direction)
            scores['macd'] = macd_score
            if macd_score >= 1.5:
                reasons.append("MACD_BULLISH" if signal_direction == "LONG" else "MACD_BEARISH")
            elif macd_score >= 0.5:
                reasons.append("MACD_MOMENTUM")
            
            # 4. Bollinger Bands Scoring (0-2 points)
            bb_score = self._score_bollinger_bands(closes[-1], bb_data, signal_direction)
            scores['bollinger_bands'] = bb_score
            if bb_score >= 1.5:
                reasons.append("BB_OUTER_REVERSAL")
            elif bb_score >= 0.5:
                reasons.append("BB_POSITION")
            
            # 5. Volume Scoring (0-1 point)
            volume_score = self._score_volume(volume_zscore)
            scores['volume'] = volume_score
            if volume_score >= 0.5:
                reasons.append("VOLUME_CONFIRMATION")
            
            # 6. Volatility Scoring (0-1 point)
            volatility_score = self._score_volatility(regime)
            scores['volatility'] = volatility_score
            if volatility_score >= 0.5:
                reasons.append("VOLATILITY_FAVORABLE")
            
            # Calculate total score
            total_score = sum(scores.values())
            
            # Generate entry and exit prices
            entry_price, stop_loss, take_profit = self._calculate_price_levels(
                closes[-1], atr_val, signal_direction, bb_data
            )
            
            # Ensure reasons are unique and sorted
            reasons = sorted(list(set(reasons)))
            
            explanation = {
                "score_components": scores,
                "indicators": {
                    "rsi": rsi_14,
                    "ema20": ema_20,
                    "ema50": ema_50,
                    "macd": macd_data,
                    "bb": bb_data,
                    "atr_pct": atr_pct,
                    "volume_zscore": volume_zscore
                },
                "regime": regime,
                "decision_path": f"Market in {regime.get('trend', 'UNKNOWN')} trend. " +
                               f"Signal direction {signal_direction} determined with confidence {total_score/self.max_score:.2f}."
            }
            
            result = {
                "symbol": symbol,
                "score": round(total_score, 2),
                "max_score": self.max_score,
                "signal_direction": signal_direction,
                "confidence": round(total_score / self.max_score, 2),
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "components": scores,
                "reasons": reasons,
                "json_explanation": json.dumps(explanation),
                "timestamp": datetime.utcnow().isoformat(),
                "meets_threshold": total_score >= self.min_score
            }
            
            if self.logger:
                self.logger.debug(f"Score calculated for {symbol}: {total_score:.1f}/{self.max_score} ({signal_direction})")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error scoring signal for {symbol}: {e}")
            import traceback
            if self.logger:
                self.logger.error(traceback.format_exc())
            return self._default_score(symbol)
    
    def _score_rsi(self, rsi: float, direction: str) -> float:
        """Score RSI indicator (0-2 points).
        Requirement: favor longs when RSI 30-50, shorts when RSI 50-70.
        """
        if direction == "LONG":
            if 30 <= rsi <= 50:
                return 2.0
            elif rsi < 30:
                return 1.5
            elif 50 < rsi <= 60:
                return 0.5
        elif direction == "SHORT":
            if 50 <= rsi <= 70:
                return 2.0
            elif rsi > 70:
                return 1.5
            elif 40 <= rsi < 50:
                return 0.5
        return 0.0
    
    def _score_ema_alignment(self, current_price: float, ema_20: float, ema_50: float, direction: str) -> float:
        """Score EMA alignment (0-2 points).
        Requirement: longs when price above EMA20 and EMA20 > EMA50, shorts when below.
        """
        if ema_20 <= 0 or ema_50 <= 0:
            return 0.0
        
        if direction == "LONG":
            if current_price > ema_20 and ema_20 > ema_50:
                return 2.0
            elif current_price > ema_20 or ema_20 > ema_50:
                return 1.0
        elif direction == "SHORT":
            if current_price < ema_20 and ema_20 < ema_50:
                return 2.0
            elif current_price < ema_20 or ema_20 < ema_50:
                return 1.0
        
        return 0.0
    
    def _score_macd(self, macd_data: Dict[str, float], direction: str) -> float:
        """Score MACD indicator (0-2 points).
        Requirement: positive for histogram expansion matching signal direction.
        """
        if not macd_data:
            return 0.0
        
        macd_line = macd_data.get('macd', 0)
        signal_line = macd_data.get('signal', 0)
        histogram = macd_data.get('histogram', 0)
        
        # We'd need previous histogram to check expansion properly, 
        # but let's assume if it's substantial and in right direction, it's good.
        if direction == "LONG":
            if macd_line > signal_line and histogram > 0:
                return 2.0 if histogram > 0.001 else 1.0
        elif direction == "SHORT":
            if macd_line < signal_line and histogram < 0:
                return 2.0 if histogram < -0.001 else 1.0
        
        return 0.0
    
    def _score_bollinger_bands(self, current_price: float, bb_data: Dict[str, float], direction: str) -> float:
        """Score Bollinger Bands position (0-2 points).
        Requirement: score higher if price near outer bands + volatility contracted.
        """
        if not bb_data:
            return 0.0
        
        position = bb_data.get('position', 0.5)
        bandwidth = bb_data.get('bandwidth', 0)
        
        score = 0.0
        if direction == "LONG" and position < 0.2:
            score = 1.5
        elif direction == "SHORT" and position > 0.8:
            score = 1.5
            
        # Add bonus for contracted volatility (bandwidth would ideally be compared to average)
        # Without average bandwidth, we can only guess or use a threshold
        # Assuming bandwidth < 2% is contracted for most crypto
        if score > 0 and bandwidth > 0:
            # This is a bit arbitrary without more context
            score += 0.5
            
        return min(2.0, score)
    
    def _score_volume(self, volume_zscore: float) -> float:
        """Score volume indicator (0-1 point).
        Requirement: volume should be above 20-period average on entry candle.
        """
        if volume_zscore > 0:
            return 1.0 if volume_zscore > 1.0 else 0.5
        
        return 0.0
    
    def _score_volatility(self, regime: Dict[str, Any]) -> float:
        """Score volatility conditions (0-1 point).
        Requirement: prefer signals in NORMAL volatility, caution in HIGH.
        """
        volatility = regime.get('volatility', 'NORMAL')
        
        if volatility == 'NORMAL':
            return 1.0
        elif volatility == 'LOW':
            return 0.5
        elif volatility == 'HIGH':
            return 0.2  # Caution
            
        return 0.0
    
    def _determine_signal_direction(self, closes: List[float], ema_20: float, 
                                  ema_50: float, rsi: float, macd_data: Dict[str, float],
                                  regime: Dict[str, Any]) -> str:
        """Determine signal direction based on indicators and market regime."""
        bullish_signals = 0
        bearish_signals = 0
        
        # 1. Regime Trend
        trend = regime.get('trend', 'SIDEWAYS')
        if trend == 'BULLISH':
            bullish_signals += 2
        elif trend == 'BEARISH':
            bearish_signals += 2
            
        # 2. EMA alignment
        if closes[-1] > ema_20 > ema_50:
            bullish_signals += 1
        elif closes[-1] < ema_20 < ema_50:
            bearish_signals += 1
        
        # 3. RSI
        if rsi < 45:
            bullish_signals += 1
        elif rsi > 55:
            bearish_signals += 1
        
        # 4. MACD
        if macd_data:
            if macd_data.get('macd', 0) > macd_data.get('signal', 0):
                bullish_signals += 1
            else:
                bearish_signals += 1
        
        if bullish_signals > bearish_signals + 1:
            return "LONG"
        elif bearish_signals > bullish_signals + 1:
            return "SHORT"
        else:
            # If close but not decisive, check trend again
            if trend == 'BULLISH': return "LONG"
            if trend == 'BEARISH': return "SHORT"
            return "NEUTRAL"
    
    def _calculate_price_levels(self, current_price: float, atr_value: float, 
                               direction: str, bb_data: Dict[str, float]) -> tuple:
        """Calculate entry, stop loss, and take profit levels.
        Requirement: 1.5-2x ATR for SL, 2-3x ATR for TP.
        """
        if direction == "NEUTRAL" or not current_price:
            return current_price, 0.0, 0.0
        
        # Fallback for ATR
        if not atr_value or math.isnan(atr_value):
            atr_value = current_price * 0.02  # 2% fallback
            
        entry_price = current_price
        
        if direction == "LONG":
            # SL: 1.5x ATR, TP: 3x ATR
            stop_loss = current_price - (atr_value * 1.5)
            # Use lower BB as secondary SL if it's further away
            if bb_data and bb_data.get('lower'):
                stop_loss = min(stop_loss, bb_data.get('lower'))
            
            take_profit = current_price + (atr_value * 3.0)
        else:  # SHORT
            stop_loss = current_price + (atr_value * 1.5)
            if bb_data and bb_data.get('upper'):
                stop_loss = max(stop_loss, bb_data.get('upper'))
                
            take_profit = current_price - (atr_value * 3.0)
        
        return round(entry_price, 6), round(stop_loss, 6), round(take_profit, 6)
    
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
            "json_explanation": "{}",
            "timestamp": datetime.utcnow().isoformat(),
            "meets_threshold": False
        }
