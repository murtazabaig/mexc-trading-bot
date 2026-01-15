"""Regime classification module for market state identification."""

from typing import Dict, List, Any, Optional
from datetime import datetime


class RegimeClassifier:
    """Classifies market regimes based on technical indicators and price action."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize regime classifier.
        
        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = None
    
    def set_logger(self, logger):
        """Set logger instance."""
        self.logger = logger
    
    def classify_regime(self, symbol: str, ohlcv_data: Dict[str, List[float]], 
                       indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Classify market regime for a symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            ohlcv_data: OHLCV data with keys 'highs', 'lows', 'closes', 'volumes'
            indicators: Calculated technical indicators
            
        Returns:
            Dictionary with regime classification results
        """
        try:
            if not ohlcv_data or not indicators:
                return self._default_regime(symbol)
            
            closes = ohlcv_data.get('closes', [])
            highs = ohlcv_data.get('highs', [])
            lows = ohlcv_data.get('lows', [])
            
            if len(closes) < 20:  # Need minimum data
                return self._default_regime(symbol)
            
            # Extract key indicators
            rsi_14 = indicators.get('rsi', {}).get('value', 50.0)
            ema_20 = indicators.get('ema', {}).get('20', closes[-1])
            ema_50 = indicators.get('ema', {}).get('50', closes[-1])
            atr_pct = indicators.get('atr_percent', {}).get('14', 0.0)
            adx = indicators.get('adx', {}).get('14', 0.0)
            
            # Simple regime classification logic
            current_price = closes[-1]
            price_vs_ema20 = (current_price - ema_20) / ema_20 * 100
            price_vs_ema50 = (current_price - ema_50) / ema_50 * 100
            
            # Determine trend regime
            if price_vs_ema20 > 2 and price_vs_ema50 > 1:
                trend = "BULLISH"
            elif price_vs_ema20 < -2 and price_vs_ema50 < -1:
                trend = "BEARISH"
            else:
                trend = "SIDEWAYS"
            
            # Determine volatility regime
            if atr_pct > 5.0:
                volatility = "HIGH"
            elif atr_pct < 2.0:
                volatility = "LOW"
            else:
                volatility = "NORMAL"
            
            # Determine momentum regime
            if rsi_14 > 70:
                momentum = "OVERBOUGHT"
            elif rsi_14 < 30:
                momentum = "OVERSOLD"
            else:
                momentum = "NEUTRAL"
            
            # Combine regimes
            regime = f"{trend}_{volatility}_{momentum}"
            
            # Confidence score based on indicator alignment
            confidence = self._calculate_regime_confidence(rsi_14, ema_20, ema_50, adx, price_vs_ema20, price_vs_ema50)
            
            result = {
                "symbol": symbol,
                "regime": regime,
                "trend": trend,
                "volatility": volatility,
                "momentum": momentum,
                "confidence": confidence,
                "timestamp": datetime.utcnow().isoformat(),
                "indicators": {
                    "rsi": rsi_14,
                    "price_vs_ema20": price_vs_ema20,
                    "price_vs_ema50": price_vs_ema50,
                    "atr_percent": atr_pct,
                    "adx": adx
                }
            }
            
            if self.logger:
                self.logger.debug(f"Regime classified for {symbol}: {regime} (confidence: {confidence:.2f})")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error classifying regime for {symbol}: {e}")
            return self._default_regime(symbol)
    
    def _default_regime(self, symbol: str) -> Dict[str, Any]:
        """Return default regime classification."""
        return {
            "symbol": symbol,
            "regime": "UNKNOWN",
            "trend": "UNKNOWN",
            "volatility": "UNKNOWN", 
            "momentum": "UNKNOWN",
            "confidence": 0.0,
            "timestamp": datetime.utcnow().isoformat(),
            "indicators": {}
        }
    
    def _calculate_regime_confidence(self, rsi: float, ema_20: float, ema_50: float, 
                                   adx: float, price_vs_ema20: float, price_vs_ema50: float) -> float:
        """Calculate confidence score for regime classification."""
        confidence = 0.5  # Base confidence
        
        # RSI contributes to confidence
        if 30 <= rsi <= 70:
            confidence += 0.1  # Normal RSI range
        
        # EMA alignment contributes to confidence
        if (price_vs_ema20 > 0 and price_vs_ema50 > 0) or (price_vs_ema20 < 0 and price_vs_ema50 < 0):
            confidence += 0.2  # EMAs aligned
        
        # ADX contributes to confidence (higher ADX = clearer trend)
        if adx > 25:
            confidence += 0.2  # Strong trend
        elif adx < 20:
            confidence -= 0.1  # Weak trend
        
        return min(1.0, max(0.0, confidence))