"""Regime classification module for market state identification."""

from typing import Dict, List, Any, Optional
from datetime import datetime
import numpy as np


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
        """Classify market regime for a symbol using REAL OHLCV data.
        
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
            volumes = ohlcv_data.get('volumes', [])
            
            if len(closes) < 20:  # Need minimum data
                return self._default_regime(symbol)
            
            # Extract key indicators
            rsi_14 = indicators.get('rsi', {}).get('value', 50.0)
            ema_data = indicators.get('ema', {})
            ema_20 = ema_data.get('20', closes[-1] if closes else 0)
            ema_50 = ema_data.get('50', closes[-1] if closes else 0)
            ema_200 = ema_data.get('200', closes[-1] if closes else 0)
            atr_pct = indicators.get('atr_percent', {}).get('14', 0.0)
            adx = indicators.get('adx', {}).get('14', 0.0)
            
            # Advanced regime classification logic using REAL data
            current_price = closes[-1] if closes else 0
            price_vs_ema20 = ((current_price - ema_20) / ema_20 * 100) if ema_20 > 0 else 0
            price_vs_ema50 = ((current_price - ema_50) / ema_50 * 100) if ema_50 > 0 else 0
            
            # Volume analysis for confirmation
            volume_confirmation = self._analyze_volume_confirmation(volumes)
            
            # Price momentum analysis
            momentum_analysis = self._analyze_price_momentum(closes, highs, lows)
            
            # Determine trend regime with real data
            trend = self._classify_trend_regime(
                current_price, ema_20, ema_50, ema_200, 
                price_vs_ema20, price_vs_ema50, 
                momentum_analysis, rsi_14
            )
            
            # Determine volatility regime
            volatility = self._classify_volatility_regime(atr_pct, volume_confirmation)
            
            # Determine momentum regime
            momentum = self._classify_momentum_regime(rsi_14, momentum_analysis)
            
            # Combine regimes with confidence weighting
            regime = self._combine_regimes(trend, volatility, momentum, volume_confirmation)
            
            # Calculate confidence score based on indicator alignment
            confidence = self._calculate_regime_confidence(
                rsi_14, ema_20, ema_50, adx, price_vs_ema20, price_vs_ema50, 
                volume_confirmation, momentum_analysis
            )
            
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
                    "adx": adx,
                    "volume_confirmation": volume_confirmation,
                    "momentum_analysis": momentum_analysis
                },
                "real_data_quality": self._assess_data_quality(closes, volumes)
            }
            
            if self.logger:
                self.logger.debug(f"Regime classified for {symbol}: {regime} (confidence: {confidence:.2f})")
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error classifying regime for {symbol}: {e}")
            return self._default_regime(symbol)
    
    def _analyze_volume_confirmation(self, volumes: List[float]) -> Dict[str, float]:
        """Analyze volume patterns for regime confirmation."""
        if len(volumes) < 20:
            return {"confirmation": 0.0, "strength": 0.0}
        
        recent_volume = np.mean(volumes[-5:])
        avg_volume = np.mean(volumes[-20:])
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 1.0
        
        return {
            "confirmation": min(1.0, max(0.0, (volume_ratio - 0.8) / 0.4)),  # Normalize around 1.0
            "strength": min(1.0, volume_ratio),  # Raw volume ratio
            "recent_vs_avg": volume_ratio
        }
    
    def _analyze_price_momentum(self, closes: List[float], highs: List[float], lows: List[float]) -> Dict[str, float]:
        """Analyze price momentum patterns."""
        if len(closes) < 10:
            return {"momentum": 0.0, "strength": 0.0}
        
        # Short-term momentum (5-period)
        short_momentum = (closes[-1] - closes[-6]) / closes[-6] if closes[-6] > 0 else 0
        
        # Medium-term momentum (10-period)  
        medium_momentum = (closes[-1] - closes[-11]) / closes[-11] if closes[-11] > 0 else 0
        
        # Volatility-adjusted momentum
        volatility = np.std(closes[-10:]) if len(closes) >= 10 else 0
        momentum_strength = abs(short_momentum) / (volatility + 1e-6)  # Avoid division by zero
        
        return {
            "short_momentum": short_momentum,
            "medium_momentum": medium_momentum,
            "momentum": short_momentum,  # Use short-term as primary
            "strength": min(1.0, momentum_strength / 2.0),  # Normalize
            "consistency": 1.0 if (short_momentum * medium_momentum) > 0 else 0.0
        }
    
    def _classify_trend_regime(self, current_price: float, ema_20: float, ema_50: float, ema_200: float,
                             price_vs_ema20: float, price_vs_ema50: float, 
                             momentum_analysis: Dict[str, float], rsi_14: float) -> str:
        """Classify trend regime using real data analysis."""
        # Multi-timeframe trend analysis
        trend_score = 0
        
        # EMA alignment analysis
        if ema_20 > ema_50 > 0:
            trend_score += 1
        elif ema_20 < ema_50:
            trend_score -= 1
        
        if ema_50 > ema_200 > 0:
            trend_score += 1
        elif ema_50 < ema_200:
            trend_score -= 1
        
        # Price vs EMAs
        if price_vs_ema20 > 2:
            trend_score += 1
        elif price_vs_ema20 < -2:
            trend_score -= 1
        
        # Momentum confirmation
        momentum_score = momentum_analysis.get('short_momentum', 0)
        if momentum_score > 0.02:  # 2% positive momentum
            trend_score += 1
        elif momentum_score < -0.02:  # 2% negative momentum
            trend_score -= 1
        
        # RSI trend confirmation
        if rsi_14 > 55:
            trend_score += 0.5
        elif rsi_14 < 45:
            trend_score -= 0.5
        
        # Classify based on composite score
        if trend_score >= 2:
            return "STRONG_BULLISH"
        elif trend_score >= 1:
            return "BULLISH"
        elif trend_score <= -2:
            return "STRONG_BEARISH"
        elif trend_score <= -1:
            return "BEARISH"
        else:
            return "SIDEWAYS"
    
    def _classify_volatility_regime(self, atr_pct: float, volume_confirmation: Dict[str, float]) -> str:
        """Classify volatility regime."""
        base_volatility = atr_pct
        
        # Adjust for volume spikes (can indicate volatility)
        volume_factor = volume_confirmation.get('strength', 1.0)
        adjusted_volatility = base_volatility * (1 + (volume_factor - 1) * 0.5)
        
        if adjusted_volatility > 4.0:
            return "HIGH"
        elif adjusted_volatility > 2.0:
            return "NORMAL"
        else:
            return "LOW"
    
    def _classify_momentum_regime(self, rsi_14: float, momentum_analysis: Dict[str, float]) -> str:
        """Classify momentum regime."""
        # Combine RSI with price momentum
        rsi_momentum = 0
        if rsi_14 > 70:
            rsi_momentum = 2  # Strong overbought
        elif rsi_14 > 60:
            rsi_momentum = 1  # Mild overbought
        elif rsi_14 < 30:
            rsi_momentum = -2  # Strong oversold
        elif rsi_14 < 40:
            rsi_momentum = -1  # Mild oversold
        
        price_momentum = momentum_analysis.get('strength', 0)
        
        # Combined momentum score
        combined_momentum = rsi_momentum + price_momentum
        
        if combined_momentum >= 2:
            return "STRONG_MOMENTUM"
        elif combined_momentum >= 1:
            return "POSITIVE_MOMENTUM"
        elif combined_momentum <= -2:
            return "NEGATIVE_MOMENTUM"
        elif combined_momentum <= -1:
            return "WEAK_MOMENTUM"
        else:
            return "NEUTRAL_MOMENTUM"
    
    def _combine_regimes(self, trend: str, volatility: str, momentum: str, volume_confirmation: Dict[str, float]) -> str:
        """Combine individual regimes into overall market regime."""
        # Weight factors
        volume_weight = min(1.0, volume_confirmation.get('strength', 1.0))
        
        # Create composite regime name
        if "STRONG" in trend:
            regime = f"{trend}_{volatility}_{momentum}"
        elif "BULLISH" in trend:
            regime = f"BULLISH_{volatility}_{momentum}"
        elif "BEARISH" in trend:
            regime = f"BEARISH_{volatility}_{momentum}"
        else:
            regime = f"SIDEWAYS_{volatility}_{momentum}"
        
        # Apply volume confirmation modifier
        if volume_confirmation.get('strength', 1.0) > 1.5:  # High volume
            regime += "_HIGH_VOLUME"
        elif volume_confirmation.get('strength', 1.0) < 0.7:  # Low volume
            regime += "_LOW_VOLUME"
        
        return regime
    
    def _assess_data_quality(self, closes: List[float], volumes: List[float]) -> Dict[str, float]:
        """Assess quality of input data."""
        if len(closes) < 20:
            return {"quality": 0.0, "completeness": 0.0}
        
        # Check for data completeness
        completeness = min(1.0, len(closes) / 50.0)  # Assume 50 is good sample size
        
        # Check for data continuity (no large gaps)
        continuity = 1.0
        for i in range(1, len(closes)):
            price_change = abs((closes[i] - closes[i-1]) / closes[i-1])
            if price_change > 0.5:  # 50% gap is suspicious
                continuity *= 0.9
        
        # Overall quality score
        quality = (completeness + continuity) / 2.0
        
        return {
            "quality": quality,
            "completeness": completeness,
            "continuity": continuity,
            "sample_size": len(closes)
        }
    
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
                                   adx: float, price_vs_ema20: float, price_vs_ema50: float,
                                   volume_confirmation: Dict[str, float], 
                                   momentum_analysis: Dict[str, float]) -> float:
        """Calculate confidence score for regime classification using real data."""
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
        
        # Volume confirmation contributes to confidence
        volume_strength = volume_confirmation.get('strength', 1.0)
        if volume_strength > 1.2:
            confidence += 0.1  # Strong volume confirmation
        elif volume_strength < 0.8:
            confidence -= 0.1  # Weak volume confirmation
        
        # Momentum consistency contributes to confidence
        momentum_consistency = momentum_analysis.get('consistency', 0.5)
        confidence += momentum_consistency * 0.1
        
        return min(1.0, max(0.0, confidence))