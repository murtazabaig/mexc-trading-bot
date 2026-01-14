"""Message formatting utilities for MEXC Futures Signal Bot."""

from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional


def format_status(uptime_seconds: int, last_scan: Optional[datetime], universe_size: int, mode: str) -> str:
    """Format bot status message.
    
    Args:
        uptime_seconds: Bot uptime in seconds
        last_scan: Last scan timestamp
        universe_size: Number of symbols in universe
        mode: Bot mode (active, paused, scanning)
    
    Returns:
        Formatted status message
    """
    # Format uptime
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        uptime_str = f"{hours}h {minutes}m"
    elif minutes > 0:
        uptime_str = f"{minutes}m {seconds}s"
    else:
        uptime_str = f"{seconds}s"
    
    # Format last scan
    if last_scan:
        now = datetime.now(timezone.utc)
        diff = now - last_scan
        if diff.total_seconds() < 60:
            last_scan_str = f"{int(diff.total_seconds())} seconds ago"
        elif diff.total_seconds() < 3600:
            last_scan_str = f"{int(diff.total_seconds() / 60)} minutes ago"
        else:
            hours_ago = int(diff.total_seconds() / 3600)
            last_scan_str = f"{hours_ago} hours ago"
    else:
        last_scan_str = "Never"
    
    # Mode emoji
    mode_emoji = {
        "active": "✅",
        "scanning": "🔍",
        "paused": "⏸️",
        "error": "❌"
    }.get(mode.lower(), "📊")
    
    return f"""🤖 *Bot Status*
⏱ Uptime: {uptime_str}
🔍 Last Scan: {last_scan_str}
🌍 Universe: {universe_size:,} symbols
📊 Mode: {mode_emoji} {mode.title()}"""


def format_signal(signal: Dict[str, Any]) -> str:
    """Format signal message.
    
    Args:
        signal: Signal dictionary with all required fields
    
    Returns:
        Formatted signal message
    """
    symbol = signal.get('symbol', 'UNKNOWN')
    timeframe = signal.get('timeframe', '1h')
    side = signal.get('side', 'LONG').upper()
    confidence = signal.get('confidence', 0) * 100
    regime = signal.get('regime', 'UNKNOWN').replace('_', ' ').title()
    entry_price = signal.get('entry_price', 0)
    entry_band_min = signal.get('entry_band_min', entry_price * 0.99)
    entry_band_max = signal.get('entry_band_max', entry_price * 1.01)
    stop_loss = signal.get('stop_loss', 0)
    tp1 = signal.get('tp1', 0)
    tp2 = signal.get('tp2', 0)
    tp3 = signal.get('tp3', 0)
    reason = signal.get('reason', {})
    
    # Side emoji
    side_emoji = "🟢" if side == "LONG" else "🔴"
    
    # Format price
    if entry_price >= 1000:
        price_str = f"${entry_price:,.0f}"
        band_str = f"${entry_band_min:,.0f} - ${entry_band_max:,.0f}"
        sl_str = f"${stop_loss:,.0f}"
        tp1_str = f"${tp1:,.0f}"
        tp2_str = f"${tp2:,.0f}"
        tp3_str = f"${tp3:,.0f}"
    else:
        price_str = f"${entry_price:.4f}"
        band_str = f"${entry_band_min:.4f} - ${entry_band_max:.4f}"
        sl_str = f"${stop_loss:.4f}"
        tp1_str = f"${tp1:.4f}"
        tp2_str = f"${tp2:.4f}"
        tp3_str = f"${tp3:.4f}"
    
    # Format reasons
    reasons_text = ""
    if reason:
        confluence = reason.get('confluence', [])
        if confluence:
            reasons_text = "Reasons:\n" + "\n".join(f"• {r}" for r in confluence)
        else:
            # Try to extract from other fields
            reasons = []
            if regime and regime != 'Unknown':
                reasons.append(f"{regime} regime")
            reasons_text = "Reasons:\n" + "\n".join(f"• {r}" for r in reasons) if reasons else ""
    
    return f"""{side_emoji} *NEW {side} SETUP*
    📈 *{symbol}* ({timeframe})
    Confidence: {confidence:.0f}% | Regime: {regime}

    Entry: {price_str} ± {band_str}
    SL: {sl_str}
    TP1: {tp1_str} | TP2: {tp2_str} | TP3: {tp3_str}

    {reasons_text}"""


def format_top_signals(signals: List[Dict[str, Any]], limit: int = 5) -> str:
    """Format top N signals message.
    
    Args:
        signals: List of signal dictionaries
        limit: Maximum number of signals to show
    
    Returns:
        Formatted top signals message
    """
    if not signals:
        return "🏆 *Top Setups*\n📭 No recent signals available"
    
    signals_to_show = signals[:limit]
    
    lines = ["🏆 *Top Setups*"]
    
    for i, signal in enumerate(signals_to_show, 1):
        symbol = signal.get('symbol', 'UNKNOWN')
        timeframe = signal.get('timeframe', '1h')
        side = signal.get('side', 'LONG').upper()
        confidence = signal.get('confidence', 0) * 100
        
        side_emoji = "🟢" if side == "LONG" else "🔴"
        confidence_str = f"{confidence:.0f}%"
        
        lines.append(f"{i}. {side_emoji} {symbol} {timeframe} {side} ({confidence_str})")
    
    return "\n".join(lines)


def format_symbol_analysis(symbol: str, regime: str, regime_confidence: float, 
                          indicators: Dict[str, Any], last_signals: List[Dict[str, Any]]) -> str:
    """Format symbol analysis message.
    
    Args:
        symbol: Trading symbol
        regime: Current market regime
        regime_confidence: Regime confidence (0-1)
        indicators: Dictionary of indicator values
        last_signals: List of recent signals for this symbol
    
    Returns:
        Formatted symbol analysis message
    """
    # Format regime
    regime_emoji = {
        "TRENDING": "📈",
        "RANGING": "📊", 
        "BREAKOUT": "⚡",
        "UNKNOWN": "❓"
    }.get(regime.upper(), "📊")
    
    confidence_pct = regime_confidence * 100
    
    # Format indicators
    indicator_lines = []
    for name, value in indicators.items():
        if name.upper() == 'EMA20':
            if value >= 1000:
                indicator_lines.append(f"EMA20: ${value:,.0f}")
            else:
                indicator_lines.append(f"EMA20: ${value:.4f}")
        elif name.upper() == 'RSI':
            indicator_lines.append(f"RSI: {value:.1f}")
        elif name.upper() == 'ATR%':
            indicator_lines.append(f"ATR%: {value:.1f}%")
        elif name.upper() == 'VWAP':
            if value >= 1000:
                indicator_lines.append(f"VWAP: ${value:,.0f}")
            else:
                indicator_lines.append(f"VWAP: ${value:.4f}")
        elif name.upper() == 'ADX':
            indicator_lines.append(f"ADX: {value:.1f}")
        elif name.upper() == 'VOLUME_ZSCORE':
            indicator_lines.append(f"Volume Z-Score: {value:.1f}")
    
    indicators_text = "\n".join(f"• {line}" for line in indicator_lines)
    
    # Format recent signals
    if last_signals:
        signal_lines = []
        for signal in last_signals[:3]:  # Show max 3 recent signals
            timestamp = signal.get('timestamp', '')
            side = signal.get('side', 'UNKNOWN').upper()
            entry_price = signal.get('entry_price', 0)
            status = signal.get('status', 'OPEN')
            
            # Parse timestamp if available
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    diff = now - dt
                    if diff.days > 0:
                        time_ago = f"{diff.days} days ago"
                    else:
                        hours = int(diff.total_seconds() / 3600)
                        if hours > 0:
                            time_ago = f"{hours} hours ago"
                        else:
                            minutes = int(diff.total_seconds() / 60)
                            time_ago = f"{minutes} minutes ago"
                except:
                    time_ago = "recently"
            else:
                time_ago = "recently"
            
            side_emoji = "🟢" if side == "LONG" else "🔴" if side == "SHORT" else "⚪"
            if entry_price >= 1000:
                price_str = f"${entry_price:,.0f}"
            else:
                price_str = f"${entry_price:.4f}"
            
            signal_lines.append(f"• {side_emoji} {side} @ {price_str} ({time_ago}, {status})")
        
        signals_text = "Recent Signals:\n" + "\n".join(signal_lines)
    else:
        signals_text = "Recent Signals:\n• No recent signals"
    
    return f"""📊 *{symbol} Analysis*
Regime: {regime_emoji} {regime} (confidence: {confidence_pct:.0f}%)

Indicators:
{indicators_text}

{signals_text}"""


def format_warning(warning: Dict[str, Any]) -> str:
    """Format warning message.
    
    Args:
        warning: Warning dictionary with all required fields
    
    Returns:
        Formatted warning message
    """
    severity = warning.get('severity', 'WARNING').upper()
    warning_type = warning.get('warning_type', 'UNKNOWN').replace('_', ' ').title()
    message = warning.get('message', 'No details available')
    triggered_value = warning.get('triggered_value')
    threshold = warning.get('threshold')
    action_taken = warning.get('action_taken', 'None')
    
    # Severity emoji
    severity_emoji = {
        "CRITICAL": "🚨",
        "WARNING": "⚠️",
        "INFO": "ℹ️"
    }.get(severity, "⚠️")
    
    # Warning type emoji
    type_emoji = {
        "BTC SHOCK": "₿",
        "BREADTH COLLAPSE": "📉",
        "CORRELATION SPIKE": "🔗",
        "VOLUME SURGE": "📊",
        "VOLATILITY SPIKE": "📈"
    }.get(warning_type.upper().replace(' ', '_'), "⚠️")
    
    # Format values
    value_str = ""
    if triggered_value is not None:
        if isinstance(triggered_value, float) and triggered_value < 1:
            value_str = f" ({triggered_value:.1%})"
        else:
            value_str = f" ({triggered_value:.2f})"
    
    threshold_str = ""
    if threshold is not None:
        if isinstance(threshold, float) and threshold < 1:
            threshold_str = f" (threshold: {threshold:.1%})"
        else:
            threshold_str = f" (threshold: {threshold:.2f})"
    
    return f"""{severity_emoji} *{severity} WARNING*
{type_emoji} Type: {warning_type}

{message}{value_str}{threshold_str}
Action: {action_taken}"""