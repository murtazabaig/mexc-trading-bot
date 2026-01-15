import json
import sqlite3
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from ..database import query_signals_by_date, query_warnings_by_date, query_closed_positions_by_date, query_uptime

@dataclass
class DailySummary:
    date: str  # YYYY-MM-DD
    total_signals: int = 0
    signals_by_type: Dict[str, int] = field(default_factory=dict)  # {LONG: count, SHORT: count}
    signals_by_regime: Dict[str, int] = field(default_factory=dict)  # {TREND_UP: count, RANGE: count, ...}
    avg_confidence: float = 0.0
    warnings_triggered: int = 0
    warnings_by_severity: Dict[str, int] = field(default_factory=dict)  # {INFO: count, WARNING: count, CRITICAL: count}
    critical_events: List[str] = field(default_factory=list)  # List of critical warning messages
    paper_positions_closed: int = 0
    paper_profit_loss_r: float = 0.0  # Total PnL in R units
    win_rate: float = 0.0  # % of closed positions with PnL > 0
    max_drawdown_r: float = 0.0  # Max consecutive loss in R
    max_portfolio_value_r: float = 0.0
    uptime_hours: float = 0.0
    top_signals: List[Dict[str, Any]] = field(default_factory=list)  # Top N signals by confidence
    metadata: Dict[str, Any] = field(default_factory=dict)

class ReportGenerator:
    async def generate_daily_summary(self, db: sqlite3.Connection, date: str = None, universe_size: int = 0) -> DailySummary:
        """
        Query signals, warnings, positions, uptime for given date
        Calculate all metrics
        Return DailySummary object
        """
        if date is None:
            # Default to yesterday
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        signals = query_signals_by_date(db, date)
        warnings = query_warnings_by_date(db, date)
        closed_positions = query_closed_positions_by_date(db, date)
        uptime_hours = query_uptime(db, date)
        
        # Signals metrics
        total_signals = len(signals)
        signals_by_type = {}
        signals_by_regime = {}
        total_confidence = 0.0
        
        for s in signals:
            side = s.get('side', 'UNKNOWN')
            signals_by_type[side] = signals_by_type.get(side, 0) + 1
            
            regime = s.get('regime', 'UNKNOWN')
            signals_by_regime[regime] = signals_by_regime.get(regime, 0) + 1
            
            total_confidence += s.get('confidence', 0.0)
            
        avg_confidence = total_confidence / total_signals if total_signals > 0 else 0.0
        
        # Sort signals by confidence for top signals
        sorted_signals = sorted(signals, key=lambda x: x.get('confidence', 0.0), reverse=True)
        top_signals = sorted_signals[:5]
        
        # Warnings metrics
        warnings_triggered = len(warnings)
        warnings_by_severity = {}
        critical_events = []
        
        for w in warnings:
            severity = w.get('severity', 'UNKNOWN')
            warnings_by_severity[severity] = warnings_by_severity.get(severity, 0) + 1
            if severity == 'CRITICAL':
                critical_events.append(w.get('message', 'No message'))
        
        # Paper trading metrics
        paper_positions_closed = len(closed_positions)
        paper_profit_loss_r = 0.0
        wins = 0
        
        # For drawdown calculation
        current_pnl_r = 0.0
        max_pnl_r = 0.0
        max_dd_r = 0.0
        
        # Sort positions by exit time to calculate DD correctly
        sorted_positions = sorted(closed_positions, key=lambda x: x.get('exit_time', ''))
        
        for p in sorted_positions:
            pnl_r = p.get('pnl_r', p.get('pnl', 0.0))
            paper_profit_loss_r += pnl_r
            if pnl_r > 0:
                wins += 1
            
            current_pnl_r += pnl_r
            if current_pnl_r > max_pnl_r:
                max_pnl_r = current_pnl_r
            
            dd = max_pnl_r - current_pnl_r
            if dd > max_dd_r:
                max_dd_r = dd
                
        win_rate = (wins / paper_positions_closed * 100) if paper_positions_closed > 0 else 0.0
        
        summary = DailySummary(
            date=date,
            total_signals=total_signals,
            signals_by_type=signals_by_type,
            signals_by_regime=signals_by_regime,
            avg_confidence=avg_confidence,
            warnings_triggered=warnings_triggered,
            warnings_by_severity=warnings_by_severity,
            critical_events=critical_events,
            paper_positions_closed=paper_positions_closed,
            paper_profit_loss_r=paper_profit_loss_r,
            win_rate=win_rate,
            max_drawdown_r=max_dd_r,
            max_portfolio_value_r=max_pnl_r,
            uptime_hours=uptime_hours,
            top_signals=top_signals,
            metadata={
                "universe_size": universe_size,
                "scans_count": 1440  # Placeholder or calculate if possible
            }
        )
        
        return summary
