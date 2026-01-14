import csv
import io
from .summarizer import DailySummary

def format_daily_summary(summary: DailySummary) -> str:
    """
    Human-readable Telegram message
    """
    long_count = summary.signals_by_type.get('LONG', 0)
    short_count = summary.signals_by_type.get('SHORT', 0)
    
    regime_dist = " | ".join([f"{k}: {v}" for k, v in summary.signals_by_regime.items()])
    warning_dist = " | ".join([f"{k}: {v}" for k, v in summary.warnings_by_severity.items()])
    
    top_signals_str = ""
    for i, s in enumerate(summary.top_signals, 1):
        top_signals_str += f"{i}. {s['symbol']} {s['timeframe']} {s['side']} ({int(s['confidence']*100)}%)\n"
    
    if not top_signals_str:
        top_signals_str = "No signals generated."

    report = f"""📈 *Daily Summary - {summary.date}*

📊 *Signals*
Total: {summary.total_signals}
LONG: {long_count} | SHORT: {short_count}
Avg Confidence: {int(summary.avg_confidence * 100)}%

Regime Distribution:
{regime_dist if regime_dist else "None"}

⚠️ *Warnings*
Total: {summary.warnings_triggered}
{warning_dist if warning_dist else "No warnings"}

💰 *Paper Trading*
Closed: {summary.paper_positions_closed}
PnL: {summary.paper_profit_loss_r:+.2f}R ({summary.win_rate:.1f}% win rate)
Max DD: -{summary.max_drawdown_r:.2f}R

🤖 *Operations*
Uptime: {summary.uptime_hours:.1f}h
Universe: {summary.metadata.get('universe_size', 'N/A')} symbols
Scans: {summary.metadata.get('scans_count', 1440)} (1 per minute)

🏆 *Top Signals*
{top_signals_str}
"""
    return report.strip()

def format_summary_csv(summary: DailySummary) -> str:
    """
    CSV format for logging/archiving
    Columns: date, total_signals, avg_confidence, warnings, pnl_r, win_rate, uptime
    """
    output = io.StringIO()
    writer = csv.writer(output)
    
    # We might want to write header only if file is new, 
    # but here we return a single row as per typical formatter usage.
    writer.writerow([
        summary.date,
        summary.total_signals,
        f"{summary.avg_confidence:.4f}",
        summary.warnings_triggered,
        f"{summary.paper_profit_loss_r:.4f}",
        f"{summary.win_rate:.2f}",
        f"{summary.uptime_hours:.2f}"
    ])
    
    return output.getvalue().strip()
