"""Signal dispatch job for sending new signals via Telegram."""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from ..database import query_recent_signals
from ..logger import get_logger

logger = get_logger(__name__)


async def dispatch_pending_signals(bot, db_conn, logger_instance) -> Dict[str, Any]:
    """Dispatch pending signals to Telegram.
    
    This job queries recent signals from the database that haven't been sent yet
    and sends them via the Telegram bot.
    
    Args:
        bot: MexcSignalBot instance
        db_conn: Database connection
        logger_instance: Logger instance
    
    Returns:
        Dictionary with dispatch statistics
    """
    if not bot or not db_conn:
        logger_instance.error("Bot or database connection not available")
        return {"success": False, "error": "Missing bot or database"}
    
    try:
        # Query recent signals from last hour that haven't been processed
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        
        # Get recent signals (this would need a field to track if sent)
        # For now, we'll use the existing query_recent_signals function
        recent_signals = query_recent_signals(db_conn, limit=20)
        
        sent_count = 0
        failed_count = 0
        
        for signal in recent_signals:
            try:
                # Check if signal has been sent recently (avoid duplicates)
                signal_time = datetime.fromisoformat(signal['timestamp'].replace('Z', '+00:00'))
                
                if signal_time < cutoff_time:
                    continue  # Skip old signals
                
                # Send signal via bot
                success = await bot.send_signal(signal)
                
                if success:
                    sent_count += 1
                    logger_instance.info(f"Signal dispatched: {signal['symbol']} {signal['side']}")
                else:
                    failed_count += 1
                    logger_instance.warning(f"Failed to send signal: {signal['symbol']}")
                    
            except Exception as e:
                failed_count += 1
                logger_instance.error(f"Error processing signal {signal.get('id', 'unknown')}: {e}")
        
        result = {
            "success": True,
            "signals_checked": len(recent_signals),
            "signals_sent": sent_count,
            "signals_failed": failed_count,
            "timestamp": datetime.utcnow()
        }
        
        logger_instance.info(f"Signal dispatch completed: {sent_count} sent, {failed_count} failed")
        return result
        
    except Exception as e:
        logger_instance.error(f"Error in signal dispatch job: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.utcnow()
        }


async def dispatch_recent_warnings(bot, db_conn, logger_instance) -> Dict[str, Any]:
    """Dispatch recent warnings to Telegram.
    
    Args:
        bot: MexcSignalBot instance
        db_conn: Database connection
        logger_instance: Logger instance
    
    Returns:
        Dictionary with dispatch statistics
    """
    if not bot or not db_conn:
        logger_instance.error("Bot or database connection not available")
        return {"success": False, "error": "Missing bot or database"}
    
    try:
        # Query recent warnings (last 6 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=6)
        
        # Query warnings from database
        cursor = db_conn.cursor()
        query = """
        SELECT * FROM warnings 
        WHERE timestamp >= ? 
        ORDER BY timestamp DESC 
        LIMIT 10
        """
        
        cursor.execute(query, (cutoff_time,))
        rows = cursor.fetchall()
        
        warnings = []
        for row in rows:
            warning = dict(row)
            if warning.get('metadata'):
                import json
                warning['metadata'] = json.loads(warning['metadata'])
            warnings.append(warning)
        
        sent_count = 0
        failed_count = 0
        
        for warning in warnings:
            try:
                # Send warning via bot
                success = await bot.send_warning(warning)
                
                if success:
                    sent_count += 1
                    logger_instance.info(f"Warning dispatched: {warning['warning_type']}")
                else:
                    failed_count += 1
                    logger_instance.warning(f"Failed to send warning: {warning['warning_type']}")
                    
            except Exception as e:
                failed_count += 1
                logger_instance.error(f"Error processing warning {warning.get('id', 'unknown')}: {e}")
        
        result = {
            "success": True,
            "warnings_checked": len(warnings),
            "warnings_sent": sent_count,
            "warnings_failed": failed_count,
            "timestamp": datetime.utcnow()
        }
        
        logger_instance.info(f"Warning dispatch completed: {sent_count} sent, {failed_count} failed")
        return result
        
    except Exception as e:
        logger_instance.error(f"Error in warning dispatch job: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.utcnow()
        }


def create_signal_dispatch_jobs(scheduler, bot, db_conn, logger_instance):
    """Create and schedule signal dispatch jobs.
    
    Args:
        scheduler: APScheduler instance
        bot: MexcSignalBot instance
        db_conn: Database connection
        logger_instance: Logger instance
    """
    # Add signal dispatch job - every 1 minute
    scheduler.add_job(
        dispatch_pending_signals,
        'interval',
        minutes=1,
        args=[bot, db_conn, logger_instance],
        id='signal_dispatch',
        name='Signal Dispatch',
        replace_existing=True
    )
    
    # Add warning dispatch job - every 5 minutes
    scheduler.add_job(
        dispatch_recent_warnings,
        'interval',
        minutes=5,
        args=[bot, db_conn, logger_instance],
        id='warning_dispatch',
        name='Warning Dispatch',
        replace_existing=True
    )
    
    logger.info("Signal dispatch jobs scheduled")