import os
import json
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ..reporting.summarizer import ReportGenerator
from ..reporting.formatters import format_daily_summary, format_summary_csv
from ..database import record_heartbeat

async def send_daily_report(bot, db, config, logger, date: str = None):
    """
    Job to generate and send daily report.
    """
    try:
        if date is None:
            # Default to yesterday
            date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
            
        logger.info(f"Generating daily report for {date}...")
        
        universe_size = bot.universe_size if bot else 0
        generator = ReportGenerator()
        summary = await generator.generate_daily_summary(db, date, universe_size)
        
        # Format message
        message = format_daily_summary(summary)
        
        # Send to Telegram
        if bot:
            await bot.send_message(config.telegram_admin_chat_id, message)
            logger.info(f"Daily report for {date} sent to Telegram.")
        else:
            logger.warning("Bot not available, daily report not sent to Telegram.")
            
        # Save CSV entry
        log_dir = Path(config.log_directory)
        log_dir.mkdir(parents=True, exist_ok=True)
        csv_path = log_dir / "daily_summary.csv"
        
        csv_row = format_summary_csv(summary)
        
        # Append to CSV
        file_exists = csv_path.exists()
        with open(csv_path, 'a', encoding='utf-8') as f:
            if not file_exists:
                f.write("date,total_signals,avg_confidence,warnings,pnl_r,win_rate,uptime\n")
            f.write(csv_row + "\n")
            
        # Save JSON
        report_dir = Path("data/reports")
        report_dir.mkdir(parents=True, exist_ok=True)
        json_path = report_dir / f"{date}.json"
        
        with open(json_path, 'w', encoding='utf-8') as f:
            # We need a way to convert dataclass to dict
            from dataclasses import asdict
            json.dump(asdict(summary), f, indent=4)
            
        logger.success(f"Daily report for {date} generated and saved.")
        
    except Exception as e:
        logger.error(f"Failed to generate daily report: {e}")
        if bot:
            try:
                await bot.send_message(
                    config.telegram_admin_chat_id, 
                    f"❌ *Error generating daily report for {date}*\n\n`{str(e)}`"
                )
            except:
                pass

def heartbeat_job(db, logger):
    """
    Job to record heartbeat.
    """
    from ..database import record_heartbeat
    record_heartbeat(db)

def create_reporting_jobs(scheduler, bot, db, config, logger):
    """
    Create and schedule reporting jobs.
    """
    # Heartbeat every minute
    scheduler.add_job(
        heartbeat_job,
        'interval',
        minutes=1,
        args=[db, logger],
        id='heartbeat',
        name='Heartbeat',
        replace_existing=True
    )
    
    # Daily report at configured time
    report_time = getattr(config, 'daily_report_time', "00:00")
    try:
        hour, minute = map(int, report_time.split(':'))
    except:
        hour, minute = 0, 0
        
    scheduler.add_job(
        send_daily_report,
        'cron',
        hour=hour,
        minute=minute,
        args=[bot, db, config, logger],
        id='daily_report',
        name='Daily Report',
        replace_existing=True
    )
    
    logger.info(f"Reporting jobs scheduled. Daily report at {report_time} UTC.")
