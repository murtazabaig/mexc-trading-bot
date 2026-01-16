"""Logging configuration for MEXC Futures Signal Bot."""

import json
from pathlib import Path
from typing import Optional, Dict, Any, Union
import sys

from loguru import logger

class JSONLFormatter:
    """Custom formatter for JSONL (JSON Lines) structured logging."""
    
    def format(self, record: Dict[str, Any]) -> str:
        """Format log record as JSONL."""
        log_entry = {
            "timestamp": record["time"].isoformat(),
            "level": record["level"].name,
            "module": record["name"],
            "function": record["function"],
            "message": record["message"],
        }
        
        # Add any extra fields as 'context'
        if record.get("extra"):
            log_entry["context"] = record["extra"]
        else:
            log_entry["context"] = {}
        
        # Add exception info if present
        if record["exception"]:
            log_entry["exception"] = {
                "type": record["exception"].type.__name__,
                "value": str(record["exception"].value)
            }
        
        return json.dumps(log_entry, sort_keys=True).replace("{", "{{").replace("}", "}}") + "\n"


def setup_logging(log_dir: str = "logs", debug: bool = False) -> None:
    """
    Configure logger with JSONL structured logging, rotation, and multiple handlers.
    
    Args:
        log_dir: Directory for log files. Defaults to 'logs'
        debug: Whether to enable debug logging
    """
    log_directory = Path(log_dir)
    log_directory.mkdir(parents=True, exist_ok=True)
    
    # Remove default handlers
    logger.remove()
    
    # Add patcher to escape brackets in messages to prevent loguru formatting errors
    # with Telegram objects like <ChatType.PRIVATE> which loguru tries to parse as color tags
    def escape_brackets(record):
        record["message"] = record["message"].replace("<", "<<").replace(">", ">>")
    
    logger.configure(patcher=escape_brackets)
    
    # Create formatter instance
    json_formatter = JSONLFormatter()
    
    log_level = "DEBUG" if debug else "INFO"
    
    # Console handler with colorized output (human-readable)
    logger.add(
        sink=sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )
    
    # File handler: logs/bot.log with daily rotation (JSONL structured logging)
    logger.add(
        sink=log_directory / "bot.log",
        format=json_formatter.format,
        level=log_level,
        rotation="00:00",  # Daily rotation at midnight
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        backtrace=True,
        diagnose=True,
    )
    
    logger.info(f"Logging initialized. Level: {log_level}, Directory: {log_directory}")


def get_logger(name: Optional[str] = None):
    """Get the configured logger instance, optionally bound with a name."""
    if name:
        return logger.bind(name=name)
    return logger
