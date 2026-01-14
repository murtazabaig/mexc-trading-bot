"""Logging configuration for MEXC Futures Signal Bot."""

import json
from pathlib import Path
from typing import Optional, Dict, Any
import sys

from loguru import logger


class JSONLFormatter:
    """Custom formatter for JSONL (JSON Lines) structured logging."""
    
    def __init__(self, include_timestamp: bool = True, include_level: bool = True, include_module: bool = True):
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_module = include_module
    
    def format(self, record: Dict[str, Any]) -> str:
        """Format log record as JSONL."""
        log_entry = {}
        
        # Add timestamp
        if self.include_timestamp:
            log_entry["timestamp"] = record["time"].isoformat()
        
        # Add severity level
        if self.include_level:
            log_entry["level"] = record["level"].name
        
        # Add module and function context
        if self.include_module:
            log_entry["module"] = record["name"]
            log_entry["function"] = record["function"]
            log_entry["line"] = record["line"]
        
        # Add message
        log_entry["message"] = record["message"]
        
        # Add any extra fields
        if record.get("extra"):
            log_entry.update(record["extra"])
        
        # Add exception info if present
        if record["exception"]:
            log_entry["exception"] = {
                "type": record["exception"].type.__name__,
                "value": str(record["exception"].value)
            }
        
        return json.dumps(log_entry, sort_keys=True)


def setup_logger(
    log_directory: Optional[Path] = None,
    log_level: str = "INFO",
    log_filename: str = "bot.log"
) -> None:
    """
    Configure logger with JSONL structured logging, rotation, and multiple handlers.
    
    Args:
        log_directory: Directory for log files. Defaults to './logs/'
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_filename: Base filename for log files
    """
    
    if log_directory is None:
        log_directory = Path("logs")
    
    # Ensure log directory exists
    log_directory = Path(log_directory)
    log_directory.mkdir(parents=True, exist_ok=True)
    
    # Remove default handlers
    logger.remove()
    
    # Create formatter instance
    json_formatter = JSONLFormatter()
    
    # Console handler with colorized output
    logger.add(
        sink=sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )
    
    # File handler for info and above logs
    logger.add(
        sink=log_directory / log_filename,
        format=json_formatter.format,
        level="INFO",
        rotation="500 MB",  # Rotate at 500MB
        retention="10 days",  # Keep logs for 10 days
        compression="zip",  # Compress rotated logs
        encoding="utf-8",
        backtrace=True,
        diagnose=True,
    )
    
    # File handler for error logs only
    logger.add(
        sink=log_directory / "errors.log",
        format=json_formatter.format,
        level="ERROR",
        rotation="100 MB",
        retention="30 days",
        compression="zip",
        encoding="utf-8",
        backtrace=True,
        diagnose=True,
    )
    
    # File handler for debug logs (only if debug enabled)
    if log_level.upper() == "DEBUG":
        logger.add(
            sink=log_directory / "debug.log",
            format=json_formatter.format,
            level="DEBUG",
            rotation="200 MB",
            retention="5 days",
            compression="zip",
            encoding="utf-8",
            backtrace=True,
            diagnose=True,
        )
    
    logger.info(f"Logger initialized with level: {log_level}")
    logger.info(f"Log directory: {log_directory.resolve()}")


def get_logger():
    """Get the configured logger instance."""
    return logger


# Default formatter configuration for use in other modules
def get_context_logger(**context):
    """Get a logger with additional context fields."""
    return logger.bind(**context)


# Module-level logger
def get_module_logger(name: str):
    """Get a logger with module context."""
    return logger.bind(module=name)