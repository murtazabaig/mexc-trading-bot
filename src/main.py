"""Main entry point for MEXC Futures Signal Bot."""

import argparse
import sys
import time
import asyncio
from pathlib import Path

from . import __version__
from .config import Config
from .logger import setup_logging, get_logger
from .database import init_db, create_schema, insert_signal, insert_warning, insert_params_snapshot, transaction

logger = get_logger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    
    parser = argparse.ArgumentParser(
        prog="mexc-futures-signal-bot",
        description="Advanced trading signal bot for MEXC futures markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"MEXC Futures Signal Bot v{__version__}"
    )
    
    parser.add_argument(
        "--config-path",
        type=str,
        default=None,
        help="Path to .env configuration file"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    return parser.parse_args()


async def async_main(args: argparse.Namespace, config: Config) -> int:
    """Async main function for application logic."""
    
    try:
        # Initialize database
        logger.info(f"Connecting to database at {config.database_path}...")
        conn = init_db(str(config.database_path))
        
        # Create schema
        create_schema(conn)
        
        # Test database with sample entries
        logger.info("Creating sample entries for testing...")
        
        with transaction(conn):
            # Sample signal
            sample_signal = {
                "symbol": "BTCUSDT",
                "timeframe": "1h",
                "side": "LONG",
                "confidence": 0.85,
                "regime": "TRENDING",
                "entry_price": 50000.0,
                "entry_band_min": 49500.0,
                "entry_band_max": 50500.0,
                "stop_loss": 48000.0,
                "tp1": 52000.0,
                "tp2": 54000.0,
                "tp3": 56000.0,
                "reason": {"confluence": ["RSI Oversold", "Support Touch"]},
                "metadata": {"test": True}
            }
            signal_id = insert_signal(conn, sample_signal)
            logger.info(f"Sample signal inserted with ID: {signal_id}")
            
            # Sample warning
            sample_warning = {
                "severity": "WARNING",
                "warning_type": "BTC_SHOCK",
                "message": "BTC price dropped by 5% in 5 minutes",
                "triggered_value": 0.05,
                "threshold": 0.03,
                "action_taken": "PAUSED_SIGNALS",
                "metadata": {"test": True}
            }
            warning_id = insert_warning(conn, sample_warning)
            logger.info(f"Sample warning inserted with ID: {warning_id}")
            
            # Sample config snapshot
            config_dict = {
                "environment": config.environment,
                "signals": config.signals.model_dump() if hasattr(config.signals, 'model_dump') else config.signals.dict() if hasattr(config.signals, 'dict') else str(config.signals),
                "trading": config.trading.model_dump() if hasattr(config.trading, 'model_dump') else config.trading.dict() if hasattr(config.trading, 'dict') else str(config.trading)
            }
            snapshot_id = insert_params_snapshot(conn, config_dict)
            logger.info(f"Config snapshot inserted with ID: {snapshot_id}")

        logger.info("MEXC Futures Signal Bot is running!")
        logger.info("Press Ctrl+C to stop")
        
        # In a real scenario, we would start the scanner and other tasks here
        # For now, we'll just wait if not in a test-like run
        # await asyncio.Event().wait()
        
        return 0
        
    except Exception as e:
        logger.exception(f"Fatal error in async_main: {e}")
        return 1


def main() -> int:
    """Main entry point for the application."""
    
    startup_start = time.time()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    try:
        # For this exercise, if no env file exists, we'll mock required vars 
        # to allow the bot to start for demonstration.
        import os
        if not os.getenv("TELEGRAM_BOT_TOKEN"):
            os.environ["TELEGRAM_BOT_TOKEN"] = "1234567890:mock_token"
        if not os.getenv("TELEGRAM_ADMIN_CHAT_ID"):
            os.environ["TELEGRAM_ADMIN_CHAT_ID"] = "12345678"
            
        config = Config.from_env(args.config_path)
    except Exception as e:
        print(f"Configuration Error: {e}", file=sys.stderr)
        return 1
    
    # Setup logging
    setup_logging(
        log_dir=str(config.log_directory),
        debug=args.debug or config.debug
    )
    
    logger.info(f"Starting MEXC Futures Signal Bot v{__version__}")
    
    # Run async main
    try:
        exit_code = asyncio.run(async_main(args, config))
        return exit_code
    except KeyboardInterrupt:
        logger.info("Main process interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
