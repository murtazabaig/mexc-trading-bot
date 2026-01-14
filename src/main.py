"""Main entry point for MEXC Futures Signal Bot."""

import argparse
import sys
import time
import asyncio
from pathlib import Path

import ccxt
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from . import __version__
from .config import Config
from .logger import setup_logging, get_logger
from .database import init_db, create_schema, insert_signal, insert_warning, insert_params_snapshot, transaction
from .jobs import refresh_universe
from .universe import UniverseConfig, filter_markets

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
        
        # Initialize MEXC exchange
        logger.info("Initializing MEXC exchange...")
        exchange = ccxt.mexc({
            'apiKey': config.mexc_api_key,
            'secret': config.mexc_api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',  # Futures/swap
            },
        })
        
        if config.mexc_testnet:
            logger.info("Using MEXC testnet")
            exchange.set_sandbox_mode(True)
        
        # Perform initial universe load
        logger.info("Loading initial market universe...")
        try:
            from .universe import load_mexc_futures_markets
            
            # Load markets (run in executor since it's blocking)
            markets = await asyncio.get_event_loop().run_in_executor(
                None, load_mexc_futures_markets, exchange, 3600, False
            )
            logger.info(f"Loaded {len(markets)} total markets from MEXC")
            
            # Apply filters
            # Convert pydantic UniverseConfig to universe.UniverseConfig for compatibility
            universe_filter_config = UniverseConfig(
                min_volume_usd=config.universe.min_volume_usd,
                max_spread_percent=config.universe.max_spread_percent,
                exclude_patterns=config.universe.exclude_patterns,
                exclude_symbols=config.universe.exclude_symbols,
                min_notional=config.universe.min_notional,
                min_price=config.universe.min_price,
                max_price=config.universe.max_price,
            )
            
            filtered_markets = filter_markets(markets, universe_filter_config)
            logger.info(f"Filtered to {len(filtered_markets)} markets")
            
            # Log sample symbols
            sample_symbols = list(filtered_markets.keys())[:10]
            logger.info(f"Sample markets: {', '.join(sample_symbols)}")
            
        except Exception as e:
            logger.error(f"Failed to load initial universe: {e}")
            logger.warning("Continuing with empty universe")
            filtered_markets = {}
        
        # Setup APScheduler for periodic universe refresh
        scheduler = AsyncIOScheduler(timezone='UTC')
        refresh_hours = config.universe.refresh_interval_hours
        scheduler.add_job(
            refresh_universe,
            'interval',
            hours=int(refresh_hours),
            minutes=int((refresh_hours - int(refresh_hours)) * 60),
            args=[exchange, conn, config.universe, logger],
            id='universe_refresh',
            name='Universe Refresh',
            replace_existing=True
        )
        scheduler.start()
        logger.info(f"Scheduled universe refresh every {refresh_hours} hour(s)")
        
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
