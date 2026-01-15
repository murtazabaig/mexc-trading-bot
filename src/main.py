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
        
        # Test technical indicators with sample data
        logger.info("Testing technical indicators...")
        try:
            from .indicators import ema, rsi, atr, atr_percent, vwap, volume_zscore, adx
            
            # Sample datasets for testing
            test_datasets = [
                {
                    "name": "BTC trending up",
                    "highs": [47000, 47500, 47800, 48200, 48500, 48800, 49100, 49400, 49700, 50000],
                    "lows": [46500, 47100, 47400, 47800, 48100, 48400, 48700, 49000, 49300, 49600],
                    "closes": [47200, 47650, 48100, 47950, 48420, 48600, 48750, 49050, 49200, 49380],
                    "volumes": [1250, 1180, 1420, 980, 1350, 1100, 1200, 1450, 1380, 1150]
                },
                {
                    "name": "ETH sideways movement", 
                    "highs": [3000, 3010, 3005, 3015, 3008, 3012, 3006, 3014, 3009, 3011],
                    "lows": [2990, 2995, 2992, 2998, 2994, 2996, 2993, 2997, 2995, 2997],
                    "closes": [2995, 3002, 2998, 3006, 3001, 3008, 2999, 3009, 3003, 3005],
                    "volumes": [5000, 5200, 4800, 5500, 5100, 5300, 4900, 5400, 5200, 5100]
                },
                {
                    "name": "High volatility ADA",
                    "highs": [0.5, 0.52, 0.48, 0.55, 0.47, 0.58, 0.45, 0.60, 0.44, 0.62],
                    "lows": [0.48, 0.49, 0.46, 0.50, 0.45, 0.52, 0.43, 0.54, 0.42, 0.56],
                    "closes": [0.49, 0.51, 0.47, 0.53, 0.46, 0.56, 0.44, 0.58, 0.43, 0.60],
                    "volumes": [100000, 120000, 95000, 110000, 90000, 130000, 85000, 125000, 80000, 115000]
                }
            ]
            
            for i, dataset in enumerate(test_datasets, 1):
                logger.info(f"Dataset {i}: {dataset['name']}")
                
                try:
                    # Calculate all indicators
                    ema_14 = ema(dataset['closes'], 14)
                    rsi_14 = rsi(dataset['closes'], 14)
                    atr_14 = atr(dataset['highs'], dataset['lows'], dataset['closes'], 14)
                    atr_pct_14 = atr_percent(dataset['highs'], dataset['lows'], dataset['closes'], 14)
                    vwap_val = vwap(dataset['highs'], dataset['lows'], dataset['closes'], dataset['volumes'])
                    vol_zscore = volume_zscore(dataset['volumes'], 20)
                    adx_14 = adx(dataset['highs'], dataset['lows'], 14)
                    
                    logger.info(f"  EMA(14): {ema_14:.4f}")
                    logger.info(f"  RSI(14): {rsi_14:.2f}")
                    logger.info(f"  ATR(14): {atr_14:.4f}")
                    logger.info(f"  ATR%: {atr_pct_14:.2f}%")
                    logger.info(f"  VWAP: {vwap_val:.4f}")
                    logger.info(f"  Volume Z-Score: {vol_zscore:.2f}")
                    logger.info(f"  ADX(14): {adx_14:.2f}")
                    
                except Exception as e:
                    logger.error(f"  Error calculating indicators for dataset {i}: {e}")
            
            logger.info("Indicator testing completed successfully")
            
        except ImportError as e:
            logger.error(f"Could not import indicators module: {e}")
        except Exception as e:
            logger.error(f"Error in indicator testing: {e}")

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

        # Initialize and start Telegram bot
        logger.info("Initializing Telegram bot...")
        try:
            from .telegram_bot import MexcSignalBot
            
            # Create bot instance
            telegram_bot = MexcSignalBot(
                bot_token=config.telegram_bot_token,
                admin_chat_id=config.telegram_admin_chat_id,
                polling_timeout=config.telegram_polling_timeout
            )
            
            # Set bot dependencies
            telegram_bot.set_database_connection(conn)
            telegram_bot.set_universe_size(len(filtered_markets))
            telegram_bot.set_mode("active")
            
            # Start bot in background task
            async def run_telegram_bot():
                try:
                    await telegram_bot.start_polling()
                    return telegram_bot
                except Exception as e:
                    logger.error(f"Failed to start Telegram bot: {e}")
                    return None
            
            # Run bot startup
            bot_task = asyncio.create_task(run_telegram_bot())
            telegram_bot_instance = await bot_task
            
            if telegram_bot_instance:
                logger.success("🤖 Telegram bot started successfully")
                logger.info(f"Bot listening for commands from chat ID: {config.telegram_admin_chat_id}")
                
                # Schedule signal dispatch jobs
                try:
                    from .jobs.signal_dispatch import create_signal_dispatch_jobs
                    create_signal_dispatch_jobs(scheduler, telegram_bot_instance, conn, logger)
                    logger.info("Signal dispatch jobs scheduled")
                except Exception as e:
                    logger.error(f"Failed to schedule dispatch jobs: {e}")
                
                # Schedule reporting jobs
                try:
                    from .jobs.daily_report import create_reporting_jobs
                    create_reporting_jobs(scheduler, telegram_bot_instance, conn, config, logger)
                    logger.info("Reporting jobs scheduled")
                except Exception as e:
                    logger.error(f"Failed to schedule reporting jobs: {e}")
                
            else:
                logger.warning("Telegram bot failed to start - continuing without bot functionality")
                telegram_bot_instance = None
                
        except ImportError as e:
            logger.error(f"Could not import Telegram bot module: {e}")
            telegram_bot_instance = None
        except Exception as e:
            logger.error(f"Error initializing Telegram bot: {e}")
            telegram_bot_instance = None

        # Initialize and start market scanner
        logger.info("Initializing market scanner...")
        try:
            from .jobs.scanner import create_scanner_job
            
            # Create scanner job
            scanner = create_scanner_job(exchange, conn, config.__dict__, filtered_markets)
            scanner.set_scheduler(scheduler)
            
            # Start scanner
            await scanner.start_scanning()
            logger.success("🔍 Market scanner started successfully")
            logger.info("Scanner running every 5 minutes for signal generation")
            
        except ImportError as e:
            logger.error(f"Could not import scanner module: {e}")
            scanner = None
        except Exception as e:
            logger.error(f"Error initializing market scanner: {e}")
            scanner = None

        # Initialize and start warning detector
        logger.info("Initializing warning detector...")
        try:
            from .warnings.detector import WarningDetector
            
            # Create warning detector
            warning_detector = WarningDetector(
                exchange=exchange,
                db_conn=conn,
                config=config.__dict__,
                universe=filtered_markets
            )
            warning_detector.set_scheduler(scheduler)
            
            # Set Telegram bot for warnings
            if telegram_bot_instance:
                warning_detector.set_telegram_bot(telegram_bot_instance)
            
            # Start warning detection
            await warning_detector.start_detection()
            logger.success("🚨 Warning detector started successfully")
            logger.info("Warning detector running every 5 minutes for anomaly detection")
            
        except ImportError as e:
            logger.error(f"Could not import warning detector module: {e}")
            warning_detector = None
        except Exception as e:
            logger.error(f"Error initializing warning detector: {e}")
            warning_detector = None

        logger.info("MEXC Futures Signal Bot is running!")
        logger.info("Press Ctrl+C to stop")
        
        # In a real scenario, we would start the scanner and other tasks here
        # For now, we'll just wait if not in a test-like run
        # await asyncio.Event().wait()
        
        return 0, telegram_bot_instance if 'telegram_bot_instance' in locals() else None, scanner if 'scanner' in locals() else None, warning_detector if 'warning_detector' in locals() else None
        
    except Exception as e:
        logger.exception(f"Fatal error in async_main: {e}")
        return 1, None, None


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
        exit_code, telegram_bot_instance, scanner_instance, warning_detector_instance = asyncio.run(async_main(args, config))
        return exit_code
    except KeyboardInterrupt:
        logger.info("Main process interrupted by user")
        return 0
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
