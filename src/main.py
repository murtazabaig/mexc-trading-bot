"""Main entry point for MEXC Futures Signal Bot."""

import argparse
import sys
from pathlib import Path

from . import __version__
from .config import Config
from .logger import setup_logger, get_logger
from .database import create_database_manager

logger = get_logger()


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    
    parser = argparse.ArgumentParser(
        prog="mexc-futures-signal-bot",
        description="Advanced trading signal bot for MEXC futures markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py
  python src/main.py --debug
  python src/main.py --config-path config/.env.production

Environment Variables:
  All configuration can be set via environment variables or .env file.
  See config/.env.example for all available options.
        """
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
        help="Path to .env configuration file (default: config/.env or project root .env)"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and development features"
    )
    
    parser.add_argument(
        "--no-docker",
        action="store_true",
        help="Disable Docker-specific optimizations"
    )
    
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Initialize database and exit (useful for first run)"
    )
    
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration and exit"
    )
    
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Display configuration values (sanitized) and exit"
    )
    
    return parser.parse_args()


def print_startup_banner(config: Config, startup_time: float) -> None:
    """Print startup banner with system information."""
    
    banner = f"""
╔══════════════════════════════════════════════════════════════════════╗
║         MEXC FUTURES SIGNAL BOT v{__version__:<20}            ║
╚══════════════════════════════════════════════════════════════════════╝

Mode:           {('DEVELOPMENT' if config.debug else config.environment.title())}
Paper Trading:  {'✓ Enabled' if config.trading.paper_trading else '⚠ Disabled'}
Database:       {config.database_path}
Log Directory:  {config.log_directory}
Log Level:      {config.log_level}

Timeframes:     {', '.join(config.signals.scan_intervals)}
Max Spread:     {config.signals.max_spread_percent}%
Min Volume:     ${config.signals.min_volume_usdt:,.0f}

Starting in {'debug' if config.debug else 'normal'} mode...
    """
    
    logger.info(banner)


async def async_main(args: argparse.Namespace, config: Config) -> int:
    """Async main function for application logic."""
    
    try:
        # Initialize database
        logger.info("Initializing database...")
        db_manager = create_database_manager(config.database_path)
        
        if args.bootstrap:
            logger.info("Bootstrap complete. Database initialized.")
            return 0
        
        # Display configuration if requested
        if args.show_config:
            logger.info("=== Configuration ===")
            logger.info(f"Telegram Admin: {config.telegram_admin_chat_id}")
            logger.info(f"Signal Intervals: {config.signals.scan_intervals}")
            logger.info(f"Max Spread: {config.signals.max_spread_percent}%")
            logger.info(f"Min Volume: ${config.signals.min_volume_usdt:,.0f}")
            logger.info(f"Paper Trading: {config.trading.paper_trading}")
            logger.info(f"Database: {config.database_path}")
            return 0
        
        # TODO: Initialize APScheduler and other components
        # scheduler = AsyncIOScheduler()
        # scheduler.start()
        
        # TODO: Initialize scanner and start scanning
        # scanner = MarketScanner(config, db_manager)
        # await scanner.start()
        
        # TODO: Initialize Telegram bot
        # telegram_bot = TelegramBot(config, db_manager, scanner)
        # await telegram_bot.start()
        
        logger.info("\n" + "="*70)
        logger.info("MEXC Futures Signal Bot is running!")
        logger.info("Press Ctrl+C to stop")
        logger.info("="*70)
        
        # Keep the event loop running
        import asyncio
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error in async_main: {e}")
        logger.exception("Full traceback:")
        return 1


def main() -> int:
    """Main entry point for the application."""
    
    import time
    
    try:
        startup_start = time.time()
        
        # Parse command line arguments
        args = parse_arguments()
        
        # Load configuration
        try:
            config = Config.from_env(args.config_path)
        except Exception as e:
            print(f"Configuration Error: {e}", file=sys.stderr)
            return 1
        
        # Override log level if debug flag is set
        if args.debug:
            config.debug = True
            config.log_level = "DEBUG"
        
        # Setup logging
        try:
            setup_logger(
                log_directory=config.log_directory,
                log_level=config.log_level
            )
        except Exception as e:
            print(f"Logging Setup Error: {e}", file=sys.stderr)
            return 1
        
        # Log Python and environment info
        import sys
        logger.info(f"Python {sys.version}")
        logger.info(f"Platform: {sys.platform}")
        
        # Validate configuration
        if args.validate_config:
            logger.info("Validating configuration...")
            config.validate()
            logger.success("Configuration validation passed!")
            return 0
        
        # Print startup banner
        print_startup_banner(config, time.time() - startup_start)
        
        # Enable uvloop for better performance if available (not in Docker by default)
        if not args.no_docker:
            try:
                import uvloop
                uvloop.install()
                logger.info("uvloop enabled for improved performance")
            except ImportError:
                logger.debug("uvloop not available, using standard asyncio")
        
        # Run async main
        try:
            import asyncio
            exit_code = asyncio.run(async_main(args, config))
            return exit_code
        except KeyboardInterrupt:
            logger.info("Main process interrupted by user")
            return 0
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        logger.exception("Full traceback:")
        return 1
    finally:
        if 'db_manager' in locals():
            db_manager.close()
            logger.info("Database connection closed")
        
        logger.info("MEXC Futures Signal Bot stopped")


if __name__ == "__main__":
    sys.exit(main())