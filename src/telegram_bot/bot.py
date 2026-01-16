"""Telegram bot implementation for MEXC Futures Signal Bot."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import telegram
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from ..logger import get_logger
from .formatters import (
    format_status,
    format_signal,
    format_symbol_analysis,
    format_warning,
)

logger = get_logger(__name__)


class MexcSignalBot:
    """Main Telegram bot class for signal distribution."""
    
    def __init__(self, bot_token: str, admin_chat_id: str, polling_timeout: int = 30, pause_state: Any = None):
        """Initialize the Telegram bot.
        
        Args:
            bot_token: Telegram bot token
            admin_chat_id: Admin chat ID for permission checking
            polling_timeout: Polling timeout in seconds
            pause_state: Pause state singleton
        """
        self.bot_token = bot_token
        self.admin_chat_id = admin_chat_id
        self.polling_timeout = polling_timeout
        self.start_time = datetime.now(timezone.utc)
        self.application: Optional[Application] = None
        self.last_scan_time: Optional[datetime] = None
        self.universe_size = 0
        self.mode = "active"
        
        # Components (will be set by main.py)
        self.db_conn = None
        self.scanner = None
        self.warning_detector = None
        self.portfolio_manager = None
        self.pause_state = pause_state
    
    def set_database_connection(self, conn):
        """Set the database connection for the bot."""
        self.db_conn = conn

    def set_scanner(self, scanner):
        """Set the scanner instance."""
        self.scanner = scanner

    def set_warning_detector(self, warning_detector):
        """Set the warning detector instance."""
        self.warning_detector = warning_detector

    def set_portfolio_manager(self, portfolio_manager):
        """Set the portfolio manager instance."""
        self.portfolio_manager = portfolio_manager

    def set_pause_state(self, pause_state):
        """Set the pause state instance."""
        self.pause_state = pause_state
    
    def set_universe_size(self, size: int):
        """Set the current universe size."""
        self.universe_size = size
    
    def set_last_scan_time(self, scan_time: datetime):
        """Set the last scan time."""
        self.last_scan_time = scan_time
    
    def set_mode(self, mode: str):
        """Set the bot mode (active, paused, scanning, error)."""
        self.mode = mode
    
    def _is_admin(self, update: Update) -> bool:
        """Check if the user is the admin.
        
        Args:
            update: Telegram update object
        
        Returns:
            True if user is admin, False otherwise
        """
        if not update.effective_user or not update.effective_chat:
            return False
        
        return str(update.effective_user.id) == self.admin_chat_id or \
               update.effective_chat.id == int(self.admin_chat_id)
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command - welcome message.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        welcome_text = f"""🤖 *MEXC Futures Signal Bot*

Welcome! I'm your advanced trading signal assistant.

📊 *What I can do:*
• Generate high-confidence trading signals
• Monitor {self.universe_size:,} futures markets
• Real-time market analysis
• Risk management alerts

🔧 *Available Commands:*
/status - Bot status and health
/help - Show all commands
/top - Top signals by confidence
/symbol <COIN> - Analyze specific symbol
/report [date] - Daily performance summary
/scanstart - Enable market scanning
/scanstop - Disable market scanning

⚡ *Ready to trade!*

This bot is in {'*TEST MODE*' if 'test' in str(self.bot_token) else '*LIVE MODE*'}."""
        
        await update.effective_message.reply_text(welcome_text, parse_mode='Markdown')
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command - list all commands.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        help_text = """🔧 *Bot Commands*

📊 *Status & Monitoring:*
/status - Bot health and statistics
/top - Show top 5 signals by confidence
/report [date] - Daily performance summary

🔍 *Symbol Analysis:*
/symbol <COIN> - Analyze specific symbol
Example: /symbol BTCUSDT

⚡ *Scanner Control:*
/scanstart - Enable market scanning
/scanstop - Disable market scanning

📱 *Bot Info:*
/start - Welcome message
/help - This help menu

🔒 *Admin Commands* (Admin only)

💡 *Tips:*
• Use /symbol BTCUSDT for Bitcoin analysis
• /top shows the highest confidence setups
• Scanner automatically finds new opportunities
• All prices are in USDT pairs

*Happy Trading!* 🚀"""
        
        await update.effective_message.reply_text(help_text, parse_mode='Markdown')
    
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - show bot status.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        # Calculate uptime
        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
        
        # Get stats from components
        scanner_stats = self.scanner.get_stats() if self.scanner else None
        warning_stats = self.warning_detector.get_stats() if self.warning_detector else None
        portfolio_stats = self.portfolio_manager.get_stats() if self.portfolio_manager else None
        
        # Use last scan time from scanner if available
        last_scan = self.last_scan_time
        if scanner_stats and scanner_stats.get('last_scan_time'):
            last_scan_val = scanner_stats.get('last_scan_time')
            if isinstance(last_scan_val, str):
                last_scan = datetime.fromisoformat(last_scan_val)
            else:
                last_scan = last_scan_val

        status_text = format_status(
            uptime_seconds=uptime_seconds,
            last_scan=last_scan,
            universe_size=self.universe_size,
            mode=self.mode,
            scanner_stats=scanner_stats,
            warning_stats=warning_stats,
            portfolio_stats=portfolio_stats,
            pause_state=self.pause_state
        )
        
        await update.effective_message.reply_text(status_text, parse_mode='Markdown')

    async def report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /report command - show daily summary.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        from datetime import timedelta
        date = context.args[0] if context.args else (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid date format. Please use YYYY-MM-DD.\nExample: /report 2025-01-15")
            return

        if not self.db_conn:
            await update.effective_message.reply_text("❌ Database not available")
            return
            
        try:
            from ..reporting.summarizer import ReportGenerator
            from ..reporting.formatters import format_daily_summary
            
            generator = ReportGenerator()
            summary = await generator.generate_daily_summary(self.db_conn, date, self.universe_size)
            
            report_text = format_daily_summary(summary)
            await update.effective_message.reply_text(report_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error generating report for {date}: {e}")
            await update.effective_message.reply_text(f"❌ Error generating report for {date}. Please try again.")
    
    async def top(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show top signals by confidence."""
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return

        if not self.db_conn:
            if update.effective_message:
                await update.effective_message.reply_text("❌ Database unavailable")
            return

        try:
            from ..database import query_recent_signals

            signals = query_recent_signals(self.db_conn, limit=10)

            real_signals = [s for s in signals if s.get('confidence', 0) > 0.3]

            if not real_signals:
                if update.effective_message:
                    await update.effective_message.reply_text(
                        "📊 No signals generated yet.\n\n"
                        "Use /scanstart to enable scanner.",
                        parse_mode='Markdown',
                    )
                return

            real_signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)

            text = "🔝 *Top Signals*\n\n"
            for i, sig in enumerate(real_signals[:5], 1):
                symbol = sig.get('symbol', 'UNKNOWN')
                side = sig.get('side', 'UNKNOWN')
                confidence = sig.get('confidence') or 0
                regime = sig.get('regime', 'UNKNOWN')
                entry_price = sig.get('entry_price') or 0

                text += (
                    f"{i}. {symbol}\n"
                    f"   Side: {side}\n"
                    f"   Confidence: {confidence:.0%}\n"
                    f"   Regime: {regime}\n"
                    f"   Price: {entry_price:.4f}\n\n"
                )

            if update.effective_message:
                await update.effective_message.reply_text(text, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error in /top: {e}")
            if update.effective_message:
                await update.effective_message.reply_text("❌ Error fetching signals")
    
    async def symbol(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /symbol command - analyze specific symbol.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        if not context.args:
            await update.effective_message.reply_text("❌ Please specify a symbol.\nExample: /symbol BTCUSDT")
            return
        
        symbol = context.args[0].upper().replace('/', '').replace('-', '')
        
        try:
            # Query recent signals for this symbol
            from ..database import query_recent_signals
            symbol_signals = query_recent_signals(self.db_conn, symbol=symbol, limit=5) if self.db_conn else []
            
            # Mock regime and indicators (in real implementation, would come from analysis)
            regime = "TRENDING"
            regime_confidence = 0.78
            
            indicators = {
                "EMA20": 47250.0,
                "RSI": 62.3,
                "ATR%": 1.2,
                "VWAP": 47205.0,
                "ADX": 28.5,
                "Volume_ZScore": 1.8
            }
            
            analysis_text = format_symbol_analysis(
                symbol=symbol,
                regime=regime,
                regime_confidence=regime_confidence,
                indicators=indicators,
                last_signals=symbol_signals
            )
            
            await update.effective_message.reply_text(analysis_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error analyzing symbol {symbol}: {e}")
            await update.effective_message.reply_text(f"❌ Error analyzing {symbol}. Please try again.")
    
    async def scanstart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstart command - enable scanning.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        
        # Actually enable the scanner
        if self.scanner:
            self.scanner.running = True
        if self.pause_state:
            self.pause_state.resume()
        self.set_mode("scanning")
        
        await update.effective_message.reply_text(
            "🔍 *Market Scanning Enabled*\n\n"
            "✅ Scanner is now active\n"
            "📊 Monitoring all symbols for opportunities\n"
            "⚡ Signals will be sent automatically\n\n"
            "Happy hunting! 🎯",
            parse_mode='Markdown'
        )
    
    async def scanstop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstop command - disable scanning.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Check admin access
        if not self._is_admin(update):
            if update.effective_message:
                await update.effective_message.reply_text("❌ Access denied. Admin only.")
            return
        
        # Actually disable the scanner
        if self.scanner:
            self.scanner.running = False
        if self.pause_state:
            self.pause_state.pause("Stopped by user via Telegram")
        self.set_mode("paused")
        
        await update.effective_message.reply_text(
            "⏸️ *Market Scanning Paused*\n\n"
            "🛑 Scanner has been stopped\n"
            "📊 No new signals will be generated\n"
            "🔄 Previous signals remain valid\n\n"
            "Use /scanstart to resume scanning.",
            parse_mode='Markdown'
        )
    
    async def send_message(self, chat_id: str, text: str, parse_mode: str = 'Markdown') -> bool:
        """Send a generic message.
        
        Args:
            chat_id: Telegram chat ID
            text: Message text
            parse_mode: Message parse mode
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            if self.application and self.application.bot:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode
                )
                return True
            else:
                logger.error("Bot application not initialized")
                return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    async def send_signal(self, signal: Dict[str, Any]) -> bool:
        """Send signal message to admin chat.
        
        Args:
            signal: Signal dictionary
        
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            signal_text = format_signal(signal)
            
            if self.application and self.application.bot:
                await self.application.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=signal_text,
                    parse_mode='Markdown'
                )
                logger.info(f"Signal sent to chat {self.admin_chat_id}: {signal.get('symbol')} {signal.get('side')}")
                return True
            else:
                logger.error("Bot application not initialized")
                return False
                
        except Exception as e:
            logger.error(f"Error sending signal: {e}")
            return False
    
    async def send_warning(self, warning: Dict[str, Any]) -> bool:
        """Send warning message to admin chat.
        
        Args:
            warning: Warning dictionary
        
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            warning_text = format_warning(warning)
            
            if self.application and self.application.bot:
                await self.application.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=warning_text,
                    parse_mode='Markdown'
                )
                logger.warning(f"Warning sent to chat {self.admin_chat_id}: {warning.get('warning_type')}")
                return True
            else:
                logger.error("Bot application not initialized")
                return False
                
        except Exception as e:
            logger.error(f"Error sending warning: {e}")
            return False
    
    async def error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all errors.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        update_id = update.update_id if update else "Unknown"
        logger.error(f"Update {update_id} caused error {context.error}")
        
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "❌ An error occurred while processing your request. "
                    "Please try again or contact the administrator."
                )
            except Exception:
                logger.error("Failed to send error message to user")
    
    async def setup_handlers(self):
        """Setup all command handlers."""
        if not self.application:
            raise RuntimeError("Application not initialized")
        
        logger.info("Registering Telegram command handlers...")
        
        # Add command handlers
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help))
        self.application.add_handler(CommandHandler("status", self.status))
        self.application.add_handler(CommandHandler("top", self.top))
        self.application.add_handler(CommandHandler("symbol", self.symbol))
        self.application.add_handler(CommandHandler("report", self.report))
        self.application.add_handler(CommandHandler("scanstart", self.scanstart))
        self.application.add_handler(CommandHandler("scanstop", self.scanstop))
        
        # Add error handler
        self.application.add_error_handler(self.error_handler)
        
        logger.info("Successfully registered 8 command handlers")
        logger.debug("Handlers: /start, /help, /status, /top, /symbol, /report, /scanstart, /scanstop")
    
    async def start_polling(self):
        """Start the bot in polling mode.
        
        Returns:
            Bot instance for further configuration
        """
        logger.info("Initializing Telegram bot...")
        
        # Create application
        self.application = Application.builder().token(self.bot_token).build()
        
        # Setup handlers
        await self.setup_handlers()
        
        logger.info(f"Starting bot polling (timeout: {self.polling_timeout}s)...")
        
        # Start polling
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling(
            timeout=self.polling_timeout,
            drop_pending_updates=True
        )
        
        logger.info("🤖 Telegram bot is listening for messages...")
        
        # Send startup message to admin
        try:
            startup_text = f"""🚀 *Bot Started*
✅ MEXC Futures Signal Bot is now online
🕒 Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
🌍 Monitoring: {self.universe_size:,} symbols
📱 Ready to receive commands!

Use /help to see available commands."""
            
            await self.application.bot.send_message(
                chat_id=self.admin_chat_id,
                text=startup_text,
                parse_mode='Markdown'
            )
            logger.info("Startup message sent to admin chat")
            
        except Exception as e:
            logger.error(f"Failed to send startup message: {e}")
        
        return self.application
    
    async def stop_polling(self):
        """Stop the bot polling."""
        if self.application and self.application.updater:
            await self.application.updater.stop()
            await self.application.stop()
            logger.info("Telegram bot polling stopped")
    
    def get_bot_info(self) -> Dict[str, Any]:
        """Get bot information.
        
        Returns:
            Dictionary with bot information
        """
        return {
            "token_configured": bool(self.bot_token),
            "admin_chat_id": self.admin_chat_id,
            "start_time": self.start_time,
            "uptime_seconds": int((datetime.now(timezone.utc) - self.start_time).total_seconds()),
            "universe_size": self.universe_size,
            "mode": self.mode,
            "last_scan": self.last_scan_time,
            "polling_timeout": self.polling_timeout
        }