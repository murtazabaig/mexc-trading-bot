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
    format_status, format_signal, format_top_signals, 
    format_symbol_analysis, format_warning
)

logger = get_logger(__name__)


class MexcSignalBot:
    """Main Telegram bot class for signal distribution."""
    
    def __init__(self, bot_token: str, admin_chat_id: str, polling_timeout: int = 30):
        """Initialize the Telegram bot.
        
        Args:
            bot_token: Telegram bot token
            admin_chat_id: Admin chat ID for permission checking
            polling_timeout: Polling timeout in seconds
        """
        self.bot_token = bot_token
        self.admin_chat_id = admin_chat_id
        self.polling_timeout = polling_timeout
        self.start_time = datetime.now(timezone.utc)
        self.application: Optional[Application] = None
        self.last_scan_time: Optional[datetime] = None
        self.universe_size = 0
        self.mode = "active"
        
        # Database connection (will be set by main.py)
        self.db_conn = None
    
    def set_database_connection(self, conn):
        """Set the database connection for the bot."""
        self.db_conn = conn
    
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
    
    async def _admin_only(self, func):
        """Decorator to ensure only admin can execute command."""
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if not self._is_admin(update):
                await update.message.reply_text("❌ Access denied. Admin only.")
                return
            
            return await func(update, context)
        return wrapper
    
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
        
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    
    @_admin_only
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command - list all commands.
        
        Args:
            update: Telegram update object
            context: Context object
        """
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
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    @_admin_only
    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - show bot status.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        # Calculate uptime
        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())
        
        status_text = format_status(
            uptime_seconds=uptime_seconds,
            last_scan=self.last_scan_time,
            universe_size=self.universe_size,
            mode=self.mode
        )
        
        await update.message.reply_text(status_text, parse_mode='Markdown')

    @_admin_only
    async def report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /report command - show daily summary.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        from datetime import timedelta
        date = context.args[0] if context.args else (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            await update.message.reply_text("❌ Invalid date format. Please use YYYY-MM-DD.\nExample: /report 2025-01-15")
            return

        if not self.db_conn:
            await update.message.reply_text("❌ Database not available")
            return
            
        try:
            from ..reporting.summarizer import ReportGenerator
            from ..reporting.formatters import format_daily_summary
            
            generator = ReportGenerator()
            summary = await generator.generate_daily_summary(self.db_conn, date, self.universe_size)
            
            report_text = format_daily_summary(summary)
            await update.message.reply_text(report_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error generating report for {date}: {e}")
            await update.message.reply_text(f"❌ Error generating report for {date}. Please try again.")
    
    @_admin_only
    async def top(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /top command - show top N signals.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        if not self.db_conn:
            await update.message.reply_text("❌ Database not available")
            return
        
        try:
            # Query top signals from database
            from ..database import query_recent_signals
            recent_signals = query_recent_signals(self.db_conn, limit=10)
            
            # Filter and sort by confidence
            valid_signals = [
                signal for signal in recent_signals 
                if signal.get('confidence', 0) > 0.5  # Only high confidence
            ]
            valid_signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            
            top_text = format_top_signals(valid_signals, limit=5)
            
            await update.message.reply_text(top_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error fetching top signals: {e}")
            await update.message.reply_text("❌ Error fetching signals. Please try again.")
    
    @_admin_only
    async def symbol(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /symbol command - analyze specific symbol.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        if not context.args:
            await update.message.reply_text("❌ Please specify a symbol.\nExample: /symbol BTCUSDT")
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
            
            await update.message.reply_text(analysis_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error analyzing symbol {symbol}: {e}")
            await update.message.reply_text(f"❌ Error analyzing {symbol}. Please try again.")
    
    @_admin_only
    async def scanstart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstart command - enable scanning.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        self.set_mode("scanning")
        
        await update.message.reply_text(
            "🔍 *Market Scanning Enabled*\\n\\n"
            "✅ Scanner is now active\\n"
            "📊 Monitoring all symbols for opportunities\\n"
            "⚡ Signals will be sent automatically\\n\\n"
            "Happy hunting! 🎯",
            parse_mode='Markdown'
        )
    
    @_admin_only
    async def scanstop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstop command - disable scanning.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        self.set_mode("paused")
        
        await update.message.reply_text(
            "⏸️ *Market Scanning Paused*\\n\\n"
            "🛑 Scanner has been stopped\\n"
            "📊 No new signals will be generated\\n"
            "🔄 Previous signals remain valid\\n\\n"
            "Use /scanstart to resume scanning.",
            parse_mode='Markdown'
        )
    
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
        logger.error(f"Update {update} caused error {context.error}")
        
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