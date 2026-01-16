"""Command handlers for MEXC Futures Signal Bot."""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from telegram import Update
from telegram.ext import ContextTypes

from ..logger import get_logger
from .formatters import (
    format_status, format_signal, format_top_signals, 
    format_symbol_analysis, format_warning
)

logger = get_logger(__name__)


class CommandHandlers:
    """Command handler class for all bot commands."""
    
    def __init__(self, bot_instance):
        """Initialize handlers with bot instance.
        
        Args:
            bot_instance: MexcSignalBot instance
        """
        self.bot = bot_instance
    
    def is_admin(self, update: Update) -> bool:
        """Check if the user is the admin.
        
        Args:
            update: Telegram update object
        
        Returns:
            True if user is admin, False otherwise
        """
        if not update.effective_user or not update.effective_chat:
            return False
        
        return str(update.effective_user.id) == self.bot.admin_chat_id or \
               update.effective_chat.id == int(self.bot.admin_chat_id)
    
    def admin_only(func):
        """Decorator to ensure only admin can execute command."""
        async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            if not self.is_admin(update):
                if update.effective_message:
                    await update.effective_message.reply_text("❌ Access denied. Admin only.")
                return
            
            return await func(self, update, context)
        return wrapper
    
    async def handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command - welcome message."""
        welcome_text = f"""🤖 *MEXC Futures Signal Bot*

Welcome! I'm your advanced trading signal assistant.

📊 *What I can do:*
• Generate high-confidence trading signals
• Monitor {self.bot.universe_size:,} futures markets
• Real-time market analysis
• Risk management alerts

🔧 *Available Commands:*
/status - Bot status and health
/help - Show all commands
/top - Top signals by confidence
/symbol <COIN> - Analyze specific symbol
/scanstart - Enable market scanning
/scanstop - Disable market scanning

⚡ *Ready to trade!*

This bot is in {'*TEST MODE*' if 'test' in str(self.bot.bot_token) else '*LIVE MODE*'}."""
        
        await update.effective_message.reply_text(welcome_text, parse_mode='Markdown')
    
    @admin_only
    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command - list all commands."""
        help_text = """🔧 *Bot Commands*

📊 *Status & Monitoring:*
/status - Bot health and statistics
/top - Show top 5 signals by confidence

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
    
    @admin_only
    async def handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command - show bot status."""
        # Calculate uptime
        uptime_seconds = int((datetime.now(timezone.utc) - self.bot.start_time).total_seconds())
        
        status_text = format_status(
            uptime_seconds=uptime_seconds,
            last_scan=self.bot.last_scan_time,
            universe_size=self.bot.universe_size,
            mode=self.bot.mode
        )
        
        await update.effective_message.reply_text(status_text, parse_mode='Markdown')

    @admin_only
    async def handle_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /report command - show daily summary."""
        from datetime import timedelta
        date = context.args[0] if context.args else (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Validate date format
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            await update.effective_message.reply_text("❌ Invalid date format. Please use YYYY-MM-DD.\nExample: /report 2025-01-15")
            return

        if not self.bot.db_conn:
            await update.effective_message.reply_text("❌ Database not available")
            return
            
        try:
            from ..reporting.summarizer import ReportGenerator
            from ..reporting.formatters import format_daily_summary
            
            generator = ReportGenerator()
            summary = await generator.generate_daily_summary(self.bot.db_conn, date, self.bot.universe_size)
            
            report_text = format_daily_summary(summary)
            await update.effective_message.reply_text(report_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error generating report for {date}: {e}")
            await update.effective_message.reply_text(f"❌ Error generating report for {date}. Please try again.")
    
    @admin_only
    async def handle_top(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /top command - show top N signals."""
        if not self.bot.db_conn:
            await update.effective_message.reply_text("❌ Database not available")
            return
        
        try:
            # Query top signals from database
            from ..database import query_recent_signals
            recent_signals = query_recent_signals(self.bot.db_conn, limit=10)
            
            # Filter and sort by confidence
            valid_signals = [
                signal for signal in recent_signals 
                if signal.get('confidence', 0) > 0.5  # Only high confidence
            ]
            valid_signals.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            
            top_text = format_top_signals(valid_signals, limit=5)
            
            await update.effective_message.reply_text(top_text, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error fetching top signals: {e}")
            await update.effective_message.reply_text("❌ Error fetching signals. Please try again.")
    
    @admin_only
    async def handle_symbol(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /symbol command - analyze specific symbol."""
        if not context.args:
            await update.effective_message.reply_text("❌ Please specify a symbol.\nExample: /symbol BTCUSDT")
            return
        
        symbol = context.args[0].upper().replace('/', '').replace('-', '')
        
        try:
            # Query recent signals for this symbol
            from ..database import query_recent_signals
            symbol_signals = query_recent_signals(self.bot.db_conn, symbol=symbol, limit=5) if self.bot.db_conn else []
            
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
    
    @admin_only
    async def handle_scanstart(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstart command - enable scanning."""
        self.bot.set_mode("scanning")
        
        await update.effective_message.reply_text(
            "🔍 *Market Scanning Enabled*\\n\\n"
            "✅ Scanner is now active\\n"
            "📊 Monitoring all symbols for opportunities\\n"
            "⚡ Signals will be sent automatically\\n\\n"
            "Happy hunting! 🎯",
            parse_mode='Markdown'
        )
    
    @admin_only
    async def handle_scanstop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /scanstop command - disable scanning."""
        self.bot.set_mode("paused")
        
        await update.effective_message.reply_text(
            "⏸️ *Market Scanning Paused*\\n\\n"
            "🛑 Scanner has been stopped\\n"
            "📊 No new signals will be generated\\n"
            "🔄 Previous signals remain valid\\n\\n"
            "Use /scanstart to resume scanning.",
            parse_mode='Markdown'
        )


class ErrorHandler:
    """Error handler for the bot."""
    
    def __init__(self, bot_instance):
        """Initialize error handler with bot instance.
        
        Args:
            bot_instance: MexcSignalBot instance
        """
        self.bot = bot_instance
        self.logger = get_logger(__name__)
    
    async def handle_error(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle all errors.
        
        Args:
            update: Telegram update object
            context: Context object
        """
        self.logger.error(f"Update {update} caused error {context.error}")
        
        # Log error details
        if context.error:
            error_type = type(context.error).__name__
            error_message = str(context.error)
            self.logger.error(f"Error type: {error_type}, Message: {error_message}")
        
        # Send user-friendly error message if update is available
        if update and update.effective_message:
            try:
                error_msg = (
                    "❌ An error occurred while processing your request.\\n\\n"
                    "🔧 Possible solutions:\\n"
                    "• Check your command syntax\\n"
                    "• Ensure you have proper permissions\\n"
                    "• Try again in a moment\\n\\n"
                    "If the problem persists, contact the administrator."
                )
                
                await update.effective_message.reply_text(error_msg, parse_mode='Markdown')
            except Exception as e:
                self.logger.error(f"Failed to send error message to user: {e}")
        
        # For network errors, log but don't crash
        if hasattr(context.error, '__class__'):
            error_classes = ['NetworkError', 'TimedOut', 'RetryAfter']
            if any(err_class in str(type(context.error)) for err_class in error_classes):
                self.logger.warning("Network-related error detected, continuing...")
                return
        
        # For other errors, also log but continue
        self.logger.warning("Bot continuing after error handling")


def setup_handlers(application, bot_instance):
    """Setup all command handlers for the application.
    
    Args:
        application: Telegram Application instance
        bot_instance: MexcSignalBot instance
    """
    handlers = CommandHandlers(bot_instance)
    error_handler = ErrorHandler(bot_instance)
    
    logger.info("Registering Telegram command handlers (via handlers.py)...")
    
    # Add command handlers
    application.add_handler(CommandHandler("start", handlers.handle_start))
    application.add_handler(CommandHandler("help", handlers.handle_help))
    application.add_handler(CommandHandler("status", handlers.handle_status))
    application.add_handler(CommandHandler("top", handlers.handle_top))
    application.add_handler(CommandHandler("symbol", handlers.handle_symbol))
    application.add_handler(CommandHandler("report", handlers.handle_report))
    application.add_handler(CommandHandler("scanstart", handlers.handle_scanstart))
    application.add_handler(CommandHandler("scanstop", handlers.handle_scanstop))
    
    # Add error handler
    application.add_error_handler(error_handler.handle_error)
    
    return handlers, error_handler