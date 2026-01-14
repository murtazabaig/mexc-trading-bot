"""Unit tests for Telegram bot functionality."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, AsyncMock, patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from telegram_bot.formatters import (
    format_status,
    format_signal,
    format_top_signals,
    format_symbol_analysis,
    format_warning
)
from telegram_bot.bot import MexcSignalBot
from telegram_bot.handlers import CommandHandlers, ErrorHandler, setup_handlers


class TestMessageFormatters:
    """Test message formatting functions."""
    
    def test_format_status_active(self):
        """Test status message formatting for active bot."""
        now = datetime.now(timezone.utc)
        last_scan = now - timedelta(minutes=15)
        
        result = format_status(
            uptime_seconds=3600,  # 1 hour
            last_scan=last_scan,
            universe_size=345,
            mode="active"
        )
        
        assert "🤖 *Bot Status*" in result
        assert "Uptime: 1h 0m" in result
        assert "Last Scan: 15 minutes ago" in result
        assert "Universe: 345 symbols" in result
        assert "✅ Active" in result
    
    def test_format_status_no_scan(self):
        """Test status message when no scan has occurred."""
        result = format_status(
            uptime_seconds=7200,  # 2 hours
            last_scan=None,
            universe_size=123,
            mode="paused"
        )
        
        assert "Last Scan: Never" in result
        assert "⏸️ Paused" in result
    
    def test_format_signal_long(self):
        """Test signal message formatting for LONG position."""
        signal = {
            'symbol': 'BTCUSDT',
            'timeframe': '1h',
            'side': 'LONG',
            'confidence': 0.85,
            'regime': 'TRENDING',
            'entry_price': 50000.0,
            'entry_band_min': 49500.0,
            'entry_band_max': 50500.0,
            'stop_loss': 48000.0,
            'tp1': 52000.0,
            'tp2': 54000.0,
            'tp3': 56000.0,
            'reason': {'confluence': ['RSI Oversold', 'Support Touch']}
        }
        
        result = format_signal(signal)
        
        assert "🟢 *NEW LONG SETUP*" in result
        assert "*BTC/USDT* (1h)" in result
        assert "Confidence: 85%" in result
        assert "Regime: Trending" in result
        assert "Entry: $50,000" in result
        assert "SL: $48,000" in result
        assert "TP1: $52,000 | TP2: $54,000 | TP3: $56,000" in result
        assert "Reasons:" in result
        assert "• RSI Oversold" in result
        assert "• Support Touch" in result
    
    def test_format_signal_short(self):
        """Test signal message formatting for SHORT position."""
        signal = {
            'symbol': 'ETHUSDT',
            'timeframe': '4h',
            'side': 'SHORT',
            'confidence': 0.78,
            'regime': 'RANGING',
            'entry_price': 2500.0,
            'stop_loss': 2600.0,
            'tp1': 2400.0,
            'tp2': 2300.0,
            'tp3': 2200.0
        }
        
        result = format_signal(signal)
        
        assert "🔴 *NEW SHORT SETUP*" in result
        assert "*ETH/USDT* (4h)" in result
        assert "Confidence: 78%" in result
        assert "Regime: Ranging" in result
        assert "Entry: $2,500" in result
        assert "SL: $2,600" in result
    
    def test_format_top_signals_multiple(self):
        """Test top signals message with multiple signals."""
        signals = [
            {'symbol': 'BTCUSDT', 'timeframe': '1h', 'side': 'LONG', 'confidence': 0.85},
            {'symbol': 'ETHUSDT', 'timeframe': '1h', 'side': 'LONG', 'confidence': 0.78},
            {'symbol': 'SOLUSDT', 'timeframe': '4h', 'side': 'SHORT', 'confidence': 0.72}
        ]
        
        result = format_top_signals(signals, limit=3)
        
        assert "🏆 *Top Setups*" in result
        assert "1. 🟢 BTCUSDT 1h LONG (85%)" in result
        assert "2. 🟢 ETHUSDT 1h LONG (78%)" in result
        assert "3. 🔴 SOLUSDT 4h SHORT (72%)" in result
    
    def test_format_top_signals_empty(self):
        """Test top signals message with no signals."""
        result = format_top_signals([], limit=5)
        
        assert "🏆 *Top Setups*" in result
        assert "📭 No recent signals available" in result
    
    def test_format_symbol_analysis(self):
        """Test symbol analysis message formatting."""
        regime = "TRENDING"
        regime_confidence = 0.85
        indicators = {
            "EMA20": 47250.0,
            "RSI": 62.3,
            "ATR%": 1.2,
            "VWAP": 47205.0,
            "ADX": 28.5,
            "Volume_ZScore": 1.8
        }
        last_signals = [
            {
                'timestamp': (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                'side': 'LONG',
                'entry_price': 47000.0,
                'status': 'OPEN'
            }
        ]
        
        result = format_symbol_analysis(
            symbol="BTCUSDT",
            regime=regime,
            regime_confidence=regime_confidence,
            indicators=indicators,
            last_signals=last_signals
        )
        
        assert "📊 *BTCUSDT Analysis*" in result
        assert "Regime: 📈 TRENDING (confidence: 85%)" in result
        assert "Indicators:" in result
        assert "• EMA20: $47,250" in result
        assert "• RSI: 62.3" in result
        assert "• ATR%: 1.2%" in result
        assert "• VWAP: $47,205" in result
        assert "Recent Signals:" in result
        assert "• 🟢 LONG @ $47,000 (2 hours ago, OPEN)" in result
    
    def test_format_warning_critical(self):
        """Test warning message formatting for critical severity."""
        warning = {
            'severity': 'CRITICAL',
            'warning_type': 'BTC_SHOCK',
            'message': 'BTC dropped 8% in 1h on volume spike',
            'triggered_value': 0.08,
            'threshold': 0.05,
            'action_taken': 'PAUSED_SIGNALS'
        }
        
        result = format_warning(warning)
        
        assert "🚨 *CRITICAL WARNING*" in result
        assert "₿ Type: BTC Shock" in result
        assert "BTC dropped 8% in 1h on volume spike (0.1%) (threshold: 0.05)" in result
        assert "Action: PAUSED_SIGNALS" in result
    
    def test_format_warning_info(self):
        """Test warning message formatting for info severity."""
        warning = {
            'severity': 'INFO',
            'warning_type': 'VOLUME_SURGE',
            'message': 'High volume detected across market',
            'triggered_value': 150.5,
            'threshold': 100.0,
            'action_taken': 'None'
        }
        
        result = format_warning(warning)
        
        assert "ℹ️ *INFO WARNING*" in result
        assert "📊 Type: Volume Surge" in result
        assert "Action: None" in result


class TestMexcSignalBot:
    """Test MexcSignalBot class."""
    
    def test_bot_initialization(self):
        """Test bot initialization."""
        bot = MexcSignalBot(
            bot_token="1234567890:test_token",
            admin_chat_id="123456789",
            polling_timeout=30
        )
        
        assert bot.bot_token == "1234567890:test_token"
        assert bot.admin_chat_id == "123456789"
        assert bot.polling_timeout == 30
        assert bot.mode == "active"
        assert bot.universe_size == 0
    
    def test_set_database_connection(self):
        """Test setting database connection."""
        bot = MexcSignalBot("token", "chat_id")
        mock_conn = Mock()
        
        bot.set_database_connection(mock_conn)
        assert bot.db_conn == mock_conn
    
    def test_set_universe_size(self):
        """Test setting universe size."""
        bot = MexcSignalBot("token", "chat_id")
        
        bot.set_universe_size(150)
        assert bot.universe_size == 150
    
    def test_set_last_scan_time(self):
        """Test setting last scan time."""
        bot = MexcSignalBot("token", "chat_id")
        scan_time = datetime.now(timezone.utc)
        
        bot.set_last_scan_time(scan_time)
        assert bot.last_scan_time == scan_time
    
    def test_set_mode(self):
        """Test setting bot mode."""
        bot = MexcSignalBot("token", "chat_id")
        
        bot.set_mode("paused")
        assert bot.mode == "paused"
    
    def test_is_admin_true(self):
        """Test admin check for admin user."""
        bot = MexcSignalBot("token", "123456789")
        
        # Mock update for admin user
        mock_update = Mock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.id = 123456789
        mock_update.effective_chat = None
        
        assert bot._is_admin(mock_update) == True
    
    def test_is_admin_false(self):
        """Test admin check for non-admin user."""
        bot = MexcSignalBot("token", "123456789")
        
        # Mock update for non-admin user
        mock_update = Mock()
        mock_update.effective_user = None
        mock_update.effective_chat = None
        
        assert bot._is_admin(mock_update) == False
    
    def test_get_bot_info(self):
        """Test getting bot information."""
        bot = MexcSignalBot("token", "123456789")
        bot.set_universe_size(100)
        bot.set_mode("scanning")
        
        info = bot.get_bot_info()
        
        assert info['token_configured'] == True
        assert info['admin_chat_id'] == "123456789"
        assert info['universe_size'] == 100
        assert info['mode'] == "scanning"
        assert 'uptime_seconds' in info
        assert 'start_time' in info


class TestCommandHandlers:
    """Test command handlers."""
    
    @pytest.fixture
    def mock_bot(self):
        """Create mock bot for testing."""
        bot = Mock()
        bot.admin_chat_id = "123456789"
        bot.universe_size = 150
        bot.bot_token = "1234567890:test"
        bot.start_time = datetime.now(timezone.utc) - timedelta(hours=1)
        bot.last_scan_time = datetime.now(timezone.utc) - timedelta(minutes=30)
        bot.mode = "active"
        bot.db_conn = Mock()
        bot.set_mode = Mock()
        return bot
    
    def test_is_admin_true(self, mock_bot):
        """Test admin check in handlers."""
        handlers = CommandHandlers(mock_bot)
        
        mock_update = Mock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.id = 123456789
        
        assert handlers.is_admin(mock_update) == True
    
    def test_is_admin_false(self, mock_bot):
        """Test non-admin check in handlers."""
        handlers = CommandHandlers(mock_bot)
        
        mock_update = Mock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.id = 999999999
        
        assert handlers.is_admin(mock_update) == False
    
    @pytest.mark.asyncio
    async def test_admin_only_decorator_denies_non_admin(self, mock_bot):
        """Test admin-only decorator blocks non-admin users."""
        handlers = CommandHandlers(mock_bot)
        
        mock_update = Mock()
        mock_update.message = Mock()
        mock_update.message.reply_text = AsyncMock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.id = 999999999  # Non-admin
        mock_update.effective_chat = None
        
        @handlers.admin_only
        async def test_command(update, context):
            return "success"
        
        result = await test_command(mock_update, None)
        assert result is None  # Should be blocked
        
        # Check that access denied message was sent
        mock_update.message.reply_text.assert_called_once_with("❌ Access denied. Admin only.")


class TestErrorHandler:
    """Test error handling."""
    
    def test_error_handler_initialization(self):
        """Test error handler initialization."""
        mock_bot = Mock()
        error_handler = ErrorHandler(mock_bot)
        
        assert error_handler.bot == mock_bot
        assert error_handler.logger is not None
    
    @pytest.mark.asyncio
    async def test_error_handler_with_update(self):
        """Test error handling with update object."""
        mock_bot = Mock()
        error_handler = ErrorHandler(mock_bot)
        
        mock_update = Mock()
        mock_update.effective_message = Mock()
        mock_update.effective_message.reply_text = AsyncMock()
        
        mock_context = Mock()
        mock_context.error = Exception("Test error")
        
        # Should not raise exception
        await error_handler.handle_error(mock_update, mock_context)
        
        # Check that error message was sent
        mock_update.effective_message.reply_text.assert_called_once()


class TestSetupHandlers:
    """Test handler setup function."""
    
    def test_setup_handlers(self):
        """Test handler setup function."""
        mock_app = Mock()
        mock_bot = Mock()
        
        handlers, error_handler = setup_handlers(mock_app, mock_bot)
        
        # Check that command handlers were added
        assert mock_app.add_handler.call_count >= 7  # 7 commands
        
        # Check that error handler was added
        assert mock_app.add_error_handler.call_count == 1


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])