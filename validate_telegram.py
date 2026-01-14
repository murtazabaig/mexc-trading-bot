#!/usr/bin/env python3
"""Simple validation script for Telegram bot functionality without external dependencies."""

import sys
import os
from datetime import datetime, timezone, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test basic imports."""
    print("🔍 Testing imports...")
    
    try:
        from telegram_bot.formatters import (
            format_status, format_signal, format_top_signals, 
            format_symbol_analysis, format_warning
        )
        print("✅ Formatter imports successful")
        
        # Mock telegram module for bot imports
        import types
        telegram = types.ModuleType('telegram')
        ext = types.ModuleType('telegram.ext')
        
        # Mock classes
        class MockUpdate:
            def __init__(self):
                self.effective_user = None
                self.effective_chat = None
                self.message = None
        
        class MockContext:
            def __init__(self):
                self.args = []
        
        class MockApplication:
            def __init__(self):
                pass
            def add_handler(self, handler):
                pass
            def add_error_handler(self, handler):
                pass
            @classmethod
            def builder(cls):
                return cls()
            def token(self, token):
                return cls()
            async def initialize(self):
                pass
            async def start(self):
                pass
            @property
            def updater(self):
                return self
            async def start_polling(self, timeout=30, drop_pending_updates=True):
                pass
            @property
            def bot(self):
                return self
        
        telegram.Update = MockUpdate
        telegram.ext = ext
        ext.Application = MockApplication
        ext.CommandHandler = MockApplication
        ext.MessageHandler = type('MockHandler', (), {})
        ext.filters = type('MockFilters', (), {})()
        
        sys.modules['telegram'] = telegram
        sys.modules['telegram.ext'] = ext
        
        # Test bot import
        from telegram_bot.bot import MexcSignalBot
        print("✅ Bot class import successful")
        
        return True
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        return False

def test_formatters():
    """Test message formatters."""
    print("\n📝 Testing formatters...")
    
    try:
        from telegram_bot.formatters import (
            format_status, format_signal, format_top_signals, 
            format_symbol_analysis, format_warning
        )
        
        # Test status formatter
        now = datetime.now(timezone.utc)
        last_scan = now - timedelta(minutes=15)
        
        status_text = format_status(
            uptime_seconds=3600,
            last_scan=last_scan,
            universe_size=345,
            mode="active"
        )
        
        assert "🤖 *Bot Status*" in status_text
        assert "Uptime: 1h 0m" in status_text
        assert "✅ Active" in status_text
        print("✅ Status formatter working")
        
        # Test signal formatter
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
        
        signal_text = format_signal(signal)
        assert "🟢 *NEW LONG SETUP*" in signal_text
        assert "*BTC/USDT* (1h)" in signal_text
        assert "Confidence: 85%" in signal_text
        assert "Entry: $50,000" in signal_text
        print("✅ Signal formatter working")
        
        # Test top signals formatter
        signals = [
            {'symbol': 'BTCUSDT', 'timeframe': '1h', 'side': 'LONG', 'confidence': 0.85},
            {'symbol': 'ETHUSDT', 'timeframe': '1h', 'side': 'LONG', 'confidence': 0.78},
        ]
        
        top_text = format_top_signals(signals, limit=5)
        assert "🏆 *Top Setups*" in top_text
        assert "1. 🟢 BTCUSDT 1h LONG (85%)" in top_text
        print("✅ Top signals formatter working")
        
        # Test symbol analysis formatter
        indicators = {
            "EMA20": 47250.0,
            "RSI": 62.3,
            "ATR%": 1.2,
            "VWAP": 47205.0,
        }
        
        analysis_text = format_symbol_analysis(
            symbol="BTCUSDT",
            regime="TRENDING",
            regime_confidence=0.85,
            indicators=indicators,
            last_signals=[]
        )
        
        assert "📊 *BTCUSDT Analysis*" in analysis_text
        assert "Regime: 📈 TRENDING (confidence: 85%)" in analysis_text
        print("✅ Symbol analysis formatter working")
        
        # Test warning formatter
        warning = {
            'severity': 'CRITICAL',
            'warning_type': 'BTC_SHOCK',
            'message': 'BTC dropped 8% in 1h',
            'action_taken': 'PAUSED_SIGNALS'
        }
        
        warning_text = format_warning(warning)
        assert "🚨 *CRITICAL WARNING*" in warning_text
        assert "₿ Type: BTC Shock" in warning_text
        print("✅ Warning formatter working")
        
        return True
        
    except Exception as e:
        print(f"❌ Formatter error: {e}")
        return False

def test_bot_class():
    """Test bot class functionality."""
    print("\n🤖 Testing bot class...")
    
    try:
        from telegram_bot.bot import MexcSignalBot
        
        # Test initialization
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
        print("✅ Bot initialization working")
        
        # Test setters
        bot.set_universe_size(150)
        assert bot.universe_size == 150
        
        bot.set_mode("paused")
        assert bot.mode == "paused"
        print("✅ Bot setters working")
        
        # Test info method
        info = bot.get_bot_info()
        assert info['token_configured'] == True
        assert info['admin_chat_id'] == "123456789"
        assert 'uptime_seconds' in info
        print("✅ Bot info method working")
        
        return True
        
    except Exception as e:
        print(f"❌ Bot class error: {e}")
        return False

def test_integration():
    """Test integration with existing codebase."""
    print("\n🔗 Testing integration...")
    
    try:
        # Test config integration
        from config import Config
        
        # Mock environment for testing
        os.environ['TELEGRAM_BOT_TOKEN'] = '1234567890:test_token'
        os.environ['TELEGRAM_ADMIN_CHAT_ID'] = '123456789'
        os.environ['TELEGRAM_POLLING_TIMEOUT'] = '30'
        
        config = Config.from_env()
        assert config.telegram_bot_token == '1234567890:test_token'
        assert config.telegram_admin_chat_id == '123456789'
        assert config.telegram_polling_timeout == 30
        print("✅ Config integration working")
        
        # Test database integration
        from database import init_db, create_schema
        
        conn = init_db(":memory:")
        create_schema(conn)
        print("✅ Database integration working")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration error: {e}")
        return False

def main():
    """Run all validation tests."""
    print("🚀 Validating Telegram Bot Implementation")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_formatters,
        test_bot_class,
        test_integration
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Telegram bot implementation is working correctly.")
        return 0
    else:
        print("⚠️ Some tests failed. Please check the implementation.")
        return 1

if __name__ == "__main__":
    sys.exit(main())