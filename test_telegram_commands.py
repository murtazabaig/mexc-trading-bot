#!/usr/bin/env python3
"""Test script to validate Telegram bot commands work correctly after fixing the admin decorator."""

import asyncio
import sys
import os
from unittest.mock import Mock, AsyncMock, MagicMock
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from telegram_bot.bot import MexcSignalBot
from telegram import Update
from telegram.ext import ContextTypes

class MockUpdate:
    """Mock Telegram Update object."""
    def __init__(self, user_id=None, chat_id=None, text="/status"):
        self.update_id = 12345
        self.effective_user = Mock()
        self.effective_user.id = user_id
        self.effective_chat = Mock()
        self.effective_chat.id = chat_id
        self.effective_message = Mock()
        self.effective_message.reply_text = AsyncMock()

class MockPauseState:
    """Mock pause state."""
    def __init__(self):
        self._paused = False
        self._reason = None
    
    def pause(self, reason=None):
        self._paused = True
        self._reason = reason
        print(f"⏸️ Scanner paused: {reason}")
    
    def resume(self):
        self._paused = False
        self._reason = None
        print("▶️ Scanner resumed")

class MockScanner:
    """Mock scanner."""
    def get_stats(self):
        return {
            'symbols_processed': 150,
            'signals_generated': 5,
            'last_scan_time': datetime.now(timezone.utc),
            'status': 'running'
        }

class MockWarningDetector:
    """Mock warning detector."""
    def get_stats(self):
        return {
            'warnings_generated': 2,
            'btc_shocks_detected': 1,
            'breadth_collapses_detected': 1,
            'correlation_spikes_detected': 0
        }

class MockPortfolioManager:
    """Mock portfolio manager."""
    def get_stats(self):
        return {
            'active_positions': 3,
            'total_pnl': 245.67,
            'win_rate': 0.73,
            'max_drawdown': 0.05
        }

class MockDatabase:
    """Mock database connection."""
    def cursor(self):
        return self
    
    def execute(self, query, params=None):
        return []
    
    def fetchall(self):
        return []
    
    def close(self):
        pass

async def test_command(bot, command, user_id, is_admin=True):
    """Test a single command."""
    admin_chat_id = str(bot.admin_chat_id)
    
    if is_admin:
        # Admin user
        update = MockUpdate(user_id=admin_chat_id, chat_id=admin_chat_id, text=command)
        print(f"🧪 Testing '{command}' as ADMIN (user_id: {admin_chat_id})")
    else:
        # Non-admin user
        update = MockUpdate(user_id="999", chat_id="999", text=command)
        print(f"🧪 Testing '{command}' as NON-ADMIN (user_id: 999)")
    
    try:
        # Get the command method
        if command == "/start":
            method = bot.start
        elif command == "/help":
            method = bot.help
        elif command == "/status":
            method = bot.status
        elif command == "/report":
            method = bot.report
        elif command == "/top":
            method = bot.top
        elif command == "/symbol":
            method = bot.symbol
        elif command == "/scanstart":
            method = bot.scanstart
        elif command == "/scanstop":
            method = bot.scanstop
        else:
            print(f"❌ Unknown command: {command}")
            return False
        
        # Call the method
        await method(update, ContextTypes.DEFAULT_TYPE)
        
        # Check result
        if is_admin:
            # Admin should not get "Access denied" message
            calls = update.effective_message.reply_text.call_args_list
            if any("❌ Access denied" in str(call) for call in calls):
                print(f"❌ FAILED: Admin got access denied message")
                return False
            else:
                print(f"✅ PASSED: Admin successfully executed command")
                return True
        else:
            # Non-admin should get "Access denied" for admin commands
            if "/start" in command:
                # /start should work for everyone
                print(f"✅ PASSED: Public command /start works for non-admin")
                return True
            else:
                # Other commands should be blocked
                calls = update.effective_message.reply_text.call_args_list
                if any("❌ Access denied" in str(call) for call in calls):
                    print(f"✅ PASSED: Non-admin correctly blocked from admin command")
                    return True
                else:
                    print(f"❌ FAILED: Non-admin should have been blocked but wasn't")
                    return False
                    
    except Exception as e:
        print(f"❌ ERROR: Exception occurred - {e}")
        return False

async def main():
    """Main test function."""
    print("🚀 Starting Telegram Bot Commands Test")
    print("=" * 50)
    
    # Create bot instance
    bot_token = "1234567890:ABCdefGHIjklMNOpqrsTUVwxyz"
    admin_chat_id = "123456789"  # Valid admin ID
    
    bot = MexcSignalBot(
        bot_token=bot_token,
        admin_chat_id=admin_chat_id,
        polling_timeout=30,
        pause_state=MockPauseState()
    )
    
    # Set up bot components
    bot.db_conn = MockDatabase()
    bot.scanner = MockScanner()
    bot.warning_detector = MockWarningDetector()
    bot.portfolio_manager = MockPortfolioManager()
    bot.set_universe_size(150)
    bot.set_mode("active")
    
    # Test all commands
    commands = ["/start", "/help", "/status", "/report", "/top", "/symbol", "/scanstart", "/scanstop"]
    
    print("\n📋 TESTING ADMIN ACCESS:")
    print("-" * 30)
    
    admin_results = []
    for command in commands:
        result = await test_command(bot, command, admin_chat_id, is_admin=True)
        admin_results.append(result)
        print()
    
    print("\n📋 TESTING NON-ADMIN ACCESS:")
    print("-" * 35)
    
    non_admin_results = []
    for command in commands:
        result = await test_command(bot, command, "999", is_admin=False)
        non_admin_results.append(result)
        print()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)
    
    admin_passed = sum(admin_results)
    non_admin_passed = sum(non_admin_results)
    
    print(f"Admin Tests: {admin_passed}/{len(commands)} passed")
    print(f"Non-Admin Tests: {non_admin_passed}/{len(commands)} passed")
    
    if admin_passed == len(commands) and non_admin_passed == len(commands):
        print("\n🎉 ALL TESTS PASSED! Commands are working correctly.")
        return True
    else:
        print("\n❌ SOME TESTS FAILED! Please check the results above.")
        return False

if __name__ == "__main__":
    # Run the test
    success = asyncio.run(main())
    sys.exit(0 if success else 1)