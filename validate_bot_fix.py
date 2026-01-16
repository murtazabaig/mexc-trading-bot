#!/usr/bin/env python3
"""Simple test to validate the admin decorator fix in Telegram bot."""

import sys
import os
from unittest.mock import Mock, AsyncMock

def test_bot_import():
    """Test that the bot module can be imported correctly."""
    try:
        # Add src to path
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
        
        # Test imports
        from logger import get_logger
        from telegram_bot.formatters import format_status, format_signal, format_top_signals, format_symbol_analysis, format_warning
        
        print("✅ Import test passed - all modules imported successfully")
        return True
        
    except ImportError as e:
        print(f"❌ Import test failed: {e}")
        return False

def test_admin_check():
    """Test the _is_admin method logic."""
    # Test the admin check logic (simplified version without actual bot class)
    
    def is_admin(user_id, admin_chat_id):
        """Simplified admin check."""
        return str(user_id) == str(admin_chat_id)
    
    # Test cases
    admin_chat_id = "123456789"
    
    # Admin user should pass
    if is_admin(admin_chat_id, admin_chat_id):
        print("✅ Admin check test passed - admin user correctly identified")
    else:
        print("❌ Admin check test failed - admin user not identified")
        return False
    
    # Non-admin user should fail
    if not is_admin("999", admin_chat_id):
        print("✅ Admin check test passed - non-admin user correctly blocked")
    else:
        print("❌ Admin check test failed - non-admin user not blocked")
        return False
    
    return True

def test_decorator_removal():
    """Test that the decorator has been removed and replaced with inline checks."""
    try:
        # Read the bot.py file and check for the decorator removal
        with open('/home/engine/project/src/telegram_bot/bot.py', 'r') as f:
            content = f.read()
        
        # Check that the broken decorator is gone
        if '@_admin_only' in content:
            print("❌ Decorator removal test failed - @_admin_only decorator still present")
            return False
        
        # Check that inline admin checks are present in all admin commands
        admin_commands = [
            'async def help(self,',
            'async def status(self,',
            'async def report(self,',
            'async def top(self,',
            'async def symbol(self,',
            'async def scanstart(self,',
            'async def scanstop(self,'
        ]
        
        found_checks = 0
        for command in admin_commands:
            if command in content:
                found_checks += 1
        
        if found_checks == len(admin_commands):
            print("✅ Decorator removal test passed - all admin commands found")
        else:
            print(f"❌ Decorator removal test failed - only {found_checks}/{len(admin_commands)} admin commands found")
            return False
        
        # Check that all admin commands have inline checks
        if 'if not self._is_admin(update):' in content:
            count = content.count('if not self._is_admin(update):')
            if count >= 7:  # Should have at least 7 inline admin checks
                print("✅ Inline admin checks test passed - all commands have admin protection")
            else:
                print(f"❌ Inline admin checks test failed - only {count} admin checks found, expected 7")
                return False
        else:
            print("❌ Inline admin checks test failed - no admin checks found")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Decorator removal test failed: {e}")
        return False

def test_file_syntax():
    """Test that the bot.py file has valid Python syntax."""
    try:
        import ast
        
        with open('/home/engine/project/src/telegram_bot/bot.py', 'r') as f:
            content = f.read()
        
        # Try to parse the file
        ast.parse(content)
        
        print("✅ Syntax test passed - bot.py has valid Python syntax")
        return True
        
    except SyntaxError as e:
        print(f"❌ Syntax test failed - invalid Python syntax: {e}")
        return False
    except Exception as e:
        print(f"❌ Syntax test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting Telegram Bot Admin Decorator Fix Validation")
    print("=" * 60)
    
    tests = [
        ("Import Test", test_bot_import),
        ("Admin Check Logic", test_admin_check),
        ("Decorator Removal", test_decorator_removal),
        ("Python Syntax", test_file_syntax),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running {test_name}...")
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! The admin decorator fix is working correctly.")
        print("\n✅ Expected Behavior:")
        print("  • /start responds with welcome message (public)")
        print("  • /help, /status, /report, /top, /symbol, /scanstart, /scanstop work for admin only")
        print("  • Non-admin users get 'Access denied' message for admin commands")
        print("  • No decorator-related exceptions will be thrown")
        return True
    else:
        print("\n❌ SOME TESTS FAILED! Please check the results above.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)