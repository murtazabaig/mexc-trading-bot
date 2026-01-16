#!/usr/bin/env python3
"""Validation script to verify the admin decorator fix in handlers.py."""

import re

def validate_handlers_fix():
    """Validate that the handlers.py file has been properly fixed."""
    
    print("🔍 Validating handlers.py admin decorator fix...")
    print("="*60)
    
    # Read the handlers.py file
    with open('src/telegram_bot/handlers.py', 'r') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    validation_results = {
        'decorator_removed': False,
        'inline_checks_count': 0,
        'commands_found': [],
        'admin_commands_protected': [],
        'public_commands': [],
        'syntax_valid': False,
        'errors': []
    }
    
    # 1. Check if decorator is removed
    if '@admin_only' not in content:
        validation_results['decorator_removed'] = True
        print("✅ Broken @admin_only decorator has been REMOVED")
    else:
        print("❌ @admin_only decorator still present - FIX FAILED")
        validation_results['errors'].append("Decorator not removed")
    
    # 2. Check that the decorator function is removed
    if 'def admin_only(func):' not in content:
        print("✅ admin_only function definition has been REMOVED")
    else:
        print("❌ admin_only function definition still present - FIX FAILED")
        validation_results['errors'].append("Decorator function not removed")
    
    # 3. Count inline admin checks
    inline_check_pattern = r'if not self\.is_admin\(update\):'
    matches = re.findall(inline_check_pattern, content)
    validation_results['inline_checks_count'] = len(matches)
    print(f"✅ Found {len(matches)} inline admin checks")
    
    if len(matches) < 7:
        print(f"❌ Expected at least 7 inline checks, found {len(matches)}")
        validation_results['errors'].append(f"Insufficient inline checks: {len(matches)}/7")
    
    # 4. Check all command methods exist
    commands = {
        'handle_start': 'public',
        'handle_help': 'admin',
        'handle_status': 'admin',
        'handle_report': 'admin',
        'handle_top': 'admin',
        'handle_symbol': 'admin',
        'handle_scanstart': 'admin',
        'handle_scanstop': 'admin'
    }
    
    for cmd, access_type in commands.items():
        pattern = f'async def {cmd}\\(self, update: Update, context: ContextTypes.DEFAULT_TYPE\\):'
        if re.search(pattern, content):
            validation_results['commands_found'].append(cmd)
            if access_type == 'admin':
                validation_results['admin_commands_protected'].append(cmd)
            else:
                validation_results['public_commands'].append(cmd)
            print(f"✅ Found {cmd} command ({access_type} access)")
        else:
            print(f"❌ {cmd} command NOT FOUND")
            validation_results['errors'].append(f"{cmd} command missing")
    
    # 5. Verify syntax
    try:
        import ast
        ast.parse(content)
        validation_results['syntax_valid'] = True
        print("✅ Python syntax is VALID")
    except SyntaxError as e:
        print(f"❌ Syntax error: {e}")
        validation_results['errors'].append(f"Syntax error: {e}")
    
    # 6. Verify that /start is public (no admin check right after it)
    start_pattern = r'async def handle_start\(self, update: Update, context: ContextTypes\.DEFAULT_TYPE\):.*?(?=async def|\Z)'
    start_match = re.search(start_pattern, content, re.DOTALL)
    
    if start_match:
        start_method = start_match.group(0)
        if 'if not self.is_admin(update):' not in start_method:
            print("✅ /start command is PUBLIC (no admin check)")
        else:
            print("❌ /start command has admin check - should be public")
            validation_results['errors'].append("/start command has admin restriction")
    
    # 7. Verify admin commands have inline checks nearby
    admin_commands = ['handle_help', 'handle_status', 'handle_report', 'handle_top', 'handle_symbol', 'handle_scanstart', 'handle_scanstop']
    admin_commands_with_checks = 0
    
    for cmd in admin_commands:
        pattern = f'async def {cmd}\\(self, update: Update, context: ContextTypes\.DEFAULT_TYPE\):.*?(?=async def|class|def|$)'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            method_content = match.group(0)
            # Check if admin check appears within first 20 lines of the method
            method_lines = method_content.split('\n')[:20]
            method_text = '\n'.join(method_lines)
            if 'if not self.is_admin(update):' in method_text:
                admin_commands_with_checks += 1
    
    print(f"✅ {admin_commands_with_checks}/{len(admin_commands)} admin commands have inline checks")
    
    # Generate final report
    print("\n" + "="*60)
    print("📋 VALIDATION REPORT")
    print("="*60)
    
    all_passed = (
        validation_results['decorator_removed'] and
        validation_results['inline_checks_count'] >= 7 and
        len(validation_results['commands_found']) == 8 and
        validation_results['syntax_valid'] and
        len(validation_results['errors']) == 0 and
        admin_commands_with_checks == 7
    )
    
    print(f"\n✅ Decorator Removed: {validation_results['decorator_removed']}")
    print(f"✅ Inline Checks: {validation_results['inline_checks_count']}/7")
    print(f"✅ Commands Found: {len(validation_results['commands_found'])}/8")
    print(f"✅ Admin Commands Protected: {admin_commands_with_checks}/7")
    print(f"✅ Syntax Valid: {validation_results['syntax_valid']}")
    print(f"❌ Errors: {len(validation_results['errors'])}")
    
    if validation_results['errors']:
        print("\nErrors found:")
        for error in validation_results['errors']:
            print(f"  • {error}")
    
    print("\n" + "="*60)
    
    if all_passed:
        print("🎉 VALIDATION SUCCESSFUL - ALL CHECKS PASSED!")
        print("\n✅ Expected Behavior After Fix:")
        print("  • /start responds with welcome message (public access)")
        print("  • /help responds with command list (admin only)")
        print("  • /status responds with bot stats (admin only)")
        print("  • /top responds with top signals (admin only)")
        print("  • /symbol <COIN> responds with analysis (admin only)")
        print("  • /report responds with daily summary (admin only)")
        print("  • /scanstart enables scanning (admin only)")
        print("  • /scanstop disables scanning (admin only)")
        print("  • Non-admin users get 'Access denied' message for admin commands")
        print("  • No decorator-related exceptions will be thrown")
        return True
    else:
        print("❌ VALIDATION FAILED - ISSUES DETECTED")
        return False

if __name__ == "__main__":
    import sys
    success = validate_handlers_fix()
    sys.exit(0 if success else 1)
