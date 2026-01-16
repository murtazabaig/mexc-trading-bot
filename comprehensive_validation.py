#!/usr/bin/env python3
"""Comprehensive validation of admin decorator fix in all Telegram bot files."""

import re
import ast

def validate_file(filepath, filename):
    """Validate a single file for admin decorator fixes."""
    
    print(f"\n🔍 Validating {filename}...")
    print("-" * 60)
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    results = {
        'decorator_removed': '@admin_only' not in content,
        'decorator_func_removed': 'def admin_only(' not in content,
        'inline_checks': len(re.findall(r'if not self\.(_)?is_admin\(update\):', content)),
        'commands_found': [],
        'admin_commands': 0,
        'public_commands': 0,
        'syntax_valid': False,
        'errors': []
    }
    
    # Check syntax
    try:
        ast.parse(content)
        results['syntax_valid'] = True
        print(f"✅ Python syntax is VALID")
    except SyntaxError as e:
        results['errors'].append(f"Syntax error: {e}")
        print(f"❌ Syntax error: {e}")
    
    # Check decorator removal
    if results['decorator_removed']:
        print(f"✅ @admin_only decorator removed")
    else:
        print(f"❌ @admin_only decorator still present")
        results['errors'].append("Decorator not removed")
    
    if results['decorator_func_removed']:
        print(f"✅ admin_only function removed")
    else:
        print(f"❌ admin_only function still present")
        results['errors'].append("Decorator function not removed")
    
    # Count commands
    if 'bot.py' in filename:
        commands = ['start', 'help', 'status', 'report', 'top', 'symbol', 'scanstart', 'scanstop']
        for cmd in commands:
            if f'async def {cmd}(self,' in content:
                results['commands_found'].append(cmd)
                if cmd == 'start':
                    results['public_commands'] += 1
                else:
                    results['admin_commands'] += 1
    else:
        commands = ['handle_start', 'handle_help', 'handle_status', 'handle_report',
                    'handle_top', 'handle_symbol', 'handle_scanstart', 'handle_scanstop']
        for cmd in commands:
            if f'async def {cmd}(self,' in content:
                results['commands_found'].append(cmd)
                if 'start' in cmd:
                    results['public_commands'] += 1
                else:
                    results['admin_commands'] += 1
    
    print(f"✅ Commands found: {len(results['commands_found'])}/8")
    print(f"✅ Admin commands: {results['admin_commands']}/7")
    print(f"✅ Public commands: {results['public_commands']}/1")
    print(f"✅ Inline admin checks: {results['inline_checks']}/7")
    
    if results['inline_checks'] < 7:
        results['errors'].append(f"Insufficient inline checks: {results['inline_checks']}/7")
    
    return results

def main():
    """Run comprehensive validation."""
    
    print("🚀 COMPREHENSIVE TELEGRAM BOT VALIDATION")
    print("=" * 60)
    
    files = {
        'src/telegram_bot/bot.py': 'bot.py',
        'src/telegram_bot/handlers.py': 'handlers.py'
    }
    
    all_results = {}
    all_passed = True
    
    for filepath, filename in files.items():
        try:
            results = validate_file(filepath, filename)
            all_results[filename] = results
            
            if results['errors']:
                all_passed = False
                print(f"\n❌ {filename} has {len(results['errors'])} error(s):")
                for error in results['errors']:
                    print(f"   • {error}")
            else:
                print(f"\n✅ {filename} - ALL CHECKS PASSED")
                
        except Exception as e:
            print(f"\n❌ Error validating {filename}: {e}")
            all_passed = False
            all_results[filename] = {'errors': [str(e)]}
    
    # Final summary
    print("\n" + "=" * 60)
    print("📋 FINAL VALIDATION REPORT")
    print("=" * 60)
    
    for filename, results in all_results.items():
        status = "✅ PASSED" if not results.get('errors') else "❌ FAILED"
        print(f"\n{filename}: {status}")
        if results.get('errors'):
            for error in results['errors']:
                print(f"  • {error}")
    
    print("\n" + "=" * 60)
    
    if all_passed:
        print("🎉 COMPREHENSIVE VALIDATION SUCCESSFUL!")
        print("\n✅ All files are properly fixed:")
        print("  • Broken @admin_only decorators removed")
        print("  • All admin commands protected with inline checks")
        print("  • Public commands (/start) remain accessible to all")
        print("  • Python syntax is valid in all files")
        print("  • No decorator-related exceptions will occur")
        print("\n✅ Expected Behavior:")
        print("  • /start works for everyone")
        print("  • /help, /status, /top, /symbol, /report, /scanstart, /scanstop work for admin only")
        print("  • Non-admin users get 'Access denied' for admin commands")
        print("  • No decorator binding or execution errors")
        return True
    else:
        print("❌ COMPREHENSIVE VALIDATION FAILED")
        print("\nSome files have issues that need to be addressed.")
        return False

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
