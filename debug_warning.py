#!/usr/bin/env python3
"""Quick test to see actual output."""

from datetime import datetime, timezone

def test_format_warning():
    warning = {
        'severity': 'CRITICAL',
        'warning_type': 'BTC_SHOCK',
        'message': 'BTC dropped 8% in 1h on volume spike',
        'triggered_value': 0.08,
        'threshold': 0.05,
        'action_taken': 'PAUSED_SIGNALS'
    }
    
    severity = warning.get('severity', 'WARNING').upper()
    warning_type = warning.get('warning_type', 'UNKNOWN').replace('_', ' ').title()
    message = warning.get('message', 'No details available')
    action_taken = warning.get('action_taken', 'None')
    
    severity_emoji = {
        "CRITICAL": "🚨",
        "WARNING": "⚠️",
        "INFO": "ℹ️"
    }.get(severity, "⚠️")
    
    type_emoji = {
        "BTC SHOCK": "₿",
        "BREADTH COLLAPSE": "📉",
        "CORRELATION SPIKE": "🔗",
        "VOLUME SURGE": "📊",
        "VOLATILITY SPIKE": "📈"
    }.get(warning_type.upper().replace(' ', '_'), "⚠️")
    
    result = f"""{severity_emoji} *{severity} WARNING*
{type_emoji} Type: {warning_type}

{message}
Action: {action_taken}"""
    
    print("Severity:", repr(severity))
    print("Warning Type:", repr(warning_type))
    print("Message:", repr(message))
    print("Action:", repr(action_taken))
    print("Severity Emoji:", repr(severity_emoji))
    print("Type Emoji:", repr(type_emoji))
    
    print("\nActual result:")
    print(repr(result))
    print("\nFormatted:")
    print(result)

if __name__ == "__main__":
    test_format_warning()