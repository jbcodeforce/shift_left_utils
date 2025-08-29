#!/usr/bin/env python3
"""
Security validation script for shift_left CLI.

This script validates that:
1. Typer CLI apps are configured to hide local variables in exceptions
2. Error sanitization patterns are working correctly  
3. Logging infrastructure automatically sanitizes sensitive data

Run this periodically to ensure security configurations remain effective.

Usage:
    cd /path/to/shift_left/core && python3 utils/validate_security.py
"""
import logging
import tempfile
import os
import sys

# Add the parent directory to sys.path to handle imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from utils.app_config import SecureFormatter, logger
    from utils.error_sanitizer import sanitize_error_message, safe_error_display
except ImportError:
    try:
        from app_config import SecureFormatter, logger
        from error_sanitizer import sanitize_error_message, safe_error_display
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please run this test from the shift_left/core directory")
        sys.exit(1)


def validate_error_sanitization():
    """Validate that error sanitization patterns work correctly."""
    
    print("🔍 Validating Error Sanitization Patterns...")
    print("-" * 50)
    
    # Test cases with expected outcomes
    test_cases = [
        ("api_key=sk-1234567890abcdef", "***MASKED***"),
        ("password=secret123", "***MASKED***"),
        ("Authorization: Bearer token123", "***MASKED***"),
        ("JWT: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test.signature", "***JWT_MASKED***"),
        ("https://user:pass@host.com", "***USER***:***PASS***"),
    ]
    
    all_passed = True
    for i, (test_input, expected_pattern) in enumerate(test_cases, 1):
        sanitized = sanitize_error_message(test_input)
        
        if expected_pattern in sanitized:
            print(f"  ✅ Test {i}: PASS")
        else:
            print(f"  ❌ Test {i}: FAIL - Expected '{expected_pattern}' in '{sanitized}'")
            all_passed = False
    
    return all_passed


def validate_logging_security():
    """Validate that logging automatically sanitizes sensitive information."""
    
    print("\n🔍 Validating Logging Security...")
    print("-" * 50)
    
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as temp_log:
        temp_log_path = temp_log.name
    
    try:
        # Create test logger with SecureFormatter
        test_logger = logging.getLogger('security_test')
        test_logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        for handler in test_logger.handlers[:]:
            test_logger.removeHandler(handler)
        
        # Add secure file handler
        file_handler = logging.FileHandler(temp_log_path)
        file_handler.setFormatter(SecureFormatter('%(levelname)s - %(message)s'))
        test_logger.addHandler(file_handler)
        
        # Log sensitive data
        test_logger.info("api_key=sk-test123456789")
        test_logger.error("password=secret_password_123")
        file_handler.flush()
        
        # Read and validate log contents
        with open(temp_log_path, 'r') as f:
            log_contents = f.read()
        
        # Check for exposed secrets
        if "sk-test123456789" in log_contents or "secret_password_123" in log_contents:
            print("  ❌ FAIL: Sensitive data exposed in logs")
            return False
        elif "***MASKED***" in log_contents:
            print("  ✅ PASS: Sensitive data properly masked in logs")
            return True
        else:
            print("  ❌ FAIL: Unexpected log format")
            return False
            
    finally:
        if os.path.exists(temp_log_path):
            os.unlink(temp_log_path)


def validate_app_config_integration():
    """Validate that app_config logger uses SecureFormatter."""
    
    print("\n🔍 Validating App Config Integration...")
    print("-" * 50)
    
    # Check if main logger uses SecureFormatter
    has_secure_formatter = any(
        isinstance(handler.formatter, SecureFormatter) 
        for handler in logger.handlers
    )
    
    if has_secure_formatter:
        print("  ✅ PASS: Main logger uses SecureFormatter")
        return True
    else:
        print("  ❌ FAIL: Main logger does NOT use SecureFormatter")
        return False


def validate_typer_security():
    """Validate that Typer apps are configured securely."""
    
    print("\n🔍 Validating Typer CLI Security...")
    print("-" * 50)
    
    try:
        # Check CLI configurations
        cli_files = [
            "../cli.py",
            "../cli_commands/pipeline.py", 
            "../cli_commands/project.py",
            "../cli_commands/table.py"
        ]
        
        all_secure = True
        for cli_file in cli_files:
            cli_path = os.path.join(current_dir, cli_file)
            if os.path.exists(cli_path):
                with open(cli_path, 'r') as f:
                    content = f.read()
                    
                if "pretty_exceptions_show_locals=False" in content:
                    print(f"  ✅ {os.path.basename(cli_file)}: Secure")
                else:
                    print(f"  ❌ {os.path.basename(cli_file)}: NOT secure")
                    all_secure = False
            else:
                print(f"  ⚠️  {os.path.basename(cli_file)}: File not found")
        
        return all_secure
        
    except Exception as e:
        print(f"  ❌ Error validating Typer security: {e}")
        return False


def main():
    """Run comprehensive security validation."""
    print("🔒 SHIFT_LEFT CLI Security Validation")
    print("=" * 60)
    print()
    
    # Run all validation checks
    results = {
        "Error Sanitization": validate_error_sanitization(),
        "Logging Security": validate_logging_security(), 
        "App Config Integration": validate_app_config_integration(),
        "Typer CLI Security": validate_typer_security()
    }
    
    # Summary
    print("\n" + "=" * 60)
    print("🔒 SECURITY VALIDATION SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for check, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {check}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🔒 ALL SECURITY VALIDATIONS PASSED!")
        print("✅ The CLI is properly configured to prevent secret exposure.")
        return 0
    else:
        print("❌ SECURITY VALIDATION FAILURES DETECTED!")
        print("⚠️  Please review and fix the failing security checks.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
