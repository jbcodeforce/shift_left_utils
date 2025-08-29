"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for error sanitization and security features.
This test suite validates that sensitive information is properly masked 
in error messages, exception outputs, and log files.
"""
import unittest
import tempfile
import os
import pathlib
import logging
from unittest.mock import patch, MagicMock

# Set environment variables before importing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.utils.error_sanitizer import (
    sanitize_error_message, 
    safe_error_display, 
    create_safe_error_handler
)
from shift_left.core.utils.app_config import SecureFormatter, logger
from ut.core.BaseUT import BaseUT


class TestErrorSanitization(BaseUT):
    """Test suite for error sanitization functionality."""

    def setUp(self):
        """Set up test environment before each test."""
        super().setUp()
        
        # Test data with sensitive information
        self.test_cases = [
            # API keys
            ('Error: api_key=sk-1234567890abcdef1234567890abcdef', 'api_key=***MASKED***'),
            ('Failed with api-key: "abc123def456ghi789"', 'api_key=***MASKED***'),
            
            # Secrets
            ('api_secret=mysecretpassword123', 'api_secret=***MASKED***'),
            ('secret: "super-secret-value-12345"', 'secret=***MASKED***'),
            
            # Passwords
            ('password=mypassword123', 'password=***MASKED***'),
            ('Failed login with password: "complex_pass_123"', 'password=***MASKED***'),
            
            # Tokens
            ('Bearer token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c', '***JWT_MASKED***'),
            ('Authorization: Bearer abc123def456', 'bearer ***MASKED***'),
            
            # SASL credentials
            ('sasl.username=myuser sasl.password=mypass', 'sasl.username=***MASKED*** sasl.password=***MASKED***'),
            
            # URLs with credentials  
            ('Connection failed to https://user:pass@kafka.example.com:9092', 'https://***USER***:***PASS***@kafka.example.com:9092'),
        ]
        
        self.edge_cases = [
            # Empty and None inputs
            ("", ""),
            (None, None),
            
            # Mixed case variations
            ("API_KEY=test123456789", "api_key=***MASKED***"),
            ("Api-Key: test123456789", "api_key=***MASKED***"),
            
            # Multiple secrets in one message
            ("Error: api_key=key123 and password=pass456", "api_key=***MASKED***"),
            
            # Common false positives (should NOT be masked)
            ("The secret to success is hard work", "The secret to success is hard work"),
            ("Password field is required", "Password field is required"),
            ("API key format is invalid", "API key format is invalid"),
        ]

    def test_sanitize_error_message_basic_patterns(self):
        """Test that basic sensitive data patterns are properly sanitized."""
        for original, expected_pattern in self.test_cases:
            with self.subTest(original=original):
                sanitized = sanitize_error_message(original)
                self.assertIn(expected_pattern, sanitized, 
                            f"Expected '{expected_pattern}' to be in sanitized message: '{sanitized}'")

    def test_sanitize_error_message_edge_cases(self):
        """Test edge cases and corner scenarios for error sanitization."""
        for original, expected in self.edge_cases:
            with self.subTest(original=original):
                if original is None:
                    sanitized = sanitize_error_message(original)
                    self.assertIsNone(sanitized, "None input should return None")
                else:
                    sanitized = sanitize_error_message(original)
                    if "***MASKED***" in expected:
                        # For masked cases, check if masking occurred
                        self.assertIn("***MASKED***", sanitized, 
                                    f"Expected masking in: '{sanitized}'")
                    else:
                        # For non-masked cases, should remain unchanged
                        self.assertEqual(sanitized, expected, 
                                       f"Expected no change but got: '{sanitized}'")

    def test_safe_error_display_with_exception(self):
        """Test that exceptions containing sensitive data are properly sanitized."""
        # Create an exception with sensitive data
        api_key = "sk-1234567890abcdef1234567890abcdef"
        password = "super_secret_password_123"
        
        try:
            raise ValueError(f"Authentication failed with api_key={api_key} and password={password}")
        except Exception as e:
            sanitized = safe_error_display(e)
            
            # Verify sensitive data is masked
            self.assertNotIn("sk-1234567890abcdef1234567890abcdef", sanitized,
                           "API key should be masked in exception display")
            self.assertNotIn("super_secret_password_123", sanitized,
                           "Password should be masked in exception display")
            
            # Verify masking occurred
            self.assertIn("***MASKED***", sanitized,
                        "Exception should contain masked placeholders")

    def test_create_safe_error_handler_production_mode(self):
        """Test safe error handler in production mode (minimal info)."""
        handler = create_safe_error_handler(show_full_traceback=False)
        
        api_key = "sk-test123456789"
        try:
            raise RuntimeError(f"Failed to connect with api_key={api_key}")
        except Exception as e:
            result = handler(e, "Database connection")
            
            # Should contain context
            self.assertIn("Database connection", result)
            
            # Should mask sensitive data
            self.assertNotIn("sk-test123456789", result)
            self.assertIn("***MASKED***", result)

    def test_create_safe_error_handler_development_mode(self):
        """Test safe error handler in development mode (more details but still sanitized)."""
        handler = create_safe_error_handler(show_full_traceback=True)
        
        password = "secret_password_123"
        try:
            raise ValueError(f"Invalid password={password}")
        except Exception as e:
            result = handler(e)
            
            # Should mask sensitive data even in development mode
            self.assertNotIn("secret_password_123", result)
            self.assertIn("***MASKED***", result)

    def test_multiple_secrets_in_single_message(self):
        """Test that multiple secrets in a single message are all masked."""
        message = "Error: api_key=sk-123456789 password=secret123 token=bearer_token_456"
        sanitized = sanitize_error_message(message)
        
        # All sensitive data should be masked
        self.assertNotIn("sk-123456789", sanitized)
        self.assertNotIn("secret123", sanitized) 
        self.assertNotIn("bearer_token_456", sanitized)
        
        # Should contain masked placeholders
        self.assertIn("***MASKED***", sanitized)

    def test_jwt_token_sanitization(self):
        """Test specific JWT token sanitization."""
        jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        message = f"Authentication failed with token: {jwt}"
        
        sanitized = sanitize_error_message(message)
        
        # JWT should be masked
        self.assertNotIn(jwt, sanitized)
        self.assertIn("***JWT_MASKED***", sanitized)

    def test_url_credentials_sanitization(self):
        """Test that URLs with embedded credentials are sanitized."""
        urls = [
            "https://user:password@kafka.example.com:9092",
            "http://admin:secret@localhost:8080/api",
            "ftp://testuser:testpass@ftp.example.com/files"
        ]
        
        for url in urls:
            with self.subTest(url=url):
                message = f"Connection failed to {url}"
                sanitized = sanitize_error_message(message)
                
                # Credentials should be masked
                self.assertIn("***USER***:***PASS***", sanitized)
                # Original credentials should not be present
                self.assertNotIn("user:password", sanitized)
                self.assertNotIn("admin:secret", sanitized) 
                self.assertNotIn("testuser:testpass", sanitized)


class TestSecureLogging(BaseUT):
    """Test suite for secure logging functionality."""

    def setUp(self):
        """Set up test environment before each test."""
        super().setUp()

    def test_secure_formatter_masks_sensitive_data(self):
        """Test that SecureFormatter automatically masks sensitive data in log messages."""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as temp_log:
            temp_log_path = temp_log.name

        try:
            # Create test logger with SecureFormatter
            test_logger = logging.getLogger('test_secure_formatter')
            test_logger.setLevel(logging.DEBUG)
            
            # Remove any existing handlers
            for handler in test_logger.handlers[:]:
                test_logger.removeHandler(handler)
            
            # Add secure file handler
            file_handler = logging.FileHandler(temp_log_path)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(SecureFormatter('%(levelname)s - %(message)s'))
            test_logger.addHandler(file_handler)
            
            # Log messages with sensitive data
            test_logger.info("Starting authentication with api_key=sk-1234567890abcdef")
            test_logger.error("Login failed with password=super_secret_password")
            test_logger.warning("Token expired: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c")
            
            # Flush the handler
            file_handler.flush()
            
            # Read and verify log contents
            with open(temp_log_path, 'r') as f:
                log_contents = f.read()
            
            # Sensitive data should be masked
            self.assertNotIn("sk-1234567890abcdef", log_contents)
            self.assertNotIn("super_secret_password", log_contents)
            self.assertNotIn("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c", log_contents)
            
            # Masked placeholders should be present
            self.assertIn("***MASKED***", log_contents)
            self.assertIn("***JWT_MASKED***", log_contents)
            
        finally:
            # Clean up
            if os.path.exists(temp_log_path):
                os.unlink(temp_log_path)

    def test_app_config_logger_uses_secure_formatter(self):
        """Test that the main app_config logger uses SecureFormatter."""
        # Check if any handler uses SecureFormatter
        has_secure_formatter = any(
            isinstance(handler.formatter, SecureFormatter) 
            for handler in logger.handlers
        )
        
        self.assertTrue(has_secure_formatter, 
                       "Main logger should use SecureFormatter to prevent secret exposure")

    def test_secure_formatter_preserves_log_structure(self):
        """Test that SecureFormatter preserves log structure while sanitizing content."""
        formatter = SecureFormatter('%(levelname)s - %(name)s - %(message)s')
        
        # Create a log record with sensitive data
        record = logging.LogRecord(
            name='test_logger',
            level=logging.INFO,
            pathname='test.py',
            lineno=42,
            msg='Authentication with api_key=%s',
            args=('sk-test123456789',),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        
        # Should preserve structure
        self.assertIn('INFO', formatted)
        self.assertIn('test_logger', formatted)
        
        # Should sanitize sensitive data
        self.assertNotIn('sk-test123456789', formatted)
        self.assertIn('***MASKED***', formatted)


class TestTyperSecurityConfiguration(BaseUT):
    """Test suite for Typer CLI security configuration."""

    def test_cli_files_have_secure_configuration(self):
        """Test that all CLI files are configured to hide local variables in exceptions."""
        # Get the source directory path - we're in shift_left/tests/ut/core, need to go to shift_left/src/shift_left
        # Go up 4 levels: test_error_security.py -> core -> ut -> tests -> shift_left/
        shift_left_root = pathlib.Path(__file__).parent.parent.parent.parent
        src_dir = shift_left_root / "src" / "shift_left"
        
        cli_files = [
            src_dir / "cli.py",
            src_dir / "cli_commands" / "pipeline.py",
            src_dir / "cli_commands" / "project.py", 
            src_dir / "cli_commands" / "table.py"
        ]
        
        for cli_file in cli_files:
            with self.subTest(file=str(cli_file)):
                if cli_file.exists():
                    with open(cli_file, 'r') as f:
                        content = f.read()
                    
                    # Should have secure configuration
                    self.assertIn("pretty_exceptions_show_locals=False", content,
                                f"{cli_file.name} should have secure Typer configuration")
                else:
                    self.fail(f"CLI file not found: {cli_file}")

    def test_error_sanitization_used_in_cli_commands(self):
        """Test that CLI command files use error sanitization functions."""
        shift_left_root = pathlib.Path(__file__).parent.parent.parent.parent  # Gets us to shift_left/
        pipeline_file = shift_left_root / "src" / "shift_left" / "cli_commands" / "pipeline.py"
        
        if pipeline_file.exists():
            with open(pipeline_file, 'r') as f:
                content = f.read()
            
            # Should import and use safe error display
            self.assertIn("safe_error_display", content,
                        "Pipeline CLI should use safe_error_display function")
        else:
            self.skipTest("Pipeline CLI file not found")


if __name__ == '__main__':
    unittest.main()