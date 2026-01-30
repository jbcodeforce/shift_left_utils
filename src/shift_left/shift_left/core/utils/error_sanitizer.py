"""
Copyright 2024-2025 Confluent, Inc.

Error sanitization utilities to prevent sensitive information exposure in logs and error messages.
"""
import re


def sanitize_error_message(error_msg: str) -> str:
    """
    Sanitize error messages by masking sensitive information patterns.
    
    Args:
        error_msg: The error message to sanitize
        
    Returns:
        Sanitized error message with sensitive data masked
    """
    if not error_msg:
        return error_msg
    
    # Define patterns for common sensitive data
    patterns = [
        # JWT tokens (check first before other token patterns)
        (r'(eyJ[a-zA-Z0-9_-]+\.eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', r'***JWT_MASKED***'),
        
        # API keys (various formats)
        (r'api[_-]?key["\']?\s*[=:]\s*["\']?([a-zA-Z0-9_-]{8,})["\']?', r'api_key=***MASKED***'),
        (r'key["\']?\s*[=:]\s*["\']?([a-zA-Z0-9_-]{15,})["\']?', r'key=***MASKED***'),
        
        # API secrets (various formats including underscore)
        (r'api[_-]?secret["\']?\s*[=:]\s*["\']?([a-zA-Z0-9_-]{8,})["\']?', r'api_secret=***MASKED***'),
        (r'secret["\']?\s*[=:]\s*["\']?([a-zA-Z0-9_-]{8,})["\']?', r'secret=***MASKED***'),
        
        # Passwords
        (r'password["\']?\s*[=:]\s*["\']?([^\s"\']{4,})["\']?', r'password=***MASKED***'),
        (r'passwd["\']?\s*[=:]\s*["\']?([^\s"\']{4,})["\']?', r'passwd=***MASKED***'),
        
        # Tokens (bearer, general tokens)
        (r'bearer\s*[=]?\s*([a-zA-Z0-9_.-]{8,})', r'bearer ***MASKED***', re.IGNORECASE),
        (r'token["\']?\s*[=:]\s*["\']?([a-zA-Z0-9_.-]{8,})["\']?', r'token=***MASKED***'),
        
        # Authorization headers
        (r'authorization["\']?\s*[=:]\s*["\']?(bearer\s+[a-zA-Z0-9_.-]+)["\']?', r'authorization=***MASKED***', re.IGNORECASE),
        (r'authorization["\']?\s*[=:]\s*["\']?(basic\s+[a-zA-Z0-9+/=]+)["\']?', r'authorization=***MASKED***', re.IGNORECASE),
        
        # SASL credentials
        (r'sasl[._]username["\']?\s*[=:]\s*["\']?([^\s"\']+)["\']?', r'sasl.username=***MASKED***'),
        (r'sasl[._]password["\']?\s*[=:]\s*["\']?([^\s"\']+)["\']?', r'sasl.password=***MASKED***'),
        
        # URLs with credentials
        (r'://([^:/@\s]+):([^@\s]+)@', r'://***USER***:***PASS***@'),
    ]
    
    sanitized = error_msg
    for pattern_info in patterns:
        if len(pattern_info) == 3:
            pattern, replacement, flags = pattern_info
            sanitized = re.sub(pattern, replacement, sanitized, flags=flags)
        else:
            pattern, replacement = pattern_info
            sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)
    
    return sanitized


def safe_error_display(error: Exception) -> str:
    """
    Safely display an error message with sensitive information masked.
    
    Args:
        error: The exception to display
        
    Returns:
        Sanitized error message safe for logging/display
    """
    error_msg = str(error)
    return sanitize_error_message(error_msg)


def create_safe_error_handler(show_full_traceback: bool = False):
    """
    Create a safe error handler function that masks sensitive information.
    
    Args:
        show_full_traceback: Whether to show full traceback (for development)
        
    Returns:
        Error handler function
    """
    def handle_error(error: Exception, context: str = "") -> str:
        """Handle an error safely by masking sensitive information."""
        if show_full_traceback:
            # In development, show more details but still sanitize
            import traceback
            full_trace = traceback.format_exc()
            sanitized_trace = sanitize_error_message(full_trace)
            return f"{context}: {sanitized_trace}" if context else sanitized_trace
        else:
            # In production, show minimal information
            sanitized_msg = safe_error_display(error)
            return f"{context}: {sanitized_msg}" if context else sanitized_msg
    
    return handle_error
