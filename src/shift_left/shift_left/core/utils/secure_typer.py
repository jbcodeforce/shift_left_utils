"""
Copyright 2024-2025 Confluent, Inc.

Secure Typer wrapper that provides debugging capabilities while sanitizing sensitive information.
"""
import sys
import traceback
import typer
from .error_sanitizer import sanitize_error_message


class SecureTyperApp(typer.Typer):
    """
    A Typer app that shows local variables for debugging but sanitizes sensitive information.

    This preserves full debugging capabilities while ensuring that API keys, passwords,
    tokens, and other sensitive data are automatically masked before display.
    """

    def __init__(self, *args, **kwargs):
        # Force show_locals to True for debugging but we'll sanitize the output
        #kwargs['pretty_exceptions_show_locals'] = True
        super().__init__(*args, **kwargs)

        # Install our custom exception handler
        self._install_secure_exception_handler()

    def _install_secure_exception_handler(self):
        """Install a secure exception handler that sanitizes traceback output."""

        # Store the original exception handler
        original_excepthook = sys.excepthook

        def secure_excepthook(exc_type, exc_value, exc_traceback):
            """Secure exception handler that sanitizes sensitive information."""

            if issubclass(exc_type, KeyboardInterrupt):
                # Don't handle keyboard interrupts
                original_excepthook(exc_type, exc_value, exc_traceback)
                return

            # Format the full traceback with locals
            tb_lines = []

            # Add the traceback header
            tb_lines.append("Traceback (most recent call last):")

            # Process each frame in the traceback
            tb = exc_traceback
            while tb is not None:
                frame = tb.tb_frame
                filename = frame.f_code.co_filename
                line_number = tb.tb_lineno
                function_name = frame.f_code.co_name

                # Add frame information
                tb_lines.append(f'  File "{filename}", line {line_number}, in {function_name}')

                # Get the source line if possible
                try:
                    import linecache
                    line = linecache.getline(filename, line_number, frame.f_globals)
                    if line:
                        tb_lines.append(f'    {line.strip()}')
                except:
                    pass

                # Add local variables (sanitized)
                if frame.f_locals:
                    tb_lines.append("")
                    tb_lines.append("  Local variables:")
                    for var_name, var_value in frame.f_locals.items():
                        if not var_name.startswith('__'):
                            # Sanitize the variable value
                            sanitized_value = sanitize_error_message(repr(var_value))
                            tb_lines.append(f"    {var_name} = {sanitized_value}")

                tb_lines.append("")
                tb = tb.tb_next

            # Add the exception message (sanitized)
            exception_msg = sanitize_error_message(str(exc_value))
            tb_lines.append(f"{exc_type.__name__}: {exception_msg}")

            # Print the sanitized traceback
            sanitized_traceback = "\n".join(tb_lines)
            final_output = sanitize_error_message(sanitized_traceback)

            print(final_output, file=sys.stderr)

        # Install our secure exception handler
        sys.excepthook = secure_excepthook

    def __call__(self, *args, **kwargs):
        """Override the call method to ensure our exception handler is active."""
        try:
            return super().__call__(*args, **kwargs)
        except Exception as e:
            # This will be handled by our secure exception handler
            raise


def create_secure_typer_app(*args, **kwargs) -> SecureTyperApp:
    """
    Create a Typer app that shows debugging information but sanitizes sensitive data.

    Args:
        *args: Arguments to pass to Typer
        **kwargs: Keyword arguments to pass to Typer

    Returns:
        SecureTyperApp instance configured for secure debugging
    """
    return SecureTyperApp(*args, **kwargs)


def install_secure_exception_handler():
    """
    Install a global secure exception handler.

    This can be called independently to secure any Python application,
    not just Typer apps.
    """
    original_excepthook = sys.excepthook

    def secure_global_excepthook(exc_type, exc_value, exc_traceback):
        """Global secure exception handler."""

        if issubclass(exc_type, KeyboardInterrupt):
            original_excepthook(exc_type, exc_value, exc_traceback)
            return

        # Create a sanitized traceback
        try:
            # Get the formatted traceback
            tb_strings = traceback.format_exception(exc_type, exc_value, exc_traceback)

            # Sanitize each line
            sanitized_lines = []
            for line in tb_strings:
                sanitized_line = sanitize_error_message(line)
                sanitized_lines.append(sanitized_line)

            # Print the sanitized traceback
            sanitized_output = "".join(sanitized_lines)
            print(sanitized_output, file=sys.stderr)

        except Exception:
            # Fallback: sanitize just the exception message
            fallback_msg = sanitize_error_message(f"Error: {exc_value}")
            print(fallback_msg, file=sys.stderr)

    sys.excepthook = secure_global_excepthook
