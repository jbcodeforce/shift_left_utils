"""
Copyright 2024-2025 Confluent, Inc.

Secure Typer wrapper that provides debugging capabilities while sanitizing sensitive information.
"""
import atexit
import sys
import traceback
import typer
from .error_sanitizer import sanitize_error_message

_original_termios = None
_exception_handler_installed = False
_terminal_restore_registered = False


def _save_terminal_attrs() -> None:
    """Capture terminal settings at startup so they can be restored on exit."""
    global _original_termios
    if _original_termios is not None:
        return
    if not sys.stdin.isatty():
        return
    try:
        import termios

        _original_termios = termios.tcgetattr(sys.stdin)
    except Exception:
        pass


def restore_terminal() -> None:
    """Reset terminal cursor, alt-screen, and termios after Rich/Click usage."""
    try:
        from rich.console import Console

        console = Console()
        console.show_cursor(True)
        if console.is_alt_screen:
            console.set_alt_screen(False)
    except Exception:
        pass

    global _original_termios
    if _original_termios is not None and sys.stdin.isatty():
        try:
            import termios

            termios.tcsetattr(sys.stdin, termios.TCSADRAIN, _original_termios)
        except Exception:
            pass


def _ensure_terminal_restore_registered() -> None:
    """Register terminal restore once so normal CLI exits return the shell cleanly."""
    global _terminal_restore_registered
    if _terminal_restore_registered:
        return
    _terminal_restore_registered = True
    _save_terminal_attrs()
    atexit.register(restore_terminal)


class SecureTyperApp(typer.Typer):
    """
    A Typer app that shows local variables for debugging but sanitizes sensitive information.

    This preserves full debugging capabilities while ensuring that API keys, passwords,
    tokens, and other sensitive data are automatically masked before display.
    """

    def __init__(self, *args, **kwargs):
        _ensure_terminal_restore_registered()
        super().__init__(*args, **kwargs)


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


def install_secure_exception_handler() -> None:
    """
    Install a global secure exception handler.

    This can be called independently to secure any Python application,
    not just Typer apps. Safe to call more than once.
    """
    global _exception_handler_installed
    _ensure_terminal_restore_registered()
    if _exception_handler_installed:
        return
    _exception_handler_installed = True

    def secure_global_excepthook(exc_type, exc_value, exc_traceback):
        """Global secure exception handler."""
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            restore_terminal()
            return

        try:
            tb_strings = traceback.format_exception(exc_type, exc_value, exc_traceback)
            sanitized_lines = []
            for line in tb_strings:
                sanitized_lines.append(sanitize_error_message(line))
            print("".join(sanitized_lines), file=sys.stderr)
        except Exception:
            fallback_msg = sanitize_error_message(f"Error: {exc_value}")
            print(fallback_msg, file=sys.stderr)
        finally:
            restore_terminal()

    sys.excepthook = secure_global_excepthook
