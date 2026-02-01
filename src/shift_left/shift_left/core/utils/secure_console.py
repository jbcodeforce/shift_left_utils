"""
Copyright 2024-2025 Confluent, Inc.

Secure Rich console that sanitizes sensitive information in tracebacks while preserving debugging capabilities.
"""
import sys
import traceback
from typing import Optional, TextIO, Any
from rich.console import Console
from rich.traceback import Traceback
from rich import pretty
from .error_sanitizer import sanitize_error_message


class SecureConsole(Console):
    """
    A Rich Console that automatically sanitizes sensitive information in tracebacks.
    
    This console preserves all debugging information (including local variables) 
    but ensures that API keys, passwords, tokens, and other sensitive data
    are automatically masked before display.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def print_exception(
        self,
        *,
        show_locals: bool = False,
        width: Optional[int] = 100,
        extra_lines: int = 3,
        theme: Optional[str] = None,
        word_wrap: bool = False,
        suppress: tuple = (),
        max_frames: int = 100,
    ) -> None:
        """
        Print exception with automatic sanitization of sensitive information.
        
        This method captures the full traceback (including locals if requested),
        sanitizes any sensitive data, and then displays the clean traceback.
        """
        try:
            # Get the current exception info
            exc_type, exc_value, exc_traceback = sys.exc_info()
            
            if exc_type is None:
                return
            
            # Format the full traceback including locals if requested
            if show_locals:
                tb_lines = traceback.format_exception(
                    exc_type, exc_value, exc_traceback, limit=max_frames
                )
                
                # Also get local variables for each frame
                tb = exc_traceback
                frame_locals = []
                while tb is not None:
                    frame_locals.append(tb.tb_frame.f_locals.copy())
                    tb = tb.tb_next
                
                # Create detailed traceback with locals
                detailed_tb = []
                tb = exc_traceback
                frame_idx = 0
                
                while tb is not None and frame_idx < max_frames:
                    frame = tb.tb_frame
                    filename = frame.f_code.co_filename
                    line_number = tb.tb_lineno
                    function_name = frame.f_code.co_name
                    
                    # Add frame info
                    detailed_tb.append(f'  File "{filename}", line {line_number}, in {function_name}')
                    
                    # Add local variables (sanitized)
                    if frame_locals[frame_idx]:
                        detailed_tb.append("    Local variables:")
                        for var_name, var_value in frame_locals[frame_idx].items():
                            if not var_name.startswith('__'):
                                sanitized_value = sanitize_error_message(str(var_value))
                                detailed_tb.append(f"      {var_name} = {sanitized_value}")
                    
                    tb = tb.tb_next
                    frame_idx += 1
                
                # Add the exception message (sanitized)
                exception_msg = sanitize_error_message(str(exc_value))
                detailed_tb.append(f"{exc_type.__name__}: {exception_msg}")
                
                # Print the sanitized traceback
                self.print(f"[red]Traceback (most recent call last):[/red]")
                for line in detailed_tb:
                    sanitized_line = sanitize_error_message(line)
                    self.print(sanitized_line)
                    
            else:
                # For simple tracebacks without locals, use Rich's standard formatting
                # but sanitize the content
                traceback_obj = Traceback.from_exception(
                    exc_type,
                    exc_value,
                    exc_traceback,
                    width=width,
                    extra_lines=extra_lines,
                    theme=theme,
                    word_wrap=word_wrap,
                    show_locals=False,
                    suppress=suppress,
                    max_frames=max_frames,
                )
                
                # Capture the Rich rendering to a string
                with self.capture() as capture:
                    self.print(traceback_obj)
                
                # Sanitize and re-print
                sanitized_output = sanitize_error_message(capture.get())
                self.print(sanitized_output)
                
        except Exception as e:
            # Fallback: if our secure printing fails, use basic sanitized output
            fallback_msg = sanitize_error_message(f"Error occurred: {e}")
            self.print(f"[red]{fallback_msg}[/red]")

    def _render_exception_with_locals(self, exc_type, exc_value, exc_traceback):
        """Helper method to render exception with sanitized locals."""
        lines = []
        lines.append("Traceback (most recent call last):")
        
        tb = exc_traceback
        while tb is not None:
            frame = tb.tb_frame
            filename = frame.f_code.co_filename
            line_number = tb.tb_lineno
            function_name = frame.f_code.co_name
            
            lines.append(f'  File "{filename}", line {line_number}, in {function_name}')
            
            # Add local variables (sanitized)
            if frame.f_locals:
                lines.append("    Local variables:")
                for var_name, var_value in frame.f_locals.items():
                    if not var_name.startswith('__'):
                        sanitized_value = sanitize_error_message(str(var_value))
                        lines.append(f"      {var_name} = {sanitized_value}")
            
            tb = tb.tb_next
        
        # Add exception message (sanitized)
        exception_msg = sanitize_error_message(str(exc_value))
        lines.append(f"{exc_type.__name__}: {exception_msg}")
        
        return "\n".join(lines)


# Create a global secure console instance
secure_console = SecureConsole(stderr=True, force_terminal=True)


def install_secure_exception_handler():
    """
    Install a global exception handler that uses secure console for all unhandled exceptions.
    """
    def secure_excepthook(exc_type, exc_value, exc_traceback):
        """Global exception handler that sanitizes sensitive information."""
        if issubclass(exc_type, KeyboardInterrupt):
            # Don't handle keyboard interrupts
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # Use our secure console to print the exception
        secure_console.print_exception(show_locals=True)
    
    # Install the secure exception handler
    sys.excepthook = secure_excepthook


def create_secure_typer_app(*args, **kwargs):
    """
    Create a Typer app configured to use secure exception handling.
    
    This preserves the show_locals functionality while sanitizing sensitive data.
    """
    import typer
    from rich.console import Console
    from rich.traceback import install as install_rich_traceback, Traceback
    from rich import pretty
    
    # Patch Rich's repr function to sanitize sensitive data
    original_pretty_repr = pretty.pretty_repr
    
    def secure_pretty_repr(obj: Any, **kwargs) -> str:
        """Secure version of pretty_repr that sanitizes sensitive data."""
        try:
            result = original_pretty_repr(obj, **kwargs)
            return sanitize_error_message(result)
        except Exception:
            # Fallback to sanitized string representation
            return sanitize_error_message(str(obj))
    
    # Monkey patch the pretty repr function
    pretty.pretty_repr = secure_pretty_repr
    
    # Also patch Traceback's _render_locals method to sanitize local variables
    original_render_locals = Traceback._render_locals
    
    def secure_render_locals(self, frame, *args, **kwargs):
        """Secure version of _render_locals that sanitizes variable values."""
        # Get the original rendered locals
        rendered = original_render_locals(self, frame, *args, **kwargs)
        
        # Sanitize each line of the rendered locals
        if rendered:
            from rich.text import Text
            if isinstance(rendered, Text):
                # Sanitize the text content
                sanitized_content = sanitize_error_message(str(rendered))
                return Text(sanitized_content)
            else:
                # Handle other render types
                return sanitize_error_message(str(rendered))
        
        return rendered
    
    # Apply the patch
    Traceback._render_locals = secure_render_locals
    
    # Override the __repr__ method for strings to sanitize sensitive content
    original_str_repr = str.__repr__
    
    def secure_str_repr(self):
        """Secure string repr that sanitizes sensitive content."""
        original = original_str_repr(self)
        return repr(sanitize_error_message(self))
    
    # This is a bit aggressive, but let's try it
    # str.__repr__ = secure_str_repr  # Commented out as it might be too broad
    
    # Install Rich traceback with our secure console
    install_rich_traceback(
        console=secure_console,
        show_locals=True,
        suppress=[typer]
    )
    
    # Create the app with locals enabled
    app = typer.Typer(*args, pretty_exceptions_show_locals=True, **kwargs)
    
    return app
