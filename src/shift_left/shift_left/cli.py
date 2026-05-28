"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
from shift_left.cli_commands import project, table, pipeline, rag
from shift_left.core.utils.secure_typer import create_secure_typer_app, install_secure_exception_handler
from shift_left.core.utils.app_config import __version__, format_config_for_debug


# Create secure Typer app that shows locals but sanitizes sensitive data
app = create_secure_typer_app(no_args_is_help=True, pretty_exceptions_show_locals=False)

# Install global secure exception handler for unhandled exceptions
install_secure_exception_handler()



app.add_typer(project.app, name="project")
app.add_typer(table.app, name="table")
app.add_typer(pipeline.app, name="pipeline")
app.add_typer(rag.app, name="rag")

@app.command()
def version(
    show_config: bool = typer.Option(
        False,
        "--config",
        help="Print effective configuration (secrets redacted) after the version line.",
    ),
):
    """Display the current version of shift-left CLI."""
    typer.echo(f"shift-left CLI version: {__version__}")
    if show_config:
        typer.echo(format_config_for_debug())


if __name__ == "__main__":
    """"
    Core CLI for the managing Flink project, with a focus on migrating from SQL batch processing.
    """
    app()
