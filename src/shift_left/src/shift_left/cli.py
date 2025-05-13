"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
from shift_left.cli_commands import project, table, pipeline





app = typer.Typer(no_args_is_help=True)
app.add_typer(project.app, name="project")
app.add_typer(table.app, name="table")
app.add_typer(pipeline.app, name="pipeline")

#__version__ = toml.load(open("pyproject.toml"))["project"]["version"]
"""
@app.callback()
def version_callback(version: bool = typer.Option(False, "--version", help="Show version and exit", is_eager=True)):
    if version:
        typer.echo(f"shift-left version: {__version__}")
        raise typer.Exit()
"""

if __name__ == "__main__":
    """"
    Core CLI for the managing Flink project, with a focus on migrating from SQL batch processing.
    """
    app()