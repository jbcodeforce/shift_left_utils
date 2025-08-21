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
__version__ = "0.1.33"

@app.command()
def version():
    """Display the current version of shift-left CLI."""
    typer.echo(f"shift-left CLI version: {__version__}")


if __name__ == "__main__":
    """"
    Core CLI for the managing Flink project, with a focus on migrating from SQL batch processing.
    """
    app()