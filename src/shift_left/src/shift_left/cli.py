import typer
import sys
from rich import print
from shift_left.cli_commands import project, table, pipeline
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Core cli for the shift-left project management.
"""
app = typer.Typer(no_args_is_help=True)
app.add_typer(project.app, name="project")
app.add_typer(table.app, name="table")
app.add_typer(pipeline.app, name="pipeline")

if __name__ == "__main__":
    """
    shift-left project management CLI
    """
    app()