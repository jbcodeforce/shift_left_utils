import typer
import os
from shift_left.cli_commands import project, table, pipeline
import logging
from logging.handlers import RotatingFileHandler
from shift_left.core.utils.app_config import get_config

log_dir = os.path.join(os.getcwd(), 'logs')
logger = logging.getLogger("shift_left")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "shift_left.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(get_config()["app"]["logging"])
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

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