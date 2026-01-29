import typer
import importlib.metadata
import subprocess
import shlex
import os
import glob
from typing import Annotated, Optional, List
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.utils.app_config import get_config

from rich import print

from shift_left.core.utils.secure_typer import create_secure_typer_app


app = create_secure_typer_app(pretty_exceptions_show_locals=False)

def _get_file_content(filepath: str) -> str:
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        raise typer.Exit(code=1)

    with open(filepath, 'r') as file:
        return file.read()


def _find_files_by_pattern(folder: str, pattern: str) -> List[str]:
    if not os.path.isdir(folder):
        print(f"Error: Folder not found: {folder}")
        raise typer.Exit(code=1)

    search_pattern = os.path.join(folder, "**", pattern)
    files = glob.glob(search_pattern, recursive=True)

    file_paths = [
        os.path.abspath(f) for f in files
        if os.path.isfile(f)
    ]

    return sorted(file_paths)

def _undeploy_or_deploy_validations(validations_filespaths: list, deploy: bool = True):

    config = get_config()

    for filepath in validations_filespaths:
        file_content = _get_file_content(filepath)
        filename_without_ext = os.path.splitext(os.path.basename(filepath))[0]
        table_name = filename_without_ext.replace("-", "_").lower()
        statement_name = f"{config['kafka']['cluster_type']}-{table_name}"

        if deploy:
            statement_mgr.post_flink_statement(
                    compute_pool_id=config['flink']['compute_pool_id'],
                    statement_name=statement_name,
                    sql_content=file_content
                )
            print(f"Deployed validation from {filepath}")
        else:
            statement_mgr.delete_statement_if_exists(statement_name)
            print(f"Undeployed validation from {filepath}")


@app.command()
def deploy_validations(validations_folder: Annotated[str, typer.Option(help= "Folder containing all validations to be deployed. It will be recursively scanned and all DDL and DML files found in that folder will be deployed.")] = 'validation/continuous'):
    _undeploy_or_deploy_validations(_find_files_by_pattern(validations_folder, 'ddl*.sql'), deploy=True)
    _undeploy_or_deploy_validations(_find_files_by_pattern(validations_folder, 'dml*.sql'), deploy=True)

@app.command()
def undeploy_validations(validations_folder: Annotated[str, typer.Option(help= "Folder containing all validations to be deployed. It will be recursively scanned and all DDL and DML files found in that folder will be undeployed.")] = 'validation/continuous'):
    _undeploy_or_deploy_validations(_find_files_by_pattern(validations_folder, 'ddl*.sql'), deploy=False)
    _undeploy_or_deploy_validations(_find_files_by_pattern(validations_folder, 'dml*.sql'), deploy=False)
