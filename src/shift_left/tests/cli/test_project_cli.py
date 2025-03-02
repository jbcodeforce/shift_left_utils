import pytest
from unittest.mock import patch
import os
from typer.testing import CliRunner

from shift_left.cli_commands.project import app

runner = CliRunner()

def test_init_project():
    result = runner.invoke(app, ["project_test_via_cli"])
    assert result.exit_code == 0
    assert "Project project_test_via_cli created in ./tmp" in result.stdout
    assert os.path.exists("./tmp/project_test_via_cli")
    assert os.path.exists("./tmp/project_test_via_cli/pipelines")
