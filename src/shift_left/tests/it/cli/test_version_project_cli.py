"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil

from shift_left.core.utils.app_config import shift_left_dir
from shift_left.cli_commands.project import app
from it.BaseIT import IntegrationTestCase

class TestProjectCLI(IntegrationTestCase):


    @classmethod
    def tearDownClass(cls):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_list_modified_files(self):
        runner = CliRunner()
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = runner.invoke(app, [ "update-tables-version", os.environ["HOME"] + "/.shift_left/modified_flink_files.json"])
        print(result.stdout)
        assert result.exit_code == 0




if __name__ == '__main__':
    unittest.main()
