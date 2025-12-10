"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil

import pathlib
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone, timedelta
import subprocess
TEST_PIPELINES_DIR = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR

import shift_left.core.project_manager as pm
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm
from shift_left.core.project_manager import ModifiedFileInfo

class TestProjectManager(unittest.TestCase):
    data_dir = ""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory
        tm.get_or_create_inventory(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES",TEST_PIPELINES_DIR))

    def test_update_tables_version(self):
        """Test update_tables_version function"""
        modified_file = TEST_PIPELINES_DIR + "/intermediates/p2/z/sql-scripts/dml.z.sql"
        z_info = ModifiedFileInfo(table_name="z", file_modified_url=modified_file,
                                same_sql_content=False,
                                running=False,
                                new_table_name="z_v2")
        to_process_tables = [z_info]
        default_version = "_v2"
        processed_files = pm.update_tables_version(to_process_tables, default_version)
        assert len(processed_files) >= 1
        for file_info in processed_files:
            print(file_info.model_dump_json(indent=3))


if __name__ == '__main__':
    unittest.main()
