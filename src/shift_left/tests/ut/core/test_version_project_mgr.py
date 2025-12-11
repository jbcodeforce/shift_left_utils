"""
Copyright 2024-2025 Confluent, Inc.
"""
import json
import unittest
import os
from pathlib import Path
import shutil

import pathlib
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone, timedelta
import subprocess

from shift_left.core.utils.file_search import FlinkTablePipelineDefinition
data_dir = str(Path(__file__).parent.parent.parent / "data")  # Path to the data directory
TEST_PIPELINES_DIR = data_dir + "/tmp"
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR

import shift_left.core.project_manager as pm
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm
from shift_left.core.project_manager import ModifiedFileInfo

class TestVersionManagement(unittest.TestCase):
    data_dir = ""
    DML_QUERY= """
    insert into users (id, name) values (1, 'test');
    """
    DDL_QUERY= """
    create table users (id int, name string);
    """
    INVENTORY_FILE= { "users": {
        "table_name": "users",
        "product_name": "c360",
        "type": "dimension",
        "dml_ref": "dml.users.sql",
        "ddl_ref": "ddl.users.sql",
        "table_folder_name": "tmp"
        },
        "groups": {
            "table_name": "groups",
            "product_name": "c360",
            "type": "dimension",
            "dml_ref": "dml.groups.sql",
            "ddl_ref": "ddl.groups.sql",
            "table_folder_name": "tmp"
        }
    }
    PIPELINE_DEFINITION_FILE= {
   "table_name": "users",
   "product_name": "c360",
   "type": "dimension",
   "dml_ref": TEST_PIPELINES_DIR + "/dml.users.sql",
   "ddl_ref": TEST_PIPELINES_DIR + "/ddl.users.sql",
   "path": "tmp",
   "complexity": {
      "number_of_regular_joins": 0,
      "number_of_left_joins": 0,
      "number_of_right_joins": 0,
      "number_of_inner_joins": 0,
      "number_of_outer_joins": 0,
      "complexity_type": "Simple",
      "state_form": "Stateless"
   },
   "parents": [],
   "children": []
   }


    @patch('shift_left.core.project_manager.read_pipeline_definition_from_file')
    @patch('shift_left.core.project_manager.get_or_build_inventory')
    def test_update_tables_version(self, mock_get_or_build_inventory, mock_read_pipeline_definition_from_file):
        """Test update_tables_version function"""
        modified_file = TEST_PIPELINES_DIR + "/dml.users.sql"
        with open(modified_file, 'w') as f:
            f.write(self.DML_QUERY)
        modified_file = TEST_PIPELINES_DIR + "/ddl.users.sql"
        with open(modified_file, 'w') as f:
            f.write(self.DDL_QUERY)
        def side_effect_get_or_build_inventory(path, path2, recreate=False):
            return self.INVENTORY_FILE
        def side_effect_read_pipeline_definition_from_file(path):
            jsonobj = FlinkTablePipelineDefinition.model_validate_json(json.dumps(self.PIPELINE_DEFINITION_FILE))
            return jsonobj

        mock_get_or_build_inventory.side_effect = side_effect_get_or_build_inventory
        mock_read_pipeline_definition_from_file.side_effect = side_effect_read_pipeline_definition_from_file
        users_info = ModifiedFileInfo(table_name="users", file_modified_url=modified_file,
                                same_sql_content=False,
                                running=False,
                                new_table_name="users_v2")
        to_process_tables = [users_info]
        default_version = "_v2"
        processed_files = pm.update_tables_version(to_process_tables, default_version)
        assert len(processed_files) >= 1
        for file_info in processed_files:
            print(file_info.model_dump_json(indent=3))
        with open(modified_file, 'r') as f:
            sql_content = f.read()
        assert "users_v2" in sql_content

if __name__ == '__main__':
    unittest.main()
