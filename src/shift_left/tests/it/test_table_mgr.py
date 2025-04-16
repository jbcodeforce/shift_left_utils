"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
from typing import Tuple
import os
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
      
import shift_left.core.table_mgr as tm
from shift_left.core.utils.table_worker import TableWorker
from shift_left.core.utils.file_search import list_src_sql_files

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent / "./data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        #os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))


    def test_drop_table(seld):
        pass

    def test_get_table_structure(self):
        pass
       


  
if __name__ == '__main__':
    unittest.main()