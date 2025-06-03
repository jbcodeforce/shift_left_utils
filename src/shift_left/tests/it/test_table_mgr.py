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
from shift_left.core.utils.app_config import get_config

class TestTableManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent / "./data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))


    def test_explain_table(seld):
        config = get_config()
        compute_pool_id = config["flink"]["compute_pool_id"]
        report = tm.explain_table("p1_fct_order", compute_pool_id=compute_pool_id, persist_report=False)
        assert report
        assert report["table_name"] == "p1_fct_order"
        assert report["trace"]

    def test_get_table_structure(self):
        pass
       


  
if __name__ == '__main__':
    unittest.main()