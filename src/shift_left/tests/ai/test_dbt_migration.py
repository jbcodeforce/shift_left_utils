"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from shift_left.core.utils.translator_to_flink_sql import get_or_build_sql_translator_agent
"""
Taking a complex dbt fact SQL statement migrates all the SQL statements to Flink SQL.
"""

class TestDbtMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging") 
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

    def _process_one_dbt_file(self, dbt_file: str):
        src_folder = os.environ["SRC_FOLDER"]
        dbt_src_file = src_folder + "/" + dbt_file
        with open(dbt_src_file, "r") as f:
            dbt_content = f.read()
        translator_agent = get_or_build_sql_translator_agent()
        dml, ddl = translator_agent.translate_to_flink_sqls(dbt_file, dbt_content)

    def setUp(self):
        pass

    def tearDown(self):
        pass