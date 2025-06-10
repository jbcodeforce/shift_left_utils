"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
import unittest
import shutil
from unittest.mock import patch
import os, pathlib
from shift_left.core.process_src_tables import migrate_one_file

DDL="""
    CREATE TABLE IF NOT EXISTS a_table (

  -- put here column definitions
  PRIMARY KEY(default_key) NOT ENFORCED
) DISTRIBUTED BY HASH(default_key) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
   'scan.bounded.mode' = 'unbounded',
   'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
"""

DML="""
INSERT INTO a_table
SELECT 
-- part to select stuff
FROM src_table
WHERE -- where condition or remove it
"""

@pytest.fixture(autouse=True)
def mock_llm_result():
    return (DDL, DML)

class TestProcessSrcTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["TOPIC_LIST_FILE"] = str(data_dir / "flink-project/src_topic_list.txt")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")
    def test_process_one_file(self,TranslatorToFlinkSqlAgent):

        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        mock_translator_agent.translate_to_flink_sqls.return_value = (DML, DDL)
     
        shutil.rmtree(os.getenv("STAGING"))
        migrate_one_file("a_table",
            os.getenv("SRC_FOLDER") + "/facts/p5/a.sql",   
            os.getenv("STAGING"),   
                os.getenv("SRC_FOLDER"),
            False)
        assert os.path.exists( os.getenv("STAGING") + "/facts/p5/")

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")
    def test_process_one_file_recurring(self, TranslatorToFlinkSqlAgent):

        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        mock_translator_agent.translate_to_flink_sqls.return_value = (DML, DDL)
        shutil.rmtree(os.getenv("STAGING"))
        migrate_one_file("a_table",
            os.getenv("SRC_FOLDER") + "/facts/p5/a.sql",   
            os.getenv("STAGING"),   
                os.getenv("SRC_FOLDER"),
            True)
        assert os.path.exists( os.getenv("STAGING") + "/sources/src_s1")
        assert os.path.exists( os.getenv("STAGING") + "/sources/src_s2")
        assert os.path.exists( os.getenv("STAGING") + "/sources/src_s2/sql-scripts/dml.src_s2.sql")
        assert os.path.exists( os.getenv("STAGING") + "/sources/src_s2/sql-scripts/ddl.src_s2.sql")
        assert os.path.exists( os.getenv("STAGING") + "/intermediates/int_p5_a")
        assert os.path.exists( os.getenv("STAGING") + "/intermediates/int_p5_a/sql-scripts/dml.int_p5_a.sql")
        assert os.path.exists( os.getenv("STAGING") + "/intermediates/int_p5_a/sql-scripts/ddl.int_p5_a.sql")
        with open(os.getenv("STAGING") + "/intermediates/int_p5_a/sql-scripts/dml.int_p5_a.sql", "r") as f:
            assert f.read() == DML
        with open(os.getenv("STAGING") + "/intermediates/int_p5_a/sql-scripts/ddl.int_p5_a.sql", "r") as f:
            assert f.read() == DDL

            
if __name__ == "__main__":
    unittest.main()