"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
import unittest
import shutil
from unittest.mock import patch
import os, pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent.parent /  "data/flink-project/pipelines")

# need to be before the import of migrate_one_file
data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
os.environ["TOPIC_LIST_FILE"] = str(data_dir / "flink-project/src_topic_list.txt")
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
        
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")

    def _get_env_var(self, var_name: str) -> str:
        """Get environment variable with proper null checking."""
        value = os.getenv(var_name)
        if value is None:
            raise ValueError(f"{var_name} environment variable is not set")
        return value

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")
    def test_process_one_file(self,TranslatorToFlinkSqlAgent):

        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        mock_translator_agent.translate_to_flink_sqls.return_value = ([DML], [DDL])
     
        staging = self._get_env_var("STAGING")
        src_folder = self._get_env_var("SRC_FOLDER")
        
        if os.path.exists(staging):
            shutil.rmtree(staging)        
        migrate_one_file("fct_users",
            src_folder + "/facts/p5/fct_users.sql",   
            staging,   
            src_folder,
            process_parents=False,
            source_type="spark",
            validate=False)
        assert os.path.exists(staging + "/facts/p5/fct_users")
        assert os.path.exists(staging + "/facts/p5/fct_users/sql-scripts/dml.p5_fct_users.sql")
        assert os.path.exists(staging + "/facts/p5/fct_users/sql-scripts/ddl.p5_fct_users.sql")
        

    @patch("shift_left.core.process_src_tables.get_or_build_sql_translator_agent")  
    def test_process_one_file_recurring(self, TranslatorToFlinkSqlAgent):

        mock_translator_agent = TranslatorToFlinkSqlAgent.return_value
        mock_translator_agent.translate_to_flink_sqls.return_value = ([DML], [DDL])
        
        staging = self._get_env_var("STAGING")
        src_folder = self._get_env_var("SRC_FOLDER")
        
        shutil.rmtree(staging)
        migrate_one_file("fct_users",
            src_folder + "/facts/p5/fct_users.sql",   
            staging,   
            src_folder,
            process_parents=True,
            source_type="spark",
            validate=False)
        assert os.path.exists(staging + "/sources/p5/raw_active_users")
        assert os.path.exists(staging + "/sources/p5/raw_active_users/sql-scripts/dml.src_p5_raw_active_users.sql")
        assert os.path.exists(staging + "/sources/p5/raw_active_users/sql-scripts/ddl.src_p5_raw_active_users.sql")
        with open(staging + "/dimensions/p5/dim_user_groups/sql-scripts/dml.p5_dim_user_groups.sql", "r") as f:
            assert f.read() == DML
        with open(staging + "/dimensions/p5/dim_user_groups/sql-scripts/ddl.p5_dim_user_groups.sql", "r") as f:
            assert f.read() == DDL

            
if __name__ == "__main__":
    unittest.main()