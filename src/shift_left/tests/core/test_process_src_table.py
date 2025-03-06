import pytest
from unittest.mock import patch
import os
from shift_left.core.process_src_tables import process_one_file

DDL="""
    CREATE TABLE IF NOT EXISTS a (

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
INSERT INTO a
SELECT 
-- part to select stuff
FROM src_table
WHERE -- where condition or remove it
"""

@pytest.fixture(autouse=True)
def mock_llm_result():
    return (DDL, DML)



def test_process_one_file(mock_llm_result):
    with patch("shift_left.core.process_src_tables.translate_to_flink_sqls", return_value=mock_llm_result) as mock_translate:
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        os.environ["PIPELINES"] = "./tests/data/flink-project/pipelines"
        os.environ["SRC_PROJECT"] = "./tests/data/src-project"
        os.environ["STAGING"] = "./tests/data/flink-project/staging"
        mock_translate.return_value = (DDL, DML)
        process_one_file("a_table",
            "./tests/data/src-project/facts/a.sql",   
            "./tests/data/flink-project/staging",   
            "./tests/data/src-project",
            False)
        assert os.path.exists("./tests/data/flink-project/staging/facts/a")

def test_process_one_file_recurring(mock_llm_result):
    with patch("shift_left.core.process_src_tables.translate_to_flink_sqls", return_value=mock_llm_result) as mock_translate:
        os.environ["CONFIG_FILE"] = "./tests/config.yaml"
        os.environ["PIPELINES"] = "./tests/data/flink-project/pipelines"
        os.environ["SRC_PROJECT"] = "./tests/data/src-project"
        os.environ["STAGING"] = "./tests/data/flink-project/staging"
        mock_translate.return_value = (DDL, DML)
        process_one_file("a_table",
            "./tests/data/src-project/facts/a.sql",
            "./tests/data/flink-project/staging",
             "./tests/data/src-project",
            True)
        assert os.path.exists("./tests/data/flink-project/staging/sources/s1")
        assert os.path.exists("./tests/data/flink-project/staging/sources/s2")
        assert os.path.exists("./tests/data/flink-project/staging/intermediates/it1")

def _test_migrate_one_file(self):
        print(f"test_migrate_one_file  {os.getcwd}")
        os.environ["CONFIG_FILE"] = "../../config.yaml"
        os.environ["STAGING"] = "../tmp/staging"
        os.environ["PIPELINES"] = "../tmp/pipelines"
        try:
            process_one_file("dim_event_action_item",
            os.path.join(os.getenv("SRC_FOLDER"), "dimensions/aqem/aqem.dim_event_action_item.sql"),
            "../tmp/staging",
             os.getenv("SRC_FOLDER"),
            False)
        except Exception as e:
            print(e)
            self.fail()
            
if __name__ == "__main__":
    test_process_one_file(mock_llm_result)
    test_process_one_file_recurring(mock_llm_result)