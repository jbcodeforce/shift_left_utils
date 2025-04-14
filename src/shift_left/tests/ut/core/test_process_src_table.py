"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
from unittest.mock import patch
import os, pathlib
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
        data_dir = pathlib.Path(__file__).parent / "../data" 
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["CONFIG_FILE"] = str(data_dir / "config.yaml")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        mock_translate.return_value = (DDL, DML)
        process_one_file("a_table",
            str(data_dir / "dbt-project/facts/a.sql"),   
            str(data_dir / "flink-project/staging"),   
            str(data_dir / "dbt-project"),
            False)
        assert os.path.exists( str(data_dir / "flink-project/staging/facts/a"))

def test_process_one_file_recurring(mock_llm_result):
    with patch("shift_left.core.process_src_tables.translate_to_flink_sqls", return_value=mock_llm_result) as mock_translate:
        data_dir = pathlib.Path(__file__).parent / "../data" 
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["CONFIG_FILE"] = str(data_dir / "config.yaml")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        mock_translate.return_value = (DDL, DML)
        process_one_file("a_table",
            str(data_dir / "dbt-project/facts/a.sql"),   
            str(data_dir / "flink-project/staging"),   
            str(data_dir / "dbt-project"),
            True)
        assert os.path.exists( str(data_dir / "flink-project/staging/sources/s1"))
        assert os.path.exists( str(data_dir / "flink-project/staging/sources/s2"))
        assert os.path.exists( str(data_dir / "flink-project/staging/intermediates/it1"))


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