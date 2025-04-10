
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../data/flink-project/pipelines")
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.statement_mgr import (
    _get_or_build_sql_content_transformer,
    get_statement_list,
    deploy_flink_statement,
    delete_statement_if_exists
)
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import get_ddl_dml_names_from_pipe_def

class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
    
    # ---- Statement related  apis tests ------------------- 

 
        
    def test_search_non_existant_statement(self):
        statement_dict = get_statement_list()
        assert statement_dict == None
        assert not statement_dict["dummy"]

    def test_get_statement_list(self):
        l = get_statement_list()
        assert l


    def test_execute_show_create_table_then_delete_statement(self):
        config= get_config()
        sql_path = os.getenv("PIPELINES") + "/intermediates/p1/int_table_1/tests/show_create_table.sql"
        statement = deploy_flink_statement(sql_path, None, "show-table", config)
        assert statement
        print(statement)
        statement_dict = get_statement_list()
        assert statement_dict
        print(f"\n -- {statement_dict["show-table"]}")
        response = delete_statement_if_exists("show_table")
        assert response

    def test_get_sql_content_transformer(self):
        sql_in="""
        CREATE TABLE table_1 (
        ) WITH (
            'key.avro-registry.schema-context' = '.flink-dev',
            'value.avro-registry.schema-context' = '.flink-dev',
            'changelog.mode' = 'upsert',
            'kafka.retention.time' = '0',
            'scan.bounded.mode' = 'unbounded',
            'scan.startup.mode' = 'earliest-offset',
            'value.fields-include' = 'all',
            'key.format' = 'avro-registry',
            'value.format' = 'avro-registry'
        )
        """
        transformer = _get_or_build_sql_content_transformer()
        assert transformer
        _, sql_out=transformer.update_sql_content(sql_in)
        assert "'key.avro-registry.schema-context' = '.flink-stage'" in sql_out
        print(sql_out)


if __name__ == '__main__':
    unittest.main()