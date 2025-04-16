import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib

from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_statement_model import Statement
from shift_left.core.statement_mgr import (
    delete_statement_if_exists,
    _get_or_build_sql_content_transformer
)

class TestStatementManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
        
    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    @patch('shift_left.core.statement_mgr.get_statement_list')
    def test_delete_flink_statement(self, mock_get_statement_list, MockConfluentCloudClient):
        sname = "statement_name"
        mock_get_statement_list.return_value = { sname: Statement(name= sname), "other" : Statement(name = "other")}
        
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.delete_flink_statement.return_value = "deleted"
        print(f"MockConfluentCloudClient: {MockConfluentCloudClient}")
        print(f"MockConfluentCloudClient.return_value: {MockConfluentCloudClient.return_value}")
        result = delete_statement_if_exists(sname)
        
        self.assertEqual(result, "deleted")
        mock_get_statement_list.assert_called_once()
        MockConfluentCloudClient.assert_called_once()
        mock_client_instance.delete_flink_statement.assert_called_once_with(sname)
       
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
        get_config().get('app')['sql_content_modifier']='shift_left.core.utils.table_worker.ReplaceEnvInSqlContent'
        get_config()['kafka']['cluster_type'] = 'stage'
        transformer = _get_or_build_sql_content_transformer()
        assert transformer
        _, sql_out=transformer.update_sql_content(sql_in)
        print(sql_out)
        assert "'key.avro-registry.schema-context' = '.flink-stage'" in sql_out
        

if __name__ == '__main__':
    unittest.main()