import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_statement_model import Statement, StatementInfo, StatementListCache
import  shift_left.core.statement_mgr as statement_mgr 

class TestStatementManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
    
    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    def _test_get_statement_list(self, MockConfluentCloudClient):
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.make_request.return_value = { "data": 
                                                        {"name": "statement_name", 
                                                           "spec": {"properties": 
                                                                     {"sql.current-catalog": "my_catalog", 
                                                                      "sql.current-database": "my_database"}, 
                                                                "statement": "CREATE TABLE table_1 (",
                                                                "compute_pool_id": "my_compute_pool_id",
                                                                "principal": "my_principal",
                                                                "status": {"phase": "RUNNING"}}}}
        statement_list = statement_mgr.get_statement_list()
        assert statement_list
        print(f"statement_list: {statement_list}")
     
    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    @patch('shift_left.core.statement_mgr.get_statement_list')
    def _test_delete_flink_statement(self, mock_get_statement_list, MockConfluentCloudClient):
        sname = "statement_name"
        mock_get_statement_list.return_value = { sname: Statement(name= sname), "other" : Statement(name = "other")}
        
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.delete_flink_statement.return_value = "deleted"
        print(f"MockConfluentCloudClient: {MockConfluentCloudClient}")
        print(f"MockConfluentCloudClient.return_value: {MockConfluentCloudClient.return_value}")
        result = statement_mgr.delete_statement_if_exists(sname)
        
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
        transformer = statement_mgr._get_or_build_sql_content_transformer()
        assert transformer
        _, sql_out=transformer.update_sql_content(sql_in)
        print(sql_out)
        assert "'key.avro-registry.schema-context' = '.flink-stage'" in sql_out
        

    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    @patch('shift_left.core.statement_mgr.delete_statement_if_exists')
    def test_get_table_structure_success(self, mock_delete_statement, MockConfluentCloudClient):
        """Test successful retrieval of table structure"""
        table_name = "test_table"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_statement = MagicMock()
        mock_statement.status.phase = "COMPLETED"
        mock_statement_result = MagicMock()
        mock_statement_result.results.data[0].row[0] = "CREATE TABLE test_table (...)"
        mock_client_instance.post_flink_statement.return_value = mock_statement
        mock_client_instance.get_statement_results.return_value = mock_statement_result
        
        result = statement_mgr.show_flink_table_structure(table_name)
        
        self.assertIsNotNone(result)
        self.assertEqual(result, "CREATE TABLE test_table (...)")
        mock_delete_statement.assert_called_with(f"drop-{table_name.replace('_', '-')}")
        mock_client_instance.delete_flink_statement.assert_called_with(f"show-{table_name.replace('_', '-')}")

    @patch('shift_left.core.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    def test_drop_table(self, MockConfluentCloudClient, mock_delete_statement_if_exists):
        table_name = "fct_order"
        mock_delete_statement_if_exists.return_value = "deleted"
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.post_flink_statement.return_value =  Statement(name= "drop-fct-order")
        result = statement_mgr.drop_table(table_name=table_name)
        
        self.assertEqual(result, "fct_order dropped")

        MockConfluentCloudClient.assert_called_once()
        config = get_config()
        cpi= config['flink']['compute_pool_id']
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    
        mock_client_instance.post_flink_statement.assert_called_with(cpi,
                                                                     "drop-fct-order",
                                                                     "drop table if exists fct_order;",
                                                                     properties)
        mock_delete_statement_if_exists.assert_called_with("drop-fct-order")


    def test_get_statement_list_cache(self):
        statement_list = StatementListCache(created_at='2025-04-20T10:15:02.853006',statement_list={                                                                          
                                            'info-1': StatementInfo(                     
                                                        name='info-1',                           
                                                        status_phase='STOPPED',                                                           
                                                        status_detail='This statement was stopped manually.',                             
                                                        sql_content=' ',
                                                        compute_pool_id='lfcp-0m07j6',                                                    
                                                        principal='u-1wg0qj',                                                             
                                                        sql_catalog='development_non-prod',                                            
                                                        sql_database='stage-us-west-2'                                          
                                                        ),                                                                                    
                                            'info-2': StatementInfo(                     
                                                        name='info-2',                           
                                                        status_phase='STOPPED',                                                           
                                                        status_detail='This statement was stopped manually.',                             
                                                        sql_content='select id\n    , tenantId\n    , sourceTemplateId\n    , createdOnDate\n',
                                                        compute_pool_id='lfcp-0m07j6',                                                    
                                                        principal='u-1wg0qj',                                                             
                                                        sql_catalog='development_non-prod',                                            
                                                        sql_database='development-us-west-2'                                    
                                            )}) 
        assert statement_list
        str_dump = json.dumps(statement_list.model_dump())
        print(isinstance(str_dump, str))
        print(f"statement_list: {str_dump}")
        statement_list_cache = StatementListCache.model_validate(json.loads(str_dump))
        assert statement_list_cache
        assert isinstance(statement_list_cache, StatementListCache)
        print(f"statement_list_cache: {statement_list_cache}")

if __name__ == '__main__':
    unittest.main()