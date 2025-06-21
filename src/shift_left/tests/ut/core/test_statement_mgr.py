""""
Copyright 2024-2025 Confluent, Inc.
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
import json
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent.parent /  "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementInfo, 
    StatementListCache, 
    Status,
    Spec, 
    Data, 
    StatementResult, 
    FlinkStatementNode,
    OpRow, 
    Metadata)
import  shift_left.core.statement_mgr as statement_mgr 
from ut.core.BaseUT import BaseUT

class TestStatementManager(BaseUT):
    """
    Verify basic statement manager functionality
    """
    def setUp(self):        
        # Reset any cached data in the statement manager
        statement_mgr._statement_list_cache = None
        statement_mgr._statement_compute_pool_map = None
        
    _statement_list = { # mockup of the statement list
        'dev-ddl-src-table-1': StatementInfo(
                name="dev-ddl-src-table-1",
                status_phase="COMPLETED",
                status_detail="Command completed successfully.",
                sql_content="CREATE TABLE src_table_1 (...)",
                compute_pool_id="lfcp-123",
                compute_pool_name="test-pool",
                principal="test-principal",
                sql_catalog="default",
                sql_database="default"
            ),
        'dev-dml-src-table-1' : StatementInfo(
                name="dev-dml-src-table-1",
                status_phase="RUNNING",
                status_detail="",
                sql_content="INSERT INTO src_table_1 (...)",
                compute_pool_id="lfcp-123",
                compute_pool_name="test-pool",
                principal="test-principal",
                sql_catalog="default",
                sql_database="default"
            ) 
    }


    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    def test_1_get_statement_list_with_mock(self, MockConfluentCloudClient):
        """Test successful retrieval of statement list with mocked ConfluentClient"""
        # Setup mock response data
        mock_response = {
            "data": [
                {
                    "name": "test-statement-1",
                    "spec": {
                        "properties": {
                            "sql.current-catalog": "default",
                            "sql.current-database": "default"
                        },
                        "statement": "CREATE TABLE test_table_1",
                        "compute_pool_id": "test-pool-1",
                        "principal": "test-principal"
                    },
                    "status": {
                        "phase": "RUNNING",
                        "detail": ""
                    },
                    "metadata": {
                        "created_at": "2025-04-20T10:15:02.853006"
                    }
                },
                {
                    "name": "test-statement-2",
                    "spec": {
                        "properties": {
                            "sql.current-catalog": "default",
                            "sql.current-database": "default"
                        },
                        "statement": "CREATE TABLE test_table_2",
                        "compute_pool_id": "test-pool-2",
                        "principal": "test-principal"
                    },
                    "status": {
                        "phase": "COMPLETED",
                        "detail": ""
                    },
                    "metadata": {
                        "created_at": "2025-04-20T10:15:02.853006"
                    }
                }
            ],
            "metadata": {
                "next": None
            }
        }

        # Setup mock client: 1/ instance of the client, 2/ mock the make_request and build_flink_url_and_auth_header methods
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.build_flink_url_and_auth_header.return_value = "https://test-url"

        # Call the function
        result = statement_mgr.get_statement_list()

        # Verify results
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        
        # Verify first statement
        self.assertIn("test-statement-1", result)
        stmt1 = result["test-statement-1"]
        self.assertEqual(stmt1.name, "test-statement-1")
        self.assertEqual(stmt1.status_phase, "RUNNING")
        self.assertEqual(stmt1.compute_pool_id, "test-pool-1")
        
        # Verify second statement
        self.assertIn("test-statement-2", result)
        stmt2 = result["test-statement-2"]
        self.assertEqual(stmt2.name, "test-statement-2")
        self.assertEqual(stmt2.status_phase, "COMPLETED")
        self.assertEqual(stmt2.compute_pool_id, "test-pool-2")

        # Verify client was called correctly
        MockConfluentCloudClient.assert_called_once()
        mock_client_instance.make_request.assert_called_once()
        mock_client_instance.build_flink_url_and_auth_header.assert_called_once()
     


    @patch('shift_left.core.statement_mgr.get_statement_list')
    def test_get_statement_status(self, mock_get_statement_list):
        """
        Test the get_statement_status_with_cache method
        """
        mock_get_statement_list.return_value = {
            "test-statement-1": StatementInfo(name= "test-statement-1", status_phase= "RUNNING"),
            "test-statement-2": StatementInfo(name= "test-statement-2", status_phase= "COMPLETED")
        }
    
        statement_info = statement_mgr.get_statement_status_with_cache("test-statement-1")
        assert statement_info
        assert isinstance(statement_info, StatementInfo)    
        self.assertEqual(statement_info.status_phase, "RUNNING")
        self.assertEqual(statement_mgr.get_statement_status_with_cache("test-statement-2").status_phase, "COMPLETED")
        statement_info = statement_mgr.get_statement_status_with_cache("test-statement-3")
        assert statement_info
        assert isinstance(statement_info, StatementInfo)    
        self.assertEqual(statement_info.status_phase, "UNKNOWN")


    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    def test_post_flink_statement(self, MockConfluentCloudClient):
        # Setup test data
        compute_pool_id = "test-pool"
        statement_name = "test-statement"
        sql_content = "SELECT * FROM test_table"
        
        # Configure mock
        mock_client = MockConfluentCloudClient.return_value
        mock_client.build_flink_url_and_auth_header.return_value = "http://test-url"
        
        # Mock successful response
        mock_response = {
            "name": statement_name,
            "status": {"phase": "RUNNING"},
            "spec": {
                "statement": sql_content,
                "compute_pool_id": compute_pool_id,
                "properties": {"sql.current-catalog": "default", "sql.current-database": "default"},
                "stopped": False,
                "principal": "principal_sa"
            }
        }
        mock_client.make_request.return_value = mock_response

        result = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_content)

        # Verify results
        assert result.name == statement_name
        assert result.status.phase == "RUNNING"
        assert result.spec.statement == sql_content
        assert result.spec.compute_pool_id == compute_pool_id

        # Verify mock calls
        mock_client.make_request.assert_called_once()
        mock_client.build_flink_url_and_auth_header.assert_called_once()



    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    @patch('shift_left.core.statement_mgr.get_statement_list')
    def test_delete_flink_statement(self, mock_get_statement_list, MockConfluentCloudClient):
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
        transformer = statement_mgr.get_or_build_sql_content_transformer()
        assert transformer
        _, sql_out=transformer.update_sql_content(sql_in)
        print(sql_out)
        assert "'key.avro-registry.schema-context' = '.flink-stage'" in sql_out
        


    @patch('shift_left.core.statement_mgr.get_statement_results')
    @patch('shift_left.core.statement_mgr.get_statement_list')
    @patch('shift_left.core.statement_mgr.post_flink_statement')
    @patch('shift_left.core.statement_mgr.delete_statement_if_exists')
    def test_get_table_structure_success(self, 
                                         mock_delete_statement,
                                         mock_post_flink_statement, 
                                         mock_get_statement_list,
                                        mock_get_statement_results):
        """Test successful retrieval of table structure"""

        _table_name = "test_table"
        _statement_name = f"show-{_table_name.replace('_', '-')}"
        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            status = Status(
                phase= "RUNNING", 
                detail= ""
            )
            spec = Spec(
                compute_pool_id=get_config().get('flink').get('compute_pool_id'),
                principal="principal_sa",
                statement=sql_content,
                properties={"sql.current-catalog": "default", "sql.current-database": "default"},
                stopped=False
            )
            metadata = Metadata(
                created_at="2025-04-20T10:15:02.853006",
                labels={},
                resource_version="1",
                self="https://test-url",
                uid="test-uid",
                updated_at="2025-04-20T10:15:02.853006"
            )
            return Statement(name= statement_name, status= status, spec=spec, metadata=metadata)

        def mock_statement_list():
            mock_info = MagicMock(spec=Statement)
            return {_statement_name: mock_info}
        
        def mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            if statement_name == _statement_name:
                op_row = OpRow(op=0, row=["CREATE TABLE test_table (...)"])
            else:
                op_row = OpRow(op=0, row=["FAIL"])
            data= Data(data= [op_row])
            result = StatementResult(results=data)
            return result
        
        mock_get_statement_list.side_effect = mock_statement_list
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_results.side_effect = mock_statement_results

        result = statement_mgr.show_flink_table_structure(_table_name)
        
        self.assertIsNotNone(result)
        self.assertEqual(result, "CREATE TABLE test_table (...)")
        mock_delete_statement.assert_called_with(_statement_name)


    @patch('shift_left.core.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    @patch('shift_left.core.statement_mgr.get_statement')
    def test_drop_table(self, 
                        mock_get_statement, 
                        MockConfluentCloudClient, 
                        mock_delete_statement_if_exists):
        print(f"test_drop_table should send drop table statement")
        table_name = "fct_order"

        mock_get_statement.side_effect = self._create_mock_statement(name= "drop-fct-order", status_phase= "COMPLETED")
        mock_delete_statement_if_exists.return_value = "deleted"
        mock_client_instance = MockConfluentCloudClient.return_value
        
        result = statement_mgr.drop_table(table_name=table_name)
        
        self.assertEqual(result, "fct_order dropped")

        MockConfluentCloudClient.assert_called_once()
        config = get_config()
        cpi= config['flink']['compute_pool_id']
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    
        mock_client_instance.make_request.assert_called_once()
        mock_delete_statement_if_exists.assert_called_with("drop-fct-order")


    def test_get_statement_list_cache(self):
        statement_list = StatementListCache(created_at=datetime.now(), statement_list={                                                                          
                                            'info-1': StatementInfo(                     
                                                        name='info-1',                           
                                                        status_phase='STOPPED',                                                           
                                                        status_detail='This statement was stopped manually.',                             
                                                        sql_content=' ',
                                                        compute_pool_id='lfcp-',                                                    
                                                        principal='u-1wg0qj',                                                             
                                                        sql_catalog='development_non-prod',                                            
                                                        sql_database='stage-us-west-2'                                          
                                                        ),                                                                                    
                                            'info-2': StatementInfo(                     
                                                        name='info-2',                           
                                                        status_phase='STOPPED',                                                           
                                                        status_detail='This statement was stopped manually.',                             
                                                        sql_content='select id\n    , tenantId\n    , sourceTemplateId\n    , createdOnDate\n',
                                                        compute_pool_id='lfcp-',                                                    
                                                        principal='u-1wg0qj',                                                             
                                                        sql_catalog='development_non-prod',                                            
                                                        sql_database='development-us-west-2'                                    
                                            )}) 
        assert statement_list
        str_dump = statement_list.model_dump_json(indent=2, warnings=False)
        print(isinstance(str_dump, str))
        print(f"statement_list: {str_dump}")
        statement_list_cache = StatementListCache.model_validate(json.loads(str_dump))
        assert statement_list_cache
        assert isinstance(statement_list_cache, StatementListCache)
        print(f"statement_list_cache: {statement_list_cache}")


    @patch('shift_left.core.statement_mgr.get_statement_list')
    @patch('shift_left.core.statement_mgr.post_flink_statement')
    def test_build_and_deploy_flink_statement_from_sql_content(self, mock_post_flink_statement, 
                                                            mock_get_statement_list):
        config = get_config()
        config['kafka']['cluster_type'] = 'stage'
        config['flink']['compute_pool_id'] = 'lfcp-'
        config['flink']['catalog_name'] = 'j9r-dev'
        config['flink']['database_name'] = 'j9r-cluster'
       
        def mock_post_statement(compute_pool_id, statement_name, sql_content) -> Statement:
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            status = Status(
                phase= "COMPLETED", 
                detail= ""
            )
            metadata = Metadata(
                created_at="2025-04-20T10:15:02.853006",
                labels={},
                resource_version="1",
                self="https://test-url",
                uid="test-uid",
                updated_at="2025-04-20T10:15:02.853006"
            )
            spec = Spec(
                compute_pool_id=compute_pool_id,
                principal="principal_sa",
                statement=sql_content,
                properties={"sql.current-catalog":  config['flink']['catalog_name'], 
                            "sql.current-database":  config['flink']['database_name']},
                stopped=False
            )
            return Statement(name= statement_name, status= status, spec=spec, metadata=metadata)
        
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_list.return_value = self._statement_list

        sql_file_path = os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql"
        node_to_process = FlinkStatementNode(
            table_name="fct_order",
            ddl_ref=sql_file_path,
            ddl_statement_name="test-statement",
            compute_pool_id=config['flink']['compute_pool_id'],
            product_name="p1"
        )
        statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(node_to_process,
                    flink_statement_file_path=sql_file_path,
                    statement_name=f"test-statement")
        assert statement
        assert isinstance(statement, Statement)
        assert statement.name == "test-statement"
        assert "'key.avro-registry.schema-context' = '.flink-stage'," in statement.spec.statement
        mock_post_flink_statement.assert_called_once()

if __name__ == '__main__':
    unittest.main()