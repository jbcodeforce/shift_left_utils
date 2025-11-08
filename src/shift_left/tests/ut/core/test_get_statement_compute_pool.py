""""
Copyright 2024-2025 Confluent, Inc.
"""

import unittest
from unittest.mock import patch

import shift_left.core.statement_mgr as statement_mgr 
from ut.core.BaseUT import BaseUT

class TestStatementManager(BaseUT):
    def setUp(self):        
        # Reset any cached data in the statement manager
        statement_mgr.reset_statement_list()


    @patch('shift_left.core.statement_mgr.ConfluentCloudClient')
    def test_get_statement_list_with_compute_pool_id(self, MockConfluentCloudClient):
        """
        Test the get_statement_list function with a compute pool id
        """
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
                        "compute_pool_id": self.TEST_COMPUTE_POOL_ID_1,
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
                        "compute_pool_id": self.TEST_COMPUTE_POOL_ID_2,
                        "principal": "test-principal"
                    },
                    "status": {
                        "phase": "RUNNING",
                        "detail": ""
                    },
                    "metadata": {
                        "created_at": "2025-04-20T10:15:02.853006"
                    }
                }
            ]
        }
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.build_flink_url_and_auth_header.return_value = ("https://test-url", "test-auth-header")


        statement_list = statement_mgr.get_statement_list(self.TEST_COMPUTE_POOL_ID_1)
        assert "test-statement-1" in statement_list
        assert len(statement_list) == 1
        print(f"statement_list: {statement_list}")
        assert statement_list['test-statement-1'].compute_pool_id == self.TEST_COMPUTE_POOL_ID_1