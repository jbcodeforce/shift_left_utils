import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.statement_mgr as statement_mgr
from shift_left.core.flink_statement_model import Statement, StatementResult, Data, OpRow

class TestDebugUnitTests(unittest.TestCase):

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

        # Execute test
        result = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_content)

        # Verify results
        assert result.name == statement_name
        assert result.status.phase == "RUNNING"
        assert result.spec.statement == sql_content
        assert result.spec.compute_pool_id == compute_pool_id

        # Verify mock calls
        mock_client.make_request.assert_called_once()
        mock_client.build_flink_url_and_auth_header.assert_called_once()

if __name__ == '__main__':
    unittest.main()