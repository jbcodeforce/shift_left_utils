"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
from typing import Tuple

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.utils.app_config import get_config

from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.deployment_mgr as dm
from shift_left.core.models.flink_statement_model import (
    Statement,
    StatementInfo
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
)
from shift_left.core.models.flink_statement_model import Statement, StatementInfo

TEST_COMPUTE_POOL_ID_1 = "test-pool-121"
TEST_COMPUTE_POOL_ID_2 = "test-pool-122"
TEST_COMPUTE_POOL_ID_3 = "test-pool-123"

class TestPoolAssignment(unittest.TestCase):
    """Test suite for the deployment manager functionality."""

    data_dir = None

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.inventory_path = os.getenv("PIPELINES")

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_statement_info(
        self,
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )

    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=TEST_COMPUTE_POOL_ID_1,
            name="dev-table-1",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_2 = ComputePoolInfo(
            id=TEST_COMPUTE_POOL_ID_2,
            name="dev-table-2",
            env_id=env_id,
            max_cfu=100,
            current_cfu=78
        )
        pool_3 = ComputePoolInfo(
            id=TEST_COMPUTE_POOL_ID_3,
            name="dev-table-3",
            env_id=env_id,
            max_cfu=10,
            current_cfu=0
        )
        return ComputePoolList(pools=[pool_1, pool_2, pool_3])

    def _create_mock_statement_node(
        self,
        table_name: str,
        product_name: str = "product1",
        dml_statement_name: str = "dml1",
        ddl_statement_name: str = "ddl1",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> FlinkStatementNode:
        """Create a mock FlinkStatementNode object."""
        return FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=dml_statement_name,
            ddl_statement_name=ddl_statement_name,
            compute_pool_id=compute_pool_id
        )


    def _mock_get_and_update_node(self, node: FlinkStatementNode) -> Statement:
        """Mock function for getting and updating node statement info."""
        node.existing_statement_info = self._create_mock_statement_info(
            compute_pool_id=TEST_COMPUTE_POOL_ID_2
        )
        return node

    def _create_mock_compute_pool_info(self,
                                       compute_pool_id: str,
                                       compute_pool_name: str = None,
                                       current_cfu: int = 0,
                                       max_cfu: int = 10) -> dict:
        """Create a mock ComputePoolInfo object to report on compute pool capacity."""
        if compute_pool_id == "high-cfu":
            return {
                "id": compute_pool_id,
                "spec": {
                    "display_name": compute_pool_name,
                    "max_cfu": 10,
                    "enable_ai": False,
                    "environment": {
                        "id": "env-00000",
                        "related": "https://api.confluent.cloud/org/v2/environments/env-00000",
                        "resource_name": "https://api.confluent.cloud/organization=9bb441c4-edef-46ac-8a41-c49e44a3fd9a/environment=env-00000"
                    },
                    "network": {
                        "id": "n-00000",
                        "environment": "string",
                        "related": "https://api.confluent.cloud/networking/v1/networks/n-00000",
                        "resource_name": "https://api.confluent.cloud/organization=9bb441c4-edef-46ac-8a41-c49e44a3fd9a/environment=env-abc123/network=n-00000"
                    }
                },
                "status": {
                    "phase": "PROVISIONED",
                    "current_cfu": 9
                }
            }
        else:
            return {
                "api_version": "fcpm/v2",
                "kind": "ComputePool",
            "id": compute_pool_id,
            "metadata": {
                "self": "https://api.confluent.cloud/fcpm/v2/compute-pools/lfcp-12345",
                "resource_name": "c45"
            },
            "spec": {
                "display_name": compute_pool_name,
                "cloud": "AWS",
                "region": "us-west-1",
                "max_cfu": max_cfu,
                "enable_ai": False
            },
            "status": {
                "phase": "PROVISIONED",
                "current_cfu": current_cfu
            }
        }

    def _mock_create_pool(self, name: str, cp_id: str):
            return {
            "api_version": "fcpm/v2",
            "kind": "ComputePool",
            "id": cp_id,
            "spec": {
                "display_name": name,
                "cloud": "AWS",
                "region": "us-west-1",
                "max_cfu": 5,
                "enable_ai": False,
                "environment": {
                "id": "env-00000",
                "related": "https://api.confluent.cloud/org/v2/environments/env-00000",
                "resource_name": "https://api.confluent.cloud/organization=9bb441c4-edef-46ac-8a41-c49e44a3fd9a/environment=env-00000"
                },
                "network": {
                "id": "n-00000",
                "environment": "string",
                "related": "https://api.confluent.cloud/networking/v1/networks/n-00000",
                "resource_name": "https://api.confluent.cloud/organization=9bb441c4-edef-46ac-8a41-c49e44a3fd9a/environment=env-abc123/network=n-00000"
                }
            },
            "status": {
                "phase": "PROVISIONED",
                "current_cfu": 0
            }
        }

    #  ----------- TESTS -----------
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_assign_compute_pool_id_to_node_using_parameter(self,
                                                            mock_get_compute_pool_list,
                                                            MockConfluentCloudClient):
        """ should assign compute_pool_id from the parameter"""
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info(TEST_COMPUTE_POOL_ID_1,"dev-table-1", 1, 20)
        node = self._create_mock_statement_node("table-1")
        node.compute_pool_id = None
        node = dm._assign_compute_pool_id_to_node(node, TEST_COMPUTE_POOL_ID_1)
        assert node.compute_pool_id == TEST_COMPUTE_POOL_ID_1
        assert node.compute_pool_name == "dev-table-1"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_assign_compute_pool_id_to_node_using_node_compute_pool_id(self,
                                                                       mock_get_compute_pool_list,
                                                                       MockConfluentCloudClient):
        """ should keep compute_pool_id already set in the node"""
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info(TEST_COMPUTE_POOL_ID_1,"dev-table-1", 2, 20)

        node = self._create_mock_statement_node("table-4")
        node.compute_pool_id = TEST_COMPUTE_POOL_ID_1
        node = dm._assign_compute_pool_id_to_node(node, None)
        assert node.compute_pool_id == TEST_COMPUTE_POOL_ID_1
        assert node.compute_pool_name == "dev-table-1"


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def _test_get_compute_pool_id_from_config(self,
                                            mock_get_compute_pool_list,
                                            MockConfluentCloudClient):
        """ should use the compute pool id from config.yaml
        06/10/25 REMOVED the logic to use the compute pool id from config.yaml
        """
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info(TEST_COMPUTE_POOL_ID_1, "dev-table-1", 3, 20)

        node = self._create_mock_statement_node("table-X")
        node.compute_pool_id = None
        self.config['flink']['compute_pool_id'] = TEST_COMPUTE_POOL_ID_2
        node = dm._assign_compute_pool_id_to_node(node, None)
        assert node.compute_pool_id == TEST_COMPUTE_POOL_ID_2
        assert node.compute_pool_name == "dev-table-2"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_get_compute_pool_id_from_matching_name(self,
                                            mock_get_compute_pool_list,
                                            MockConfluentCloudClient):
        """ should use the compute pool id from config.yaml"""
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info(TEST_COMPUTE_POOL_ID_1, "dev-table-1", 3, 20)

        node = self._create_mock_statement_node("table-3")
        node.compute_pool_id = None
        node = dm._assign_compute_pool_id_to_node(node, None)
        assert node.compute_pool_id == TEST_COMPUTE_POOL_ID_3
        assert node.compute_pool_name == "dev-table-3"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_too_high_cfu(self,
                         mock_get_compute_pool_list,
                         MockConfluentCloudClient):
        """When the cfu is too high create a new compute pool if not already exists"""
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.side_effect = self._create_mock_compute_pool_info
        mock_client_instance.create_compute_pool.return_value = self._mock_create_pool("cp-name-1", "cp-id-1")
        node = self._create_mock_statement_node(table_name="table-X", compute_pool_id="high-cfu")
        self.config['flink']['compute_pool_id'] = TEST_COMPUTE_POOL_ID_3
        node = dm._assign_compute_pool_id_to_node(node, None)
        assert node.compute_pool_id == 'cp-id-1'
        assert node.compute_pool_name == "cp-name-1"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_pool_creation(self,
                         mock_get_compute_pool_list,
                         MockConfluentCloudClient):
        """When could not find a matching compute pool, create a new one"""
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info("cp-id-1", "cp-name-1",  5, 50)
        mock_client_instance.create_compute_pool.return_value = self._mock_create_pool("cp-name-1", "cp-id-1")
        node = self._create_mock_statement_node("table-7", compute_pool_id='')
        self.config['flink']['compute_pool_id'] = ''
        node = dm._assign_compute_pool_id_to_node(node, '')
        assert node.compute_pool_id == "cp-id-1"
        assert node.compute_pool_name == "cp-name-1"

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.ConfluentCloudClient')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    def test_pool_creation_failure(self,
                                  mock_get_compute_pool_list,
                                  MockConfluentCloudClient):
        """When the pool creation fails, the node should not be assigned a compute pool"""
        create_pool_error = {
            "api_version": "fcpm/v2",
            "kind": "ComputePool",
            "id": "cp-id-1",
            "errors": [
                    {
                    "id": "ed42afdc-f0d5-4c0d-b428-9fc6ed6e279d",
                    "status": "409",
                    "code": "resource_already_exists",
                    "title": "Resource Already exists",
                    "detail": "The entitlement '91e3e86f-fca6-4f14-98f5-a48e64113ce2' already exists."
                    }]
        }
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_response={}
        mock_client_instance.make_request.return_value = mock_response
        mock_client_instance.get_compute_pool_info.return_value = self._create_mock_compute_pool_info("cp-id-1", "cp-name-1",  5, 50)
        mock_client_instance.create_compute_pool.return_value = create_pool_error
        node = self._create_mock_statement_node("table-8")
        node.compute_pool_id = ''
        try:
            node = dm._assign_compute_pool_id_to_node(node, '')
        except Exception as e:
            assert node.compute_pool_id == ''
            assert node.compute_pool_name == ''



if __name__ == '__main__':
    unittest.main()
