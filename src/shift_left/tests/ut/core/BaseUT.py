"""
Base class for all unit tests
"""
import unittest
from shift_left.core.utils.report_mgr import DeploymentReport, StatementBasicInfo
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementInfo,
    Status,
    Spec,
    Metadata
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
import os

class BaseUT(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.TEST_COMPUTE_POOL_ID_1 = "test-pool-121"
        self.TEST_COMPUTE_POOL_ID_2 = "test-pool-122"
        self.TEST_COMPUTE_POOL_ID_3 = "test-pool-123"
        self.inventory_path = os.getenv("PIPELINES")

    def setUp(self):
        """
        Set up the test environment
        """
        pass
    
    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_get_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = "test-pool-123"
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _create_mock_statement(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN"
    ) -> Statement:
        """Create a mock Statement object."""
        status = Status(phase=status_phase)
        return Statement(name=name, status=status)


    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
            name="test-pool",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_2 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_2,
            name="test-pool-2",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_3 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_3,
            name="dev-p1-fct-order",
            env_id=env_id,
            max_cfu=10,
            current_cfu=0
        )
        return ComputePoolList(pools=[pool_1, pool_2, pool_3])
    
    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node. deployment_mgr._assign_compute_pool_id_to_node()"""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node