import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
import time

#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
#os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"]= os.getenv("HOME") + ".shift_left/config-dev.yaml"

from shift_left.core.utils.app_config import get_config, shift_left_dir, logger
from shift_left.core.models.flink_statement_model import ( 
    Statement, 
    StatementInfo, 
    FlinkStatementNode
)
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.deployment_mgr as dm
import shift_left.core.test_mgr as test_mgr
import shift_left.core.table_mgr as table_mgr
from shift_left.core.utils.file_search import build_inventory
import shift_left.core.deployment_mgr as deployment_mgr
from ut.core.BaseUT import BaseUT

class TestDebugUnitTests(BaseUT):
        
    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        cls.inventory_path = os.getenv("PIPELINES")
        #pipeline_mgr.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def _test_explain_table(self):
        result = """== Physical Plan ==\n\nStreamSink [20]\n  +- StreamUnion [19]\n    +- StreamUnion [17]\n    :  +- StreamUnion [15]\n    :  :  +- StreamCalc [13]\n    :  :  :  +- StreamJoin [12]\n    :  :  :    +- StreamExchange [5]\n    :  :  :    :  +- StreamChangelogNormalize [4]\n    :  :  :    :    +- StreamExchange [3]\n    :  :  :    :      +- StreamCalc [2]\n    :  :  :    :        +- StreamTableSourceScan [1]\n    :  :  :    +- StreamExchange [11]\n    :  :  :      +- StreamCalc [10]\n    :  :  :        +- StreamChangelogNormalize [9]\n    :  :  :          +- StreamExchange [8]\n    :  :  :            +- StreamCalc [7]\n    :  :  :              +- StreamTableSourceScan [6]\n    :  :  +- StreamCalc [14]\n    :  :    +- (reused) [9]\n    :  +- StreamCalc [16]\n    :    +- (reused) [9]\n    +- StreamCalc [18]\n      +- (reused) [9]\n\n== Physical Details ==\n\n[1] StreamTableSourceScan\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`src_identity_metadata`\nPrimary key: (id,tenant_id)\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\nState size: low\nStartup mode: earliest-offset\n\n[2] StreamCalc\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\n\n[3] StreamExchange\nChangelog mode: upsert\nUpsert key: (id,tenant_id)\n\n[4] StreamChangelogNormalize\nChangelog mode: retract\nUpsert key: (id,tenant_id)\nState size: medium\nState TTL: never\n\n[5] StreamExchange\nChangelog mode: retract\nUpsert key: (id,tenant_id)\n\n[6] StreamTableSourceScan\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`stage_tenant_dimension`\nPrimary key: (tenant_id)\nChangelog mode: upsert\nUpsert key: (tenant_id)\nState size: low\nStartup mode: earliest-offset\n\n[7] StreamCalc\nChangelog mode: upsert\nUpsert key: (tenant_id)\n\n[8] StreamExchange\nChangelog mode: upsert\nUpsert key: (tenant_id)\n\n[9] StreamChangelogNormalize\nChangelog mode: retract\nUpsert key: (tenant_id)\nState size: medium\nState TTL: never\n\n[10] StreamCalc\nChangelog mode: retract\n\n[11] StreamExchange\nChangelog mode: retract\n\n[12] StreamJoin\nChangelog mode: retract\nState size: medium\nState TTL: never\n\n[13] StreamCalc\nChangelog mode: retract\n\n[14] StreamCalc\nChangelog mode: retract\n\n[15] StreamUnion\nChangelog mode: retract\n\n[16] StreamCalc\nChangelog mode: retract\n\n[17] StreamUnion\nChangelog mode: retract\n\n[18] StreamCalc\nChangelog mode: retract\n\n[19] StreamUnion\nChangelog mode: retract\n\n[20] StreamSink\nTable: `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`aqem_dim_role`\nPrimary key: (sid)\nChangelog mode: upsert\nState size: high\nState TTL: never\n\n== Warnings ==\n\n1. For StreamSink [20]: The primary key does not match the upsert key derived from the query. If the primary key and upsert key don't match, the system needs to add a state-intensive operation for correction. Please revisit the query (upsert key: null) or the table declaration for `stage-flink-us-west-2-b`.`stage-flink-us-west-2-b`.`aqem_dim_role` (primary key: [sid]). For more information, see https://cnfl.io/primary_vs_upsert_key.\n2. Entire statement: Your query includes one or more highly state-intensive operators but does not set a time-to-live (TTL) value, which means that the system potentially needs to store an infinite amount of state. This can result in a DEGRADED statement and higher CFU consumption. If possible, change your query to use a different operator, or set a time-to-live (TTL) value. For more information, see https://cnfl.io/high_state_intensive_operators.\n'"""
        print(result)

    def _test_explain_tables_for_product(self):
      table_mgr.explain_table(table_name="aqem_dim_role", compute_pool_id="lfcp-0725o5")
    
    
    def _test_static_parsing_of_explain_table(self):
        explain_reports = []
        for root, dirs, files in os.walk(shift_left_dir):
            for file in files:
                if file.endswith("_explain_report.json"):
                    report_path = os.path.join(root, file)
                    print(f"Found explain report: {report_path}")
                    with open(report_path, 'r') as f:
                        explain_reports.append(json.load(f))
                    print(f"--> {table_mgr._summarize_trace(explain_reports[-1]['trace'])}")
    
    TEST_COMPUTE_POOL_ID_1 = "lfcp-121"
    def _create_mock_get_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> StatementInfo:
        """Mock the call to get statement info"""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node. deployment_mgr._assign_compute_pool_id_to_node()"""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node
    
    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
            name="test-pool-1",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        return ComputePoolList(pools=[pool_1])
    
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    def test_deploy_product_using_parallel(self, 
                                           mock_delete, 
                                           mock_drop,
                                           mock_post):
        def _drop_table(table_name: str, compute_pool_id: str) -> str:
            print(f"@@@@ drop_table {table_name} {compute_pool_id}")
            logger.info(f"@@@@ drop_table {table_name} {compute_pool_id}")
            time.sleep(1)
            return "deleted"
        
        def _post_flink_statement(compute_pool_id: str, statement_name: str, sql_content: str) -> Statement:
            print(f"\n@@@@ post_flink_statement {compute_pool_id} {statement_name} {sql_content}")
            logger.info(f"@@@@ post_flink_statement {compute_pool_id} {statement_name}\n {sql_content}")
            time.sleep(1)
            if "ddl" in statement_name:
                return self._create_mock_statement(name=statement_name, status_phase="COMPLETED")
            return self._create_mock_statement(name=statement_name, status_phase="RUNNING")
        
        def _delete_statement(statement_name: str):
            print(f"@@@@ delete statement {statement_name}")
            logger.info(f"@@@@ delete statement {statement_name}")
            time.sleep(1)
            return "deleted"
        config=get_config()
        mock_delete.side_effect = _delete_statement
        mock_drop.side_effect = _drop_table
        mock_post.side_effect = _post_flink_statement

        deployment_mgr.build_deploy_pipelines_from_product(product_name="qx", 
                                                           inventory_path=self.inventory_path, 
                                                           execute_plan=True,
                                                           force_ancestors=True,
                                                           sequential=False)

   

if __name__ == '__main__':
    unittest.main()