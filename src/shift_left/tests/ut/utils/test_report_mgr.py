"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from shift_left.core.models.flink_statement_model import (
    FlinkStatementExecutionPlan, 
    FlinkStatementNode, 
    StatementInfo
)
from shift_left.core.utils.report_mgr import (
    build_deployment_report,
    build_summary_from_execution_plan,
    build_simple_report
)
from shift_left.core.models.flink_compute_pool_model import (
    ComputePoolList,
    ComputePoolInfo
)
from shift_left.core.models.flink_statement_model import (
    Statement,
    Spec, 
    Status,
    Metadata
)

class TestReportMgr(unittest.TestCase):

    def setUp(self):
        node2= FlinkStatementNode(
            table_name="test_table_2",
            dml_statement_name="test_statement_2",
            compute_pool_id="test_compute_pool_2",
            upgrade_mode="stateless",
            to_run=True,
            existing_statement_info=StatementInfo(
                status_phase="running",
                execution_time=100
            )
        )
        node1 = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_compute_pool",
            upgrade_mode="stateful",
            to_run=True,
            existing_statement_info=StatementInfo(
                status_phase="running",
                execution_time=100
            ),
            parents=[node2]
        )
        
        self.execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_environment",
            nodes=[node1, node2]
        )
        self.compute_pool_list = ComputePoolList(
            pools=[
                ComputePoolInfo(
                    id="test_compute_pool",
                    name="test_compute_pool",
                    env_id="test_environment",
                    max_cfu=100,
                    region="us-west-2",
                    status_phase="PROVISIONED",
                    current_cfu=1
                ),
                ComputePoolInfo(
                    id="test_compute_pool_2",
                    name="test_compute_pool_2",
                    env_id="test_environment",
                    max_cfu=100,
                    region="us-west-2",
                    status_phase="PROVISIONED",
                    current_cfu=1
                )
            ]
        )
    def test_build_deployment_report(self):
        print(f"test_build_deployment_report")
        statements = [
            Statement(
                name="test_statement",
                environment_id="test_environment",
                created_at="2024-01-01",
                uid="test_uid",
                metadata=Metadata(
                    created_at="2024-01-01",
                    labels={},
                    resource_name="test_resource_name",
                    self="test_self",
                    uid="test_uid"
                ),
                spec=Spec(
                    compute_pool_id="test_compute_pool",
                    principal="test_principal",
                    properties={},
                    statement="insert into test_table values (1, 'test')",
                    stopped=False
                ),
                status=Status(
                    phase="running",
                    detail="test_detail"
                )
            )
        ]
        report = build_deployment_report(
            table_name="test_table",
            dml_ref="test_statement",
            may_start_children=True,
            statements=statements
        )   
        print(f"\n\n{report}\n\n")
        assert "test_table" in report.table_name
        assert "test_statement" in report.flink_statements_deployed[0].name
        assert "running" in report.flink_statements_deployed[0].status
        assert "test_compute_pool" in report.flink_statements_deployed[0].compute_pool_id
        

    def test_build_summary_from_execution_plan(self):
        print(f"test_build_summary_from_execution_plan")
        summary = build_summary_from_execution_plan(self.execution_plan, self.compute_pool_list)
        print(f"\n\n{summary}\n\n")
        assert "test_table" in summary
        assert "test_table_2" in summary
        assert "test_statement" in summary
        assert "test_statement_2" in summary
        assert "running" in summary
        assert "test_compute_pool" in summary
        assert "test_compute_pool_2" in summary
        
    def test_build_simple_report(self):
        print(f"test_build_simple_report")
        report = build_simple_report(self.execution_plan)
        print(f"\n\n{report}\n\n")
        assert "test_table" in report
        assert "test_statement" in report
        assert "running" in report
        assert "test_compute_p" in report

