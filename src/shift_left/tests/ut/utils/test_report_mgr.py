"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from shift_left.core.models.flink_statement_model import (
    FlinkStatementExecutionPlan,
    FlinkStatementNode,
    StatementInfo
)
from shift_left.core.utils.report_mgr import (
    build_deployment_report,
    build_summary_from_execution_plan,
    build_simple_report,
    pad_or_truncate,
    build_TableReport,
    build_TableInfo,
    persist_table_reports,
    _build_statement_basic_info,
    TableReport,
    TableInfo
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

    def test_pad_or_truncate_string_truncate(self):
        """Test pad_or_truncate with string input that needs truncation."""
        result = pad_or_truncate("this is a long string", 10)
        self.assertEqual(result, "this is a ")
        self.assertEqual(len(result), 10)

    def test_pad_or_truncate_string_pad(self):
        """Test pad_or_truncate with string input that needs padding."""
        result = pad_or_truncate("short", 10)
        self.assertEqual(result, "short     ")
        self.assertEqual(len(result), 10)

    def test_pad_or_truncate_string_exact_length(self):
        """Test pad_or_truncate with string input of exact target length."""
        result = pad_or_truncate("exact", 5)
        self.assertEqual(result, "exact")
        self.assertEqual(len(result), 5)

    def test_pad_or_truncate_non_string_input(self):
        """Test pad_or_truncate with non-string input (number)."""
        result = pad_or_truncate(12345, 10)
        self.assertEqual(result, "12345     ")
        self.assertEqual(len(result), 10)

    def test_pad_or_truncate_custom_padding_char(self):
        """Test pad_or_truncate with custom padding character."""
        result = pad_or_truncate("test", 8, '*')
        self.assertEqual(result, "test****")
        self.assertEqual(len(result), 8)

    @patch('shift_left.core.utils.report_mgr.get_config')
    @patch('shift_left.core.utils.report_mgr.metrics_mgr')
    def test_build_table_report_with_from_date_and_metrics(self, mock_metrics, mock_config):
        """Test build_TableReport with from_date and get_metrics=True."""
        # Setup mocks
        mock_config.return_value = {
            'confluent_cloud': {'environment_id': 'test_env'},
            'flink': {'catalog_name': 'test_catalog', 'database_name': 'test_db'}
        }
        mock_metrics.get_pending_records.return_value = {'test_statement': 100}
        mock_metrics.get_num_records_out.return_value = {'test_statement': 200}
        mock_metrics.get_num_records_in.return_value = {'test_statement': 300}

        # Create node with existing statement info
        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_pool",
            existing_statement_info=StatementInfo(
                name="test_statement",
                status_phase="RUNNING"
            )
        )

        with patch('shift_left.core.utils.report_mgr.build_TableInfo') as mock_build_info:
            mock_table_info = TableInfo(table_name="test_table", status="RUNNING")
            mock_build_info.return_value = mock_table_info

            result = build_TableReport("test_report", [node], from_date="2024-01-01", get_metrics=True)

            self.assertEqual(result.report_name, "test_report")
            self.assertEqual(result.environment_id, "test_env")
            self.assertEqual(len(result.tables), 1)
            mock_metrics.get_pending_records.assert_called_once()
            mock_metrics.get_num_records_out.assert_called_once()
            mock_metrics.get_num_records_in.assert_called_once()

    @patch('shift_left.core.utils.report_mgr.get_config')
    @patch('shift_left.core.utils.report_mgr.logger')
    @patch('shift_left.core.utils.report_mgr.metrics_mgr')
    def test_build_table_report_with_metrics_no_existing_statement(self, mock_metrics, mock_logger, mock_config):
        """Test build_TableReport with get_metrics=True but node has no existing_statement_info."""
        # Setup mocks
        mock_config.return_value = {
            'confluent_cloud': {'environment_id': 'test_env'},
            'flink': {'catalog_name': 'test_catalog', 'database_name': 'test_db'}
        }
        mock_metrics.get_pending_records.return_value = {}
        mock_metrics.get_num_records_out.return_value = {}
        mock_metrics.get_num_records_in.return_value = {}

        # Create node without existing statement info
        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_pool",
            existing_statement_info=None
        )

        with patch('shift_left.core.utils.report_mgr.build_TableInfo') as mock_build_info:
            mock_table_info = TableInfo(table_name="test_table", status="UNKNOWN")
            mock_build_info.return_value = mock_table_info

            result = build_TableReport("test_report", [node], from_date="", get_metrics=True)

            # Should log error for missing existing_statement_info
            mock_logger.error.assert_called_once()
            self.assertEqual(len(result.tables), 1)

    @patch('shift_left.core.utils.report_mgr.get_config')
    def test_build_table_report_without_from_date_and_no_metrics(self, mock_config):
        """Test build_TableReport without from_date and get_metrics=False."""
        # Setup mocks
        mock_config.return_value = {
            'confluent_cloud': {'environment_id': 'test_env'},
            'flink': {'catalog_name': 'test_catalog', 'database_name': 'test_db'}
        }

        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_pool"
        )

        with patch('shift_left.core.utils.report_mgr.build_TableInfo') as mock_build_info:
            mock_table_info = TableInfo(table_name="test_table")
            mock_build_info.return_value = mock_table_info

            result = build_TableReport("test_report", [node], from_date="", get_metrics=False)

            self.assertEqual(result.report_name, "test_report")
            self.assertEqual(len(result.tables), 1)
            # build_TableInfo should be called with get_metrics=False
            mock_build_info.assert_called_once_with(node, get_metrics=False)

    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    @patch('shift_left.core.utils.report_mgr.metrics_mgr')
    def test_build_table_info_with_existing_statement_and_pool(self, mock_metrics, mock_compute_pool):
        """Test build_TableInfo with existing statement info and valid compute pool."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_list.return_value = self.compute_pool_list
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]
        mock_metrics.get_retention_size.return_value = 1000

        # Create node with existing statement info
        existing_statement = StatementInfo(
            name="test_statement",
            status_phase="RUNNING",
            compute_pool_id="test_compute_pool",
            created_at=datetime(2024, 1, 1, 12, 0, 0)
        )
        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_compute_pool",
            type="TABLE",
            upgrade_mode="stateless",
            to_restart=True,
            to_run=False,
            existing_statement_info=existing_statement
        )

        result = build_TableInfo(node, get_metrics=True)

        self.assertEqual(result.table_name, "test_table")
        self.assertEqual(result.type, "TABLE")
        self.assertEqual(result.upgrade_mode, "stateless")
        self.assertEqual(result.statement_name, "test_statement")
        self.assertEqual(result.status, "RUNNING")
        self.assertEqual(result.compute_pool_id, "test_compute_pool")
        self.assertEqual(result.compute_pool_name, "test_compute_pool")
        self.assertTrue(result.to_restart)
        self.assertEqual(result.retention_size, 1000)
        mock_metrics.get_retention_size.assert_called_once_with("test_table")

    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_table_info_without_existing_statement(self, mock_compute_pool):
        """Test build_TableInfo without existing statement info."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_list.return_value = self.compute_pool_list
        mock_compute_pool.get_compute_pool_with_id.return_value = None

        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="unknown_pool",
            type="VIEW",
            upgrade_mode="stateful",
            to_restart=False,
            to_run=True,
            existing_statement_info=None
        )

        result = build_TableInfo(node, get_metrics=False)

        self.assertEqual(result.table_name, "test_table")
        self.assertEqual(result.type, "VIEW")
        self.assertEqual(result.upgrade_mode, "stateful")
        self.assertEqual(result.statement_name, "test_statement")
        self.assertEqual(result.status, "UNKNOWN")
        self.assertEqual(result.compute_pool_id, "")
        self.assertEqual(result.compute_pool_name, "UNKNOWN")
        self.assertTrue(result.to_restart or result.to_run)  # to_restart = node.to_restart or node.to_run

    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_table_info_with_unknown_compute_pool(self, mock_compute_pool):
        """Test build_TableInfo with existing statement but unknown compute pool."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_list.return_value = self.compute_pool_list
        mock_compute_pool.get_compute_pool_with_id.return_value = None  # Pool not found

        existing_statement = StatementInfo(
            name="test_statement",
            status_phase="PENDING",
            compute_pool_id="unknown_pool",
            created_at=datetime(2024, 1, 1, 12, 0, 0)
        )
        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="unknown_pool",
            existing_statement_info=existing_statement
        )

        result = build_TableInfo(node, get_metrics=False)

        self.assertEqual(result.status, "PENDING")
        self.assertEqual(result.compute_pool_id, "unknown_pool")
        self.assertEqual(result.compute_pool_name, "UNKNOWN")

    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    @patch('shift_left.core.utils.report_mgr.metrics_mgr')
    def test_build_table_info_running_without_metrics(self, mock_metrics, mock_compute_pool):
        """Test build_TableInfo with RUNNING status but get_metrics=False."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_list.return_value = self.compute_pool_list
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]

        existing_statement = StatementInfo(
            name="test_statement",
            status_phase="RUNNING",
            compute_pool_id="test_compute_pool"
        )
        node = FlinkStatementNode(
            table_name="test_table",
            existing_statement_info=existing_statement
        )

        result = build_TableInfo(node, get_metrics=False)

        self.assertEqual(result.status, "RUNNING")
        self.assertEqual(result.retention_size, 0)  # Should not call metrics when get_metrics=False
        mock_metrics.get_retention_size.assert_not_called()

    @patch('shift_left.core.utils.report_mgr.metrics_mgr')
    def test_build_simple_report_with_no_existing_statement(self, mock_metrics):
        """Test build_simple_report with nodes that have no existing_statement_info."""
        # Setup mocks
        mock_metrics.get_pending_records.return_value = {}
        mock_metrics.get_num_records_out.return_value = {}
        mock_metrics.get_num_records_in.return_value = {}

        # Create node without existing statement info
        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_pool",
            existing_statement_info=None
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node]
        )

        result = build_simple_report(execution_plan)

        # Should contain headers but no node data (since node has no existing_statement_info)
        self.assertIn("Ancestor Table Name", result)
        self.assertIn("Statement Name", result)
        self.assertIn("Status", result)
        self.assertIn("Compute Pool", result)
        # Since node has no existing_statement_info, it won't be included in the report body
        lines = result.split('\n')
        self.assertEqual(len([line for line in lines if line.strip() and not line.startswith('-') and 'Ancestor Table Name' not in line]), 0)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_summary_with_restart_nodes(self, mock_compute_pool):
        """Test build_summary_from_execution_plan with nodes that have to_restart=True."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]

        # Create nodes with different restart/run statuses
        node1 = FlinkStatementNode(
            table_name="parent_table",
            dml_statement_name="parent_statement",
            compute_pool_id="test_compute_pool",
            to_run=True,
            to_restart=True,
            existing_statement_info=StatementInfo(status_phase="RUNNING")
        )
        node2 = FlinkStatementNode(
            table_name="child_table",
            dml_statement_name="child_statement",
            compute_pool_id="test_compute_pool",
            to_run=False,
            to_restart=True,
            existing_statement_info=StatementInfo(status_phase="PENDING")
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node1, node2]
        )

        result = build_summary_from_execution_plan(execution_plan, self.compute_pool_list)

        self.assertIn("Ancestors:", result)
        self.assertIn("Children to restart", result)
        self.assertIn("Restart", result)  # Should show "Restart" action for to_restart nodes
        self.assertIn("parent_table", result)
        self.assertIn("child_table", result)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_summary_with_no_parents(self, mock_compute_pool):
        """Test build_summary_from_execution_plan with no parent nodes."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]

        # Create only child nodes (to_restart=True, to_run=False, not running)
        # Note: A node with existing_statement_info and RUNNING status will be considered both parent and child
        node = FlinkStatementNode(
            table_name="child_table",
            dml_statement_name="child_statement",
            compute_pool_id="test_compute_pool",
            to_run=False,
            to_restart=True,
            existing_statement_info=StatementInfo(status_phase="STOPPED")  # Use STOPPED so is_running() returns False
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node]
        )

        result = build_summary_from_execution_plan(execution_plan, self.compute_pool_list)

        # Node appears in both sections since logic separates by (to_run or is_running()) vs to_restart
        # But we test that ancestors section exists if the node is_running() or to_run=True
        self.assertIn("Children to restart", result)
        self.assertIn("child_table", result)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_summary_with_no_children(self, mock_compute_pool):
        """Test build_summary_from_execution_plan with no child nodes."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]

        # Create only parent nodes (to_run=True, to_restart=False)
        node = FlinkStatementNode(
            table_name="parent_table",
            dml_statement_name="parent_statement",
            compute_pool_id="test_compute_pool",
            to_run=True,
            to_restart=False,
            existing_statement_info=StatementInfo(status_phase="RUNNING")
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node]
        )

        result = build_summary_from_execution_plan(execution_plan, self.compute_pool_list)

        self.assertIn("--- Ancestors:", result)
        # Should not contain Children section
        self.assertNotIn("Children to restart", result)
        self.assertIn("parent_table", result)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_summary_with_unknown_pool(self, mock_compute_pool):
        """Test build_summary_from_execution_plan with unknown compute pool."""
        # Setup mocks - return None for unknown pool
        mock_compute_pool.get_compute_pool_with_id.return_value = None

        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="unknown_pool",
            to_run=True,
            existing_statement_info=StatementInfo(status_phase="RUNNING")
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node]
        )

        result = build_summary_from_execution_plan(execution_plan, self.compute_pool_list)

        self.assertIn("test_table", result)
        # Should handle case where pool is not found gracefully
        self.assertIn("unknown_pool", result)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.compute_pool_mgr')
    def test_build_summary_with_no_existing_statement_info(self, mock_compute_pool):
        """Test build_summary_from_execution_plan with nodes that have no existing_statement_info."""
        # Setup mocks
        mock_compute_pool.get_compute_pool_with_id.return_value = self.compute_pool_list.pools[0]

        node = FlinkStatementNode(
            table_name="test_table",
            dml_statement_name="test_statement",
            compute_pool_id="test_compute_pool",
            to_run=True,
            existing_statement_info=None  # No existing statement info
        )

        execution_plan = FlinkStatementExecutionPlan(
            start_table_name="test_table",
            environment_id="test_env",
            nodes=[node]
        )

        result = build_summary_from_execution_plan(execution_plan, self.compute_pool_list)

        self.assertIn("test_table", result)
        self.assertIn("Not dep", result)  # Shows "Not dep" (truncated) when no existing_statement_info

    def test_build_deployment_report_with_none_statements(self):
        """Test build_deployment_report with None statements in the list."""
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
            ),
            None,  # Test with None statement
            Statement(
                name="test_statement_2",
                environment_id="test_environment",
                created_at="2024-01-01",
                uid="test_uid_2",
                metadata=Metadata(
                    created_at="2024-01-01",
                    labels={},
                    resource_name="test_resource_name_2",
                    self="test_self_2",
                    uid="test_uid_2"
                ),
                spec=Spec(
                    compute_pool_id="test_compute_pool_2",
                    principal="test_principal",
                    properties={},
                    statement="insert into test_table_2 values (2, 'test2')",
                    stopped=False
                ),
                status=Status(
                    phase="pending",
                    detail=None
                )
            )
        ]

        report = build_deployment_report(
            table_name="test_table",
            dml_ref="DML",
            may_start_children=False,
            statements=statements
        )

        # Should only include non-None statements
        self.assertEqual(len(report.flink_statements_deployed), 2)
        self.assertEqual(report.flink_statements_deployed[0].name, "test_statement")
        self.assertEqual(report.flink_statements_deployed[1].name, "test_statement_2")
        self.assertFalse(report.update_children)

    @patch('shift_left.core.utils.report_mgr.shift_left_dir', '/tmp')
    @patch('shift_left.core.utils.report_mgr.get_config')
    def test_persist_table_reports_with_mixed_status(self, mock_config):
        """Test persist_table_reports with mix of RUNNING and non-RUNNING tables."""
        # Setup mocks
        mock_config.return_value = {
            'confluent_cloud': {'environment_id': 'test_env'},
            'flink': {'catalog_name': 'test_catalog', 'database_name': 'test_db'}
        }

        # Create table report with mixed status tables
        table_report = TableReport(
            report_name="test_report",
            environment_id="test_env",
            catalog_name="test_catalog",
            database_name="test_db",
            created_at=datetime(2024, 1, 1, 12, 0, 0),
            tables=[
                TableInfo(
                    table_name="running_table",
                    status="RUNNING",
                    created_at=datetime(2024, 1, 1, 12, 0, 0),
                    pending_records=100,
                    num_records_in=500,
                    num_records_out=400
                ),
                TableInfo(
                    table_name="stopped_table",
                    status="STOPPED",
                    created_at=datetime(2024, 1, 1, 12, 0, 0),
                    pending_records=0,
                    num_records_in=0,
                    num_records_out=0
                ),
                TableInfo(
                    table_name="pending_table",
                    status="PENDING",
                    created_at=datetime(2024, 1, 1, 12, 0, 0),
                    pending_records=50,
                    num_records_in=100,
                    num_records_out=0
                )
            ]
        )

        with patch('builtins.open', unittest.mock.mock_open()) as mock_open:
            result = persist_table_reports(table_report, "test_base")

            # Should count RUNNING vs non-RUNNING correctly
            self.assertIn("Running tables: 1", result)
            self.assertIn("Non running tables: 2", result)
            self.assertIn("running_table", result)
            self.assertIn("stopped_table", result)
            self.assertIn("pending_table", result)

            # Should write both CSV and JSON files
            self.assertEqual(mock_open.call_count, 2)  # CSV and JSON files

    def test_build_statement_basic_info_with_status_detail(self):
        """Test _build_statement_basic_info with statement that has status and detail."""
        statement = Statement(
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
                detail="Statement is running successfully"
            ),
            execution_time=150.5
        )

        result = _build_statement_basic_info(statement)

        self.assertEqual(result.name, "test_statement")
        self.assertEqual(result.environment_id, "test_environment")
        self.assertEqual(result.uid, "test_uid")
        self.assertEqual(result.compute_pool_id, "test_compute_pool")
        self.assertEqual(result.status, "running")
        self.assertEqual(result.status_details, "Statement is running successfully")
        self.assertEqual(result.execution_time, 150.5)

    def test_build_statement_basic_info_without_status_detail(self):
        """Test _build_statement_basic_info with statement that has no status detail."""
        statement = Statement(
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
                phase="pending",
                detail=None  # No detail
            ),
            execution_time=0
        )

        result = _build_statement_basic_info(statement)

        self.assertEqual(result.name, "test_statement")
        self.assertEqual(result.status, "pending")
        self.assertEqual(result.status_details, "")  # Should be empty string when no detail
        self.assertEqual(result.execution_time, 0)

    def test_build_statement_basic_info_with_no_status(self):
        """Test _build_statement_basic_info with statement that has no status at all."""
        statement = Statement(
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
            status=None,  # No status
            execution_time=0
        )

        result = _build_statement_basic_info(statement)

        self.assertEqual(result.name, "test_statement")
        self.assertEqual(result.status, "UNKNOWN")  # Should be "UNKNOWN" when no status
        self.assertEqual(result.status_details, "")  # Should be empty string when no status

if __name__ == '__main__':
    unittest.main()
