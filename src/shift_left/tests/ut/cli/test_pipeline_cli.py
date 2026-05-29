"""
Copyright 2024-2026 Confluent, Inc.

Unit tests for the pipeline Typer app (CLI-first ownership).
"""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from shift_left.cli import app
from shift_left.core.compute_pool_mgr import ComputePoolInfo, ComputePoolList
from shift_left.core.models.flink_statement_model import StatementInfo
from shift_left.core.utils.file_search import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.report_mgr import TableInfo, TableReport

import shift_left.core.pipeline_mgr as pm
import shift_left.core.table_mgr as tm
from ut.BaseUT import SLUnitTestCase


class TestPipelineCLI(SLUnitTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        tm.get_or_create_inventory(cls.pipelines)
        pm.delete_all_metada_files(cls.pipelines)
        pm.build_all_pipeline_definitions(cls.pipelines)

    def test_report_command_success(self):
        result = self.runner.invoke(
            app, ["pipeline", "report", "p1_fct_order", self.pipelines]
        )
        assert result.exit_code == 0, result.stdout
        assert "p1_fct_order" in result.stdout
        assert "int_p1_table_1" in result.stdout

    def test_report_command_error(self):
        result = self.runner.invoke(
            app, ["pipeline", "report", "non_existent_table", self.pipelines]
        )
        assert result.exit_code == 1
        assert "Table not found" in result.stdout

    def test_build_metadata_command_error_invalid_file(self):
        result = self.runner.invoke(
            app, ["pipeline", "build-metadata", "invalid_file.txt", self.pipelines]
        )
        assert result.exit_code == 1
        assert "the first parameter needs to be a dml sql file" in result.stdout

    def test_build_metadata_success(self):
        pm.delete_all_metada_files(self.pipelines)
        dml_path = os.path.join(
            self.pipelines,
            "facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql",
        )
        pipeline_json = os.path.join(
            self.pipelines, "facts/p1/fct_order", PIPELINE_JSON_FILE_NAME
        )
        try:
            result = self.runner.invoke(
                app, ["pipeline", "build-metadata", dml_path, self.pipelines]
            )
            assert result.exit_code == 0, result.stdout
            assert os.path.exists(pipeline_json)
        finally:
            pm.build_all_pipeline_definitions(self.pipelines)

    def test_delete_all_metadata(self):
        pm.build_all_pipeline_definitions(self.pipelines)
        fct_order_json = os.path.join(
            self.pipelines, "facts/p1/fct_order", PIPELINE_JSON_FILE_NAME
        )
        assert os.path.exists(fct_order_json)

        result = self.runner.invoke(
            app, ["pipeline", "delete-all-metadata", self.pipelines]
        )
        assert result.exit_code == 0, result.stdout
        assert "Delete pipeline definitions from" in result.stdout
        assert not os.path.exists(fct_order_json)

        pm.build_all_pipeline_definitions(self.pipelines)

    def test_build_all_metadata(self):
        pm.delete_all_metada_files(self.pipelines)
        result = self.runner.invoke(
            app, ["pipeline", "build-all-metadata", self.pipelines]
        )
        assert result.exit_code == 0, result.stdout
        assert "Build all pipeline definitions for all tables" in result.stdout
        assert os.path.exists(
            os.path.join(self.pipelines, "facts/p1/fct_order", PIPELINE_JSON_FILE_NAME)
        )
        assert os.path.exists(
            os.path.join(
                self.pipelines,
                "facts/c360/fct_user_per_group",
                PIPELINE_JSON_FILE_NAME,
            )
        )

    def test_report_running_statements_command_error_no_params(self):
        result = self.runner.invoke(
            app, ["pipeline", "report-running-statements", self.pipelines]
        )
        assert result.exit_code == 1
        assert "either table-name, product-name or dir must be provided" in result.stdout

    @patch(
        "shift_left.cli_commands.pipeline.deployment_mgr.report_running_flink_statements_for_a_table"
    )
    def test_report_running_statements_smoke(self, mock_report):
        mock_report.return_value = "Running statements for p1_fct_order"
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "report-running-statements",
                self.pipelines,
                "--table-name",
                "p1_fct_order",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "Running statements for p1_fct_order" in result.stdout
        mock_report.assert_called_once()

    def test_build_execution_plan_missing_params(self):
        result = self.runner.invoke(
            app, ["pipeline", "build-execution-plan", self.pipelines]
        )
        assert result.exit_code == 1
        assert "table-list-file-name must be" in result.stdout

    @patch("shift_left.core.deployment_mgr.build_deploy_pipeline_from_table")
    def test_build_execution_plan_smoke(self, mock_build_from_table):
        mock_report = TableReport(
            report_name="p1_fct_order",
            tables=[
                TableInfo(
                    table_name="p1_fct_order",
                    status="RUNNING",
                    pending_records=0,
                    num_records_out=0,
                )
            ],
        )
        mock_build_from_table.return_value = ("Execution plan summary", mock_report, True)
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "build-execution-plan",
                self.pipelines,
                "--table-name",
                "p1_fct_order",
                "--compute-pool-id",
                "test-pool-121",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "Build an execution plan for table p1_fct_order" in result.stdout
        mock_build_from_table.assert_called_once()

    def test_deploy_missing_params(self):
        result = self.runner.invoke(app, ["pipeline", "deploy", self.pipelines])
        assert result.exit_code == 1
        assert "table-list-file-name must be" in result.stdout

    @patch("shift_left.core.deployment_mgr.build_deploy_pipeline_from_table")
    def test_deploy_smoke(self, mock_build_from_table):
        mock_report = TableReport(
            report_name="p1_fct_order",
            tables=[
                TableInfo(
                    table_name="p1_fct_order",
                    status="RUNNING",
                    pending_records=0,
                    num_records_out=100,
                )
            ],
        )
        mock_build_from_table.return_value = ("Deployed", mock_report, True)
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "deploy",
                self.pipelines,
                "--table-name",
                "p1_fct_order",
                "--compute-pool-id",
                "test-pool-121",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "Pipeline DEPLOYED" in result.stdout
        mock_build_from_table.assert_called_once()

    def test_undeploy_missing_params(self):
        result = self.runner.invoke(
            app, ["pipeline", "undeploy", self.pipelines, "--no-ack"]
        )
        assert result.exit_code == 1
        assert "table-name or product-name must be provided" in result.stdout

    @patch("shift_left.core.deployment_mgr.full_pipeline_undeploy_from_table")
    def test_undeploy_smoke(self, mock_undeploy):
        mock_undeploy.return_value = "Undeployed p1_fct_order"
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "undeploy",
                self.pipelines,
                "--table-name",
                "p1_fct_order",
                "--no-ack",
            ],
        )
        assert result.exit_code == 0, result.stdout
        assert "Undeployed p1_fct_order" in result.stdout
        mock_undeploy.assert_called_once()

    @patch("shift_left.core.deployment_mgr.statement_mgr.post_flink_statement")
    @patch("shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists")
    def test_prepare_smoke(self, mock_delete, mock_post):
        mock_delete.return_value = "deleted"
        mock_post.return_value = MagicMock()
        sql_path = os.path.join(self.pipelines, "alter_table_avro_debezium.sql")
        result = self.runner.invoke(
            app,
            ["pipeline", "prepare", sql_path, "--compute-pool-id", "test-pool-121"],
        )
        assert result.exit_code == 0, result.stdout
        assert "alter_table_avro_debezium.sql" in result.stdout
        mock_post.assert_called()

    @patch("shift_left.cli_commands.pipeline.field_lineage_module.run_field_lineage_from_table")
    def test_field_lineage_smoke(self, mock_run_lineage):
        with tempfile.TemporaryDirectory() as tmp_dir:
            mock_run_lineage.return_value = tmp_dir
            result = self.runner.invoke(
                app,
                [
                    "pipeline",
                    "field-lineage",
                    "p1_fct_order",
                    self.pipelines,
                    "--output-dir",
                    tmp_dir,
                ],
            )
            assert result.exit_code == 0, result.stdout
            assert "Field lineage written to" in result.stdout
            mock_run_lineage.assert_called_once()

    @patch("shift_left.cli_commands.pipeline.compute_pool_mgr.get_compute_pool_list")
    @patch("shift_left.cli_commands.pipeline.statement_mgr.get_statement_list")
    def test_healthcheck_smoke(self, mock_get_statement_list, mock_get_pool_list):
        mock_get_statement_list.return_value = {
            "dml.p1_fct_order": StatementInfo(
                name="dml.p1_fct_order",
                status_phase="RUNNING",
                compute_pool_id="test-pool-121",
            )
        }
        mock_get_pool_list.return_value = ComputePoolList(
            pools=[
                ComputePoolInfo(
                    id="test-pool-121",
                    name="test-pool",
                    env_id="test-env",
                    max_cfu=10,
                    current_cfu=1,
                )
            ]
        )
        result = self.runner.invoke(
            app, ["pipeline", "healthcheck", "p1", self.pipelines]
        )
        assert result.exit_code == 0, result.stdout
        assert "Health Report" in result.stdout


if __name__ == "__main__":
    unittest.main()
