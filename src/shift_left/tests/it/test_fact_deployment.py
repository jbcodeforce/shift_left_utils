
"""
Copyright 2024-2026 Confluent, Inc.

Live Confluent Cloud integration tests for fact deployment.

05/2026: This is the new approach to do the integration tests. Get minimum test for maximum coverage
and use DebugIT to do chirurgical tests when needed.
"""
import json
import os
import pathlib
import unittest
from it.BaseIT import (
    _assert_cli_ok,
    _assert_no_new_errors,
    _cli_log_path,
    _log_file_size,
    _run_integration_tests,
    _SKIP_MSG,
    dump_effective_config_for_debug,
)
from typer.testing import CliRunner

from shift_left.cli import app
from shift_left.core.utils.app_config import get_config

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONFIG_CCLOUD = _TESTS_ROOT / "config-ccloud.yaml"
_PIPELINES = _TESTS_ROOT / "data" / "flink-project" / "pipelines"


_RUN_IT = _run_integration_tests()


@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestFactPipelineDeployment(unittest.TestCase):
    """Fact pipeline deploy flow against Confluent Cloud (opt-in)."""

    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SL_CONFIG_FILE", str(_CONFIG_CCLOUD))
        os.environ.setdefault("PIPELINES", str(_PIPELINES))

    def setUp(self):
        self.runner = CliRunner(mix_stderr=False)
        self.pipelines = os.environ["PIPELINES"]

    def test_0_version(self):   # 1 verify config is read and loaded
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app, ['version']
        )
        _assert_cli_ok(result, "shift_left version")
        assert 'shift-left CLI version' in result.stdout
        print(f"Validate config is read and loaded: {result.stdout}")
        if os.environ.get("SL_IT_DEBUG_CONFIG", "").lower() in (
            "1",
            "true",
            "yes",
        ):
            print("--- Effective config (redacted); set SL_IT_DEBUG_CONFIG unset to hide ---")
            print(dump_effective_config_for_debug())
        _assert_no_new_errors(log_path, log_start)

    def test_1_delete_all_metadata(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app, ["pipeline", "delete-all-metadata", self.pipelines]
        )
        _assert_cli_ok(result, "pipeline delete-all-metadata")
        assert "Delete pipeline definitions from" in result.stdout
        _assert_no_new_errors(log_path, log_start)

    def test_2_build_inventory(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app, ["table", "build-inventory", self.pipelines]
        )
        _assert_cli_ok(result, "table build-inventory")
        assert "Table inventory created" in result.stdout

        inventory_path = pathlib.Path(self.pipelines) / "inventory.json"
        assert inventory_path.exists(), f"inventory.json not found at {inventory_path}"

        with open(inventory_path, "r") as f:
            inventory = json.load(f)

        # inventory.json is a dict keyed by table name, or a list of objects with name/table_name
        found = False
        if isinstance(inventory, list):
            found = any(
                (item.get("name") == "sl_c360_fct_user_per_group"
                 or item.get("table_name") == "sl_c360_fct_user_per_group")
                for item in inventory
                if isinstance(item, dict)
            )
        elif isinstance(inventory, dict):
            if "tables" in inventory and isinstance(inventory["tables"], list):
                found = any(
                    (t.get("name") == "sl_c360_fct_user_per_group"
                     or t.get("table_name") == "sl_c360_fct_user_per_group")
                    for t in inventory["tables"]
                    if isinstance(t, dict)
                )
            else:
                found = "sl_c360_fct_user_per_group" in inventory

        assert found, "sl_c360_fct_user_per_group not found in inventory.json"
        _assert_no_new_errors(log_path, log_start)

    def test_3_build_all_metadata(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app, ["pipeline", "build-all-metadata", self.pipelines]
        )
        _assert_cli_ok(result, "pipeline build-all-metadata")
        assert "Build all pipeline definitions for all tables" in result.stdout
        _assert_no_new_errors(log_path, log_start)
        pipeline_definition_path =  pathlib.Path(self.pipelines) / "facts" / "c360" / "fct_user_per_group" / "pipeline_definition.json"
        assert pipeline_definition_path.exists(), f"pipeline_definition.json not found at {pipeline_definition_path}"

    def test_4_build_execution_plan(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        result = self.runner.invoke(
            app, ["pipeline", "build-execution-plan",
                  self.pipelines,
                  "--table-name",
                "sl_c360_fct_user_per_group",
                "--compute-pool-id",
                compute_pool_id,]
        )
        _assert_cli_ok(result, "pipeline build-execution-plan")
        assert "Build an execution plan" in result.stdout
        print(f"Build execution plan: {result.stdout}")
        _assert_no_new_errors(log_path, log_start)

    def test_4_1_prepare_tables(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        # first is to deploy the orphan table to get the statement name
        result = self.runner.invoke(
            app,
            ["pipeline", "deploy", self.pipelines, "--table-name", "orphan", "--compute-pool-id", compute_pool_id],
        )
        _assert_cli_ok(result, "pipeline deploy orphan")
        assert "Pipeline DEPLOYED" in result.stdout
        print(f"Pipeline deployed: {result.stdout}")

        result = self.runner.invoke(
            app,
            ["pipeline", "prepare", self.pipelines + "/test_prepare_tables_integration.sql", "--compute-pool-id", compute_pool_id],
        )
        _assert_cli_ok(result, "pipeline prepare")
        print(f"Prepare tables: {result.stdout}")
        _assert_no_new_errors(log_path, log_start)

    def test_5_deploy_pipeline(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "deploy",
                self.pipelines,
                "--table-name",
                "sl_c360_fct_user_per_group",
                "--compute-pool-id",
                compute_pool_id,
            ],
        )
        _assert_cli_ok(result, "pipeline deploy sl_c360_fct_user_per_group")
        assert "Pipeline DEPLOYED" in result.stdout
        print(f"Pipeline deployed: {result.stdout}")
        _assert_no_new_errors(log_path, log_start)

    def test_6_report_running_statements(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "report-running-statements",
                self.pipelines,
                "--table-name",
                "sl_c360_fct_user_per_group",
            ],
        )
        _assert_cli_ok(result, "pipeline report-running-statements sl_c360_fct_user_per_group")
        assert "Assess running Flink DMLs" in result.stdout
        _assert_no_new_errors(log_path, log_start)

    def test_7_undeploy_product(self):
        log_path = _cli_log_path()
        log_start = _log_file_size(log_path)
        result = self.runner.invoke(
            app,
            [
                "pipeline",
                "undeploy",
                self.pipelines,
                "--product-name",
                "c360",
                "--no-ack",
            ],
        )
        _assert_cli_ok(result, "pipeline undeploy c360")
        print(f"Undeployed pipeline: {result.stdout}")


if __name__ == "__main__":
    unittest.main()
