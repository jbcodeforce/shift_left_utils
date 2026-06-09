"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
import tempfile
import json
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
os.environ.setdefault("SL_CONFIG_FILE", str(_TESTS_ROOT / "config.yaml"))
# Dummy credentials so get_config()/validate_config succeed when pytest imports CLI modules
# (integration_test_mgr and others call get_config() at import time).
if not os.environ.get("SL_CONFLUENT_CLOUD_API_KEY"):
    os.environ.setdefault("SL_KAFKA_API_KEY", "test")
    os.environ.setdefault("SL_KAFKA_API_SECRET", "test")
    os.environ.setdefault("SL_CONFLUENT_CLOUD_API_KEY", "test")
    os.environ.setdefault("SL_CONFLUENT_CLOUD_API_SECRET", "test")
    os.environ.setdefault("SL_FLINK_API_KEY", "test")
    os.environ.setdefault("SL_FLINK_API_SECRET", "test")
    os.environ.setdefault("SL_CCLOUD_KAFKA_CLUSTER_ID", "lkc-test")
    os.environ.setdefault("SL_CLOUD_PROVIDER", "aws")
    os.environ.setdefault("SL_CLOUD_REGION", "us-west-2")
    os.environ.setdefault("SL_CLOUD_ORGANIZATION_ID", "id-org-test")
    os.environ.setdefault("SL_FLINK_ENV_ID", "env-nknqp3")
    os.environ.setdefault("SL_CONFLUENT_PRINCIPAL_ID", "sa-test")
    os.environ.setdefault("SL_FLINK_COMPUTE_POOL_ID", "lfcp-xvrvmz")
    os.environ.setdefault("SL_FLINK_ENV_NAME", "j9r-env")
    os.environ.setdefault("SL_FLINK_DATABASE_NAME", "j9r-kafka")

from shift_left.core.utils.app_config import get_config
from shift_left.cli_commands.project import app
import shift_left.core.pipeline_mgr as pm
import subprocess
from ut.core.BaseUT import BaseUT
class TestProjectCLI(BaseUT):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runner = CliRunner()

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES",""))

    @classmethod
    def tearDownClass(cls):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


    def test_init_project(self):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        result = self.runner.invoke(app, [ "init", "project_test_via_cli", str(temp_dir)])
        print(result.stdout)
        assert result.exit_code == 0
        assert "Project project_test_via_cli created in " in result.stdout
        assert os.path.exists(temp_dir / "project_test_via_cli")
        assert os.path.exists(temp_dir / "project_test_via_cli/pipelines")

    def test_init_data_product_project(self):
        temp_dir = pathlib.Path(__file__).parent / "../tmp"
        result = self.runner.invoke(
            app,
            ["init", "project_data_product_cli", str(temp_dir), "--project-type", "data_product"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_dir / "project_data_product_cli/pipelines/data_product_1")


    # ------------- Config -------------
    #
    def test_validate_current_config(self):
        result = self.runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert result.exit_code == 0
        assert "validated" in result.stdout
        assert "./tests/config" in result.stdout


    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_valid(self, mock_get_config):
        """Test validate_config with a valid configuration"""
        # Create a valid config for testing
        valid_config = {
            "kafka": {
                "bootstrap.servers": "test_bootstrap_servers:9092",
                "src_topic_prefix": "test_prefix",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "cloud_region": "us-west-2",
                "cloud_provider": "aws",
                "organization_id": "org-123",
                "service_account_id": "sa-123",
                "api_key": "test_key",
                "api_secret": "test_secret",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": 10,
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": 60,
                "timezone": "UTC",
                "logging": "INFO",
                "data_limit_column_name_to_select_from": "created_at",
                "products": ["product1", "product2"],
                "accepted_common_products": ["common1"],
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }

        # Mock get_config to return our test configuration
        mock_get_config.return_value = valid_config

        result = self.runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert result.exit_code == 0
        assert "validated" in result.stdout
        config_file = os.environ.get("SL_CONFIG_FILE", "config.yaml")
        assert config_file in result.stdout
        assert "Configuration validation failed" not in result.stdout

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_missing_sections(self, mock_get_config):
        """Test validate_config with missing required sections"""
        # Create config missing required sections
        invalid_config = {
            "kafka": {
                "src_topic_prefix": "test_prefix"
            }
            # Missing confluent_cloud, flink, app sections
        }

        # Mock get_config to return our test configuration
        mock_get_config.return_value = invalid_config

        result = self.runner.invoke(app, ["validate-config"])
        # Should still exit with 0 but show validation errors in output
        assert "Configuration validation failed" in result.stdout
        assert "missing confluent_cloud section" in result.stdout
        assert "missing flink section" in result.stdout
        assert "missing app section" in result.stdout

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_placeholder_values(self, mock_get_config):
        """Test validate_config with placeholder values that need to be replaced"""
        # Create config with placeholder values
        config_with_placeholders = {
            "kafka": {
                 "bootstrap.servers": "test_bootstrap_servers:9092",
                "src_topic_prefix": "<TO_FILL>",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-123",
                "api_key": "<kafka-api-key>",
                "api_secret": "<kafka-api-key_secret>",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": 10,
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": 60,
                "timezone": "UTC",
                "logging": "INFO",
                "data_limit_column_name_to_select_from": "created_at",
                "products": ["product1"],
                "accepted_common_products": ["common1"],
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }

        # Mock get_config to return our test configuration
        mock_get_config.return_value = config_with_placeholders

        result = self.runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert "Configuration validation failed" in result.stdout
        assert "placeholder value '<TO_FILL>'" in result.stdout


    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_invalid_data_types(self, mock_get_config):
        """Test validate_config with invalid data types"""
        # Create config with invalid data types
        invalid_types_config = {
            "kafka": {
                "src_topic_prefix": "test_prefix",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-123",
                "api_key": "test_key",
                "api_secret": "test_secret",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": "not_a_number",  # Should be numeric
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": "not_a_number",  # Should be numeric
                "timezone": "UTC",
                "logging": "INVALID_LEVEL",  # Should be valid log level
                "data_limit_column_name_to_select_from": "created_at",
                "products": "not_a_list",  # Should be a list
                "accepted_common_products": "not_a_list",  # Should be a list
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }

        # Mock get_config to return our test configuration
        mock_get_config.return_value = invalid_types_config

        result = self.runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert "Configuration validation failed" in result.stdout
        assert "must be a number" in result.stdout
        assert "must be a valid log level" in result.stdout
        assert "must be a list" in result.stdout

    # ------------- table management within a project -------------
    #
    def test_list_tables_with_one_child(self):
        result = self.runner.invoke(app, [ "list-tables-with-one-child"])
        print(result.stdout)
        assert result.exit_code == 0
        assert "sl_c360_dim_users" in result.stdout
        assert "sl_c360_dim_groups" in result.stdout
        assert "tables_with_one_child.txt" in result.stdout

    def test_report_table_cross_products(self):
        result = self.runner.invoke(app, [ "report-table-cross-products"])
        print(result.stdout)
        assert result.exit_code == 0
        assert "sl_cmn_src_tenants" in result.stdout
        assert "table_cross_products.txt" in result.stdout

    @patch("shift_left.core.project_manager.statement_mgr.get_statement")
    @patch("shift_left.core.project_manager.subprocess.run")
    def test_list_modified_files_success(self, mock_subprocess_run, mock_get_statement):
        """Test list_modified_files command with mocked git and temp project (no real git)."""
        from shift_left.core.models.flink_statement_model import Statement, Spec, Status

        mock_statement = MagicMock(spec=Statement)
        mock_statement.spec = MagicMock(spec=Spec)
        mock_statement.spec.statement = "INSERT INTO sl_c360_src_users SELECT 1"
        mock_statement.status = MagicMock(spec=Status)
        mock_statement.status.phase = "RUNNING"
        mock_get_statement.return_value = mock_statement

        with tempfile.TemporaryDirectory() as project_tmp:
            pipelines = pathlib.Path(project_tmp) / "pipelines"
            (pipelines / "sources/c360/src_users/sql-scripts").mkdir(parents=True)
            (pipelines / "facts/c360/fct_user_per_group/sql-scripts").mkdir(parents=True)
            (pipelines / "sources/c360/src_users/sql-scripts").joinpath("dml.src_c360_users.sql").write_text(
                "INSERT INTO sl_c360_src_users SELECT 1"
            )
            (pipelines / "facts/c360/fct_user_per_group/sql-scripts").joinpath(
                "ddl.c360_fct_user_per_group.sql"
            ).write_text("CREATE TABLE c360_fct_user_per_group (id INT)")

            with tempfile.TemporaryDirectory() as output_tmp:
                out_shift_left = pathlib.Path(output_tmp) / ".shift_left"
                out_shift_left.mkdir()
                old_home = os.environ.get("HOME")
                try:
                    os.environ["HOME"] = output_tmp
                    _modified_log = get_config().get("app", {}).get(
                        "modified_flink_files_file_name", "modified_flink_files.txt"
                    )
                    output_txt = out_shift_left / _modified_log

                    mock_subprocess_run.side_effect = [
                        MagicMock(stdout="feature-branch\n", stderr="", returncode=0),
                        MagicMock(stdout="", stderr="", returncode=0),
                        MagicMock(
                            stdout=(
                                "pipelines/sources/c360/src_users/sql-scripts/dml.src_c360_users.sql\n"
                                "pipelines/facts/c360/fct_user_per_group/sql-scripts/ddl.c360_fct_user_per_group.sql\n"
                                "src/some_file.py\ndocs/readme.md\n"
                            ),
                            stderr="",
                            returncode=0,
                        ),
                        MagicMock(stdout="abc123\n", stderr="", returncode=0),
                        MagicMock(
                            stdout="CREATE TABLE c360_fct_user_per_group (id INT)",
                            stderr="",
                            returncode=0,
                        ),
                    ]

                    result = self.runner.invoke(
                        app,
                        [
                            "list-modified-files",
                            "main",
                            "--file-filter", ".sql",
                            "--project-path", project_tmp,
                        ],
                    )

                    assert result.exit_code == 0
                    assert "Found 4 total modified files" in result.stdout
                    assert "Found 2 modified files matching filter '.sql'" in result.stdout

                    assert output_txt.exists()
                    content = output_txt.read_text()
                    assert "sl_c360_src_users" in content
                    assert "c360_fct_user_per_group" in content
                    mock_get_statement.assert_called()
                finally:
                    if old_home is not None:
                        os.environ["HOME"] = old_home
                    else:
                        os.environ.pop("HOME", None)

    @patch("shift_left.core.project_manager.get_git_baseline_content")
    @patch("shift_left.core.project_manager.subprocess.run")
    def test_list_modified_files_schema_diff(self, mock_subprocess_run, mock_baseline):
        """Production DDL modifications include schema_diff with added columns."""
        mock_baseline.return_value = (
            "CREATE TABLE sl_raw_transactions (txn_id STRING NOT NULL, amount DECIMAL)",
            "since:2025-01-01",
        )
        with tempfile.TemporaryDirectory() as project_tmp:
            pipelines = pathlib.Path(project_tmp) / "pipelines"
            ddl_dir = pipelines / "seeds/common/raw_transactions/sql-scripts"
            ddl_dir.mkdir(parents=True)
            ddl_path = ddl_dir / "ddl.raw_transactions.sql"
            ddl_path.write_text(
                "CREATE TABLE sl_raw_transactions ("
                "txn_id STRING NOT NULL, amount DECIMAL, currency STRING)"
            )
            with tempfile.TemporaryDirectory() as output_tmp:
                out_shift_left = pathlib.Path(output_tmp) / ".shift_left"
                out_shift_left.mkdir()
                old_home = os.environ.get("HOME")
                try:
                    os.environ["HOME"] = output_tmp
                    json_path = out_shift_left / "modified_flink_files.json"
                    mock_subprocess_run.side_effect = [
                        MagicMock(stdout="develop\n", stderr="", returncode=0),
                        MagicMock(
                            stdout=(
                                "pipelines/seeds/common/raw_transactions/sql-scripts/"
                                "ddl.raw_transactions.sql\n"
                            ),
                            stderr="",
                            returncode=0,
                        ),
                    ]
                    result = self.runner.invoke(
                        app,
                        [
                            "list-modified-files",
                            "develop",
                            "--file-filter", ".sql",
                            "--project-path", project_tmp,
                            "--since", "2025-01-01",
                            "--baseline", "since",
                        ],
                    )
                    assert result.exit_code == 0
                    assert json_path.exists()
                    payload = json.loads(json_path.read_text())
                    ddl_entries = [
                        f for f in payload["file_list"]
                        if f["table_name"] == "sl_raw_transactions"
                    ]
                    assert len(ddl_entries) == 1
                    schema_diff = ddl_entries[0]["schema_diff"]
                    assert schema_diff["baseline_ref"] == "since:2025-01-01"
                    assert schema_diff["added"] == ["currency"]
                    assert schema_diff["removed"] == []
                    assert schema_diff["modified"] == []
                    mock_baseline.assert_called_once()
                finally:
                    if old_home is not None:
                        os.environ["HOME"] = old_home
                    else:
                        os.environ.pop("HOME", None)

    @patch('shift_left.core.project_manager.subprocess.run')
    def test_list_modified_files_no_matches(self, mock_subprocess_run):
        """Test list_modified_files when no files match the filter (mocked git, no real git)."""
        runner = CliRunner()

        with tempfile.TemporaryDirectory() as output_tmp:
            out_shift_left = pathlib.Path(output_tmp) / ".shift_left"
            out_shift_left.mkdir()
            old_home = os.environ.get("HOME")
            try:
                os.environ["HOME"] = output_tmp
                _modified_log = get_config().get("app", {}).get(
                    "modified_flink_files_file_name", "modified_flink_files.txt"
                )
                output_txt = out_shift_left / _modified_log

                # Mock git log output: no .sql under pipelines
                mock_subprocess_run.side_effect = [
                    MagicMock(stdout="feature-branch\n", stderr="", returncode=0),
                    MagicMock(stdout="main\n", stderr="", returncode=0),
                    MagicMock(
                        stdout="src/some_file.py\ndocs/readme.md\nconfig.yaml\n",
                        stderr="",
                        returncode=0,
                    ),
                ]

                result = runner.invoke(app, [
                    "list-modified-files",
                    "main",
                    "--file-filter", ".sql",
                ])

                assert result.exit_code == 0
                assert "Total modified files: 0" in result.stdout
                assert "Found 3 total modified files" in result.stdout
                assert "Found 0 modified files matching filter '.sql'" in result.stdout
                assert output_txt.exists()
                assert output_txt.read_text() == ""
            finally:
                if old_home is not None:
                    os.environ["HOME"] = old_home
                else:
                    os.environ.pop("HOME", None)

    @patch('shift_left.core.project_manager.subprocess.run')
    def test_list_modified_files_git_error(self, mock_subprocess_run):
        """Test list_modified_files when git fails (mocked, no real git)."""
        runner = CliRunner()
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            128, "git rev-parse", stderr="fatal: not a git repository"
        )

        result = runner.invoke(app, ["list-modified-files", "main"])

        assert result.exit_code == 1
        assert "Git command failed" in result.stdout

    def test_list_impacted_tables_from_fixture(self):
        """Test list_impacted_tables via CLI with real project_manager integration."""
        pipelines_root = os.environ["PIPELINES"]
        modified_files = {
            "description": "Modified files in branch:'cc-client'",
            "file_list": [
                {
                    "table_name": "sl_raw_groups",
                    "file_modified_url": os.path.join(
                        pipelines_root,
                        "seeds/c360/raw_groups/sql-scripts/ddl.raw_groups.sql",
                    ),
                    "same_sql_content": False,
                    "running": False,
                    "new_table_name": "sl_raw_groups",
                },
                {
                    "table_name": "sl_c360_src_groups",
                    "file_modified_url": os.path.join(
                        pipelines_root,
                        "sources/c360/src_groups/sql-scripts/ddl.src_c360_groups.sql",
                    ),
                    "same_sql_content": False,
                    "running": False,
                    "new_table_name": "sl_c360_src_groups",
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            modified_json = pathlib.Path(tmp) / "modified_flink_files.json"
            modified_json.write_text(json.dumps(modified_files))
            output_json = pathlib.Path(tmp) / "impacted_tables.json"

            result = self.runner.invoke(
                app,
                [
                    "list-impacted-tables",
                    "--modified-files-path",
                    str(modified_json),
                    "--project-path",
                    pipelines_root,
                    "--output-file",
                    str(output_json),
                ],
            )

            assert result.exit_code == 0, result.stdout
            assert output_json.exists()
            payload = json.loads(output_json.read_text())
            assert set(payload["ddl_modified_tables"]) == {"sl_raw_groups", "sl_c360_src_groups"}
            assert "sl_c360_dim_groups" in payload["tables"]
            assert "sl_c360_fct_user_per_group" in payload["tables"]
            assert "Impacted tables saved to" in result.stdout
            ut_paths = " ".join(payload["unit_test_ddl_paths"])
            assert "dimensions/c360/dim_groups/tests/ddl_src_c360_groups.sql" in ut_paths

    def test_list_impacted_tables_missing_pipelines(self):
        """Test list_impacted_tables exits when PIPELINES is unset and no --project-path."""
        with tempfile.TemporaryDirectory() as tmp:
            modified_json = pathlib.Path(tmp) / "modified_flink_files.json"
            modified_json.write_text(json.dumps({"description": "test", "file_list": []}))
            old_pipelines = os.environ.pop("PIPELINES", None)
            try:
                result = self.runner.invoke(
                    app,
                    ["list-impacted-tables", "--modified-files-path", str(modified_json)],
                )
                assert result.exit_code != 0
                assert "set PIPELINES or pass --project-path" in result.stdout
            finally:
                if old_pipelines is not None:
                    os.environ["PIPELINES"] = old_pipelines

    def test_update_ut_ddl_syncs_foundation_ddl(self):
        """Foundation UT DDL is refreshed from production DDL via update-ut-ddl CLI."""
        pipelines_root = os.environ["PIPELINES"]
        ut_relative = "dimensions/c360/dim_groups/tests/ddl_src_c360_groups.sql"
        ut_path = os.path.join(pipelines_root, ut_relative)
        with open(ut_path, "r") as f:
            original_ut_ddl = f.read()
        stripped_ut_ddl = original_ut_ddl.replace("  updated_at TIMESTAMP,\n", "")

        with tempfile.TemporaryDirectory() as tmp:
            impacted_json = pathlib.Path(tmp) / "impacted_tables.json"
            impacted_json.write_text(
                json.dumps(
                    {
                        "ddl_modified_tables": ["sl_c360_src_groups"],
                        "impacted_tables": ["sl_c360_dim_groups"],
                        "production_ddl_paths": [],
                        "unit_test_ddl_paths": [ut_relative],
                    }
                )
            )
            try:
                with open(ut_path, "w") as f:
                    f.write(stripped_ut_ddl)

                result = self.runner.invoke(
                    app,
                    ["update-ut-ddl", str(impacted_json), "--full-replace", "--project-path", pipelines_root],
                )

                assert result.exit_code == 0, result.stdout
                with open(ut_path, "r") as f:
                    synced = f.read()
                assert "updated_at TIMESTAMP" in synced
                post_fix = get_config().get("app", {}).get("post_fix_unit_test", "_ut")
                assert f"sl_c360_src_groups{post_fix}" in synced
            finally:
                with open(ut_path, "w") as f:
                    f.write(original_ut_ddl)

    def test_update_ut_ddl_skips_empty_ddl_modified(self):
        """update-ut-ddl succeeds when ddl_modified_tables is empty."""
        pipelines_root = os.environ["PIPELINES"]
        with tempfile.TemporaryDirectory() as tmp:
            impacted_json = pathlib.Path(tmp) / "impacted_tables.json"
            impacted_json.write_text(
                json.dumps(
                    {
                        "ddl_modified_tables": [],
                        "impacted_tables": [],
                        "production_ddl_paths": [],
                        "unit_test_ddl_paths": [],
                    }
                )
            )
            result = self.runner.invoke(
                app,
                ["update-ut-ddl", str(impacted_json), "--project-path", pipelines_root],
            )
            assert result.exit_code == 0, result.stdout

    @patch("shift_left.core.project_manager.ensure_git_branch")
    @patch("shift_left.core.project_manager._production_column_context")
    def test_update_ut_ddl_merge_columns_cli(self, mock_prod_context, mock_git_branch):
        """update-ut-ddl with --git-branch merges UT DDL, insert, and validation files."""
        mock_git_branch.return_value = None
        pipelines_root = os.environ["PIPELINES"]
        ut_ddl_rel = "dimensions/c360/dim_groups/tests/ddl_src_c360_groups.sql"
        insert_rel = "dimensions/c360/dim_groups/tests/insert_src_c360_groups_1.sql"
        validate_rel = "dimensions/c360/dim_groups/tests/validate_c360_dim_groups_1.sql"
        paths = {
            ut_ddl_rel: os.path.join(pipelines_root, ut_ddl_rel),
            insert_rel: os.path.join(pipelines_root, insert_rel),
            validate_rel: os.path.join(pipelines_root, validate_rel),
        }
        originals = {rel: pathlib.Path(path).read_text() for rel, path in paths.items()}

        with tempfile.TemporaryDirectory() as tmp:
            modified_json = pathlib.Path(tmp) / "modified_flink_files.json"
            impacted_json = pathlib.Path(tmp) / "impacted_tables.json"
            modified_json.write_text(
                json.dumps(
                    {
                        "description": "test",
                        "file_list": [
                            {
                                "table_name": "sl_c360_src_groups",
                                "file_modified_url": "ignored",
                                "same_sql_content": False,
                                "running": False,
                                "new_table_name": "sl_c360_src_groups",
                                "schema_diff": {
                                    "baseline_ref": "since:2025-01-01",
                                    "added": ["created_by", "updated_by"],
                                    "removed": [],
                                    "modified": [],
                                },
                            },
                            {
                                "table_name": "sl_c360_dim_groups",
                                "file_modified_url": "ignored",
                                "same_sql_content": False,
                                "running": False,
                                "new_table_name": "sl_c360_dim_groups",
                                "schema_diff": {
                                    "baseline_ref": "since:2025-01-01",
                                    "added": ["audit_source"],
                                    "removed": [],
                                    "modified": [],
                                },
                            },
                        ],
                    }
                )
            )
            impacted_json.write_text(
                json.dumps(
                    {
                        "ddl_modified_tables": [
                            "sl_c360_src_groups",
                            "sl_c360_dim_groups",
                        ],
                        "impacted_tables": ["sl_c360_dim_groups"],
                        "production_ddl_paths": [],
                        "unit_test_ddl_paths": [ut_ddl_rel],
                    }
                )
            )

            def prod_context_side_effect(table, inventory, parser):
                if table == "sl_c360_src_groups":
                    return (
                        {
                            "created_by": {"type": "STRING"},
                            "updated_by": {"type": "STRING"},
                        },
                        {
                            "created_by": "created_by STRING",
                            "updated_by": "updated_by STRING",
                        },
                    )
                if table == "sl_c360_dim_groups":
                    return (
                        {"audit_source": {"type": "STRING"}},
                        {"audit_source": "audit_source STRING"},
                    )
                return ({}, {})

            mock_prod_context.side_effect = prod_context_side_effect

            try:
                result = self.runner.invoke(
                    app,
                    [
                        "update-ut-ddl",
                        str(impacted_json),
                        "--git-branch",
                        "test/ut-schema-merge",
                        "--modified-files-path",
                        str(modified_json),
                        "--project-path",
                        pipelines_root,
                    ],
                )
                assert result.exit_code == 0, result.stdout
                ut_ddl = pathlib.Path(paths[ut_ddl_rel]).read_text()
                insert_sql = pathlib.Path(paths[insert_rel]).read_text()
                validate_sql = pathlib.Path(paths[validate_rel]).read_text()
                assert "created_by STRING" in ut_ddl
                assert "`created_by`" in insert_sql
                assert "audit_source" in validate_sql
                assert "audit_source_check" in validate_sql
                mock_git_branch.assert_called_once()
            finally:
                for rel, path in paths.items():
                    pathlib.Path(path).write_text(originals[rel])

    @patch("shift_left.core.project_manager.ConfluentCloudClient")
    def test_list_topics_writes_file(self, mock_client_cls):
        """Test list-topics CLI writes topic_list.txt from ConfluentCloudClient."""
        mock_client_cls.return_value.list_topics.return_value = {
            "data": [{"cluster_id": "lkc-1", "topic_name": "orders", "partitions_count": 6}]
        }
        with tempfile.TemporaryDirectory() as tmp:
            result = self.runner.invoke(app, ["list-topics", tmp])
            assert result.exit_code == 0, result.stdout
            topic_file = pathlib.Path(tmp) / "topic_list.txt"
            assert topic_file.exists()
            assert "orders" in topic_file.read_text()
            assert "Topic list saved in" in result.stdout

    def test_isolate_data_product(self):
        """Test isolate-data-product CLI copies product hierarchy to target folder."""
        pipelines_root = os.environ["PIPELINES"]
        with tempfile.TemporaryDirectory() as tgt:
            result = self.runner.invoke(
                app,
                ["isolate-data-product", "c360", pipelines_root, tgt],
            )
            assert result.exit_code == 0, result.stdout
            assert os.path.exists(
                os.path.join(tgt, "pipelines/facts/c360/fct_user_per_group/sql-scripts/dml.c360_fct_user_per_group.sql")
            )
            assert "Data product isolated in" in result.stdout

    @patch("shift_left.core.project_manager.update_tables_version")
    def test_update_tables_version(self, mock_update):
        """Smoke test update-tables-version CLI wiring."""
        mock_update.return_value = set()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                {
                    "file_list": [
                        {
                            "table_name": "f",
                            "file_modified_url": "/facts/p2/f/sql-scripts/dml.f.sql",
                            "same_sql_content": False,
                            "running": False,
                            "new_table_name": "f",
                        }
                    ]
                },
                f,
            )
            path = f.name
        try:
            result = self.runner.invoke(app, ["update-tables-version", path])
            assert result.exit_code == 0, result.stdout
            mock_update.assert_called_once()
        finally:
            os.unlink(path)

    # ------------- compute pool management -------------
    @patch("shift_left.cli_commands.project.compute_pool_mgr.delete_all_compute_pools_of_product")
    def test_delete_all_compute_pools_without_list_file(self, mock_delete_all):
        result = self.runner.invoke(app, ["delete-all-compute-pools", "mv"])
        assert result.exit_code == 0, result.stdout
        mock_delete_all.assert_called_once_with("mv")

    @patch("shift_left.cli_commands.project.compute_pool_mgr.delete_compute_pools_by_ids")
    def test_delete_all_compute_pools_with_list_file_only(self, mock_delete_by_ids):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("lfcp-ab0\n# comment\nlfcp-ab1\n")
            list_file = f.name
        try:
            result = self.runner.invoke(
                app,
                ["delete-all-compute-pools", "--compute-pool-list-file", list_file],
            )
            assert result.exit_code == 0, result.stdout
            mock_delete_by_ids.assert_called_once_with(["lfcp-ab0", "lfcp-ab1"], product_name=None)
        finally:
            os.unlink(list_file)

    @patch("shift_left.cli_commands.project.compute_pool_mgr.delete_compute_pools_by_ids")
    def test_delete_all_compute_pools_with_list_file_and_product(self, mock_delete_by_ids):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("lfcp-ab0\n")
            list_file = f.name
        try:
            result = self.runner.invoke(
                app,
                ["delete-all-compute-pools", "mv", "--compute-pool-list-file", list_file],
            )
            assert result.exit_code == 0, result.stdout
            mock_delete_by_ids.assert_called_once_with(["lfcp-ab0"], product_name="mv")
        finally:
            os.unlink(list_file)

    def test_delete_all_compute_pools_requires_product_or_list_file(self):
        result = self.runner.invoke(app, ["delete-all-compute-pools"])
        assert result.exit_code != 0
        assert "provide PRODUCT_NAME and/or --compute-pool-list-file" in result.stdout

    def test_delete_all_compute_pools_missing_list_file(self):
        result = self.runner.invoke(
            app,
            ["delete-all-compute-pools", "--compute-pool-list-file", "/nonexistent/pools.txt"],
        )
        assert result.exit_code != 0


if __name__ == '__main__':
    unittest.main()
