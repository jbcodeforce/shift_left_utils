"""Unit tests for flink_sql_adapter submit paths and deployment error helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from confluent_sql.exceptions import OperationalError

import shift_left.core.deployment_mgr as deployment_dm
from shift_left.core.deployment_mgr import FlinkStatementNode
from shift_left.core.models.flink_statement_model import ErrorData, StatementError
from shift_left.core.utils.flink_sql_adapter import submit_flink_statement


@pytest.fixture
def minimal_flink_config() -> dict:
    return {
        "confluent_cloud": {
            "environment_id": "env-test",
            "organization_id": "org-test",
            "cloud_region": "us-east-1",
        },
        "flink": {
            "api_key": "test-key",
            "api_secret": "test-secret",
            "compute_pool_id": "lfcp-test",
            "database_name": "default",
            "catalog_name": "env-test",
            "submission_timeout_seconds": "60",
            "poll_timer": "1",
        },
    }


def test_submit_flink_statement_operational_error_includes_http_status(minimal_flink_config: dict) -> None:
    mock_conn = MagicMock()
    with patch("shift_left.core.utils.flink_sql_adapter.flink_connection") as mock_flink_cm:
        mock_flink_cm.return_value.__enter__.return_value = mock_conn
        with patch(
            "shift_left.core.utils.flink_sql_adapter._submit_inner",
            side_effect=OperationalError("rate limited", 429),
        ):
            result = submit_flink_statement(
                minimal_flink_config,
                compute_pool_id="lfcp-test",
                statement_name="stmt-1",
                sql_content="CREATE TABLE foo (i INT)",
                properties={"sql.current-catalog": "c", "sql.current-database": "d"},
            )
    assert isinstance(result, StatementError)
    assert len(result.errors) == 1
    assert "rate limited" in result.errors[0].detail
    assert "HTTP 429" in result.errors[0].detail


def test_submit_flink_statement_operational_error_no_http_suffix_when_none(minimal_flink_config: dict) -> None:
    mock_conn = MagicMock()
    with patch("shift_left.core.utils.flink_sql_adapter.flink_connection") as mock_flink_cm:
        mock_flink_cm.return_value.__enter__.return_value = mock_conn
        with patch(
            "shift_left.core.utils.flink_sql_adapter._submit_inner",
            side_effect=OperationalError("connection reset"),
        ):
            result = submit_flink_statement(
                minimal_flink_config,
                compute_pool_id="lfcp-test",
                statement_name="stmt-2",
                sql_content="CREATE TABLE bar (i INT)",
                properties={"sql.current-catalog": "c", "sql.current-database": "d"},
            )
    assert isinstance(result, StatementError)
    assert result.errors[0].detail == "connection reset"


def test_statement_error_user_message_joins_entries() -> None:
    err = StatementError(
        errors=[
            ErrorData(id="a", status="FAILED", detail="first"),
            ErrorData(id="b", status="FAILED", detail="second"),
        ]
    )
    msg = deployment_dm._statement_error_user_message(err)
    assert "a [FAILED]: first" in msg
    assert "b [FAILED]: second" in msg


def test_statement_error_user_message_empty_errors() -> None:
    err = StatementError(errors=[])
    msg = deployment_dm._statement_error_user_message(err)
    assert "no error details" in msg.lower()


@patch.object(deployment_dm.statement_mgr, "delete_statement_if_exists")
@patch.object(deployment_dm.compute_pool_mgr, "save_compute_pool_info_in_metadata")
@patch.object(deployment_dm.statement_mgr, "build_and_deploy_flink_statement_from_sql_content")
def test_deploy_dml_statement_error_skips_completed_message(
    mock_build: MagicMock,
    mock_save: MagicMock,
    mock_del: MagicMock,
) -> None:
    mock_build.return_value = StatementError(
        errors=[ErrorData(id="dml-x", status="FAILED", detail="API refused")]
    )
    node = FlinkStatementNode(
        table_name="mytbl",
        dml_statement_name="dml-s",
        dml_ref="x.sql",
        compute_pool_id="pool",
    )
    with patch("builtins.print") as mock_print:
        out = deployment_dm._deploy_dml(node, dml_already_deleted=True)
    assert isinstance(out, StatementError)
    printed = " ".join(c.args[0] for c in mock_print.call_args_list if c.args)
    assert "DML deployment completed" not in printed
    assert "DML submit failed" in printed
    assert "API refused" in printed
