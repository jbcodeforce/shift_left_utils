"""Unit tests for DDL schema diff utilities."""
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from shift_left.core.utils.ddl_schema_diff import (
    diff_column_metadata,
    get_git_baseline_content,
)
from shift_left.core.utils.sql_parser import SQLparser


def _col(name: str, col_type: str = "STRING", nullable: bool = True, primary_key: bool = False):
    return {
        "name": name,
        "type": col_type,
        "nullable": nullable,
        "primary_key": primary_key,
    }


class TestDiffColumnMetadata:
    def test_added_columns(self):
        old = {"id": _col("id", nullable=False)}
        new = {"id": _col("id", nullable=False), "region": _col("region")}
        result = diff_column_metadata(old, new, "since:2025-01-01")
        assert result.added == ["region"]
        assert result.removed == []
        assert result.modified == []
        assert result.baseline_ref == "since:2025-01-01"

    def test_removed_columns(self):
        old = {"id": _col("id"), "legacy": _col("legacy")}
        new = {"id": _col("id")}
        result = diff_column_metadata(old, new, "branch:main")
        assert result.added == []
        assert result.removed == ["legacy"]
        assert result.modified == []

    def test_modified_column_type(self):
        old = {"amount": _col("amount", "INT")}
        new = {"amount": _col("amount", "DECIMAL")}
        result = diff_column_metadata(old, new, "since:2025-01-01")
        assert result.added == []
        assert result.removed == []
        assert len(result.modified) == 1
        assert result.modified[0].name == "amount"
        assert result.modified[0].old_type == "INT"
        assert result.modified[0].new_type == "DECIMAL"

    def test_modified_nullable_and_primary_key(self):
        old = {"id": _col("id", nullable=True, primary_key=False)}
        new = {"id": _col("id", nullable=False, primary_key=True)}
        result = diff_column_metadata(old, new, "since:2025-01-01")
        assert len(result.modified) == 1
        m = result.modified[0]
        assert m.old_nullable is True
        assert m.new_nullable is False
        assert m.old_primary_key is False
        assert m.new_primary_key is True

    def test_empty_baseline_all_added(self):
        new = {"id": _col("id"), "name": _col("name")}
        result = diff_column_metadata({}, new, "since:2025-01-01")
        assert sorted(result.added) == ["id", "name"]
        assert result.removed == []

    def test_opaque_row_type_change_reported_as_modified(self):
        """Top-level ROW columns are compared as opaque type strings."""
        old = {"after": _col("after", "ROW<TENANT_ID STRING>")}
        new = {"after": _col("after", "ROW<TENANT_ID STRING, TENANT_REGION STRING>")}
        result = diff_column_metadata(old, new, "since:2025-01-01")
        assert result.added == []
        assert result.removed == []
        assert len(result.modified) == 1
        assert result.modified[0].name == "after"


class TestGetGitBaselineContent:
    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_since_mode(self, mock_run):
        mock_run.side_effect = [
            MagicMock(stdout="abc123\n", returncode=0),
            MagicMock(stdout="CREATE TABLE t (id STRING)", returncode=0, stderr=""),
        ]
        content, ref = get_git_baseline_content(
            "pipelines/seeds/ddl.sql", "since", "2025-09-10", "main"
        )
        assert content == "CREATE TABLE t (id STRING)"
        assert ref == "since:2025-09-10"
        mock_run.assert_any_call(
            ["git", "rev-list", "-n", "1", "--before=2025-09-10T00:00:00", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        mock_run.assert_any_call(
            ["git", "show", "abc123:pipelines/seeds/ddl.sql"],
            capture_output=True,
            text=True,
        )

    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_branch_mode(self, mock_run):
        mock_run.return_value = MagicMock(
            stdout="CREATE TABLE t (id STRING)", returncode=0, stderr=""
        )
        content, ref = get_git_baseline_content(
            "pipelines/seeds/ddl.sql", "branch", "2025-09-10", "main"
        )
        assert content == "CREATE TABLE t (id STRING)"
        assert ref == "branch:main"
        mock_run.assert_called_once_with(
            ["git", "show", "main:pipelines/seeds/ddl.sql"],
            capture_output=True,
            text=True,
        )

    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_since_mode_file_absent_at_baseline(self, mock_run):
        mock_run.side_effect = [
            MagicMock(stdout="abc123\n", returncode=0),
            MagicMock(stdout="", returncode=128, stderr="path does not exist"),
        ]
        content, ref = get_git_baseline_content(
            "pipelines/seeds/new.sql", "since", "2025-09-10", "main"
        )
        assert content is None
        assert ref == "since:2025-09-10"

    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_since_mode_no_commit_before_date(self, mock_run):
        mock_run.return_value = MagicMock(stdout="", returncode=0)
        content, ref = get_git_baseline_content(
            "pipelines/seeds/ddl.sql", "since", "2099-01-01", "main"
        )
        assert content is None
        assert ref == "since:2099-01-01"
