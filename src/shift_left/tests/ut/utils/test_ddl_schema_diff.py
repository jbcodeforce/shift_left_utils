"""Unit tests for DDL schema diff utilities."""
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from shift_left.core.utils.ddl_schema_diff import (
    diff_column_metadata,
    ensure_git_branch,
    get_git_baseline_content,
    merge_added_columns_into_ddl,
    merge_added_columns_into_insert_sql,
    merge_added_columns_into_validation_sql,
    placeholder_value_for_column,
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


class TestPlaceholderValueForColumn:
    def test_string_placeholder(self):
        assert placeholder_value_for_column("created_by", "STRING", 1) == "'created_by_1'"

    def test_boolean_alternates(self):
        assert placeholder_value_for_column("flag", "BOOLEAN", 1) == "false"
        assert placeholder_value_for_column("flag", "BOOLEAN", 2) == "true"


class TestMergeAddedColumnsIntoDdl:
    UT_DDL = """CREATE TABLE IF NOT EXISTS sl_c360_src_groups_ut (
  group_id STRING NOT NULL,
  tenant_id STRING NOT NULL,
  PRIMARY KEY(tenant_id, group_id) NOT ENFORCED
) DISTRIBUTED BY HASH(tenant_id, group_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert'
);"""

    def test_appends_before_primary_key(self):
        updated, added = merge_added_columns_into_ddl(
            self.UT_DDL,
            {"created_by": "created_by STRING", "updated_by": "updated_by STRING"},
        )
        assert added == ["created_by", "updated_by"]
        assert "created_by STRING" in updated
        assert "updated_by STRING" in updated
        assert updated.index("created_by STRING") < updated.index("PRIMARY KEY")

    def test_idempotent(self):
        updated, _ = merge_added_columns_into_ddl(
            self.UT_DDL, {"created_by": "created_by STRING"}
        )
        again, added = merge_added_columns_into_ddl(
            updated, {"created_by": "created_by STRING"}
        )
        assert added == []
        assert again == updated


class TestMergeAddedColumnsIntoInsertSql:
    INSERT = """insert into src_c360_groups_ut
(`group_id`, `tenant_id`)
values
('group_id_1', 'tenant_id_1'),
('group_id_2', 'tenant_id_2');"""

    def test_appends_columns_and_values(self):
        meta = {
            "created_by": {"type": "STRING"},
            "updated_by": {"type": "STRING"},
        }
        updated, added = merge_added_columns_into_insert_sql(
            self.INSERT, ["created_by", "updated_by"], meta
        )
        assert added == ["created_by", "updated_by"]
        assert "`created_by`" in updated
        assert "'created_by_1'" in updated
        assert "'created_by_2'" in updated


class TestMergeAddedColumnsIntoValidationSql:
    VALIDATE = """with expected_results as (
    select
    'group_id_1' as expected_group_id,
    'tenant_id_1' as expected_tenant_id
    union all
    select
    'group_id_2' as expected_group_id,
    'tenant_id_2' as expected_tenant_id
),
actual_results as (
    select
        group_id,
        tenant_id
    from c360_dim_groups_ut
),
validation_check as (
    select
        e.expected_group_id,
        e.expected_tenant_id,
        case when a.group_id = e.expected_group_id then 'PASS' else 'FAIL' end as group_id_check,
        case when a.tenant_id = e.expected_tenant_id then 'PASS' else 'FAIL' end as tenant_id_check
    from expected_results e
    join actual_results a
),
overall_result as (
    select
        count(*) as total_expected_records,
        sum(case when group_id_check = 'PASS' AND tenant_id_check = 'PASS' then 1 else 0 end) as passing_records,
        (select count(*) from actual_results) as actual_record_count
    from validation_check
)
select test_result from overall_result;"""

    def test_patches_all_ctes(self):
        meta = {"region": {"type": "STRING"}}
        updated, added = merge_added_columns_into_validation_sql(
            self.VALIDATE, ["region"], meta
        )
        assert added == ["region"]
        assert "expected_region" in updated
        assert "region_check" in updated
        assert "a.region" in updated
        assert "AND region_check = 'PASS'" in updated


class TestEnsureGitBranch:
    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_creates_branch_when_missing(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(stdout="", returncode=0),
            MagicMock(returncode=1),
            MagicMock(returncode=0),
        ]
        ensure_git_branch("/repo", "feature/ut-schema")
        mock_run.assert_any_call(
            ["git", "-C", "/repo", "checkout", "-b", "feature/ut-schema"],
            capture_output=True,
            text=True,
            check=True,
        )

    @patch("shift_left.core.utils.ddl_schema_diff.subprocess.run")
    def test_fails_on_dirty_tree(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(stdout=" M file.sql\n", returncode=0),
        ]
        with pytest.raises(ValueError, match="uncommitted changes"):
            ensure_git_branch("/repo", "feature/ut-schema")
