"""
Compare top-level DDL column metadata between a git baseline and the current file.

Top-level only: nested ROW fields are compared as a single opaque type string.
Also provides merge helpers for unit-test DDL, insert, and validation SQL artifacts.
"""
from __future__ import annotations

import re
import subprocess
from typing import Dict, Literal

from pydantic import BaseModel, Field

from shift_left.core.utils.app_config import logger
from shift_left.core.utils.sql_parser import SQLparser

BaselineMode = Literal["since", "branch"]


class ColumnChange(BaseModel):
    name: str
    change_type: Literal["modified"] = "modified"
    old_type: str | None = None
    new_type: str | None = None
    old_nullable: bool | None = None
    new_nullable: bool | None = None
    old_primary_key: bool | None = None
    new_primary_key: bool | None = None


class SchemaDiff(BaseModel):
    baseline_ref: str = Field(description="Baseline identifier, e.g. since:2025-09-10 or branch:main")
    added: list[str] = Field(default_factory=list)
    removed: list[str] = Field(default_factory=list)
    modified: list[ColumnChange] = Field(default_factory=list)


def diff_column_metadata(
    old: Dict[str, Dict],
    new: Dict[str, Dict],
    baseline_ref: str,
) -> SchemaDiff:
    """Diff two column metadata dicts produced by SQLparser.build_column_metadata_from_sql_content."""
    old_names = set(old.keys())
    new_names = set(new.keys())
    modified: list[ColumnChange] = []
    for name in sorted(old_names & new_names):
        old_col = old[name]
        new_col = new[name]
        if (
            old_col.get("type") != new_col.get("type")
            or old_col.get("nullable") != new_col.get("nullable")
            or old_col.get("primary_key") != new_col.get("primary_key")
        ):
            modified.append(
                ColumnChange(
                    name=name,
                    old_type=old_col.get("type"),
                    new_type=new_col.get("type"),
                    old_nullable=old_col.get("nullable"),
                    new_nullable=new_col.get("nullable"),
                    old_primary_key=old_col.get("primary_key"),
                    new_primary_key=new_col.get("primary_key"),
                )
            )
    return SchemaDiff(
        baseline_ref=baseline_ref,
        added=sorted(new_names - old_names),
        removed=sorted(old_names - new_names),
        modified=modified,
    )


def get_git_repo_root() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_git_baseline_content(
    repo_relative_path: str,
    baseline_mode: BaselineMode,
    since: str,
    branch_name: str,
) -> tuple[str | None, str]:
    """
    Return DDL content at the configured git baseline and a baseline_ref label.

    For since mode, uses the last commit strictly before since 00:00:00 UTC.
    Returns (None, baseline_ref) when the file did not exist at the baseline commit.
    """
    repo_relative_path = repo_relative_path.replace("\\", "/")
    if baseline_mode == "branch":
        baseline_ref = f"branch:{branch_name}"
        return _git_show(f"{branch_name}:{repo_relative_path}"), baseline_ref

    baseline_ref = f"since:{since}"
    rev_result = subprocess.run(
        ["git", "rev-list", "-n", "1", f"--before={since}T00:00:00", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    baseline_rev = rev_result.stdout.strip()
    if not baseline_rev:
        logger.warning(f"No git commit found before {since}; treating baseline as empty")
        return None, baseline_ref
    return _git_show(f"{baseline_rev}:{repo_relative_path}"), baseline_ref


def _git_show(rev_path: str) -> str | None:
    result = subprocess.run(
        ["git", "show", rev_path],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.debug(f"git show {rev_path} failed: {result.stderr.strip()}")
        return None
    return result.stdout


def ensure_git_branch(repo_root: str, branch_name: str) -> None:
    """Checkout an existing branch or create it from current HEAD. Fails on dirty work tree."""
    def _git(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
        cmd = ["git", "-C", repo_root, *args]
        return subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            check=check,
        )

    _git("rev-parse", "--is-inside-work-tree")
    status = _git("status", "--porcelain")
    if status.stdout.strip():
        raise ValueError(
            "Git working tree has uncommitted changes. Commit or stash before merging UT artifacts."
        )
    verify = _git("rev-parse", "--verify", f"refs/heads/{branch_name}", check=False)
    if verify.returncode == 0:
        _git("checkout", branch_name)
        logger.info(f"Checked out existing branch {branch_name}")
    else:
        _git("checkout", "-b", branch_name)
        logger.info(f"Created and checked out branch {branch_name}")


def placeholder_value_for_column(col_name: str, col_type: str, row_index: int) -> str:
    """Return a synthetic INSERT/expected value for a column type and row index."""
    col_type_upper = col_type.upper()
    if col_type_upper in ("BOOLEAN", "BOOL"):
        return "true" if row_index % 2 == 0 else "false"
    if "TIMESTAMP" in col_type_upper:
        return "TIMESTAMP '2021-01-01 00:00:00'"
    if col_type_upper in ("INT", "INTEGER", "BIGINT", "DECIMAL", "DOUBLE", "FLOAT"):
        return str(row_index)
    if col_type_upper in ("STRING", "VARCHAR", "BYTES"):
        return f"'{col_name}_{row_index}'"
    return "NULL"


def _is_constraint_or_watermark_line(col_def: str) -> bool:
    upper = col_def.strip().upper()
    return upper.startswith(
        ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "CHECK", "CONSTRAINT", "WATERMARK")
    )


def _create_table_body_span(sql_content: str) -> tuple[int, int] | None:
    """Return start/end indices of the CREATE TABLE column list body (inside parens)."""
    m = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:[`\"]?[\w.-]+[`\"]?(?:\.[`\"]?[\w.-]+[`\"]?)*)?\s*\(",
        sql_content,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None
    body_start = m.end()
    depth = 1
    i = body_start
    n = len(sql_content)
    while i < n and depth > 0:
        c = sql_content[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return body_start, i
        i += 1
    return None


def merge_added_columns_into_ddl(
    ut_ddl: str,
    column_defs: dict[str, str],
    parser: SQLparser | None = None,
) -> tuple[str, list[str]]:
    """Append missing column definition lines to a CREATE TABLE before constraints."""
    parser = parser or SQLparser()
    existing = parser.build_column_metadata_from_sql_content(ut_ddl)
    to_add = [name for name in column_defs if name not in existing]
    if not to_add:
        return ut_ddl, []

    span = _create_table_body_span(ut_ddl)
    if not span:
        logger.warning("Could not locate CREATE TABLE column list for DDL merge")
        return ut_ddl, []
    body_start, body_end = span
    columns_section = ut_ddl[body_start:body_end]
    parts = parser._split_by_comma_respecting_parens(columns_section)
    insert_idx = len(parts)
    for idx, part in enumerate(parts):
        if _is_constraint_or_watermark_line(part):
            insert_idx = idx
            break

    new_parts = parts[:insert_idx] + [column_defs[name] for name in to_add] + parts[insert_idx:]
    separator = ",\n  " if "\n" in columns_section else ", "
    new_section = separator.join(new_parts)
    return ut_ddl[:body_start] + new_section + ut_ddl[body_end:], to_add


def merge_added_columns_into_insert_sql(
    insert_sql: str,
    added_columns: list[str],
    column_metadata: dict[str, dict],
) -> tuple[str, list[str]]:
    """Append missing columns and placeholder values to an INSERT statement."""
    parser = SQLparser()
    parsed = parser.parse_insert_sql_to_dict(insert_sql)
    existing_cols = list(parsed.keys())
    to_add = [col for col in added_columns if col not in existing_cols]
    if not to_add:
        return insert_sql, []

    table_match = re.search(
        r"insert\s+into\s+([`\"]?[\w.-]+[`\"]?)",
        insert_sql,
        re.IGNORECASE,
    )
    if not table_match:
        raise ValueError("Could not extract table name from INSERT statement")
    table_name = table_match.group(1)
    num_rows = len(next(iter(parsed.values()))) if parsed else 0
    if num_rows == 0:
        return insert_sql, []

    all_cols = existing_cols + to_add
    for col in to_add:
        meta = column_metadata.get(col, {})
        col_type = meta.get("type", "STRING")
        parsed[col] = [
            placeholder_value_for_column(col, col_type, row_idx + 1)
            for row_idx in range(num_rows)
        ]

    col_list = ", ".join(f"`{col}`" for col in all_cols)
    value_rows = []
    for row_idx in range(num_rows):
        values = [parsed[col][row_idx] for col in all_cols]
        value_rows.append("(" + ", ".join(values) + ")")

    updated = (
        f"insert into {table_name}\n({col_list})\nvalues\n"
        + ",\n".join(value_rows)
        + ";"
    )
    return updated, to_add


def _patch_actual_results_cte(sql: str, cols: list[str]) -> str:
    pattern = re.compile(
        r"(actual_results\s+as\s*\(\s*select\s+)(.*?)(\s+from\s+)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql)
    if not match:
        raise ValueError("actual_results CTE not found in validation SQL")

    select_part = match.group(2).rstrip().rstrip(",")
    additions = ",\n        ".join(cols)
    replacement = match.group(1) + select_part + ",\n        " + additions + match.group(3)
    return sql[: match.start()] + replacement + sql[match.end() :]


def _patch_expected_results_cte(
    sql: str, cols: list[str], column_metadata: dict[str, dict]
) -> str:
    pattern = re.compile(
        r"(expected_results\s+as\s*\()(.*?)(\)\s*,\s*actual_results)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql)
    if not match:
        raise ValueError("expected_results CTE not found in validation SQL")

    body = match.group(2)
    rows = re.split(r"\bunion\s+all\b", body, flags=re.IGNORECASE)
    new_rows = []
    for row_idx, row in enumerate(rows):
        row = row.strip()
        additions = []
        for col in cols:
            additions.append(f"'{col}_{row_idx + 1}' as expected_{col}")
        if additions:
            row = row.rstrip().rstrip(",") + ",\n    " + ",\n    ".join(additions)
        new_rows.append(row)
    new_body = "\n    union all\n    ".join(new_rows)
    return sql[: match.start(2)] + new_body + sql[match.end(2) :]


def _patch_validation_check_cte(sql: str, cols: list[str]) -> str:
    pattern = re.compile(
        r"(validation_check\s+as\s*\(\s*select\s+)(.*?)(\s+from\s+expected_results)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql)
    if not match:
        raise ValueError("validation_check CTE not found in validation SQL")

    select_part = match.group(2).rstrip().rstrip(",")
    additions: list[str] = []
    for col in cols:
        additions.append(f"e.expected_{col}")
        additions.append(
            f"case when a.{col} = e.expected_{col} then 'PASS' else 'FAIL' end as {col}_check"
        )
    add_str = ",\n        ".join(additions)
    replacement = match.group(1) + select_part + ",\n        " + add_str + match.group(3)
    return sql[: match.start()] + replacement + sql[match.end() :]


def _patch_overall_result_cte(sql: str, cols: list[str]) -> str:
    pattern = re.compile(
        r"(sum\s*\(\s*case\s+when\s+)(.*?)(\s+then\s+1\s+else\s+0\s+end\s*\)\s+as\s+passing_records)",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(sql)
    if not match:
        raise ValueError("overall_result passing_records expression not found in validation SQL")

    conditions = match.group(2).strip()
    for col in cols:
        conditions += f" AND {col}_check = 'PASS'"
    replacement = match.group(1) + conditions + match.group(3)
    return sql[: match.start()] + replacement + sql[match.end() :]


def merge_added_columns_into_validation_sql(
    validate_sql: str,
    added_columns: list[str],
    column_metadata: dict[str, dict],
) -> tuple[str, list[str]]:
    """Surgically append columns to the four CTE sections of a unit-test validation SQL file."""
    parser = SQLparser()
    actual_match = re.search(
        r"actual_results\s+as\s*\(\s*select\s+(.*?)\s+from\s+",
        validate_sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not actual_match:
        raise ValueError("Validation SQL missing actual_results CTE")

    existing_cols = [
        item.strip()
        for item in parser._split_by_comma_respecting_parens(actual_match.group(1))
        if item.strip()
    ]
    to_add = [col for col in added_columns if col not in existing_cols]
    if not to_add:
        return validate_sql, []

    updated = validate_sql
    updated = _patch_actual_results_cte(updated, to_add)
    updated = _patch_expected_results_cte(updated, to_add, column_metadata)
    updated = _patch_validation_check_cte(updated, to_add)
    updated = _patch_overall_result_cte(updated, to_add)
    return updated, to_add
