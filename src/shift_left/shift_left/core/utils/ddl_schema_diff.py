"""
Compare top-level DDL column metadata between a git baseline and the current file.

Top-level only: nested ROW fields are compared as a single opaque type string.
"""
from __future__ import annotations

import subprocess
from typing import Dict, Literal

from pydantic import BaseModel, Field

from shift_left.core.utils.app_config import logger

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
