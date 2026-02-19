"""
Copyright 2024-2026 Confluent, Inc.

Corpus loader for ksql-to-Flink SQL example pairs.

Reads flink-references subfolders: one folder per sample, each with ksql and
matching ddl/dml (sql-scripts/ddl.*.sql, dml.*.sql). KSQL is read from a .ksql
file in the same subfolder, from an optional manifest, or from sources/<name>.ksql.
"""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class ExamplePair:
    """A single (ksql, Flink DDL, Flink DML) example for RAG."""

    name: str
    ksql_text: str
    flink_ddl: str
    flink_dml: str

    def __post_init__(self) -> None:
        self.flink_ddl = self.flink_ddl or ""
        self.flink_dml = self.flink_dml or ""


def _read_ksql_from_folder(folder: Path) -> str:
    """Read KSQL from .ksql file(s) in the example folder (one folder per example)."""
    parts: List[str] = []
    for f in sorted(folder.glob("*.ksql")):
        if f.is_file():
            parts.append(f.read_text(encoding="utf-8").strip())
    return "\n\n".join(parts) if parts else ""


def _read_sql_files(folder: Path, pattern: str) -> str:
    """Read and concatenate SQL files in folder matching pattern (e.g. ddl.*.sql)."""
    sql_scripts_folder = folder / "sql-scripts"
    if not sql_scripts_folder.is_dir():
        return "No SQL files found in folder"
    parts: List[str] = []
    for f in sorted(sql_scripts_folder.glob(pattern)):
        if f.is_file():
            parts.append(f.read_text(encoding="utf-8").strip())
    return "\n\n".join(parts) if parts else ""


def _folder_basename(folder_path: str) -> str:
    """Last component of path (e.g. 'tutorial/acting_events' -> 'acting_events')."""
    return os.path.basename(folder_path.rstrip("/"))


def load_ksql_flink_corpus(
    corpus_root: str | Path,
    *,
    manifest_path: Optional[str | Path] = None,
) -> List[ExamplePair]:
    """
    Load (ksql, Flink DDL, Flink DML) example pairs from a ksql-project layout.

    Expects:
      - corpus_root/flink-references/<folder>/ with sql-scripts/ddl.*.sql and dml.*.sql
      - KSQL in the same folder (*.ksql), or corpus_root/sources/*.ksql, or rag_manifest.yaml

    One folder per example. For each flink-references subfolder that has at least one ddl.*.sql:
      - ksql_text: from manifest[folder], else *.ksql file(s) in that subfolder, else sources/<basename>.ksql
      - flink_ddl: concatenation of all ddl.*.sql
      - flink_dml: concatenation of all dml.*.sql

    Returns:
      List of ExamplePair (name=folder path, ksql_text, flink_ddl, flink_dml).
    """
    root = Path(corpus_root)
    refs_dir = root / "flink-references"
    sources_dir = root / "sources"

    manifest: dict = {}
    if manifest_path is None:
        manifest_path = root / "rag_manifest.yaml"
    if manifest_path is not None:
        mp = Path(manifest_path)
        if mp.exists():
            with open(mp, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict) and "mapping" in data:
                manifest = data["mapping"] or {}
            elif isinstance(data, dict):
                manifest = data

    pairs: List[ExamplePair] = []

    if not refs_dir.is_dir():
        return pairs

    for item in sorted(refs_dir.iterdir()):
        if not item.is_dir():
            continue
        scripts_dir = item / "sql-scripts"
        if not scripts_dir.is_dir():
            continue
        ddl_sql = _read_sql_files(item, "ddl.*.sql")
        if not ddl_sql:
            continue
        dml_sql = _read_sql_files(item, "dml.*.sql")
        folder_key = item.name

        ksql_text = ""
        if manifest and folder_key in manifest:
            src_file = root / manifest[folder_key] if not Path(manifest[folder_key]).is_absolute() else Path(manifest[folder_key])
            if src_file.exists():
                ksql_text = src_file.read_text(encoding="utf-8").strip()
        if not ksql_text:
            ksql_text = _read_ksql_from_folder(item)
        if not ksql_text and sources_dir.is_dir():
            for candidate in (sources_dir / f"{folder_key}.ksql", sources_dir / f"{item.relative_to(refs_dir)}.ksql".replace(os.sep, "_")):
                if candidate.exists():
                    ksql_text = candidate.read_text(encoding="utf-8").strip()
                    break
        if not ksql_text:
            continue
        pairs.append(
            ExamplePair(
                name=folder_key,
                ksql_text=ksql_text,
                flink_ddl=ddl_sql,
                flink_dml=dml_sql,
            )
        )

    # Nested folders (e.g. tutorial/acting_events)
    for sub in sorted(refs_dir.iterdir()):
        if not sub.is_dir():
            continue
        for item in sorted(sub.iterdir()):
            if not item.is_dir():
                continue
            scripts_dir = item / "sql-scripts"
            if not scripts_dir.is_dir():
                continue
            ddl_sql = _read_sql_files(item, "ddl.*.sql")
            if not ddl_sql:
                continue
            dml_sql = _read_sql_files(item, "dml.*.sql")
            folder_key = f"{sub.name}/{item.name}"

            ksql_text = ""
            if manifest and folder_key in manifest:
                src_file = root / manifest[folder_key] if not Path(manifest[folder_key]).is_absolute() else Path(manifest[folder_key])
                if src_file.exists():
                    ksql_text = src_file.read_text(encoding="utf-8").strip()
            if not ksql_text:
                ksql_text = _read_ksql_from_folder(item)
            if not ksql_text and sources_dir.is_dir():
                basename = _folder_basename(folder_key)
                candidate = sources_dir / f"{basename}.ksql"
                if candidate.exists():
                    ksql_text = candidate.read_text(encoding="utf-8").strip()
            if not ksql_text:
                continue
            pairs.append(
                ExamplePair(
                    name=folder_key,
                    ksql_text=ksql_text,
                    flink_ddl=ddl_sql,
                    flink_dml=dml_sql,
                )
            )

    return pairs
