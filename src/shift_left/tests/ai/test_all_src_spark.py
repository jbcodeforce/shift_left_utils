"""
Copyright 2024-2025 Confluent, Inc.

Run Spark SQL to Flink SQL migration for every .sql file under tests/data/spark-project.
The list of files is dynamic: no hardcoded list. Excludes flink-references and macros.
"""
import os
import pathlib
import unittest
from unittest.mock import MagicMock

data_dir = pathlib.Path(__file__).resolve().parent.parent / "data"
os.environ["CONFIG_FILE"] = str(data_dir.parent / "config-ccloud.yaml")

import shift_left.core.utils.app_config as app_config
from shift_left.core.utils.app_config import logger
from shift_left.ai.process_src_tables import migrate_one_file

app_config.validate_config = MagicMock()

spark_project_dir = data_dir / "spark-project"
staging_base = data_dir / "flink-project" / "staging" / "ut" / "all_spark"

# Path segments that identify non-source trees to skip
_EXCLUDE_DIRS = ("flink-references", "macros")


def _table_name_from_stem(stem: str) -> str:
    """Derive a valid table name from the SQL file stem (e.g. src_bpm_participant -> src_bpm_participant)."""
    return stem.replace("-", "_")


def _collect_spark_sql_files():
    """List all .sql files under spark-project, excluding flink-references and macros, sorted by path."""
    if not spark_project_dir.is_dir():
        return []
    collected = []
    for path in sorted(spark_project_dir.rglob("*.sql")):
        parts = path.relative_to(spark_project_dir).parts
        if any(ex in parts for ex in _EXCLUDE_DIRS):
            continue
        collected.append(path)
    return collected


class TestAllSparkMigration(unittest.TestCase):
    """
    Migrate every Spark SQL file in tests/data/spark-project to Flink SQL.
    """

    @classmethod
    def setUpClass(cls):
        cls.project_dir = spark_project_dir
        cls.staging = str(staging_base)
        cls.sql_files = _collect_spark_sql_files()
        os.environ["STAGING"] = cls.staging
        os.environ["SRC_FOLDER"] = str(cls.project_dir)

    def test_migrate_all_spark_sources(self):
        """Run migration for each .sql file under spark-project (excluding flink-references and macros)."""
        if not self.sql_files:
            self.skipTest("No .sql files found under tests/data/spark-project (excluding flink-references, macros)")

        for sql_path in self.sql_files:
            table_name = _table_name_from_stem(sql_path.stem)
            rel = sql_path.relative_to(self.project_dir)
            with self.subTest(sql_file=str(rel), table_name=table_name):
                try:
                    migrate_one_file(
                        table_name=table_name,
                        sql_src_file=str(sql_path),
                        staging_target_folder=self.staging,
                        source_type="spark",
                        product_name="ut",
                        validate=False,
                    )
                    out_dir = staging_base / "ut" / table_name.lower()
                    scripts_dir = out_dir / "sql-scripts"
                    self.assertTrue(out_dir.exists(), f"Expected output dir: {out_dir}")
                    self.assertTrue(scripts_dir.exists(), f"Expected sql-scripts: {scripts_dir}")
                    ddl_files = [f.name for f in scripts_dir.iterdir() if f.name.startswith("ddl.") and f.suffix == ".sql"]
                    dml_files = [f.name for f in scripts_dir.iterdir() if f.name.startswith("dml.") and f.suffix == ".sql"]
                    self.assertTrue(
                        ddl_files or dml_files,
                        f"Expected at least one ddl.*.sql or dml.*.sql in {scripts_dir}",
                    )
                    logger.info("Translation successful for %s", rel)
                    print("PASSED", flush=True)
                except Exception as e:
                    print("FAILED", flush=True)
                    logger.error("Translation failed for %s: %s", rel, e)
                    self.fail(f"Translation failed for {rel}: {e}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    unittest.main(verbosity=2)
