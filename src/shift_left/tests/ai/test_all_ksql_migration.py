"""
Copyright 2024-2026 Confluent, Inc.

Run KSQL to Flink SQL migration for every .ksql file under tests/data/ksql-project/sources.
The list of files is dynamic: no hardcoded list.
"""
import os
import pathlib
import unittest
from unittest.mock import MagicMock, patch

data_dir = pathlib.Path(__file__).resolve().parent.parent / "data"
os.environ["SL_CONFIG_FILE"] = str(data_dir.parent / "config-ccloud.yaml")

import shift_left.core.utils.app_config as app_config
from shift_left.ai.process_src_tables import migrate_one_file

app_config.validate_config = MagicMock()
ksql_sources_dir = data_dir / "ksql-project" / "sources"
staging_base = data_dir / "ksql-project" / "staging" / "ut" / "from_ksql"


def _table_name_from_stem(stem: str) -> str:
    """Derive a valid table name from the KSQL file stem (e.g. ddl-bigger-file -> ddl_bigger_file)."""
    return stem.replace("-", "_")


def _collect_ksql_files():
    """List all .ksql files under ksql-project/sources, sorted by name."""
    if not ksql_sources_dir.is_dir():
        return []
    return sorted(ksql_sources_dir.glob("*.ksql"))


class TestAllKsqlMigration(unittest.TestCase):
    """
    Migrate every KSQL file in tests/data/ksql-project/sources to Flink SQL.
    """

    @classmethod
    def setUpClass(cls):
        cls.sources_dir = ksql_sources_dir
        cls.staging = str(staging_base)
        cls.ksql_files = _collect_ksql_files()
        os.environ["STAGING"] = cls.staging
        os.environ["SRC_FOLDER"] = str(cls.sources_dir.parent)

    @patch("builtins.input")
    def test_migrate_all_ksql_sources(self, mock_input):
        """Run migration for each .ksql file under ksql-project/sources."""
        mock_input.return_value = "n"
        if not self.ksql_files:
            self.skipTest("No .ksql files found under tests/data/ksql-project/sources")

        for ksql_path in self.ksql_files:
            table_name = _table_name_from_stem(ksql_path.stem)
            with self.subTest(ksql_file=ksql_path.name, table_name=table_name):
                try:
                    print(f"Migrating {ksql_path.name} to {self.staging} {table_name}...", flush=True)
                    migrate_one_file(
                        table_name=table_name,
                        sql_src_file=str(ksql_path),
                        staging_target_folder=self.staging,
                        source_type="ksql",
                        product_name="ut",
                        validate=False,
                    )
                    out_dir = staging_base / "ut" / table_name.lower()
                    scripts_dir = out_dir / "sql-scripts"
                    self.assertTrue(out_dir.exists(), f"Expected output dir: {out_dir}")
                    self.assertTrue(scripts_dir.exists(), f"Expected sql-scripts: {scripts_dir}")
                    ddl_files = [f.name for f in scripts_dir.iterdir() if f.name.startswith("ddl.") and f.suffix == ".sql"]
                    dml_files = [f.name for f in scripts_dir.iterdir() if f.name.startswith("dml.") and f.suffix == ".sql"]
                    for dml in dml_files:
                        print("--------------------------------", flush=True)
                        with open(scripts_dir / dml, "r") as f:
                            print(f.read(), flush=True)
                            print("--------------------------------", flush=True)
                    self.assertTrue(ddl_files or dml_files, f"Expected at least one ddl.*.sql or dml.*.sql in {scripts_dir}")
                    print("PASSED", flush=True)
                except Exception as e:
                    print("FAILED", flush=True)
                    self.fail(f"Migration failed for {ksql_path.name}: {e}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    unittest.main(verbosity=2)
