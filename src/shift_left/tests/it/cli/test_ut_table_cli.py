"""
Copyright 2024-2025 Confluent, Inc.

Integration tests for the table Typer CLI.

For commands that need your real Confluent/demo project, run from `src/shift_left` after:

  source set_demo_env

`set_demo_env` sets SL_CONFIG_FILE, PIPELINES, STAGING, SRC_FOLDER, and API credentials.
Local CI uses tests/config.yaml + dummy env from tests/it/conftest.py when demo env is not loaded.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from it.BaseIT import IntegrationTestCase, _run_integration_tests, _SKIP_MSG, _PIPELINES
from typer.testing import CliRunner

import shift_left.core.test_mgr as test_mgr
from shift_left.cli_commands.table import app
# True after `source set_demo_env` (export added there)

_DEMO_IT = os.environ.get("SL_IT_USE_DEMO_ENV", "").lower() in ("1", "true", "yes")
_DEFAULT_PIPELINES = os.environ.get("PIPELINES", str(_PIPELINES))


_RUN_IT = _run_integration_tests()
@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestTableUnitTestsCLI(IntegrationTestCase):
    """Table CLI:
    - run-unit-tests
    - validate-unit-tests
    - delete-unit-tests
    """

    def _assert_topics_exist(self, topic_names: list[str]) -> None:
        """Assert Kafka topics exist in the cluster (refresh topic list cache first)."""
        test_mgr._topic_list_cache = None
        for topic_name in topic_names:
            self.assertTrue(
                test_mgr._table_exists(topic_name),
                msg=f"Kafka topic {topic_name} was not found in the cluster",
            )

    def test_running_unit_tests(self):

        print("-"*40)
        print("Running unit tests for the user dimension table")
        print("-"*40)
        result = self.runner.invoke(app, ["run-unit-tests", "sl_c360_dim_users", "--test-case-name", "test_c360_dim_users_1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        print(result.stdout)
        self._assert_topics_exist(["sl_c360_src_users_ut", "sl_c360_dim_groups_ut", "sl_c360_dim_users_ut"])

        print("-"*40)
        print("Validating unit tests for the user dimension table")
        print("-"*40)
        result = self.runner.invoke(app, ["validate-unit-tests", "sl_c360_dim_users", "--test-case-name", "test_c360_dim_users_1"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        print(result.stdout)

        print("-"*40)
        print("Deleting unit tests for the user dimension table")
        print("-"*40)
        result = self.runner.invoke(app, ["delete-unit-tests", "sl_c360_dim_users", "--post-fix-unit-test", "_ut"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        print(result.stdout)


if __name__ == "__main__":
    unittest.main()
