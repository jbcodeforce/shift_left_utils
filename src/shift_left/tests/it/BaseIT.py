import os
import pathlib
import unittest
from shift_left.core.utils.app_config import get_config, session_log_dir
from typer.testing import CliRunner
from shift_left.cli import app

_TESTS_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONFIG_CCLOUD = _TESTS_ROOT / "config-ccloud.yaml"
_PIPELINES = _TESTS_ROOT / "data" / "flink-project" / "pipelines"
_DEFAULT_SRC = _TESTS_ROOT / "data" / "spark-project"
_DEFAULT_STAGING = _TESTS_ROOT / "data" / "flink-project" / "staging"

_SKIP_DESTRUCTIVE = (
    "Set SHIFT_LEFT_RUN_COMPUTE_POOL_DESTRUCTIVE=1 to run the pool delete test"
)

_SKIP_MSG = (
    "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent credentials "
    "(e.g. source set_demo_env from src/shift_left). "
    "conftest's SL_CONFLUENT_CLOUD_API_KEY=test yields empty API results."
)

def _run_integration_tests() -> bool:
    """True when cloud IT is requested and API key is not the conftest placeholder."""
    cloud = os.environ.get("SHIFT_LEFT_RUN_CLOUD_IT", "").lower() in (
        "1",
        "true",
        "yes",
    )
    key = os.environ.get("SL_CONFLUENT_CLOUD_API_KEY", "")
    return bool(cloud and key not in ("", "test"))

class IntegrationTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SL_CONFIG_FILE", str(_CONFIG_CCLOUD))
        os.environ.setdefault("PIPELINES", str(_PIPELINES))
        cls._config = get_config()
        cls.pipelines = os.environ["PIPELINES"]
        cls.runner = CliRunner()

    def _cli_log_path(self) -> pathlib.Path:
        """Path to ``shift_left_cli.log`` for the current process session."""
        return pathlib.Path(session_log_dir) / "shift_left_cli.log"


    def _log_file_size(self, log_path: pathlib.Path) -> int:
        if not log_path.exists():
            return 0
        return log_path.stat().st_size


    def _read_log_appended(self, log_path: pathlib.Path, start_offset: int) -> str:
        if not log_path.exists():
            return ""
        with log_path.open("rb") as f:
            f.seek(start_offset)
            return f.read().decode("utf-8", errors="replace")


    def _assert_no_new_errors(self, log_path: pathlib.Path, start_offset: int) -> None:
        text = self._read_log_appended(log_path, start_offset)
        assert " - ERROR " not in text, (
            f"{log_path} contains ERROR lines appended after byte offset {start_offset}:\n"
            f"{text[-4000:]}"
        )


    def _cli_result_stderr_for_log(self, result) -> str:
        """Safe stderr text for CliRunner results (mix_stderr=True merges streams)."""
        if getattr(result, "stderr_bytes", None) is None:
            return (
                "(stderr merged into stdout by CliRunner default mix_stderr=True; "
                "use CliRunner(mix_stderr=False) to capture separately)\n"
            )
        return result.stderr


    def _assert_cli_ok(self, result, label: str) -> None:
        """Assert Typer CliRunner result is OK; print stdout/stderr on failure for CI logs."""
        if result.exit_code != 0:
            print(
                f"\n--- CLI failed: {label} ---\n"
                f"exit_code={result.exit_code}\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{self._cli_result_stderr_for_log(result)}"
            )
        assert result.exit_code == 0, f"{label} failed (exit {result.exit_code})"


    def _generate_messages(self, input_table_name: str, num_messages: int) -> str:
        query = f"INSERT INTO {input_table_name} (id,name) VALUES"
        for i in range(num_messages):
            message = f"({i}, 'name_{i}'),"
            query += message
        query = query[:-1]+";"
        return query
