import os
import pathlib

from shift_left.core.utils.app_config import format_config_for_debug, session_log_dir


def _run_integration_tests() -> bool:
    """True when cloud IT is requested and API key is not the conftest placeholder."""
    cloud = os.environ.get("SHIFT_LEFT_RUN_CLOUD_IT", "").lower() in (
        "1",
        "true",
        "yes",
    )
    key = os.environ.get("SL_CONFLUENT_CLOUD_API_KEY", "")
    return bool(cloud and key not in ("", "test"))


_SKIP_MSG = (
    "Set SHIFT_LEFT_RUN_CLOUD_IT=1 and use real Confluent credentials "
    "(e.g. source set_demo_env from src/shift_left). "
    "conftest's SL_CONFLUENT_CLOUD_API_KEY=test yields empty API results."
)


def _cli_log_path() -> pathlib.Path:
    """Path to ``shift_left_cli.log`` for the current process session."""
    return pathlib.Path(session_log_dir) / "shift_left_cli.log"


def _log_file_size(log_path: pathlib.Path) -> int:
    if not log_path.exists():
        return 0
    return log_path.stat().st_size


def _read_log_appended(log_path: pathlib.Path, start_offset: int) -> str:
    if not log_path.exists():
        return ""
    with log_path.open("rb") as f:
        f.seek(start_offset)
        return f.read().decode("utf-8", errors="replace")


def _assert_no_new_errors(log_path: pathlib.Path, start_offset: int) -> None:
    text = _read_log_appended(log_path, start_offset)
    assert " - ERROR " not in text, (
        f"{log_path} contains ERROR lines appended after byte offset {start_offset}:\n"
        f"{text[-4000:]}"
    )


def _cli_result_stderr_for_log(result) -> str:
    """Safe stderr text for CliRunner results (mix_stderr=True merges streams)."""
    if getattr(result, "stderr_bytes", None) is None:
        return (
            "(stderr merged into stdout by CliRunner default mix_stderr=True; "
            "use CliRunner(mix_stderr=False) to capture separately)\n"
        )
    return result.stderr


def _assert_cli_ok(result, label: str) -> None:
    """Assert Typer CliRunner result is OK; print stdout/stderr on failure for CI logs."""
    if result.exit_code != 0:
        print(
            f"\n--- CLI failed: {label} ---\n"
            f"exit_code={result.exit_code}\n"
            f"--- stdout ---\n{result.stdout}\n"
            f"--- stderr ---\n{_cli_result_stderr_for_log(result)}"
        )
    assert result.exit_code == 0, f"{label} failed (exit {result.exit_code})"


def dump_effective_config_for_debug() -> str:
    """Redacted JSON of effective ``get_config()`` for IT / support debugging."""
    return format_config_for_debug()


def _generate_messages(input_table_name: str, num_messages: int) -> str:
    query = f"INSERT INTO {input_table_name} (id,name) VALUES"
    for i in range(num_messages):
        message = f"({i}, 'name_{i}'),"
        query += message
    query = query[:-1]+";"
    return query
