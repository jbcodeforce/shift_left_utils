# CLI integration test coverage

This document tracks which `shift_left` Typer commands under `shift_left/cli_commands/` (and the root `version` command in `shift_left/cli.py`) have an **integration test** that exercises the command through the CLI (`CliRunner` or equivalent), versus coverage only via Python APIs or no automated IT yet.

## Conventions

- **CLI name**: Typer exposes commands with kebab-case (e.g. `build-inventory`). Invoke as `shift_left <group> <command> ...` or `uv run python -m shift_left.cli ...` depending on your install.
- **Full IT (CLI)**: A test under `tests/it/` calls `CliRunner().invoke(...)` for that command (possibly behind an opt-in skip).
- **Opt-in**: Test exists but is skipped unless an environment variable is set (e.g. `SHIFT_LEFT_RUN_CLOUD_IT`, `SHIFT_LEFT_IT_USE_DEMO_ENV`).
- **Gap**: No `tests/it` CLI invocation found; consider adding `tests/it/cli/test_<group>_cli.py` or extending an existing file.
- **Not registered**: Function exists in source but is not wired as a Typer command (e.g. commented `@app.command()`).
- **Debug config snapshot**: Set `SHIFT_LEFT_IT_DEBUG_CONFIG=1` when running cloud IT so `test_fact_deployment.test_0_version` prints a redacted effective config (`dump_effective_config_for_debug()` from `it.BaseIT`, backed by `format_config_for_debug()` in `app_config`).

Source modules: `shift_left/cli_commands/project.py`, `table.py`, `pipeline.py`, `rag.py`; root app: `shift_left/cli.py`.

## Refactoring tracking

Move test classes as inheriting IntegrationTestCase

| file | status |
|-----|-----|
| `test_fact_deployment.py` | ok - executed| 

## Root (`shift_left/cli.py`)

| CLI command | Full IT (CLI) | Test file / notes |
|-------------|---------------|-------------------|
| `version` | Yes | `test_fact_deployment.py` |

## `project` (`shift_left project ...`)

| CLI command | Full IT (CLI) | Test file / notes |
|-------------|---------------|-------------------|
| `init` | NA  | done in unit test |
| `list-topics` | Yes | `test_project_cli.py` |
| `list-compute-pools` | Yes | `test_project_cli.py` |
| `delete-all-compute-pools` | Gap | Destructive; no dedicated IT |
| `housekeep-statements` | Yes (same) | `test_project_cli.py` |
| `validate-config` | Yes (same) | `test_project_cli.py` |
| `report-table-cross-products` | Gap | Uses `PIPELINES` / inventory |
| `list-environments` | Yes (same) | `test_project_cli.py` |
| `list-tables-with-one-child` | Gap | |
| `list-modified-files` | Yes (same) | `test_project_cli.py` |
| `update-tables-version` | Yes | `cli/test_version_project_cli.py` (expects `~/.shift_left/modified_flink_files.json`) |
| `init-integration-tests` | Gap | `tests/it/test_itg_test_mgr.py` covers manager APIs, not CLI |
| `run-integration-tests` | Gap | |
| `delete-integration-tests` | Gap | |
| `isolate-data-product` | Gap | |
| `get-statement-list` | Gap | |
| `assess-unused-tables` | Gap | |
| `delete-unused-tables` | Gap | |
| `update-all-makefiles` | Not registered | `#@app.command()` in `project.py` |

## `table` (`shift_left table ...`)

| CLI command                  | Tests | Test file / notes          | Exec Status |
|------------------------------|-------|----------------------------|-------------|
| `init`                       |   UT  | `ut/cli/test_table_cli.py` |.  ok        |
| `build-inventory` | UT | `ut/cli/test_table_cli.py`;  |
| `search-source-dependencies` | UT | `ut/cli/test_table_cli.py` |
| `migrate` | Opt-in | `cli/test_table_cli.py` (`SHIFT_LEFT_IT_USE_DEMO_ENV`); `tests/ai/test_migrate_cli_ksql.py` (AI area, ksql) |
| `update-makefile` | NO | |
| `update-all-makefiles` | NO |  |
| `validate-table-names` | Yes | `ut/cli/test_table_cli.py` |
| `update-tables` | Yes | `ut/cli/test_table_cli.py` (basic, `--ddl`, `--both-ddl-dml`) |
| `init-unit-tests` | UT | `ut/cli/test_table_cli.py` |
| `run-unit-tests` | IT (cloud) | `it/cli/test_ut_test_mgr.py` uses root `app` + real env |
| `run-validation-tests` | IT | tit/cli/test_ut_test_mgr.py |
| `delete-unit-tests` | IT | it/cli/test_ut_test_mgr.py-|
| `explain` | Opt-in / error path | `cli/test_table_cli.py` — cloud paths need `SHIFT_LEFT_IT_USE_DEMO_ENV`; no-args error covered locally |

## `pipeline` (`shift_left pipeline ...`)

| CLI command | Full IT (CLI) | Test file / notes |
|-------------|---------------|-------------------|
| `field-lineage` | Gap | |
| `build-metadata` | Yes | `test_fact_deployment.py` |
| `delete-all-metadata` | Yes | `test_fact_deployment.py` |
| `build-all-metadata` | Yes | `test_fact_deployment.py`|
| `report` | Yes | `test_fact_deployment.py`) |
| `healthcheck` | Gap | Needs product + live statements/pools |
| `deploy` | Yes | `test_fact_deployment` |
| `build-execution-plan` | Yes | `test_fact_deployment.py` |
| `report-running-statements` | Yes | `test_fact_deployment.py` |
| `undeploy` | Yes | `test_fact_deployment.py` |
| `prepare` | Yes | `test_fact_deployment.py` |
| `analyze-pool-usage` | Opt-in | `cli/test_pipeline_cli.py` (`SHIFT_LEFT_RUN_CLOUD_IT`) |

## `rag` (`shift_left rag ...`)

| CLI command | Full IT (CLI) | Test file / notes |
|-------------|---------------|-------------------|
| `build` | Gap | Needs corpus fixture with `flink-references/`; candidate: new `tests/it/cli/test_rag_cli.py` using `tests/data/...` |

## `test_fact_deployment.py`

`tests/it/test_fact_deployment.py` (opt-in: `SHIFT_LEFT_RUN_CLOUD_IT`) chains root-app CLI calls: `pipeline delete-all-metadata`, `table build-inventory`, `pipeline build-all-metadata`, `pipeline deploy`, `pipeline report-running-statements` against the bundled flink-project pipelines fixture (see `BaseIT._run_integration_tests`).

## Summary counts (approximate)

- **Project**: many reporting / cleanup / integration-test commands still have **Gap** for CLI-level IT.
- **Table**: unit-test lifecycle commands (`init-unit-tests`, `run-validation-tests`, `validate-unit-tests`, `delete-unit-tests`) are **Gap** at CLI IT level.
- **Pipeline**: `field-lineage` and `healthcheck` are **Gap**.
- **Rag**: **Gap**.

## Maintenance

When adding a Typer command in `cli_commands/`:

1. Add or extend a test under `tests/it/cli/` (or a focused `tests/it/test_*.py`) using `CliRunner` and the same argv users would type.
2. Update this table in the same PR.

When a command is intentionally not covered by CLI IT (too destructive or needs secrets), mark it **Gap** and add a one-line **reason** in the notes column after review.
