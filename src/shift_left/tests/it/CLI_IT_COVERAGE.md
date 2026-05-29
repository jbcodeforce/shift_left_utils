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

## Coverage tracking

Use skill `.cursor/skills/cli_first_test_coverage/` when consolidating UT/IT overlap for a CLI group.

- [x] project CLI, UT coverage (CLI-first; core private helpers only)
- [x] table CLI, UT/IT coverage and overlap
- [x] pipeline CLI, UT/IT coverage and overlap

## Refactoring tracking

Move test classes as inheriting IntegrationTestCase

| file                        | status          |
|-----------------------------|-----------------|
| `test_fact_deployment.py`   | ok - executed   |

## Root (`shift_left/cli.py`)

| CLI command | Full IT (CLI)  | Test file / notes           | Exec Status |
|-------------|----------------|-----------------------------|-------------|
| `version`   | Yes            | `test_fact_deployment.py`   | —           |

## `project` (`shift_left project ...`)

Status on 05/2026

| CLI command                      | Tests  | Test file / notes                      | Status|
|----------------------------------|--------|----------------------------------------|-------|
| `init`                           | UT     |  `ut/cli/test_project_cli.py`          | okay  |
| `delete-all-compute-pools`       | UT     |  `ut/cli/test_project_cli.py`          | okay  |
| `list-topics`                    | UT + IT| `ut/cli/test_project_cli.py`, `it/cli/test_project_cli.py` | okay  |
| `list-compute-pools`             | IT     | `it/cli/test_project_cli.py`           | okay  |   
| `housekeep-statements`           | Gap    |   Tested by Srini                      | okay  |
| `validate-config`                | UT     |  `ut/cli/test_project_cli.py`          | okay  |
| `report-table-cross-products`    | UT     |  `ut/cli/test_project_cli.py`          | okay  |
| `list-environments`              | IT     | `it/cli/test_project_cli.py`           | okay  |
| `list-tables-with-one-child`     | UT     |   `ut/cli/test_project_cli.py`         | okay  |
| `list-modified-files`            | UT     |  `ut/cli/test_project_cli.py`          | okay  |
| `list_impacted_tables`           | UT     |  `ut/cli/test_project_cli.py` (real fixture integration) | okay  | 
| `update-tables-version`          | UT     |  `ut/cli/test_project_cli.py` (CLI wiring smoke)         | okay  |
| `update-ut-ddl`                  | UT     |  `ut/cli/test_project_cli.py`                          | okay  |
| `init-integration-tests`         | Gap    | `tests/it/test_itg_test_mgr.py` covers manager APIs, not CLI  | |
| `run-integration-tests`          | Gap    | —                                      |      |
| `delete-integration-tests`       | Gap    | —                                      |      |
| `isolate-data-product`           | UT     | `ut/cli/test_project_cli.py`           | okay  | 
| `get-statement-list`             | IT     | `it/cli/test_project_cli.py`           | okay  |
| `assess-unused-tables`           | Gap    | —                 |
| `delete-unused-tables`           | Gap    | —                 |

## `table` (`shift_left table ...`)

| CLI command                    | Tests                | Test file / notes                                                                                      | Exec Status                  |
|--------------------------------|----------------------|--------------------------------------------------------------------------------------------------------|------------------------------|
| `init`                         | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `build-inventory`              | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `search-source-dependencies`   | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `migrate`                      | UT + opt-in AI       | `ut/cli/test_table_cli.py` (smoke); `tests/ai/test_migrate_cli_ksql.py`                                | ok                           |
| `update-makefile`              | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `update-all-makefiles`         | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `validate-table-names`         | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `update-tables`                | UT                   | `ut/cli/test_table_cli.py` (basic, `--ddl`, `--both-ddl-dml`)                                          | ok                           |
| `init-unit-tests`              | UT                   | `ut/cli/test_table_cli.py`                                                                             | ok                           |
| `run-unit-tests`               | UT smoke + IT cloud  | `ut/cli/test_table_cli.py`; `it/cli/test_ut_table_cli.py`                                              | ok                           |
| `run-validation-tests`         | IT                   | `it/cli/test_ut_table_cli.py`                                                                          | —                            |
| `validate-unit-tests`          | UT smoke             | `ut/cli/test_table_cli.py` (delegates to run-validation-tests)                                           | ok                           |
| `delete-unit-tests`            | UT smoke + IT cloud  | `ut/cli/test_table_cli.py`; `it/cli/test_ut_table_cli.py`                                              | ok                           |
| `explain`                      | UT error + IT demo   | `ut/cli/test_table_cli.py` (no-args error); `it/cli/test_table_cli.py` (demo happy paths)              | ok                           |

## `pipeline` (`shift_left pipeline ...`)

| CLI command                   | Tests                | Test file / notes                                                                                      | Exec Status |
|-------------------------------|----------------------|--------------------------------------------------------------------------------------------------------|-------------|
| `field-lineage`               | UT smoke             | `ut/cli/test_pipeline_cli.py`                                                                          | ok          |
| `build-metadata`              | UT + IT cloud chain  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `delete-all-metadata`         | UT + IT cloud chain  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `build-all-metadata`          | UT + IT cloud chain  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `report`                      | UT                   | `ut/cli/test_pipeline_cli.py` (success, error)                                                         | ok          |
| `healthcheck`                 | UT smoke             | `ut/cli/test_pipeline_cli.py` (mocked statements/pools)                                              | ok          |
| `deploy`                      | UT smoke + IT cloud  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `build-execution-plan`        | UT smoke + IT cloud  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `report-running-statements`   | UT + IT cloud        | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `undeploy`                    | UT smoke + IT cloud  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_fact_deployment.py`                                        | ok          |
| `prepare`                     | UT smoke + IT cloud  | `ut/cli/test_pipeline_cli.py`; `it/cli/test_pipeline_cli.py`, `it/cli/test_fact_deployment.py`         | ok          |
| `analyze-pool-usage`          | IT opt-in            | `it/cli/test_pipeline_cli.py` (`SHIFT_LEFT_RUN_CLOUD_IT`)                                              | ok          |

## `rag` (`shift_left rag ...`)

| CLI command | Full IT (CLI)  | Test file / notes                                                                                                      |
|-------------|----------------|------------------------------------------------------------------------------------------------------------------------|
| `build`     | Gap            | Needs corpus fixture with `flink-references/`; candidate: new `tests/it/cli/test_rag_cli.py` using `tests/data/...`    |

## `test_fact_deployment.py`

`tests/it/test_fact_deployment.py` (opt-in: `SHIFT_LEFT_RUN_CLOUD_IT`) chains root-app CLI calls: `pipeline delete-all-metadata`, `table build-inventory`, `pipeline build-all-metadata`, `pipeline deploy`, `pipeline report-running-statements` against the bundled flink-project pipelines fixture (see `BaseIT._run_integration_tests`).

## Summary counts (approximate)

- **Project**: many reporting / cleanup / integration-test commands still have **Gap** for CLI-level IT.
- **Table**: unit-test lifecycle commands (`init-unit-tests`, `run-validation-tests`, `validate-unit-tests`, `delete-unit-tests`) are **Gap** at CLI IT level.
- **Pipeline**: `field-lineage` and `healthcheck` covered at UT CLI smoke level; cloud chain remains in `test_fact_deployment.py`.
- **Rag**: **Gap**.

## Maintenance

When adding a Typer command in `cli_commands/`:

1. Add or extend a test under `tests/it/cli/` (or a focused `tests/it/test_*.py`) using `CliRunner` and the same argv users would type.
2. Update this table in the same PR.

When a command is intentionally not covered by CLI IT (too destructive or needs secrets), mark it **Gap** and add a one-line **reason** in the notes column after review.
