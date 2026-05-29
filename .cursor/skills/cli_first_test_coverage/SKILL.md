---
name: cli-first-test-coverage
description: >-
  Consolidates shift_left test coverage across UT, CLI, and IT layers using a
  CLI-first ownership model. Maps module functions to Typer commands, removes
  UT/core overlap, adds missing tests with lowest-level mocks, and updates
  CLI_IT_COVERAGE.md. Use when assessing or improving test coverage for
  project_manager, table_mgr, pipeline_mgr, or any shift_left cli_commands
  module; when reducing duplicate UT vs CLI tests; or when user mentions
  coverage overlap, CLI-first tests, or IT coverage gaps.
---

# CLI-First Test Coverage (shift_left)

## Principle

User-facing behavior is tested at the **CLI layer** (`tests/ut/cli/` and `tests/it/cli/`). Core unit tests (`tests/ut/core/`) keep **private helpers** and **deep algorithm tests** only. Avoid duplicating the same assertion in UT/core and UT/cli.

## Three layers

| Layer | Path | Role |
|-------|------|------|
| **UT CLI** | `tests/ut/cli/test_<group>_cli.py` | Primary owner: Typer args, exit codes, stdout, file outputs, mocked externals |
| **UT core** | `tests/ut/core/test_<module>_mgr.py` | Private helpers, pure functions, graph/version logic not worth full CLI setup |
| **IT CLI** | `tests/it/cli/test_<group>_cli.py` | Real fixture data; optional cloud (`SHIFT_LEFT_RUN_CLOUD_IT`, `SL_IT_USE_DEMO_ENV`) |

Track coverage in [CLI_IT_COVERAGE.md](src/shift_left/tests/it/CLI_IT_COVERAGE.md).

## Workflow checklist

Copy and track when consolidating a module:

```
- [ ] 1. Inventory source module public APIs
- [ ] 2. Map each API to Typer command (cli_commands/<group>.py)
- [ ] 3. List existing tests in ut/core, ut/cli, it/cli for that API
- [ ] 4. Mark overlap (same behavior tested twice)
- [ ] 5. Decide ownership (CLI-first vs core-only)
- [ ] 6. Delete overlapping tests (prefer delete from ut/core)
- [ ] 7. Add missing CLI tests + update CLI_IT_COVERAGE.md
- [ ] 8. Fix mock level (lowest external dependency)
- [ ] 9. Run targeted pytest and verify pass
```

## Step 1–3: Build the coverage map

1. Read the manager module (e.g. `shift_left/core/project_manager.py`).
2. Grep CLI wiring: `grep -n "project_manager\|table_mgr\|pipeline_mgr" shift_left/cli_commands/*.py`
3. Grep tests: `grep -rn "def test_" tests/ut/cli/test_*_cli.py tests/ut/core/test_*_mgr.py tests/it/cli/test_*_cli.py`

Produce a table:

| Function / behavior | CLI command | UT CLI | UT core | IT CLI | Action |
|---------------------|-------------|--------|---------|--------|--------|

## Step 4–5: Ownership rules

**Prefer UT CLI** when:
- A Typer command wraps the function
- Test needs stdout, exit code, or output file paths
- User-level smoke is enough

**Keep UT core** when:
- Function is private (`_assess_*`, `_normalize_*`)
- Deep logic already has dedicated tests (e.g. `test_version_update.py`)
- CLI path would mock the entire manager (no real stack exercised)

**Prefer IT CLI** when:
- Command touches Confluent/Flink APIs or full fixture pipelines
- UT would require excessive mocking
- Mark **Opt-in** if secrets/env required

**Delete overlap** from `tests/ut/core/` first; keep one behavioral owner per public API.

## Step 6–7: Mock at lowest level

Patch **external leaves**, not mid-stack helpers:

| Behavior | Patch (low) | Avoid (high) |
|----------|-------------|--------------|
| Git in `list_modified_files` | `subprocess.run` | entire `list_modified_files` |
| Flink statement state | `statement_mgr.get_statement` | `_assess_flink_statement_state` |
| Confluent topics | `ConfluentCloudClient.list_topics` | `get_topic_list` |
| CLI wiring smoke for heavy fn | `project_manager.update_tables_version` | only when testing Typer args/load JSON |

UT CLI tests use `typer.testing.CliRunner`, temp dirs, and `tests/data/flink-project/pipelines` via `$PIPELINES`.

## Step 8: IT coverage

For each CLI command in the module:

1. Check [CLI_IT_COVERAGE.md](src/shift_left/tests/it/CLI_IT_COVERAGE.md) row.
2. If **Gap**: add `tests/it/cli/test_<group>_cli.py` test with `CliRunner` or extend existing IT file.
3. Reuse [BaseIT.py](src/shift_left/tests/it/BaseIT.py) helpers; put shared assertions in BaseIT per integration-test-reuse rule.
4. Cloud IT: `@unittest.skipUnless(SHIFT_LEFT_RUN_CLOUD_IT, ...)` or demo env guard.
5. Update the coverage table in the same change.

**IT vs UT CLI**: UT CLI mocks externals; IT CLI uses real fixture paths and optionally real cloud.

## Step 9: Verify

```bash
cd src/shift_left
uv run python -m pytest tests/ut/cli/test_<group>_cli.py tests/ut/core/test_<module>_mgr.py -v
uv run python -m pytest tests/it/cli/test_<group>_cli.py -v   # when IT added/changed
```

## Module entry points (shift_left)

| CLI group | Source module | UT CLI | UT core | IT CLI |
|-----------|---------------|--------|---------|--------|
| `project` | `core/project_manager.py` | `ut/cli/test_project_cli.py` | `ut/core/test_project_mgr.py` (private only) | `it/cli/test_project_cli.py` |
| `table` | `core/table_mgr.py`, `test_mgr.py` | `ut/cli/test_table_cli.py` | `ut/core/test_table_mgr.py`, `test_test_mgr*.py` | `it/cli/test_table_cli.py`, `test_ut_table_cli.py` |
| `pipeline` | `core/pipeline_mgr.py`, `deployment_mgr.py` | (sparse; many in IT) | `ut/core/test_pipeline_mgr.py`, `test_deployment_mgr.py` | `it/cli/test_pipeline_cli.py`, `test_fact_deployment.py` |

## Adding a new CLI test (template)

```python
@patch("shift_left.core.project_manager.ConfluentCloudClient")  # lowest external mock
def test_list_topics_writes_file(self, mock_client_cls):
    mock_client_cls.return_value.list_topics.return_value = {
        "data": [{"cluster_id": "lkc-1", "topic_name": "orders", "partitions_count": 6}]
    }
    with tempfile.TemporaryDirectory() as tmp:
        result = self.runner.invoke(app, ["list-topics", tmp])
        assert result.exit_code == 0, result.stdout
        assert (pathlib.Path(tmp) / "topic_list.txt").exists()
```

Integration (no mock on manager):

```python
def test_list_impacted_tables_from_fixture(self):
    pipelines_root = os.environ["PIPELINES"]
    # write modified_flink_files.json with absolute paths under pipelines_root
    result = self.runner.invoke(app, [
        "list-impacted-tables",
        "--modified-files-path", str(modified_json),
        "--project-path", pipelines_root,
        "--output-file", str(output_json),
    ])
    assert result.exit_code == 0
    payload = json.loads(output_json.read_text())
    assert "sl_c360_dim_groups" in payload["tables"]
```

## Anti-patterns

- Mocking `project_manager.<public_fn>` in UT CLI while claiming to test that function
- Same integration assertion in `test_project_mgr.py` and `test_project_cli.py`
- IT test that only calls Python API without `CliRunner.invoke`
- Fragile paths (`$HOME/.shift_left/...`) instead of `tempfile.TemporaryDirectory`
- Hardcoded config values (e.g. `_ut`) instead of `get_config().get("app", {}).get("post_fix_unit_test")`

## Reference case

The `project` group consolidation is documented in [examples.md](examples.md).

## Maintenance

When adding a Typer command:

1. Add UT CLI test (mocked externals).
2. Add or mark IT CLI in CLI_IT_COVERAGE.md.
3. Do not add UT core test unless private/deep logic warrants it.
