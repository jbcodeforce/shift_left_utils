# Examples: CLI-first coverage consolidation

## Reference: `project_manager` (completed)

### Before

| Area | UT core (`test_project_mgr.py`) | UT CLI (`test_project_cli.py`) |
|------|--------------------------------|--------------------------------|
| `build_project_structure` | 2 direct tests | `test_init_project` |
| `report_table_cross_products` | integration test | CLI test |
| `list_modified_files` | 7 mocked unit tests | 3 tests |
| `impacted_tables_by_modifications` | integration test | mocked entirely |
| `update_unit_test_ddl_*` | integration test | none |
| `_assess_flink_statement_state` | 1 test | indirect |

**Problem**: 21 PM-related tests, duplicated behavior, IT coverage doc out of sync.

### After

| Area | UT core | UT CLI |
|------|---------|--------|
| All public APIs | deleted | kept/added CLI tests |
| `_assess_flink_statement_state` | 1 test (mocks `statement_mgr.get_statement`) | exercised via `list_modified_files` |
| `get_topic_list` | — | `test_list_topics_writes_file` |
| `isolate_data_product` | deleted | `test_isolate_data_product` |
| `impacted_tables_by_modifications` | deleted | real fixture, no manager mock |

**Result**: 1 UT core + 24 UT CLI tests; single behavioral owner per command.

### Key edits

1. **Mock fix** — `test_list_modified_files_success`:
   - Before: `@patch('...project_manager._assess_flink_statement_state')`
   - After: `@patch('shift_left.core.project_manager.statement_mgr.get_statement')`

2. **Integration fix** — replaced mocked `test_list_impacted_tables_writes_json` with `test_list_impacted_tables_from_fixture` using flink-project paths.

3. **Config-aware assertion** — UT DDL sync:
   ```python
   post_fix = get_config().get("app", {}).get("post_fix_unit_test", "_ut")
   assert f"sl_c360_src_groups{post_fix}" in synced
   ```

---

## Reference: `table` group (completed)

### Before

| Area | UT core | UT CLI | IT CLI |
|------|---------|--------|--------|
| `init` / folder structure | `test_create_table_structure` | `test_init_table` | — |
| `update-makefile` / `update-all-makefiles` | 2 direct tests | — | — |
| `update-tables` | `update_sql_content_*` (helper only) | — | 3 tests (cloud-gated, never ran in CI) |
| `delete-unit-tests` | `test_delete_test_artifacts` | — | cloud IT |
| `explain` error path | — | — | cloud-gated with happy paths |

### After

| Area | UT core | UT CLI | IT CLI |
|------|---------|--------|--------|
| Naming / `update_sql_content_for_file` | kept (8 tests) | — | — |
| All table Typer commands | overlap deleted | 16 tests | explain demo only (3 tests) |
| `test_test_mgr` deep helpers | kept (~24 tests) | — | — |

**Result**: Moved local tests out of cloud-gated IT class; UT CLI is primary owner.

### Key edits

1. **IT split** — `TestTableCLICloud` in `it/cli/test_table_cli.py` keeps only demo `explain` tests; `update-tables` moved to UT.
2. **Makefile CLI** — `test_update_makefile` / `test_update_all_makefiles` replace core duplicates.
3. **UT lifecycle smokes** — `delete-unit-tests`, `run-unit-tests`, `validate-unit-tests`, `migrate` with lowest-level mocks.

---

## Reference: `pipeline` group (completed)

### Before

| Area | UT core | UT CLI | IT CLI |
|------|---------|--------|--------|
| `report` | 2 report tests | — | 2 local IT tests |
| `build-all-metadata` | `test_all_pipeline_def` | — | cloud chain only |
| `prepare` | `test_prepare_table` | — | cloud opt-in |
| `deploy` / `undeploy` / execution plan | deep `test_deployment_mgr.py` | — | cloud chain |
| `field-lineage` / `healthcheck` | — | — | Gap |

### After

| Area | UT core | UT CLI | IT CLI |
|------|---------|--------|--------|
| Build graph depth | kept (4 tests in `test_pipeline_mgr.py`) | — | — |
| Deploy graph/sort/undeploy | kept (~12 tests in `test_deployment_mgr.py`) | — | — |
| All pipeline Typer commands | overlap deleted | 17 tests in `test_pipeline_cli.py` | cloud only (4 tests) |

**Result**: UT CLI is primary owner; IT slimmed to `TestPipelineCLICloud` for Confluent API commands.

### Key edits

1. **New file** — `tests/ut/cli/test_pipeline_cli.py` with moved local IT tests plus metadata, deploy, prepare, field-lineage, and healthcheck smokes.
2. **IT split** — `TestPipelineCLICloud` keeps `prepare` and `analyze-pool-usage` only.
3. **Core trim** — removed 3 report/build-all tests from `test_pipeline_mgr.py` and `test_prepare_table` from `test_deployment_mgr.py`.

---

## Template: assess next CLI group

## CLI_IT_COVERAGE.md update snippet

When adding `test_update_ut_ddl_syncs_foundation_ddl`:

```markdown
| `update-ut-ddl` | UT | `ut/cli/test_project_cli.py` | okay |
```

When adding IT for `list-topics` already in IT file:

```markdown
| `list-topics` | UT + IT | `ut/cli/test_project_cli.py`, `it/cli/test_project_cli.py` | okay |
```
