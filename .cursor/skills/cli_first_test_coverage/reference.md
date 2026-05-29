# shift_left test layout reference

## Directory structure

```
src/shift_left/
├── shift_left/
│   ├── cli_commands/
│   │   ├── project.py      → project_manager
│   │   ├── table.py        → table_mgr, test_mgr
│   │   └── pipeline.py     → pipeline_mgr, deployment_mgr
│   └── core/
│       ├── project_manager.py
│       ├── table_mgr.py
│       ├── pipeline_mgr.py
│       └── ...
└── tests/
    ├── ut/
    │   ├── cli/              # CliRunner, mocked externals
    │   │   ├── test_project_cli.py
    │   │   ├── test_table_cli.py
    │   │   └── ...
    │   └── core/             # Direct module calls
    │       ├── test_project_mgr.py
    │       ├── test_table_mgr.py
    │       ├── test_pipeline_mgr.py
    │       └── ...
    ├── it/
    │   ├── cli/              # CliRunner + fixture/cloud
    │   │   ├── test_project_cli.py
    │   │   ├── test_table_cli.py
    │   │   ├── test_pipeline_cli.py
    │   │   └── test_ut_table_cli.py
    │   ├── test_fact_deployment.py   # pipeline chain (cloud opt-in)
    │   ├── BaseIT.py
    │   └── CLI_IT_COVERAGE.md        # coverage tracker (update in PR)
    └── data/
        └── flink-project/pipelines/    # default $PIPELINES fixture
```

## Environment for tests

| Variable | UT CLI typical | IT typical |
|----------|----------------|------------|
| `SL_CONFIG_FILE` | `tests/config.yaml` | demo or `tests/config.yaml` |
| `PIPELINES` | `tests/data/flink-project/pipelines` | same or demo path |
| `SHIFT_LEFT_RUN_CLOUD_IT` | unset | `1` for cloud IT |
| `SL_IT_USE_DEMO_ENV` | unset | `1` for demo table IT |

UT CLI sets dummy Confluent credentials at import in `test_project_cli.py` so config validation succeeds.

## Typer command naming

Python `def list_impacted_tables` → CLI `list-impacted-tables`.

Invoke in tests:

```python
from shift_left.cli_commands.project import app
result = self.runner.invoke(app, ["list-impacted-tables", ...])
```

## Coverage doc location

Always update `src/shift_left/tests/it/CLI_IT_COVERAGE.md` when adding/removing CLI tests.

Columns: CLI command | Tests (UT / IT / Gap) | Test file | Status

## Related project rules

- [.cursorrules](../../.cursorrules) — CLI command list
- `.cursor/rules/integration-test-reuse.mdc` — shared IT helpers in BaseIT
- `.cursor/rules/tdd.mdc` — test discipline

## pytest commands

```bash
cd src/shift_left

# Single group UT
uv run python -m pytest tests/ut/cli/test_project_cli.py -v

# Core private helpers only
uv run python -m pytest tests/ut/core/test_project_mgr.py -v

# IT (local)
uv run python -m pytest tests/it/cli/test_pipeline_cli.py -v

# Cloud IT (opt-in)
SHIFT_LEFT_RUN_CLOUD_IT=1 uv run python -m pytest tests/it/test_fact_deployment.py -v
```

## Decision tree: where does this test go?

```
Is there a Typer command?
├─ No → UT core (or ut/utils if utility)
└─ Yes → Does it call external APIs (Confluent/Flink/git)?
    ├─ Yes, heavy → IT CLI (opt-in if secrets)
    │              + UT CLI with mocks at leaf (subprocess, ConfluentCloudClient, statement_mgr)
    └─ No / light mock → UT CLI primary
        └─ Private helper used only internally?
            └─ Yes → optional single UT core test with lowest mock
```
