# Local execution

`pyfabric-dev` runs Fabric notebooks in a normal Python process by
mocking the parts of Fabric that aren't available off-cloud.

## What gets mocked

| Fabric provides | We provide |
|---|---|
| `notebookutils`, `mssparkutils` | `pyfabric_dev.mock_notebookutils` injected into `sys.modules` |
| `notebookutils.runtime.context` (workspace ID etc.) | A static fake context |
| `/lakehouse/default/Files/...` paths | Files under `$DEV_BASE_DIR/lakehouse/default/Files/` |
| Pre-built `_spark` session | A locally-built Delta-enabled session via `pyfabric_dev.spark.create_spark_session()` |
| `notebook.exit()` | A controlled exit that surfaces to the caller |

Importing `pyfabric_dev.local_env` triggers all of this. Test
conftests and run-CLI entry points do this automatically; you only need
to do it explicitly if you write your own driver.

## The runner

`pyfabric_dev.runners.NotebookRunner` parses a `notebook-content.py`,
extracts cells, recursively resolves `%run` dependencies, and executes
each cell in a shared globals dict — so once `%run common_functions`
runs, every subsequent cell can see those helpers.

```python
from pyfabric_dev.runners import NotebookRunner

runner = NotebookRunner(
    notebook_path=Path("silver/20_silver_transform_sales.Notebook/notebook-content.py"),
    project_root=Path.cwd(),
)
runner.run()
```

The `pyfabric-run-notebook` CLI is a thin wrapper around this.

## The pipeline runner

`pyfabric_dev.runners.PipelineRunner` reads a `pipeline-content.json`,
builds a DAG via Kahn's algorithm, and dispatches each activity:

- `TridentNotebook` → `NotebookRunner.execute_notebook()`
- `InvokePipeline` → recursive `run_pipeline()`
- `SetVariable`, `IfCondition` → in-process emulation

It also understands `dependsOn` / `dependencyConditions` (Succeeded /
Failed / Skipped / Completed), so a pipeline that's supposed to run a
cleanup step even on failure will do so locally.

```python
from pyfabric_dev.runners import FabricIdResolver, PipelineRunner

resolver = FabricIdResolver(Path.cwd())
runner = PipelineRunner(Path.cwd(), resolver)
runner.run_pipeline(Path("sales_etl.DataPipeline/pipeline-content.json"))
```

## Where local data lives

Both runners write Delta tables and lakehouse Files/ under one base
directory. The location is `$DEV_BASE_DIR`, or by default
`~/.fabric_dev/<8-char-cwd-hash>/`. The hash isolates parallel
checkouts of the same project so they don't share a metastore.

```
~/.fabric_dev/abc12345/
  lakehouse/default/
    Tables/          ← spark.write Delta tables land here
    Files/           ← `cf_get_lakehouse_path("Files/foo")` resolves under here
  metastore/         ← Derby metastore for the local Spark session
  bronze_lakehouse/
    Tables/          ← per-lakehouse isolation for multi-lakehouse projects
```

Each lakehouse referenced in `config/lakehouse_config.json` gets its
own subdirectory so tables with the same name in different lakehouses
(e.g. `silver_lakehouse.accounts` vs `gold_lakehouse.accounts`) don't
collide.

## Environment overrides

Several knobs are env-var driven so consumers can override defaults
without editing the package:

| Variable | Default | Purpose |
|---|---|---|
| `DEV_BASE_DIR` | `~/.fabric_dev/<hash>` | Full path to the local dev base |
| `FABRIC_DEV_BASE_DIR_NAME` | `.fabric_dev` | Directory name under `~` |
| `FABRIC_MEDALLION_LAYERS` | `bronze,silver,gold` | Comma-separated layer names |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `FABRIC_WORKSPACE_ID` | `local-dev-workspace-id` | Workspace ID returned by mocked context |
| `KEY_VAULT_URL` | (unset) | If set, mock notebookutils delegates secret reads to Azure |

Drop a `.env` file at the project root; `python-dotenv` loads it on import.
