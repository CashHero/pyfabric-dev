# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`pyfabric-dev` is **framework code** (a pip-installable library), not an application. It lets people develop Microsoft Fabric notebooks as plain Python modules, generate Fabric `.Notebook` artifacts from them, and run + test those notebooks locally with mocked Fabric APIs (`notebookutils`/`mssparkutils`). The package under `pyfabric_dev/` is the framework; `examples/minimal-medallion/` is a working *consumer* project used for manual testing and docs.

Keep the line in CONTRIBUTING.md in mind: business-logic helpers (specific data sources, schemas, ERP/accounting integrations) do **not** belong here. Only generic-to-any-Fabric-medallion-project code does.

## Commands

Requires Python 3.13/3.14 and a JDK 17+ on `$PATH` (PySpark needs the JVM).

```bash
pip install -e ".[dev]"        # dev install
pytest                          # run the framework's own test suite (tests/)
pytest tests/test_smoke.py -v   # single test file
pytest tests/test_functions_merge.py::test_name   # single test
```

The four console scripts (defined in `[project.scripts]`) all take `--project-root <dir>` (default CWD) and operate on a *consumer* project, not this repo:

| Command | Entry point | Purpose |
|---|---|---|
| `pyfabric-generate` | `cli/generate_notebooks.py` | Compile `src/` modules → `.Notebook/` artifacts |
| `pyfabric-run-notebook` | `cli/run_notebook.py` | Execute one notebook locally (resolves `%run` deps) |
| `pyfabric-run-pipeline` | `cli/run_pipeline.py` | Execute a `pipeline-content.json` DAG locally |
| `pyfabric-test` | `cli/run_tests_parallel.py` | Parallel pytest runner with isolated per-worker metastores |

Releasing (maintainers): bump `version` in `pyproject.toml` + add a dated `CHANGELOG.md` section, merge to `main`, then from a clean `main` checkout run `./scripts/release.sh <version>` (`--dry-run` to preview). It enforces preconditions (version matches `pyproject.toml`, clean tree, on `main`, tag unused) then tags + pushes `v<version>` and creates a GitHub release — publishing that release triggers `.github/workflows/publish.yml` to ship to PyPI via trusted publishing. Don't tag or push releases by hand.

To exercise the full flow end-to-end, run them against the example:

```bash
cd examples/minimal-medallion
pyfabric-generate     --project-root .
pyfabric-test         --project-root . --config config/test_batches.json
pyfabric-run-pipeline --project-root . --dry-run sales_etl
```

## Architecture

Three subsystems, loosely coupled. Read them in this order.

### 1. Generation (`cli/generate_notebooks.py`, ~1700 lines)

The source-of-truth inversion: a consumer writes modules under `src/{bronze,silver,gold,backup,common}/`, and the generator compiles each into a committed Fabric `.Notebook/` directory (`notebook-content.py` + `.platform`). Consumers edit only `src/`; the generated artifacts are committed because Fabric's git integration reads `.Notebook` dirs, not Python modules.

Key mechanics:
- A module is a "pipeline module" iff it defines a top-level `run(spark, logger)`. The generated notebook inlines the whole module body and calls `run()`.
- `src.common.*` and `pyfabric_dev.*` imports are **stripped** — those packages don't exist in the Fabric runtime. Shared helpers arrive instead via an inlined `%run common_functions` cell. `src/common/functions.py` → `common/common_functions.Notebook`; `src/common/defs.py` → `common/common_defs.Notebook`.
- Layer → filename prefix mapping lives in `LAYER_PREFIXES` (`bronze`→`10_bronze_`, `silver`→`20_silver_`, `gold`→`30_gold_`). These prefixes drive both notebook naming and pipeline ordering.
- `config/lakehouse_config.json` (per-layer lakehouse UUIDs) is embedded into each notebook's METADATA block so Fabric attaches the right lakehouse at runtime.
- The generator has its own static validator (`validate_all_notebooks`, AST-based undefined-name detection) — `--strict-validate` makes validation failures exit non-zero. Test notebooks set `RUN_MAIN = False` before `%run`-ing the production notebook so helpers load without executing `run()`.

The intended consumer CI check: run `pyfabric-generate` and assert a clean tree (`git diff --exit-code`) to prevent source/notebook drift.

### 2. Local execution (`runners/`, `spark.py`, `mock_notebookutils.py`)

`runners/notebook.py` (`NotebookRunner`) parses `notebook-content.py` — splitting on `# CELL`/`# PARAMETERS CELL`/`# METADATA` markers — and `exec`s each cell into one **shared `globals_dict`**, so post-run state mirrors Fabric. `%run` targets are resolved to local files and executed recursively (deduped via `executed_notebooks`). It auto-registers Delta tables found on disk as a Spark database matching the notebook's `default_lakehouse_name`.

`runners/pipeline.py` (`PipelineRunner`) parses `pipeline-content.json`, resolves activity `logicalId`s to local paths via `FabricIdResolver` (which scans every `.platform` file), topologically sorts activities into levels, and dispatches `TridentNotebook` activities to a **single shared** `NotebookRunner` (so multi-notebook pipelines share namespace/Spark session). Supports `InvokePipeline`, `SetVariable`, `IfCondition`; skips `Teams`/`RefreshDataflow`. `dependencyConditions` (Succeeded/Failed/Completed/Skipped) are honored.

`spark.py` is the environment-detection seam. `get_environment()` returns `"fabric"` vs `"local"` by inspecting whether `notebookutils` is the real module or the mock. Each public helper (`create_spark_session`, `get_lakehouse_path`, etc.) branches on that and delegates to `fabric.py` (real) or a local impl. `cf_*` aliases at the bottom exist because generated notebook code calls those names. The local Spark session is fully Delta-configured and points at a per-checkout Derby metastore.

`mock_notebookutils.py` stands in for `notebookutils`/`mssparkutils` locally (secrets resolve via Azure Key Vault if `KEY_VAULT_URL` is set, else env vars). It's installed into `sys.modules` as a side effect of importing `pyfabric_dev.local_env` — that's why CLIs and conftests do `import pyfabric_dev.local_env  # noqa: F401`.

**Extension via `RunnerHooks`** (`runners/hooks.py`): the CLIs run hook-less. Consumers needing project-specific behavior import `NotebookRunner`/`PipelineRunner` directly and pass hooks: `initial_globals` (seed Fabric-API replacements), `common_functions_overrides` (re-applied after each `%run common_functions` so local helpers aren't clobbered), `notebook_globals` (per-notebook symbol injection for stripped imports).

### 3. Parallel testing (`cli/run_tests_parallel.py`)

Runs pytest files across a `ProcessPoolExecutor`, giving each worker a distinct `DEV_BASE_DIR` so concurrent Spark sessions don't collide on Derby locks. Batching comes from a JSON config (`config/test_batches.json`): `groups` each have a `stage` (stage-1 groups run parallel; stage-2 runs after and is skipped if stage-1 fails), plus `stage_order`, `stage_aliases` (powers `--stage bronze`), and `notebook_runner_scripts` (files invoked with `python` instead of `pytest`). No config ⇒ auto-discover `tests/test_*.py` as one stage.

## Configuration & isolation

`local_config.py` is the central config module, entirely env-var driven so consumers override without editing the package (loads a `.env` from CWD via python-dotenv). Critical detail: `DEV_BASE_DIR` defaults to `~/.fabric_dev/<md5(cwd)[:8]>` — the **per-worktree hash isolates parallel checkouts** so their lakehouses/metastores don't collide. Other knobs: `FABRIC_MEDALLION_LAYERS` (default `bronze,silver,gold`), `SPARK_MASTER`, `FABRIC_DEV_*_MEMORY`, `FABRIC_WORKSPACE_ID`, `KEY_VAULT_URL`.

## Conventions

- Pre-1.0 (`v0.5.2`); minor versions may break. No backwards-compat shims, feature flags, or speculative abstractions (per CONTRIBUTING.md).
- Comments only when WHY is non-obvious — don't restate WHAT.
- Tests stay green on `main`; fix flaky tests rather than `@skip`.
- Commits are DCO-signed (`git commit -s`).
- Update `CHANGELOG.md` for user-facing changes.
