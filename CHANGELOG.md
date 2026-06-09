# Changelog

All notable changes to `pyfabric-dev` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-06-09

### Fixed
- Local CLI notebook runs (`pyfabric-run-notebook` / `pyfabric-run-pipeline`)
  now create a Delta-enabled Spark session before any cell executes. A
  hook-less run previously fell through to the notebook's own
  `SparkSession.builder.getOrCreate()`, yielding a session without Delta wired
  in, so managed-table writes failed. Runs that supply `_spark` via hooks
  (e.g. `pyfabric-test`) are unaffected.
- `examples/minimal-medallion` now runs end-to-end locally and in CI:
  - Consolidated the three per-layer lakehouses into a single `lakehouse` so
    unqualified table reads/writes resolve identically locally and in Fabric.
  - Re-export `Logger` and `pyspark.sql.functions as F` from
    `common_functions` so generated layer notebooks (which strip those
    imports) resolve them at runtime.
  - Replaced `mode("overwrite").saveAsTable` with a `cf_overwrite_table`
    helper (`DROP TABLE` + `saveAsTable`) that also works on the open-source
    Delta build used in local dev.

### Added
- `pyfabric_dev.local_config` now exports the resolved `DEV_BASE_DIR` into
  `os.environ` (via `setdefault`, preserving any value the parallel test
  runner injects). This lets inlined notebook code detect local execution and
  route lakehouse paths under `DEV_BASE_DIR` without importing the module —
  which it can't under Fabric.
- `examples/minimal-medallion/stage_data.py` (copies the sample CSV into the
  local lakehouse `Files/` tree) and `requirements.txt` (Fabric Runtime 1.3
  pins: Spark 3.5 / Delta 3.2). CI now runs the pipeline end-to-end
  (stage → bronze → silver → gold) alongside the unit tests.

## [0.4.1] - 2026-06-09

### Changed
- Removed leftover CashHero branding from framework defaults, comments, and
  docstrings: dropped the stale `.cashhero_fabric_dev/` `.gitignore` entry
  (superseded by `.fabric_dev/`), and genericized example values in
  `functions.py`, `run_tests_parallel.py`, and the generator's config docs.
  No behavior change.

## [0.4.0] - 2026-06-09

### Added
- **Config-driven generator** via an optional `config/notebook_generation.json`.
  The generator no longer hardcodes any consumer's module or notebook names;
  behavior is declared per project:
  - `inline_framework_modules` — inline the `defs`/`functions`/`fabric` runtime
    modules straight from the installed `pyfabric_dev` package instead of from
    vendored copies under `src/common`, so consumers keep only their own code in
    `src/common`.
  - `common_functions_extra_modules` — extra `src/common` modules to inline into
    `common_functions` after the primary functions module.
  - `helper_notebooks` — standalone notebooks that each inline a single
    `src/common` module (replaces the hardcoded QuickBooks generators).
  - `notebook_run_dependencies` — per-notebook `%run` dependencies, with an
    optional `suppress_main` wrapper.

### Changed
- Removed all hardcoded CashHero-specific names (`cashhero_org`, `quickbooks_*`,
  `ingest_from_priority`, `onboard_org`, …) from the generator; collapsed the
  two QuickBooks notebook generators into one parametrized
  `generate_helper_notebook`. The default (no config file) path is unchanged and
  byte-identical to prior output.

## [0.3.1] - 2026-06-09

### Added
- GitHub Actions Trusted Publishing workflow (`.github/workflows/publish.yml`)
  that builds and publishes to PyPI on release.

### Changed
- Install instructions across the README and docs now use PyPI
  (`pip install pyfabric-dev`), replacing the previous Git-URL install.
- Removed documentation links to the closed-source internal pipeline repo.

## [0.3.0] - 2026-06-03

First release published to **PyPI** — install with `pip install pyfabric-dev`.

### Added
- `cf_merge_into_table`: new optional `not_matched_by_source_condition`
  parameter — a SQL predicate on the `target` alias (e.g.
  `"target.server IN ('acme')"`) that **scopes** `whenNotMatchedBySource`
  deletes, so a source that snapshots only a subset of the table doesn't
  delete rows outside its scope.

### Fixed
- `cf_merge_into_table`: the `whenNotMatchedBySourceDelete()` clause was
  silently dropped — the Delta builder returns a new object, but the result
  wasn't reassigned, so the clause never reached `execute()` and deletes never
  ran (framework-wide). Now reassigned and effective.

### Changed
- **BREAKING (behavioral):** `cf_merge_into_table`'s
  `delete_when_not_matched_by_source` now defaults to `False` (was `True`).
  Because the delete clause was previously a no-op, no caller actually got
  deletes; the new default matches the prior observed behavior and prevents the
  bugfix above from silently enabling deletes for existing callers. Opt in
  explicitly where deletes are wanted.

## [0.2.0] - 2026-05-12

### Added
- `examples/minimal-medallion/` — a working bronze/silver/gold project
  over toy sales data, exercising all four CLIs end-to-end. Generates,
  tests, and runs locally.
- `docs/` — getting-started, source-of-truth pattern, local execution,
  testing, hooks reference.
- GitHub Actions CI matrix (`.github/workflows/ci.yml`) running pytest
  and the example's full generate+test cycle on Python 3.13 and 3.14.

### Changed
- **BREAKING**: minimum Python is now **3.13** (was 3.10). Python 3.13
  and 3.14 are the supported matrix going forward.
- `pyfabric-generate` now accepts `--project-root` and threads it
  through every path computation.
- `pyfabric-generate` no longer writes empty stub notebooks for
  CashHero-specific modules (`quickbooks_auth`, `quickbooks_client`)
  when the consumer doesn't ship those sources. The corresponding
  generators are gated on file existence.

### Migration from 0.1.0
- Bump your Python interpreter to 3.13 or 3.14.
- If you were invoking `pyfabric-generate` from the package's install
  directory and relying on the implicit project-root, add an explicit
  `--project-root .` from your project root.

## [0.1.0] - 2026-05-12

### Added
- `pyfabric_dev.runners` subpackage with `NotebookRunner`, `PipelineRunner`,
  `FabricIdResolver`, and `RunnerHooks`. Consumers register hooks to inject
  project-specific globals (`initial_globals`), override Fabric-bound
  helpers after `%run common_functions` executes
  (`common_functions_overrides`), or supply per-notebook symbols
  (`notebook_globals`) — keeping the runners themselves agnostic of any
  particular project's code.
- CLI entry points `pyfabric-run-notebook` and `pyfabric-run-pipeline`,
  bringing the total to four console scripts.
- Test-batch JSON config (loaded via `--config`) so `pyfabric-test` can
  mirror an arbitrary project's Fabric test pipeline structure without
  hardcoding stage/group names. Auto-discovers `tests/test_*.py` as a
  single stage when no config is supplied.

### Changed
- `pyfabric-test` no longer hardcodes CashHero stage groupings.
- Worker tmpdir now lives under `DEV_BASE_DIR / _parallel_workers`
  instead of a hardcoded `~/.cashhero_fabric_dev/...` path.

## [0.1.0-alpha] - 2026-05-12

Initial extraction from the internal production pipeline.

### Added
- `pyfabric_dev` package with `defs`, `spark`, `fabric`, `functions`,
  `mock_notebookutils`, `local_config`, `local_env`.
- CLIs: `pyfabric-generate`, `pyfabric-test`.
- Apache 2.0 license.

### Known limitations
- Path assumptions in the generator expect a CashHero-shaped repo layout
  (`src/<layer>/*.py`, `<layer>/*.Notebook/`). Configuration hooks for other
  layouts will land before `v0.1.0`.
- Local notebook and pipeline runners (`run_notebook.py`, `run_pipeline.py`)
  are not yet shipped — they had ~10 CashHero coupling points; will land in
  `v0.1.0` after a cleanup pass.
- No `examples/` project yet.
- No CI matrix yet.
