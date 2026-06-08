# Changelog

All notable changes to `pyfabric-dev` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
