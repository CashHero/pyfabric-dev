# Changelog

All notable changes to `pyfabric-dev` are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

Initial extraction from the
[cashhero-fabric](https://github.com/CashHero/cashhero-fabric) production
pipeline.

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
- No `examples/` project yet — see the README and the cashhero-fabric repo
  for real-world usage.
- No CI matrix yet.
