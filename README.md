# pyfabric-dev

A development framework for [Microsoft Fabric](https://www.microsoft.com/fabric)
notebooks. Write plain Python modules in `src/`, generate Fabric `.Notebook`
artifacts from them, and run + test everything locally with mocked Fabric APIs.

> **Status:** `v0.3.0`. APIs are stabilizing but pre-1.0; minor versions may break.
>
> **Disclaimer:** not affiliated with or endorsed by Microsoft.

## Why

Fabric notebooks have two pain points:

1. **They can't `import` Python modules.** Code must be inlined into notebook
   cells, which fights every Python convention.
2. **They're nearly impossible to test locally.** `mssparkutils` and
   `notebookutils` only exist in the Fabric runtime.

`pyfabric-dev` solves both:

- A **source-of-truth pattern**: you write modules under `src/`; the generator
  compiles them into `.Notebook/` artifacts (committed to git, ready for Fabric
  Git integration).
- A **local execution layer**: mocks `mssparkutils` / `notebookutils`, sets up
  PySpark + Delta with a per-checkout Derby metastore, and runs your notebooks
  and pipelines off-cloud.
- A **parallel test runner** that mirrors a Fabric pipeline's batching, so the
  same suite that passes locally is the one that runs in CI.

## Install

Requires **Python 3.13 or 3.14** and a JDK 17+ on `$PATH` (for PySpark).

```bash
pip install pyfabric-dev
```

## CLIs

After installing, four console scripts are on your `PATH`:

| Command | Purpose |
|---|---|
| `pyfabric-generate` | Compile `src/` modules into Fabric `.Notebook/` artifacts |
| `pyfabric-run-notebook` | Execute one notebook locally (resolves `%run` deps) |
| `pyfabric-run-pipeline` | Execute a `pipeline-content.json` DAG locally |
| `pyfabric-test` | Parallel pytest runner with isolated per-worker metastores |

All four accept `--project-root <dir>` (default: CWD).

## Five-minute walkthrough

```bash
git clone https://github.com/CashHero/pyfabric-dev.git
cd pyfabric-dev/examples/minimal-medallion

pyfabric-generate    --project-root .
pyfabric-test        --project-root . --config config/test_batches.json
pyfabric-run-pipeline --project-root . --dry-run sales_etl
```

`examples/minimal-medallion/` is a working bronze/silver/gold pipeline over
toy sales data. See its `README.md` for details.

## Documentation

- [Getting started](docs/getting-started.md)
- [Source-of-truth pattern](docs/source-of-truth-pattern.md) — how `src/` → `.Notebook/` works
- [Local execution](docs/local-execution.md) — how the runner mocks Fabric APIs
- [Testing](docs/testing.md) — fixtures, parallelism, batching
- [Hooks reference](docs/hooks-reference.md) — customizing the runners for your project

## Used in production by

- CashHero — the internal pipeline this framework was extracted from.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs welcome.

## License

[Apache 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.
