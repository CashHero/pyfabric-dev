# Getting started

`pyfabric-dev` is a local development framework for Microsoft Fabric
notebooks. It solves two specific problems:

1. **Fabric notebooks can't import Python modules.** You write `.py`
   files in `src/`, the framework compiles them into Fabric `.Notebook`
   artifacts you commit to git, and Fabric runs them. Reviews happen on
   the Python sources, not the notebooks.
2. **Fabric notebooks are nearly impossible to test locally.** The
   framework mocks `notebookutils` / `mssparkutils`, builds a
   Delta-enabled Spark session, and runs your notebook code in a normal
   Python process.

## Install

```bash
pip install pyfabric-dev
```

Or pin in `requirements.txt`:

```
pyfabric-dev==0.5.2
```

Requirements:

- Python **3.13** or **3.14**
- Java 17+ (for PySpark)
- PySpark and `delta-spark` are pulled in transitively

## Five-minute walkthrough

```bash
# 1. Clone the example
git clone https://github.com/CashHero/pyfabric-dev.git
cd pyfabric-dev/examples/minimal-medallion

# 2. Generate notebooks from the Python sources in src/
pyfabric-generate --project-root .

# 3. Run the tests in parallel
pyfabric-test --project-root . --config config/test_batches.json

# 4. Run the full pipeline locally
pyfabric-run-pipeline --project-root . sales_etl
```

The example is described in `examples/minimal-medallion/README.md`.

## CLI surface

| Command | Purpose |
|---|---|
| `pyfabric-generate` | Compile Python modules under `src/` into Fabric `.Notebook` artifacts |
| `pyfabric-run-notebook` | Execute one notebook locally (resolves `%run` deps) |
| `pyfabric-run-pipeline` | Execute a `pipeline-content.json` DAG locally |
| `pyfabric-test` | Run pytest in parallel with isolated Spark metastores per worker |

All four accept `--project-root <dir>` to target a consumer project (defaults to CWD).

## Next steps

- [Source-of-truth pattern](source-of-truth-pattern.md) — how `src/` →
  `.Notebook/` works and why
- [Local execution](local-execution.md) — how the runner mocks Fabric APIs
- [Testing](testing.md) — fixtures, parallelism, batching
- [Hooks reference](hooks-reference.md) — customize the runners for your project
