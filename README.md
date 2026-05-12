# pyfabric-dev

A development framework for [Microsoft Fabric](https://www.microsoft.com/fabric)
notebooks. Write plain Python modules in `src/`, generate Fabric `.Notebook`
artifacts from them, and run + test everything locally with mocked Fabric APIs.

> **Status:** early. Tagged `v0.1.0-alpha`. APIs may shift before `v0.1.0`.
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

```bash
pip install "pyfabric-dev @ git+https://github.com/CashHero/pyfabric-dev.git@v0.1.0-alpha"
```

(or pin to a SHA for reproducibility).

## CLIs

After installing, two console scripts are on your `PATH`:

| Command | Purpose |
|---|---|
| `pyfabric-generate` | Compile `src/` modules into Fabric `.Notebook/` artifacts |
| `pyfabric-test` | Parallel pytest runner with isolated per-worker metastores |

> `pyfabric-run-notebook` and `pyfabric-run-pipeline` (local notebook/pipeline
> executors) exist in source but have CashHero-specific assumptions baked in
> and aren't exposed yet. They'll be cleaned up and shipped in `v0.1.0`.

## Python API

```python
from pyfabric_dev.spark import get_spark_session, get_lakehouse_path
from pyfabric_dev.functions import cf_merge_into_table, cf_overwrite_table
from pyfabric_dev.defs import BRONZE_LAKEHOUSE_NAME, SILVER_LAKEHOUSE_NAME

spark = get_spark_session()
```

## Status & roadmap

This release is the **MVP scaffolding** of code first developed inside the
[CashHero Fabric](https://github.com/CashHero/cashhero-fabric) production
pipeline. The next milestones are:

- [ ] Strip remaining repo-shaped assumptions (project-root path layout)
- [ ] `examples/minimal-medallion` runnable example project
- [ ] GitHub Actions CI (Python 3.10 / 3.11 / 3.12 matrix)
- [ ] Deep docs under `docs/` (currently just this README)
- [ ] Tag `v0.1.0`

## License

[Apache 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.
