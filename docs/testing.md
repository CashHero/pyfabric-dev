# Testing

`pyfabric-dev` doesn't impose a test framework — write normal pytest.
What it adds is two pieces of infrastructure:

1. A test runner (`pyfabric-test`) that runs pytest files in parallel
   with per-worker isolated Spark metastores, mirroring how Fabric runs
   its test pipeline.
2. Conventions for organizing tests so the generator can produce
   Fabric-runnable test notebooks from the same source.

## Writing a test

The example project's `tests/test_silver_transform_sales.py`:

```python
from src.silver.transform_sales import clean_sales


def test_clean_sales_casts_types(spark):
    raw = spark.createDataFrame(
        [("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99")],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()[0]
    assert out["order_id"] == 1001
```

Two patterns make this work:

- **`clean_sales` is a pure transformation.** It takes a DataFrame, returns
  a DataFrame. The `run(spark, logger)` function is a thin wrapper that
  reads → transforms → writes. Keeping the transformation pure means
  tests don't need a lakehouse — just an in-memory DataFrame.
- **The `spark` fixture comes from conftest.** Use the framework's
  Delta-enabled session, not `SparkSession.builder.getOrCreate()`
  directly, so tests see the same configuration production does.

## conftest.py

A minimal conftest:

```python
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyfabric_dev.local_env  # noqa: F401 — mocks notebookutils, ensures dev dirs
from pyfabric_dev.spark import create_spark_session


@pytest.fixture(scope="session")
def spark():
    return create_spark_session()


@pytest.fixture(scope="session")
def logger():
    return logging.getLogger("tests")
```

## Running tests

Plain pytest works fine for one-off runs:

```bash
pytest tests/ -v
```

For CI and full-suite runs, use `pyfabric-test` — it parallelizes
across processes and isolates each worker's Spark metastore so Derby
locks don't collide:

```bash
pyfabric-test --project-root . --config config/test_batches.json
```

## Test batching

`pyfabric-test` reads its stage/group structure from a JSON file. This
mirrors a Fabric test pipeline that runs layer-by-layer with intra-layer
batches:

```json
{
  "notebook_runner_scripts": ["tests/test_customizations_processor.py"],
  "groups": {
    "common":        {"stage": 1, "files": ["tests/test_common_defs.py"]},
    "bronze_batch1": {"stage": 1, "files": ["tests/test_bronze_ingest.py"]},
    "bronze_batch2": {"stage": 2, "files": ["tests/test_bronze_validate.py"]},
    "silver":        {"stage": 1, "files": ["tests/test_silver_*.py"]}
  },
  "stage_order":   ["common", "bronze_batch1", "bronze_batch2", "silver"],
  "stage_aliases": {"bronze": ["bronze_batch1", "bronze_batch2"]}
}
```

- **`stage`** controls execution order. Stage 1 groups run in parallel;
  stage 2 starts after all stage-1 groups finish. Stage 2 is skipped if
  any stage-1 group fails.
- **`stage_order`** controls the deterministic listing for dry-run output.
- **`stage_aliases`** powers `--stage bronze` (run only bronze batches).
- **`notebook_runner_scripts`** lists files that should be invoked with
  `python <file>` instead of `pytest`. Useful for thin scripts that
  drive `NotebookRunner` directly.

If you don't ship a config, `pyfabric-test` auto-discovers
`tests/test_*.py` and runs them as a single stage.

## Test notebooks for Fabric

The generator also produces a test notebook for each pytest file
whose name matches a generated source notebook
(e.g. `tests/test_silver_transform_sales.py` →
`silver/tests/test_20_silver_transform_sales.Notebook/`). That lets
Fabric run the same test code on-cloud.

Test notebooks set `RUN_MAIN = False` before `%run`-ing the production
notebook, which loads constants and helpers without executing the
`run()` function.
