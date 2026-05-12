# minimal-medallion

A tiny end-to-end Microsoft Fabric medallion pipeline that exercises every
piece of `pyfabric-dev`: source-of-truth Python modules → generated `.Notebook`
artifacts → local Spark/Delta execution → parallel pytest.

## Domain

A point-of-sale system records sales events. Raw events arrive as CSV with
inconsistent types. We use the medallion architecture to land them, clean
them, and surface a daily summary.

```
data/sales.csv          (raw input)
  ↓ bronze.ingest_sales         — land as Delta, append-only
bronze_lakehouse.sales_raw
  ↓ silver.transform_sales      — type, drop nulls, dedupe
silver_lakehouse.sales_clean
  ↓ gold.build_daily_summary    — aggregate revenue per day
gold_lakehouse.sales_daily_summary
```

## Layout

```
src/
  common/defs.py                — table-name constants
  bronze/ingest_sales.py        — CSV → Delta
  silver/transform_sales.py     — clean & type
  gold/build_daily_summary.py   — aggregate
tests/
  test_silver_transform_sales.py
  test_gold_build_daily_summary.py
config/
  lakehouse_config.json
  test_batches.json
data/
  sales.csv
sales_etl.DataPipeline/
  pipeline-content.json         — bronze → silver → gold DAG
# Generated artifacts (committed; do not edit by hand):
bronze/10_bronze_ingest_sales.Notebook/
silver/20_silver_transform_sales.Notebook/
gold/30_gold_build_daily_summary.Notebook/
common/common_defs.Notebook/
common/common_functions.Notebook/
```

## Workflow

From this directory:

```bash
# 1. Generate notebooks from src/
pyfabric-generate --project-root .

# 2. Run a single notebook locally
pyfabric-run-notebook --project-root . silver/20_silver_transform_sales.Notebook

# 3. Run the full pipeline locally
pyfabric-run-pipeline --project-root . sales_etl

# 4. Run tests in parallel (uses config/test_batches.json)
pyfabric-test --project-root . --config config/test_batches.json
```

## What's intentionally absent

This example is intentionally tiny. It does **not** demonstrate:

- Watermark-based incremental ingestion
- Multi-source routing
- Customization hooks (`RunnerHooks`) — see the `pyfabric_dev.runners`
  docstrings for that
- Backup snapshots

If you need a richer reference, look at
[cashhero-fabric](https://github.com/CashHero/cashhero-fabric) — the
production pipeline this framework was extracted from.
