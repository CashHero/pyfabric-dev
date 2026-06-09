# minimal-medallion

A tiny end-to-end Microsoft Fabric medallion pipeline that exercises every
piece of `pyfabric-dev`: source-of-truth Python modules → generated `.Notebook`
artifacts → local Spark/Delta execution → parallel pytest.

## Domain

A point-of-sale system records sales events. Raw events arrive as CSV with
inconsistent types. We use the medallion architecture to land them, clean
them, and surface a daily summary.

```
data/sales.csv          (raw input, staged into the lakehouse Files/ area)
  ↓ bronze.ingest_sales         — land as a Delta table (full overwrite)
lakehouse.sales_raw
  ↓ silver.transform_sales      — type, drop nulls, dedupe
lakehouse.sales_clean
  ↓ gold.build_daily_summary    — aggregate revenue per day
lakehouse.sales_daily_summary
```

All three tiers live in a **single lakehouse**; the table names (`sales_raw`,
`sales_clean`, `sales_daily_summary`) mark the medallion layer. Keeping it to one
lakehouse means unqualified table reads resolve the same way locally and in
Fabric.

## Layout

```
src/
  common/defs.py                — table-name constants
  common/functions.py           — cf_* helpers inlined into common_functions
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
requirements.txt                — Fabric Runtime 1.3 pins (Spark 3.5 / Delta 3.2)
stage_data.py                   — copy the sample CSV into the local lakehouse Files/
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
# 0. Install Fabric-matching dependencies (Spark 3.5 / Delta 3.2).
#    Installing the bare package would pull Spark 4.x, which no Fabric
#    runtime uses — so always install via requirements.txt.
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Generate notebooks from src/
pyfabric-generate --project-root .

# 2. Stage the sample CSV into the local lakehouse Files/ area.
#    (In Fabric you upload data/sales.csv to the lakehouse instead.)
python stage_data.py

# 3. Run the full pipeline locally (bronze → silver → gold)
pyfabric-run-pipeline --project-root . sales_etl

# 4. Run tests in parallel (uses config/test_batches.json)
pyfabric-test --project-root . --config config/test_batches.json
```

You can also run one notebook on its own, e.g. the self-contained bronze
ingest (after step 2): `pyfabric-run-notebook --project-root .
bronze/10_bronze_ingest_sales.Notebook`. A downstream notebook expects its
upstream tables to exist, so run the pipeline (step 3) for the full chain.

## What's intentionally absent

This example is intentionally tiny. It does **not** demonstrate:

- Watermark-based incremental ingestion
- Multi-source routing
- Customization hooks (`RunnerHooks`) — see the `pyfabric_dev.runners`
  docstrings for that
- Backup snapshots

This minimal example is the public reference. The framework was extracted from
an internal production pipeline.
