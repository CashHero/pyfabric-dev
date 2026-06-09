# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000001",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-00000000aaaa",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# CELL ********************

RUN_MAIN = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run common_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 30_gold_build_daily_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "_logger" not in globals():
    _logger = cf_create_logger("test_30_gold_build_daily_summary")

if "_spark" not in globals():
    _spark = cf_create_spark_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Gold-layer aggregation is also pure — test it with synthetic data."""



def test_aggregate_daily_sums_revenue_per_day(spark):
    df = spark.createDataFrame(
        [
            (1001, dt.datetime(2026, 5, 10, 9, 0), "WIDGET-A", 2, 19.99),
            (1002, dt.datetime(2026, 5, 10, 12, 0), "WIDGET-B", 1, 49.00),
            (1003, dt.datetime(2026, 5, 11, 9, 0), "WIDGET-A", 3, 19.99),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    rows = {r["sales_date"]: r for r in aggregate_daily(df).collect()}

    assert rows[dt.date(2026, 5, 10)]["revenue"] == 2 * 19.99 + 1 * 49.00
    assert rows[dt.date(2026, 5, 10)]["units_sold"] == 3
    assert rows[dt.date(2026, 5, 10)]["orders"] == 2

    assert rows[dt.date(2026, 5, 11)]["revenue"] == 3 * 19.99
    assert rows[dt.date(2026, 5, 11)]["units_sold"] == 3
    assert rows[dt.date(2026, 5, 11)]["orders"] == 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the tests
run_tests(_spark, _logger)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
