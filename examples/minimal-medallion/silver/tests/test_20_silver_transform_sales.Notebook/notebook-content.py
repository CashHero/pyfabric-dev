# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000001",
# META       "default_lakehouse_name": "bronze_lakehouse",
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

%run 20_silver_transform_sales

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if "_logger" not in globals():
    _logger = cf_create_logger("test_20_silver_transform_sales")

if "_spark" not in globals():
    _spark = cf_create_spark_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Silver-layer cleaning is a pure transformation, so we can test it
against an in-memory DataFrame without any lakehouse plumbing."""


def test_clean_sales_casts_types(spark):
    raw = spark.createDataFrame(
        [("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99")],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()[0]

    assert out["order_id"] == 1001
    assert out["quantity"] == 2
    assert out["unit_price"] == 19.99


def test_clean_sales_drops_rows_with_null_quantity(spark):
    raw = spark.createDataFrame(
        [
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
            ("1002", "2026-05-10T10:00:00Z", "WIDGET-B", None, "7.50"),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()

    assert len(out) == 1
    assert out[0]["order_id"] == 1001


def test_clean_sales_deduplicates_by_order_id(spark):
    raw = spark.createDataFrame(
        [
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()

    assert len(out) == 1

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
