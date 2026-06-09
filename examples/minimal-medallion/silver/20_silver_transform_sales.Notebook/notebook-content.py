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
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000001"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run common_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Allow callers/tests to inject an existing Spark session / logger before executing this
# notebook (e.g., setting globals()['_spark'] or globals()['_logger']).
if "_logger" not in globals():
    _logger = cf_create_logger("20_silver_transform_sales")

if "_spark" not in globals():
    _spark = cf_create_spark_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Silver: cast types, drop nulls, deduplicate the raw sales rows."""




def clean_sales(df):
    """Pure transformation — testable without touching a lakehouse."""
    return (
        df
        .withColumn("order_id", F.col("order_id").cast(IntegerType()))
        .withColumn("occurred_at", F.col("occurred_at").cast(TimestampType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .dropna(subset=["order_id", "occurred_at", "sku", "quantity", "unit_price"])
        .dropDuplicates(["order_id"])
    )


def run(spark: SparkSession, logger: Logger) -> None:
    raw = spark.table(BRONZE_TABLE_SALES_RAW)
    logger.info(f"Reading {BRONZE_TABLE_SALES_RAW}: {raw.count()} rows")

    clean = clean_sales(raw)
    logger.info(f"After clean: {clean.count()} rows")

    cf_overwrite_table(spark, clean, SILVER_TABLE_SALES_CLEAN)  # noqa: F821
    logger.info(f"Wrote {SILVER_TABLE_SALES_CLEAN}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the pipeline
if "RUN_MAIN" not in globals():
    RUN_MAIN = True

if RUN_MAIN:
    run(_spark, _logger)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
