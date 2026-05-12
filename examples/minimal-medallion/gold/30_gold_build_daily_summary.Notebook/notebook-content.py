# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-000000000003",
# META       "default_lakehouse_name": "gold_lakehouse",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-00000000aaaa",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-000000000002"
# META         },
# META         {
# META           "id": "00000000-0000-0000-0000-000000000003"
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
    _logger = cf_create_logger("30_gold_build_daily_summary")

if "_spark" not in globals():
    _spark = cf_create_spark_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""Gold: roll the cleaned events up into a per-day revenue summary."""




def aggregate_daily(df):
    return (
        df
        .withColumn("sales_date", F.to_date("occurred_at"))
        .groupBy("sales_date")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy("sales_date")
    )


def run(spark: SparkSession, logger: Logger) -> None:
    clean = spark.table(SILVER_TABLE_SALES_CLEAN)
    daily = aggregate_daily(clean)
    logger.info(f"Built daily summary: {daily.count()} day(s)")

    (
        daily.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(GOLD_TABLE_SALES_DAILY_SUMMARY)
    )
    logger.info(f"Wrote {GOLD_TABLE_SALES_DAILY_SUMMARY}")

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
