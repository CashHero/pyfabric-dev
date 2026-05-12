"""Gold: roll the cleaned events up into a per-day revenue summary."""
from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.defs import GOLD_TABLE_SALES_DAILY_SUMMARY, SILVER_TABLE_SALES_CLEAN


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
