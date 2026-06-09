"""Silver: cast types, drop nulls, deduplicate the raw sales rows."""
from logging import Logger

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from src.common.defs import BRONZE_TABLE_SALES_RAW, SILVER_TABLE_SALES_CLEAN


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
