"""Bronze: read sales CSV drops and land them as a Delta table.

In Fabric this notebook reads from the Files/ tree of the lakehouse.
Locally the same path resolves under DEV_BASE_DIR's lakehouse/default/Files.
"""
from logging import Logger

from pyspark.sql import SparkSession

from src.common.defs import BRONZE_TABLE_SALES_RAW


def run(spark: SparkSession, logger: Logger) -> None:
    csv_path = cf_get_lakehouse_path("Files/data/sales.csv")  # noqa: F821
    logger.info(f"Reading sales CSV from {csv_path}")

    df = (
        spark.read.option("header", "true").csv(csv_path)
    )
    logger.info(f"Read {df.count()} raw rows")

    cf_overwrite_table(spark, df, BRONZE_TABLE_SALES_RAW)  # noqa: F821
    logger.info(f"Wrote {BRONZE_TABLE_SALES_RAW}")
