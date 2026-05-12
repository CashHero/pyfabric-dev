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
    _logger = cf_create_logger("10_bronze_ingest_sales")

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

"""Bronze: read sales CSV drops and land them as a Delta table.

In Fabric this notebook reads from the Files/ tree of bronze_lakehouse.
Locally the same path resolves under DEV_BASE_DIR's lakehouse/default/Files.
"""




def run(spark: SparkSession, logger: Logger) -> None:
    csv_path = cf_get_lakehouse_path("Files/data/sales.csv")  # noqa: F821
    logger.info(f"Reading sales CSV from {csv_path}")

    df = (
        spark.read.option("header", "true").csv(csv_path)
    )
    logger.info(f"Read {df.count()} raw rows")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE_SALES_RAW)
    )
    logger.info(f"Wrote {BRONZE_TABLE_SALES_RAW}")

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
