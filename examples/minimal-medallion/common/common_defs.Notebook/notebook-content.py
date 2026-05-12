# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""Table-name constants for the minimal-medallion example."""

# Bronze: raw events as ingested from the CSV drop.
BRONZE_TABLE_SALES_RAW = "sales_raw"

# Silver: cleaned, typed, deduped events.
SILVER_TABLE_SALES_CLEAN = "sales_clean"

# Gold: business-ready aggregate.
GOLD_TABLE_SALES_DAILY_SUMMARY = "sales_daily_summary"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
