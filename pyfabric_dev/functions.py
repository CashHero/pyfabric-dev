"""
Common DataFrame utility functions shared across all pipeline layers.

This module contains all the DataFrame manipulation utilities used throughout
the data pipeline. These functions are environment-agnostic and work with
any SparkSession.
"""

import json
import logging
import os
import random
import re
import shutil
import uuid
import zipfile
import builtins
from datetime import datetime, timedelta, timezone
from functools import reduce
from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from delta.tables import DeltaTable

from pyfabric_dev.defs import (
    BRONZE_LAKEHOUSE_NAME,
    SILVER_LAKEHOUSE_NAME,
    DEFAULT_BACKUP_TABLE_REFRESH_PERIOD_DAYS,
)


# Logging utilities

def cf_create_logger(name: str) -> Logger:
    """
    Create a logger with standard formatting.

    Args:
        name: Name for the logger (typically the notebook/module name)

    Returns:
        Configured Logger instance
    """
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - L%(lineno)d - %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    return logging.getLogger(name)


# Table name helpers

def get_lakehouse_table(lakehouse: str, table: str) -> str:
    """Get fully qualified table name (lakehouse.table)."""
    return f"{lakehouse}.{table}"


def get_default_table(table: str) -> str:
    """Get qualified table name for default lakehouse (default.table_name)."""
    return f"default.{table}"


def get_bronze_table(table: str) -> str:
    """Get fully qualified bronze table name."""
    return get_lakehouse_table(BRONZE_LAKEHOUSE_NAME, table)


def get_silver_table(table: str) -> str:
    """Get fully qualified silver table name."""
    return get_lakehouse_table(SILVER_LAKEHOUSE_NAME, table)


# String/Column utilities

def cf_camel_to_snake(text: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def cf_columns_camel_to_snake(df: DataFrame) -> DataFrame:
    """Rename all columns from camelCase to snake_case."""
    return df.select([
        col(col_name).alias(cf_camel_to_snake(col_name))
        for col_name in df.columns
    ])


def cf_clean_column_names(df: DataFrame) -> DataFrame:
    """Remove dots and spaces from column names."""
    for col_name in df.columns:
        new_col_name = col_name.replace(".", "").replace(" ", "")
        if new_col_name != col_name:
            df = df.withColumnRenamed(col_name, new_col_name)
    return df


# Surrogate key generation

def random_64bit() -> int:
    """Generate a random 64-bit integer (JavaScript-safe, max 2^53-1)."""
    return random.randint(0, 2**53 - 1)


def cf_add_surrogate_key_column(df: DataFrame, sk_col_name: str) -> DataFrame:
    """Add a surrogate key column as the first column."""
    random_64bit_udf = udf(random_64bit, LongType())
    df = df.withColumn(sk_col_name, random_64bit_udf())
    # Make sure the sk is first
    cols = [sk_col_name] + [c for c in df.columns if c != sk_col_name]
    return df.select(cols)


# Type conversion utilities

def cf_str_to_bool(df: DataFrame, col_name: str) -> DataFrame:
    """Convert 'X' to True, everything else to False."""
    return df.withColumn(
        col_name,
        F.when(F.col(col_name) == "X", F.lit(True)).otherwise(F.lit(False))
    )


# Validation utilities

def cf_raise_exception_on_dupes(df: DataFrame, col_name: str, logger: Logger):
    """Raise an exception if duplicate values exist in the specified column."""
    dupes = (
        df
        .groupBy(col_name)
        .count()
        .filter(col("count") > 1)
    )

    if dupes.count() > 0:
        logger.error(f"{dupes.count()} duplicates found in column '{col_name}': {dupes.collect()}")
        raise Exception(f"{dupes.count()} duplicates found in column '{col_name}'! Please resolve before proceeding.")


# Logging utilities

def cf_log_dataframe(logger: Logger, df: DataFrame, level: str = "info", max_rows: int = 20):
    """Log DataFrame rows to the logger."""
    rows = df.limit(max_rows).toPandas()
    log_msg = f"\n{rows.to_string(index=False)}"
    if level == "info":
        logger.info(log_msg)
    elif level == "debug":
        logger.debug(log_msg)
    elif level == "warning":
        logger.warning(log_msg)
    else:
        logger.info(log_msg)


def cf_format_rows_as_table(rows: list[dict[str, object]]) -> str:
    """
    Format a list of row dicts as a readable ASCII table for logging.

    Args:
        rows: List of dicts with consistent keys (e.g. from Row.asDict()).

    Returns:
        A string with header, separator line, and data rows; empty string if rows is empty.
    """
    if not rows:
        return ""
    # Union keys across all rows to handle dicts with varying keys
    seen = set()
    cols = []
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                cols.append(k)
    widths = [
        max(len(str(col)), max((len(str(r.get(col, ""))) for r in rows), default=0))
        for col in cols
    ]
    header = " | ".join(str(col).ljust(w) for col, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    data_rows = [
        " | ".join(str(r.get(c, "")).ljust(w) for c, w in zip(cols, widths))
        for r in rows
    ]
    return "\n".join([header, sep] + data_rows)


# Schema utilities

def cf_add_missing_columns(target_table_name: str, source_df: DataFrame, spark: SparkSession = None) -> DataFrame:
    """Add columns from target table that are missing in source DataFrame."""
    if spark is None:
        spark = source_df.sparkSession
    target_df = spark.table(target_table_name)
    target_columns = target_df.columns
    target_schema = {field.name: field.dataType for field in target_df.schema}

    for c in target_columns:
        if c not in source_df.columns:
            dtype = target_schema.get(c)
            source_df = source_df.withColumn(c, lit(None).cast(dtype))

    return source_df


# Table operations

def cf_overwrite_table(df: DataFrame, table_name: str, logger: Logger):
    """Overwrite a Delta table with new data.

    Uses overwrite-in-place so the table location stays a valid Delta table.
    Safe for continuous pipelines: no DROP TABLE, so the path is never left
    non-empty and non-Delta. When the table already exists, overwrites by
    writing to its location (avoids Spark's truncate-in-batch-mode issue).
    """
    logger.info(f"Overwriting table: {table_name} with {df.count()} items")
    spark = df.sparkSession
    if spark.catalog.tableExists(table_name):
        try:
            detail = spark.sql(
                f"DESCRIBE DETAIL `{table_name.replace('`', '``')}`"
            ).first()
            format_name = (detail["format"] or "").lower()
            location = detail["location"]
        except Exception as e:
            logger.warning(f"DESCRIBE DETAIL failed for {table_name}, will recreate table: {e}")
            format_name = None
            location = None

        escaped = table_name.replace('`', '``')

        # Only use overwrite-by-path when we are sure the existing table is a healthy Delta table
        if format_name == "delta" and location:
            try:
                df.write.option("overwriteSchema", True).format("delta").mode("overwrite").save(location)
            except Exception as e:
                logger.warning(f"Path-based overwrite failed for {table_name}, dropping and recreating: {e}")
                logger.warning(f"DROP TABLE will lose table properties/comments/ACLs for {table_name}")
                spark.sql(f"DROP TABLE IF EXISTS `{escaped}`")
                # Preserve original location to avoid converting external tables to managed
                df.write.option("overwriteSchema", True).format("delta").mode("overwrite").save(location)
                escaped_location = location.replace("'", "''")
                spark.sql(f"CREATE TABLE `{escaped}` USING DELTA LOCATION '{escaped_location}'")
        else:
            # Existing table is not Delta (or DESCRIBE DETAIL failed) — try saveAsTable first
            # to avoid orphaning external table storage by dropping prematurely.
            try:
                df.write.option("overwriteSchema", True).format("delta").mode("overwrite").saveAsTable(table_name)
            except Exception as e:
                logger.warning(f"saveAsTable overwrite failed for {table_name}, dropping and recreating: {e}")
                logger.warning(f"DROP TABLE will lose table properties/comments/ACLs for {table_name}")
                spark.sql(f"DROP TABLE IF EXISTS `{escaped}`")
                if location:
                    escaped_location = location.replace("'", "''")
                    df.write.option("overwriteSchema", True).format("delta").mode("overwrite").save(location)
                    spark.sql(f"CREATE TABLE `{escaped}` USING DELTA LOCATION '{escaped_location}'")
                else:
                    # Avoid Spark V2 overwrite/truncate path after DROP TABLE by recreating via CTAS.
                    temp_view_name = f"tmp_cf_overwrite_{uuid.uuid4().hex}"
                    try:
                        df.createOrReplaceTempView(temp_view_name)
                        spark.sql(
                            f"CREATE TABLE `{escaped}` USING DELTA "
                            f"AS SELECT * FROM `{temp_view_name}`"
                        )
                    finally:
                        spark.catalog.dropTempView(temp_view_name)
    else:
        df.write.option("overwriteSchema", True).format("delta").saveAsTable(table_name)
    logger.info("Done")


def cf_combine_and_write_dfs(dfs: list[DataFrame], table_name: str, logger: Logger):
    """Combine multiple DataFrames and write to a table."""
    if dfs:
        combined_dfs = reduce(DataFrame.unionAll, dfs)
        cf_overwrite_table(combined_dfs, table_name, logger)


def cf_transform_bronze_table_columns(
    bronze_table_name: str,
    column_mapping: dict[str, str],
    logger: Logger,
    spark: SparkSession = None
) -> DataFrame:
    """Read a bronze table and rename columns according to mapping."""
    if spark is None:
        # Backward compatibility: try to get from globals (notebook context)
        spark = getattr(builtins, '_spark', None) or globals().get('_spark')
        if spark is None:
            raise ValueError("spark parameter is required when not running in notebook context")
    bronze_table = get_bronze_table(bronze_table_name)
    logger.info(f"Reading from bronze table '{bronze_table}'")
    aliased_cols = [F.col(old).alias(new) for old, new in column_mapping.items()]
    return spark.table(bronze_table).select(*aliased_cols)


# Merge operations

def _get_merge_metrics_from_history(delta_table, logger: Logger) -> int:
    """Extract merge statistics from Delta table history (zero extra scans)."""
    try:
        history_row = delta_table.history(1).select("operationMetrics").collect()
        if not history_row:
            logger.info("Merge complete (no history available for metrics)")
            return 0

        metrics = history_row[0]["operationMetrics"] or {}
        num_inserted = int(metrics.get("numTargetRowsInserted", "0"))
        num_updated = int(metrics.get("numTargetRowsUpdated",
                          metrics.get("numTargetRowsMatchedUpdated", "0")))
        num_deleted = int(metrics.get("numTargetRowsDeleted",
                          metrics.get("numTargetRowsMatchedDeleted", "0")))
        total_changed = num_inserted + num_updated + num_deleted

        logger.info(
            f"Merge complete. Net inserted: {num_inserted}, updated: {num_updated}, "
            f"deleted: {num_deleted}, total changed: {total_changed}"
        )
        return total_changed
    except Exception as e:
        logger.warning(f"Could not read merge metrics from history: {e}")
        return 0


def cf_merge_into_table(
    source_df: DataFrame,
    key_columns: list[str],
    target_table_name: str,
    spark: SparkSession,
    logger: Logger,
    surrogate_key_col_name: str = None,
    delete_when_not_matched_by_source: bool = False,
    not_matched_by_source_condition: str = None
) -> int:
    """
    Merge source_df into a Delta table, preserving surrogate keys.

    Args:
        source_df: DataFrame to merge
        key_columns: Columns to use for matching
        target_table_name: Name of the target Delta table
        spark: SparkSession
        logger: Logger instance
        surrogate_key_col_name: Name of surrogate key column to preserve
        delete_when_not_matched_by_source: Whether to delete target rows not
            matched by the source. Defaults to False. (Historically this
            defaulted to True but the delete clause was silently dropped — see
            note below — so callers effectively never deleted; False makes the
            default match that real behavior.)
        not_matched_by_source_condition: Optional SQL predicate (referencing the
            ``target`` alias, e.g. ``"target.server IN ('cashhero')"``) that
            restricts which unmatched target rows are deleted. Only applies when
            ``delete_when_not_matched_by_source`` is True. Use it when the source
            is a complete snapshot of only a subset of the target (e.g. one data
            source within a shared table) so deletes don't reach rows the source
            never intended to cover.

    Returns:
        Total number of rows inserted or updated
    """
    total_updated = 0

    if not spark.catalog.tableExists(target_table_name):
        total_updated = source_df.count()
        logger.info(f"Table {target_table_name} doesn't exist. Creating table with {total_updated} items")

        try:
            source_df.write.format("delta").saveAsTable(target_table_name)
        except Exception as e:
            error_msg = str(e)
            if "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in error_msg or "not empty and also not a Delta table" in error_msg:
                try:
                    warehouse_dir = spark.conf.get("spark.sql.warehouse.dir", "unknown")
                    table_location = f"{warehouse_dir}/{target_table_name.split('.')[-1]}"

                    if DeltaTable.isDeltaTable(spark, table_location):
                        logger.warning(f"Table location '{table_location}' exists and is a valid Delta table, but not registered in catalog.")
                        logger.warning(f"Registering existing Delta table in catalog instead of recreating.")
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {target_table_name}
                            USING DELTA
                            LOCATION '{table_location}'
                        """)
                        logger.info("Successfully registered existing Delta table in catalog.")
                    else:
                        logger.error(f"ERROR: Table location for '{target_table_name}' exists but is not a Delta table!")
                        raise
                except Exception as diag_error:
                    logger.error(f"Could not diagnose or fix the issue: {diag_error}")
                    raise
            else:
                raise

        logger.info("Done")
    else:
        logger.info(f"Merging into {target_table_name}")

        # Schema evolution for TransactionTypeDesc and Details columns
        target_df = spark.table(target_table_name)
        target_schema = {field.name: field.dataType for field in target_df.schema}
        schema_needs_evolution = False

        for col_name in ["TransactionTypeDesc", "Details"]:
            if col_name in target_schema:
                target_type = str(target_schema[col_name])
                if "StringType" not in target_type:
                    logger.warning(f"Column {col_name} in target table is {target_type}, but should be StringType. Evolving schema...")
                    schema_needs_evolution = True

        if schema_needs_evolution:
            _evolve_schema(target_table_name, target_df, target_schema, spark, logger)
            target_df = spark.table(target_table_name)
            target_schema = {field.name: field.dataType for field in target_df.schema}

        # Align source columns with target schema
        target_schema = _ensure_target_has_source_columns(
            target_table_name, source_df, target_schema, spark, logger
        )
        source_df = _align_source_schema(source_df, target_schema, logger)

        # Prepare update and insert column sets
        all_columns = source_df.columns
        if surrogate_key_col_name:
            if surrogate_key_col_name not in all_columns:
                raise Exception(f"Surrogate key '{surrogate_key_col_name}' provided, but does not exist in source_df")
            update_columns = [c for c in all_columns if c != surrogate_key_col_name]
        else:
            update_columns = all_columns

        merge_condition = " AND ".join([
            f"(target.{c} <=> source.{c})"
            for c in key_columns
        ])
        update_condition = " OR ".join([
            f"(target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL) OR (target.{c} IS NOT NULL AND source.{c} IS NULL))"
            for c in update_columns
        ])
        update_set = {c: f"source.{c}" for c in update_columns}

        delta_table = DeltaTable.forName(spark, target_table_name)

        insert_values = {c: f"source.{c}" for c in all_columns}

        merge_builder = delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        ).whenNotMatchedInsert(
            values=insert_values
        )

        if delete_when_not_matched_by_source:
            # NOTE: the whenNotMatched* builder methods return a NEW builder
            # rather than mutating in place, so the result must be reassigned or
            # the delete clause is silently dropped.
            if not_matched_by_source_condition:
                merge_builder = merge_builder.whenNotMatchedBySourceDelete(
                    condition=not_matched_by_source_condition
                )
            else:
                merge_builder = merge_builder.whenNotMatchedBySourceDelete()

        merge_builder.execute()

        total_updated = _get_merge_metrics_from_history(delta_table, logger)

    return total_updated


def _ensure_target_has_source_columns(
    target_table_name: str,
    source_df: DataFrame,
    target_schema: dict[str, object],
    spark: SparkSession,
    logger: Logger
) -> dict[str, object]:
    """Add missing source columns to target table so merge update clauses resolve."""
    missing_fields = [
        field for field in source_df.schema.fields
        if field.name not in target_schema
    ]
    if not missing_fields:
        return target_schema

    def _escape_identifier(identifier: str) -> str:
        return identifier.replace("`", "``")

    safe_target_table_name = _quote_table_identifier(target_table_name)
    # Add one column per statement to avoid commas in complex types (e.g. struct<a:int,b:string>)
    # breaking a multi-column ADD COLUMNS clause.
    for field in missing_fields:
        col_sql = f"`{_escape_identifier(field.name)}` {field.dataType.simpleString()}"
        try:
            spark.sql(f"ALTER TABLE {safe_target_table_name} ADD COLUMNS ({col_sql})")
            logger.info(
                "Added missing column to target table %s: %s",
                target_table_name,
                field.name,
            )
        except Exception as err:
            if _is_duplicate_column_error(err):
                logger.info(
                    "Skipping ADD COLUMN %s for %s due to concurrent column creation: %s",
                    field.name,
                    target_table_name,
                    err,
                )
            else:
                raise

    refreshed_df = spark.table(target_table_name)
    return {field.name: field.dataType for field in refreshed_df.schema}


def _is_duplicate_column_error(err: Exception) -> bool:
    """Return True when ALTER TABLE ADD COLUMNS failed because columns already exist."""
    message = str(err).upper()
    duplicate_markers = (
        "COLUMN_ALREADY_EXISTS",
        "FIELDS_ALREADY_EXISTS",
        "DUPLICATE COLUMN",
    )
    return any(marker in message for marker in duplicate_markers)


def _quote_table_identifier(table_identifier: str) -> str:
    """Quote table identifier parts safely for SQL statements."""
    if not isinstance(table_identifier, str):
        raise ValueError("Table identifier must be a string")

    raw_parts = table_identifier.split(".")
    if not raw_parts:
        raise ValueError("Table identifier cannot be empty")

    quoted_parts = []
    for raw_part in raw_parts:
        part = raw_part.strip()
        if not part:
            raise ValueError(f"Invalid table identifier '{table_identifier}'")

        # Normalize optional surrounding backticks and always re-quote consistently.
        if part.startswith("`") and part.endswith("`") and len(part) >= 2:
            part = part[1:-1]
            part = part.replace("``", "`")

        part = part.replace("`", "``")
        quoted_parts.append(f"`{part}`")

    return ".".join(quoted_parts)


def _evolve_schema(target_table_name: str, target_df: DataFrame, target_schema: dict, spark: SparkSession, logger: Logger):
    """Evolve table schema to ensure TransactionTypeDesc and Details are strings."""
    logger.info("Evolving table schema to ensure TransactionTypeDesc and Details are strings")

    target_evolved = target_df
    for col_name in ["TransactionTypeDesc", "Details"]:
        if col_name in target_schema:
            target_type = str(target_schema[col_name])
            if target_type != "StringType":
                target_evolved = target_evolved.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNotNull(), F.col(col_name).cast("string"))
                    .otherwise(F.lit(None).cast("string"))
                )

    # Get table location and recreate
    table_path = None
    try:
        table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {target_table_name}").collect()
        for row in table_info:
            if row[0] == "Location":
                table_path = row[1]
                break
    except Exception as e:
        logger.warning(f"Could not get table location from DESCRIBE: {e}")

    spark.sql(f"DROP TABLE IF EXISTS {target_table_name}")

    if table_path:
        local_path = table_path.replace("file://", "").replace("file:", "")
        if os.path.exists(local_path):
            logger.info(f"Deleting directory: {local_path}")
            shutil.rmtree(local_path)

        target_evolved.write.format("delta").save(table_path)
        spark.sql(f"CREATE TABLE {target_table_name} USING DELTA LOCATION '{table_path}'")
        logger.info(f"Schema evolution complete - wrote to {table_path} and re-registered table")
    else:
        target_evolved.write.format("delta").saveAsTable(target_table_name)
        logger.info("Schema evolution complete - table recreated")


def _align_source_schema(source_df: DataFrame, target_schema: dict, logger: Logger) -> DataFrame:
    """Align source DataFrame columns with target schema types."""
    type_mapping = {
        "StringType": "string",
        "IntegerType": "int",
        "LongType": "bigint",
        "DoubleType": "double",
        "FloatType": "float",
        "BooleanType": "boolean",
        "DateType": "date",
        "TimestampType": "timestamp",
    }

    def dtype_to_cast_string(dtype):
        dtype_str = str(dtype)
        dtype_clean = dtype_str.split("(")[0]
        return type_mapping.get(dtype_clean, dtype_str.lower().replace("type", ""))

    source_aligned = source_df
    for col_name in source_df.columns:
        if col_name in target_schema:
            target_type_obj = target_schema[col_name]
            target_type_str = dtype_to_cast_string(target_type_obj)
            source_type = dict(source_df.dtypes)[col_name]

            if col_name in ["TransactionTypeDesc", "Details"]:
                if source_type != "string":
                    source_aligned = source_aligned.withColumn(col_name, F.col(col_name).cast("string"))
            elif source_type != target_type_str:
                try:
                    source_aligned = source_aligned.withColumn(col_name, F.col(col_name).cast(target_type_str))
                except Exception as cast_error:
                    logger.debug(f"Could not cast {col_name} from {source_type} to {target_type_str}: {cast_error}")

    return source_aligned


# Delta table utilities

def should_write_to_delta(
    spark: SparkSession,
    table_identifier: str,
    logger: Logger,
    by_path: bool = False,
    days_since_last_update: int = DEFAULT_BACKUP_TABLE_REFRESH_PERIOD_DAYS
) -> bool:
    """Check if a Delta table should be written to (based on last write time)."""
    current_time = datetime.now(timezone.utc)
    try:
        if by_path:
            if not DeltaTable.isDeltaTable(spark, table_identifier):
                logger.info(f"Table {table_identifier} doesn't exist")
                return True
            delta_table = DeltaTable.forPath(spark, table_identifier)
        else:
            if not spark.catalog.tableExists(table_identifier):
                logger.info(f"Table {table_identifier} doesn't exist")
                return True
            delta_table = DeltaTable.forName(spark, table_identifier)

        last_op_df = delta_table.history(1)
        last_op_row = last_op_df.select("timestamp").collect()
        if last_op_row:
            last_write = last_op_row[0]["timestamp"]
            logger.info(f"{table_identifier}: last_write = {last_write}, current_time = {current_time}")
            if isinstance(last_write, str):
                last_write = datetime.fromisoformat(last_write)
            if last_write.tzinfo is None:
                last_write = last_write.replace(tzinfo=timezone.utc)
            return last_write < (current_time - timedelta(days=days_since_last_update))
        else:
            return True
    except Exception:
        logger.warning(f"Exception while processing {table_identifier}")
        return True


def cf_backup_and_truncate_table(table_name: str, spark: SparkSession, logger: Logger):
    """Backup a table and truncate it."""
    if spark.catalog.tableExists(table_name):
        backup_table_name = f"{table_name}_backup"

        if should_write_to_delta(spark, backup_table_name, logger):
            cf_overwrite_table(spark.table(table_name), backup_table_name, logger)
        else:
            logger.debug(f"Not writing backup to {backup_table_name} at this time")

        logger.info(f"Truncating table {table_name}")
        spark.sql(f"TRUNCATE TABLE {table_name}")
    else:
        logger.warning(f"Table {table_name} doesn't exist. Skipping backup and truncation")


def cf_table_saved(table_name: str, spark: SparkSession, logger: Logger) -> bool:
    """Check if a table was successfully saved."""
    try:
        if not spark.catalog.tableExists(table_name):
            logger.error(f"Table {table_name} does not exist in catalog")
            return False

        delta_table = DeltaTable.forName(spark, table_name)
        # history() returns most-recent-first; use limit(1) to get the latest entry
        history_rows = delta_table.history().limit(1).collect()

        if history_rows:
            # Convert Row to dict so we can use .get() safely
            history = history_rows[0].asDict()
            operation = history["operation"]
            operation_metrics = history.get("operationMetrics") or {}

            files_added_str = (
                operation_metrics.get("numFilesAdded") or
                operation_metrics.get("numFiles") or
                operation_metrics.get("numOutputFiles")
            )
            num_files_added = int(files_added_str) if files_added_str else 0
            num_rows_added = int(operation_metrics.get("numOutputRows", "0") or 0)

            if operation in ["WRITE", "APPEND", "CREATE TABLE AS SELECT"] and num_files_added > 0:
                logger.info(f"SUCCESS: {operation} - Added {num_files_added} files, {num_rows_added} rows")
                return True
            else:
                logger.error(f"Write failed: operation={operation}, files_added={num_files_added}")
                return False
        else:
            logger.error("No history found")
            return False

    except Exception as e:
        logger.error(f"FAILED: {str(e)}")
        return False


# File utilities

def cf_fix_xlsx_empty_styles(path: str):
    """Fix empty fill tags in xlsx files that cause read errors."""
    base_dir = os.path.dirname(path)
    temp_filename = f"temp_fix_styles_{uuid.uuid4().hex}.xlsx"
    temp_path = os.path.join(base_dir, temp_filename)

    with zipfile.ZipFile(path, "r") as zin:
        with zipfile.ZipFile(temp_path, "w") as zout:
            for item in zin.infolist():
                buffer = zin.read(item.filename)
                if item.filename == "xl/styles.xml":
                    styles = buffer.decode("utf-8")
                    styles = styles.replace("<x:fill />", "")
                    buffer = styles.encode("utf-8")
                zout.writestr(item, buffer)

    os.replace(temp_path, path)
