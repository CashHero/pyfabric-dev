"""Spark integration tests for cf_merge_into_table scoped delete."""
import logging

import pytest

import pyfabric_dev.local_env  # noqa: F401  (bootstrap mocks + local dirs)
from pyfabric_dev.spark import create_spark_session
from pyfabric_dev.functions import cf_merge_into_table


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


@pytest.fixture(scope="module")
def logger():
    log = logging.getLogger("pyfabric-merge-test")
    log.setLevel(logging.INFO)
    return log


def _df(spark, data):
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    schema = StructType([
        StructField("server", StringType()),
        StructField("id", StringType()),
        StructField("val", LongType()),
    ])
    return spark.createDataFrame(data, schema)


def _seed(spark, table):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    _df(spark, [("A", "a1", 1), ("A", "a2", 2), ("B", "b1", 9)]) \
        .write.format("delta").saveAsTable(table)


def test_scoped_not_matched_by_source_delete_preserves_other_sources(spark, logger):
    table = "test_scoped_delete_tbl"
    _seed(spark, table)
    # Snapshot for server A only: a1 updated, a3 new, a2 absent (must be deleted).
    source = _df(spark, [("A", "a1", 100), ("A", "a3", 3)])

    cf_merge_into_table(
        source, ["server", "id"], table, spark, logger,
        delete_when_not_matched_by_source=True,
        not_matched_by_source_condition="target.server = 'A'",
    )

    result = {(r["server"], r["id"]): r["val"] for r in spark.table(table).collect()}
    # a1 updated, a3 inserted, a2 deleted (within scope), b1 preserved (out of scope).
    assert result == {("A", "a1"): 100, ("A", "a3"): 3, ("B", "b1"): 9}
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_unscoped_delete_removes_other_sources(spark, logger):
    """Without the condition, whenNotMatchedBySource deletes everything not in
    the source — including the other source's rows (the behavior the scoped
    condition exists to prevent)."""
    table = "test_unscoped_delete_tbl"
    _seed(spark, table)
    source = _df(spark, [("A", "a1", 100), ("A", "a3", 3)])

    cf_merge_into_table(
        source, ["server", "id"], table, spark, logger,
        delete_when_not_matched_by_source=True,
    )

    result = {(r["server"], r["id"]) for r in spark.table(table).collect()}
    assert result == {("A", "a1"), ("A", "a3")}  # b1 deleted, a2 deleted
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_default_does_not_delete(spark, logger):
    """The default (delete_when_not_matched_by_source omitted) must NOT delete —
    it upserts only. Guards the safe default that matches historical behavior."""
    table = "test_default_no_delete_tbl"
    _seed(spark, table)
    source = _df(spark, [("A", "a1", 100), ("A", "a3", 3)])

    cf_merge_into_table(source, ["server", "id"], table, spark, logger)

    result = {(r["server"], r["id"]): r["val"] for r in spark.table(table).collect()}
    # a1 updated, a3 inserted; a2 and b1 retained (no deletes).
    assert result == {("A", "a1"): 100, ("A", "a2"): 2, ("A", "a3"): 3, ("B", "b1"): 9}
    spark.sql(f"DROP TABLE IF EXISTS {table}")
