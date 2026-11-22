"""The local session mirrors Fabric Runtime 2.0's defaults, not upstream Spark 4.1's."""
import pytest

import pyfabric_dev.local_env  # noqa: F401  (bootstrap mocks + local dirs)
from pyfabric_dev.spark import create_spark_session


@pytest.fixture(scope="module")
def spark():
    return create_spark_session()


def test_ansi_mode_off_like_fabric(spark):
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"
    # Under ANSI (the upstream Spark 4 default) this raises instead of returning null.
    assert spark.sql("SELECT CAST('x' AS INT) AS v").first()["v"] is None


def test_session_time_zone_is_utc_like_fabric(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    # 23:30 UTC is already the next day east of UTC; in UTC the date stays put.
    row = spark.sql(
        "SELECT to_date(timestamp_seconds(1767223800)) AS d"  # 2025-12-31 23:30:00 UTC
    ).first()
    assert str(row["d"]) == "2025-12-31"


def test_python_and_spark_agree_on_time_like_fabric(spark):
    import datetime
    import time

    assert time.strftime("%Z") == "UTC"
    row = spark.sql("SELECT TIMESTAMP'2024-01-01 00:00:00' AS ts").first()
    assert row["ts"] == datetime.datetime(2024, 1, 1, 0, 0)


def test_partitioned_overwrite_replaces_whole_table_like_fabric(spark):
    table = "test_session_static_overwrite_tbl"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        spark.createDataFrame([(1, "a"), (2, "b")], "id INT, p STRING") \
            .write.format("delta").partitionBy("p").saveAsTable(table)
        spark.createDataFrame([(3, "a")], "id INT, p STRING") \
            .write.format("delta").mode("overwrite").saveAsTable(table)
        # Dynamic mode would have kept partition "b".
        assert sorted(r["id"] for r in spark.table(table).collect()) == [3]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_new_delta_tables_get_deletion_vectors(spark):
    table = "test_session_deletion_vectors_tbl"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.range(1).write.format("delta").saveAsTable(table)
    try:
        props = spark.sql(f"DESCRIBE DETAIL {table}").first()["properties"]
        assert props.get("delta.enableDeletionVectors") == "true"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
