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


def test_new_delta_tables_get_deletion_vectors(spark):
    table = "test_session_deletion_vectors_tbl"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.range(1).write.format("delta").saveAsTable(table)
    try:
        props = spark.sql(f"DESCRIBE DETAIL {table}").first()["properties"]
        assert props.get("delta.enableDeletionVectors") == "true"
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
