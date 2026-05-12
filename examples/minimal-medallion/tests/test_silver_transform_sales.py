"""Silver-layer cleaning is a pure transformation, so we can test it
against an in-memory DataFrame without any lakehouse plumbing."""
from src.silver.transform_sales import clean_sales


def test_clean_sales_casts_types(spark):
    raw = spark.createDataFrame(
        [("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99")],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()[0]

    assert out["order_id"] == 1001
    assert out["quantity"] == 2
    assert out["unit_price"] == 19.99


def test_clean_sales_drops_rows_with_null_quantity(spark):
    raw = spark.createDataFrame(
        [
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
            ("1002", "2026-05-10T10:00:00Z", "WIDGET-B", None, "7.50"),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()

    assert len(out) == 1
    assert out[0]["order_id"] == 1001


def test_clean_sales_deduplicates_by_order_id(spark):
    raw = spark.createDataFrame(
        [
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
            ("1001", "2026-05-10T09:14:22Z", "WIDGET-A", "2", "19.99"),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    out = clean_sales(raw).collect()

    assert len(out) == 1
