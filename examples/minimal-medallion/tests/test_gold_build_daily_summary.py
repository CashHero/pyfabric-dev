"""Gold-layer aggregation is also pure — test it with synthetic data."""
import datetime as dt

from src.gold.build_daily_summary import aggregate_daily


def test_aggregate_daily_sums_revenue_per_day(spark):
    df = spark.createDataFrame(
        [
            (1001, dt.datetime(2026, 5, 10, 9, 0), "WIDGET-A", 2, 19.99),
            (1002, dt.datetime(2026, 5, 10, 12, 0), "WIDGET-B", 1, 49.00),
            (1003, dt.datetime(2026, 5, 11, 9, 0), "WIDGET-A", 3, 19.99),
        ],
        ["order_id", "occurred_at", "sku", "quantity", "unit_price"],
    )
    rows = {r["sales_date"]: r for r in aggregate_daily(df).collect()}

    assert rows[dt.date(2026, 5, 10)]["revenue"] == 2 * 19.99 + 1 * 49.00
    assert rows[dt.date(2026, 5, 10)]["units_sold"] == 3
    assert rows[dt.date(2026, 5, 10)]["orders"] == 2

    assert rows[dt.date(2026, 5, 11)]["revenue"] == 3 * 19.99
    assert rows[dt.date(2026, 5, 11)]["units_sold"] == 3
    assert rows[dt.date(2026, 5, 11)]["orders"] == 1
