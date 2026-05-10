"""Unit tests for core PySpark transformations."""

import pytest
from pyspark.sql import SparkSession

from src.cleaning import clean_customers, clean_orders, normalize_date_column
from src.transformations import enrich_orders_with_customers, find_orphaned_order_items


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("psi-etl-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_customer_tier_is_lowercase(spark):
    df = spark.createDataFrame([("C1", "2024-01-01", "Ghana", " GOLD ", "a@example.com")], ["customer_id", "signup_date", "country", "customer_tier", "email"])

    result = clean_customers(df).first()

    assert result.customer_tier == "gold"


def test_negative_amount_flag_is_added_correctly(spark):
    df = spark.createDataFrame(
        [("O1", "C1", "2024-01-01", "shipped", -10.0, None), ("O2", "C1", "2024-01-02", "shipped", 25.0, 5.0)],
        ["order_id", "customer_id", "order_date", "status", "total_amount", "discount_pct"],
    )

    result = {row.order_id: row.is_negative_amount for row in clean_orders(df).select("order_id", "is_negative_amount").toLocalIterator()}

    assert result == {"O1": True, "O2": False}


def test_net_amount_calculation(spark):
    orders = spark.createDataFrame(
        [("O1", "C1", "2024-01-01", "shipped", 100.0, 15.0, False)],
        ["order_id", "customer_id", "order_date", "status", "total_amount", "discount_pct", "is_negative_amount"],
    ).transform(lambda frame: normalize_date_column(frame, "order_date"))
    customers = spark.createDataFrame(
        [("C1", "2023-01-01", "Ghana", "gold", "a@example.com")],
        ["customer_id", "signup_date", "country", "customer_tier", "email"],
    ).transform(lambda frame: normalize_date_column(frame, "signup_date"))

    result = enrich_orders_with_customers(orders, customers).first()

    assert result.net_amount == 85.0
    assert result.order_year == 2024
    assert result.order_month == 1


def test_date_normalization_supports_two_formats(spark):
    df = spark.createDataFrame([("O1", "2024-01-05"), ("O2", "06/01/2024")], ["order_id", "order_date"])

    result = {row.order_id: str(row.order_date) for row in normalize_date_column(df, "order_date").toLocalIterator()}

    assert result == {"O1": "2024-01-05", "O2": "2024-01-06"}


def test_orphaned_order_items_detection(spark):
    orders = spark.createDataFrame([("O1",)], ["order_id"])
    items = spark.createDataFrame([("I1", "O1"), ("I2", "MISSING")], ["item_id", "order_id"])

    result = find_orphaned_order_items(items, orders).first()

    assert result.item_id == "I2"
    assert result.order_id == "MISSING"
