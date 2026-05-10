"""Data quality cleaning functions."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_date_column(df: DataFrame, column_name: str) -> DataFrame:
    """Normalize supported date formats to Spark DateType (ISO when written)."""
    return df.withColumn(
        column_name,
        F.coalesce(F.to_date(F.col(column_name), "yyyy-MM-dd"), F.to_date(F.col(column_name), "dd/MM/yyyy")),
    )


def clean_orders(df: DataFrame) -> DataFrame:
    """Clean orders while flagging, not dropping, negative amounts."""
    return (
        df.dropDuplicates()
        .transform(lambda frame: normalize_date_column(frame, "order_date"))
        .where(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))
        .withColumn("is_negative_amount", F.col("total_amount") < F.lit(0.0))
    )


def clean_customers(df: DataFrame) -> DataFrame:
    """Clean customers and standardize tier casing."""
    return (
        df.dropDuplicates()
        .transform(lambda frame: normalize_date_column(frame, "signup_date"))
        .where(F.col("customer_id").isNotNull())
        .withColumn("customer_tier", F.lower(F.trim(F.col("customer_tier"))))
    )


def clean_order_items(df: DataFrame) -> DataFrame:
    """Clean order items and keep uncategorized rows in an explicit bucket."""
    return (
        df.dropDuplicates()
        .where(F.col("item_id").isNotNull() & F.col("order_id").isNotNull())
        .withColumn("category", F.coalesce(F.lower(F.trim(F.col("category"))), F.lit("unknown")))
    )


def clean_returns(df: DataFrame) -> DataFrame:
    """Clean returns and normalize return dates."""
    return (
        df.dropDuplicates()
        .transform(lambda frame: normalize_date_column(frame, "return_date"))
        .where(F.col("return_id").isNotNull() & F.col("order_id").isNotNull())
    )
