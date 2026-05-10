"""Idempotent output writers."""

from pyspark.sql import DataFrame


def write_parquet_partitioned(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(path)


def write_csv(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").option("header", True).csv(path)


def write_rejected(df: DataFrame, path: str) -> None:
    write_csv(df, path)
