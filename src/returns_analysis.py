"""Return-rate and refund anomaly analysis."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_return_rate_by_category(enriched_df: DataFrame, returns_df: DataFrame) -> DataFrame:
    returned_orders = returns_df.select("order_id").dropDuplicates().withColumn("was_returned", F.lit(1))
    return (
        enriched_df.select("category", "order_id").dropDuplicates()
        .join(returned_orders, "order_id", "left")
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(F.when(F.col("was_returned") == 1, F.col("order_id"))).alias("returned_orders"),
        )
        .withColumn("return_rate", F.round(F.col("returned_orders") / F.col("total_orders"), 6))
        .select("category", "total_orders", "returned_orders", "return_rate")
    )


def calculate_return_rate_by_customer_tier(order_level_df: DataFrame, returns_df: DataFrame) -> DataFrame:
    returned_orders = returns_df.select("order_id").dropDuplicates().withColumn("was_returned", F.lit(1))
    return (
        order_level_df.join(returned_orders, "order_id", "left")
        .groupBy("customer_tier")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct(F.when(F.col("was_returned") == 1, F.col("order_id"))).alias("returned_orders"),
        )
        .withColumn("return_rate", F.round(F.col("returned_orders") / F.col("total_orders"), 6))
        .select("customer_tier", "total_orders", "returned_orders", "return_rate")
    )


def calculate_top_refund_customers(order_level_df: DataFrame, returns_df: DataFrame) -> DataFrame:
    return (
        returns_df.join(order_level_df.select("order_id", "customer_id", "country", "customer_tier"), "order_id", "inner")
        .groupBy("customer_id", "country", "customer_tier")
        .agg(
            F.round(F.sum("refund_amount"), 2).alias("total_refund_amount"),
            F.count("return_id").alias("refund_count"),
        )
        .orderBy(F.col("total_refund_amount").desc(), F.col("customer_id"))
        .limit(10)
        .select("customer_id", "country", "customer_tier", "total_refund_amount", "refund_count")
    )


def identify_refund_anomalies(order_level_df: DataFrame, returns_df: DataFrame) -> DataFrame:
    return (
        returns_df.join(order_level_df.select("order_id", "customer_id", "net_amount"), "order_id", "inner")
        .withColumn("refund_exceeds_order", F.col("refund_amount") > F.col("net_amount"))
        .where(F.col("refund_exceeds_order"))
        .select("return_id", "order_id", "customer_id", "refund_amount", "net_amount", "refund_exceeds_order", "reason", "return_date")
    )
