"""Analytical summary calculations that use Spark window functions."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def calculate_customer_lifetime_rankings(enriched_df: DataFrame) -> DataFrame:
    order_level = enriched_df.dropDuplicates(["order_id"])
    lifetime = (
        order_level.groupBy("country", "customer_id", "customer_tier")
        .agg(F.round(F.sum("net_amount"), 2).alias("lifetime_net_spend"))
    )
    ranking_window = Window.partitionBy("country").orderBy(F.col("lifetime_net_spend").desc(), F.col("customer_id"))
    return lifetime.withColumn("country_rank", F.dense_rank().over(ranking_window)).select(
        "country", "customer_id", "customer_tier", "lifetime_net_spend", "country_rank"
    )


def calculate_rolling_order_counts(orders_df: DataFrame) -> DataFrame:
    order_level = orders_df.dropDuplicates(["order_id"]).withColumn("order_ts", F.col("order_date").cast("timestamp").cast("long"))
    seven_days_seconds = 7 * 24 * 60 * 60
    rolling_window = Window.partitionBy("customer_id").orderBy("order_ts").rangeBetween(-seven_days_seconds, 0)
    return order_level.withColumn("rolling_7_day_order_count", F.count("order_id").over(rolling_window)).select(
        "customer_id", "order_id", "order_date", "rolling_7_day_order_count"
    )


def calculate_category_monthly_revenue_share(enriched_df: DataFrame) -> DataFrame:
    monthly_category = (
        enriched_df.groupBy("order_year", "order_month", "category")
        .agg(F.round(F.sum("line_item_amount"), 2).alias("category_revenue"))
    )
    month_window = Window.partitionBy("order_year", "order_month")
    return (
        monthly_category.withColumn("monthly_total_revenue", F.round(F.sum("category_revenue").over(month_window), 2))
        .withColumn(
            "category_revenue_share",
            F.round(F.col("category_revenue") / F.col("monthly_total_revenue"), 6),
        )
        .select("order_year", "order_month", "category", "category_revenue", "monthly_total_revenue", "category_revenue_share")
    )
