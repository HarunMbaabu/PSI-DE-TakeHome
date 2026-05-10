"""Join and enrichment transformations."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def enrich_orders_with_customers(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    """Left join orders to customers and calculate order-level derived fields."""
    # Left join preserves every valid order; the DQ gate later verifies all orders found customer details.
    # customers is a small dimension table, so broadcasting avoids a large shuffle during the orders-to-customers join.
    return (
        orders_df.join(broadcast(customers_df), "customer_id", "left")
        .withColumn("net_amount", F.round(F.col("total_amount") * (F.lit(1.0) - F.col("discount_pct") / F.lit(100.0)), 2))
        .withColumn("order_year", F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
    )


def find_orphaned_order_items(order_items_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """Find order items whose order_id is absent from cleaned orders."""
    return order_items_df.join(orders_df.select("order_id").dropDuplicates(), "order_id", "left_anti")


def create_final_enriched_dataset(orders_customers_df: DataFrame, order_items_df: DataFrame) -> DataFrame:
    """Create the item-level analytical table using only items with valid orders."""
    return (
        orders_customers_df.join(order_items_df, "order_id", "inner")
        .withColumn("line_item_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .select(
            "order_id",
            "customer_id",
            "order_date",
            "order_year",
            "order_month",
            "status",
            "total_amount",
            "discount_pct",
            "net_amount",
            "is_negative_amount",
            "country",
            "customer_tier",
            "signup_date",
            "email",
            "item_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_item_amount",
            "category",
        )
    )


def separate_negative_net_amount(enriched_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split suspicious negative net amount rows from final DQ-ready analytical rows."""
    suspicious = enriched_df.where(F.col("net_amount") < F.lit(0.0))
    valid = enriched_df.where(F.col("net_amount").isNotNull() & (F.col("net_amount") >= F.lit(0.0)))
    return valid, suspicious
