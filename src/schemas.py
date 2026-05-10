"""Explicit schemas for the e-commerce CSV inputs."""

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

orders_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
    ]
)

order_items_schema = StructType(
    [
        StructField("item_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("category", StringType(), True),
    ]
)

customers_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_tier", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

returns_schema = StructType(
    [
        StructField("return_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("return_date", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("refund_amount", DoubleType(), True),
    ]
)

SCHEMAS = {
    "orders": orders_schema,
    "order_items": order_items_schema,
    "customers": customers_schema,
    "returns": returns_schema,
}
