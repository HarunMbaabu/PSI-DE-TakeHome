"""Main entry point for the PSI e-commerce PySpark ETL pipeline."""

import argparse
from pathlib import Path

from src.aggregations import (
    calculate_category_monthly_revenue_share,
    calculate_customer_lifetime_rankings,
    calculate_rolling_order_counts,
)
from src.cleaning import clean_customers, clean_order_items, clean_orders, clean_returns
from src.ingestion import read_dataset_with_rejections
from src.quality_gates import run_quality_gate
from src.returns_analysis import (
    calculate_return_rate_by_category,
    calculate_return_rate_by_customer_tier,
    calculate_top_refund_customers,
    identify_refund_anomalies,
)
from src.schemas import customers_schema, order_items_schema, orders_schema, returns_schema
from src.spark_session import create_spark_session
from src.transformations import (
    create_final_enriched_dataset,
    enrich_orders_with_customers,
    find_orphaned_order_items,
    separate_negative_net_amount,
)
from src.writer import write_csv, write_parquet_partitioned, write_rejected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PSI e-commerce PySpark ETL pipeline.")
    parser.add_argument("--input", default="data", help="Input directory containing source CSV files.")
    parser.add_argument("--output", default="output", help="Output directory for Parquet and CSV results.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input)
    output_dir = Path(args.output)

    spark = create_spark_session()
    try:
        orders_raw, orders_rejected = read_dataset_with_rejections(
            spark, str(input_dir / "orders.csv"), orders_schema, ["order_id", "customer_id"], ["order_date"]
        )
        order_items_raw, order_items_rejected = read_dataset_with_rejections(
            spark, str(input_dir / "order_items.csv"), order_items_schema, ["item_id", "order_id"], []
        )
        customers_raw, customers_rejected = read_dataset_with_rejections(
            spark, str(input_dir / "customers.csv"), customers_schema, ["customer_id"], ["signup_date"]
        )
        returns_raw, returns_rejected = read_dataset_with_rejections(
            spark, str(input_dir / "returns.csv"), returns_schema, ["return_id", "order_id"], ["return_date"]
        )

        write_rejected(orders_rejected, str(output_dir / "rejected" / "orders"))
        write_rejected(order_items_rejected, str(output_dir / "rejected" / "order_items"))
        write_rejected(customers_rejected, str(output_dir / "rejected" / "customers"))
        write_rejected(returns_rejected, str(output_dir / "rejected" / "returns"))

        orders = clean_orders(orders_raw)
        order_items = clean_order_items(order_items_raw)
        customers = clean_customers(customers_raw)
        returns = clean_returns(returns_raw)

        orphaned_items = find_orphaned_order_items(order_items, orders)
        write_csv(orphaned_items, str(output_dir / "orphaned_order_items"))

        orders_customers = enrich_orders_with_customers(orders, customers)
        enriched_items = create_final_enriched_dataset(orders_customers, order_items)

        print("Formatted explain plan for final item-level enriched dataset:")
        enriched_items.explain(mode="formatted")

        final_enriched, negative_net_amount = separate_negative_net_amount(enriched_items)
        write_rejected(negative_net_amount, str(output_dir / "rejected" / "negative_net_amount"))

        run_quality_gate(final_enriched)
        write_parquet_partitioned(final_enriched, str(output_dir / "enriched_orders"))

        order_level = final_enriched.dropDuplicates(["order_id"])

        write_csv(
            calculate_customer_lifetime_rankings(final_enriched),
            str(output_dir / "summaries" / "customer_lifetime_rankings"),
        )
        write_csv(
            calculate_rolling_order_counts(order_level),
            str(output_dir / "summaries" / "customer_7_day_rolling_orders"),
        )
        write_csv(
            calculate_category_monthly_revenue_share(final_enriched),
            str(output_dir / "summaries" / "category_monthly_revenue_share"),
        )
        write_csv(
            calculate_return_rate_by_category(final_enriched, returns),
            str(output_dir / "summaries" / "return_rate_by_category"),
        )
        write_csv(
            calculate_return_rate_by_customer_tier(order_level, returns),
            str(output_dir / "summaries" / "return_rate_by_customer_tier"),
        )
        write_csv(
            calculate_top_refund_customers(order_level, returns),
            str(output_dir / "summaries" / "top_10_refund_customers"),
        )
        write_csv(
            identify_refund_anomalies(order_level, returns),
            str(output_dir / "summaries" / "refund_anomalies"),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
