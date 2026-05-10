"""Quality gate assertions for final analytical output."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _assert_no_rows(df: DataFrame, message: str) -> None:
    """Raise ValueError if the DataFrame contains at least one row."""
    if df.limit(1).count() > 0:
        raise ValueError(message)


def run_quality_gate(final_df: DataFrame) -> None:
    """Validate final enriched output before writing it to Parquet."""
    _assert_no_rows(
        final_df.where(F.col("country").isNull() | F.col("customer_tier").isNull() | F.col("email").isNull()),
        "Quality gate failed: at least one joined order is missing customer information.",
    )
    _assert_no_rows(final_df.where(F.col("net_amount").isNull()), "Quality gate failed: net_amount contains NULL values.")
    _assert_no_rows(final_df.where(F.col("net_amount") < 0), "Quality gate failed: net_amount contains negative values.")
