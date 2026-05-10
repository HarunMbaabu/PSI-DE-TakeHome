"""CSV ingestion and rejected-record detection."""

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructType

DATE_FORMATS = ("yyyy-MM-dd", "dd/MM/yyyy")


def _string_schema(schema: StructType) -> StructType:
    """Return a schema with the same field names but StringType columns."""
    from pyspark.sql.types import StringType, StructField

    return StructType([StructField(field.name, StringType(), True) for field in schema.fields])


def read_csv_with_schema(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Read a CSV file with an explicit schema and permissive parsing."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def read_csv_as_strings(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Read all fields as strings so rejected-record logic can inspect raw values."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(_string_schema(schema))
        .csv(path)
    )


def _parsed_date_expr(column_name: str):
    return F.coalesce(*[F.to_date(F.col(column_name), fmt) for fmt in DATE_FORMATS])


def _reason_expr(reason: str, condition):
    return F.when(condition, F.lit(reason))


def identify_rejected_records(
    raw_df: DataFrame,
    schema: StructType,
    required_columns: Iterable[str],
    date_columns: Iterable[str],
) -> DataFrame:
    """Identify rows with missing IDs, invalid dates, or invalid numeric casts.

    Spark CSV schema coercion can convert bad numerics to NULL. This function runs
    against an all-string read so bad source values are not lost before rejection.
    """
    reason_columns = []

    for column_name in required_columns:
        reason_columns.append(
            _reason_expr(
                f"missing_required_{column_name}",
                F.col(column_name).isNull() | (F.trim(F.col(column_name)) == ""),
            )
        )

    for column_name in date_columns:
        reason_columns.append(
            _reason_expr(
                f"invalid_{column_name}",
                F.col(column_name).isNotNull()
                & (F.trim(F.col(column_name)) != "")
                & _parsed_date_expr(column_name).isNull(),
            )
        )

    for field in schema.fields:
        if isinstance(field.dataType, (DoubleType, IntegerType)):
            reason_columns.append(
                _reason_expr(
                    f"invalid_numeric_{field.name}",
                    F.col(field.name).isNotNull()
                    & (F.trim(F.col(field.name)) != "")
                    & F.col(field.name).cast(field.dataType).isNull(),
                )
            )

    if "total_amount" in raw_df.columns:
        reason_columns.append(_reason_expr("missing_total_amount", F.col("total_amount").isNull() | (F.trim(F.col("total_amount")) == "")))

    return raw_df.withColumn("rejection_reason", F.concat_ws("; ", *reason_columns)).where(
        F.col("rejection_reason") != ""
    )


def valid_rows_from_raw(raw_df: DataFrame, rejected_df: DataFrame, schema: StructType) -> DataFrame:
    """Remove rejected raw rows and cast remaining rows to the expected explicit schema."""
    valid_raw = raw_df.join(rejected_df.select("_source_row_id"), "_source_row_id", "left_anti")
    return valid_raw.select([F.col(field.name).cast(field.dataType).alias(field.name) for field in schema.fields])


def read_dataset_with_rejections(
    spark: SparkSession,
    path: str,
    schema: StructType,
    required_columns: Iterable[str],
    date_columns: Iterable[str],
) -> tuple[DataFrame, DataFrame]:
    """Read data and return `(typed_valid_df, rejected_df)` without using inferSchema."""
    # The typed read enforces the assignment requirement and catches malformed CSV rows permissively.
    _ = read_csv_with_schema(spark, path, schema)
    raw_df = read_csv_as_strings(spark, path, schema).withColumn("_source_row_id", F.monotonically_increasing_id())
    rejected_df = identify_rejected_records(raw_df, schema, required_columns, date_columns)
    valid_df = valid_rows_from_raw(raw_df, rejected_df, schema)
    return valid_df, rejected_df
