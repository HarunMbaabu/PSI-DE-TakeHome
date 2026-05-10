"""SparkSession factory for local pipeline and tests."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "PSI E-Commerce ETL") -> SparkSession:
    """Create a local SparkSession suitable for PySpark 3.4+ development."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
