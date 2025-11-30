"""
Common helpers for the TPCH Lakeflow demo.

Reusable functions for the different layers (bronze, silver, gold).
"""
from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# Source schema: sample TPCH tables that already exist in Databricks
_SOURCE_CATALOG = "samples"
_SOURCE_SCHEMA = "tpch"

def _get_spark() -> SparkSession:
    """
    Returns the active SparkSession. If it does not exist, creates one.
    This avoids relying on the global `spark` variable.
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return spark

    
def source_table(table_name: str) -> DataFrame:
    """
    Returns a DataFrame for a sample TPCH table.

    Parameters
    ----------
    table_name:
        Name of the table within `samples.tpch`, e.g.
        'customer', 'orders', etc.

    Returns
    -------
    DataFrame
        Original data from the requested table.
    """
    full_name = f"{_SOURCE_CATALOG}.{_SOURCE_SCHEMA}.{table_name}"
    return _get_spark().table(full_name)


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    """
    Adds standard ingest metadata columns.

    Currently only adds `ingest_ts` with the current timestamp.
    """
    return df.withColumn("ingest_ts", F.current_timestamp())