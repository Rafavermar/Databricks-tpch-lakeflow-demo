# silver.py
"""Silver layer (dimensional model) for the TPC-H Lakeflow demo."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    weekofyear,
    quarter,
    date_format,
)

from common import _get_spark


def bronze_table(table_name: str) -> DataFrame:
    """Convenience helper to read bronze tables from this pipeline."""
    spark = _get_spark()
    return spark.table(table_name)


@dp.materialized_view(
    name="dim_customer",
    comment="Customer dimension built from bronze_customer.",
)
def dim_customer() -> DataFrame:
    df = bronze_table("bronze_customer")

    return df.select(
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "_ingest_ts",
        "_ingest_file",
    )


@dp.materialized_view(
    name="dim_part",
    comment="Part dimension built from bronze_part.",
)
def dim_part() -> DataFrame:
    df = bronze_table("bronze_part")

    return df.select(
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "_ingest_ts",
        "_ingest_file",
    )


@dp.materialized_view(
    name="dim_supplier",
    comment="Supplier dimension built from bronze_supplier.",
)
def dim_supplier() -> DataFrame:
    df = bronze_table("bronze_supplier")

    return df.select(
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "_ingest_ts",
        "_ingest_file",
    )


@dp.materialized_view(
    name="dim_nation",
    comment="Nation dimension built from bronze_nation.",
)
def dim_nation() -> DataFrame:
    df = bronze_table("bronze_nation")

    return df.select(
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment",
        "_ingest_ts",
        "_ingest_file",
    )


@dp.materialized_view(
    name="dim_region",
    comment="Region dimension built from bronze_region.",
)
def dim_region() -> DataFrame:
    df = bronze_table("bronze_region")

    return df.select(
        "r_regionkey",
        "r_name",
        "r_comment",
        "_ingest_ts",
        "_ingest_file",
    )


@dp.materialized_view(
    name="dim_calendar",
    comment="Calendar dimension derived from order dates in bronze_orders.",
)
def dim_calendar() -> DataFrame:
    df = bronze_table("bronze_orders")

    calendar = (
        df.select(col("o_orderdate").alias("date"))
        .distinct()
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMM"))
    )

    return calendar