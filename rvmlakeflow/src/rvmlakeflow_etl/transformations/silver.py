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


@dp.expect(
    "customer_pk_not_null",
    "customer_id IS NOT NULL",
)

@dp.materialized_view(
    name="dim_customer",
    comment="Customer dimension built from bronze_customer.",
)
def dim_customer() -> DataFrame:
    df = bronze_table("bronze_customer")

    return df.select(
        col("c_custkey").alias("customer_id"),
        col("c_name").alias("customer_name"),
        col("c_address").alias("customer_address"),
        col("c_nationkey").alias("nation_id"),
        col("c_phone").alias("customer_phone"),
        col("c_acctbal").alias("account_balance"),
        col("c_mktsegment").alias("market_segment"),
        col("_ingest_ts"),
        col("_ingest_file"),
    )


@dp.expect(
    "part_pk_not_null",
    "part_id IS NOT NULL",
)

@dp.materialized_view(
    name="dim_part",
    comment="Part dimension built from bronze_part.",
)
def dim_part() -> DataFrame:
    df = bronze_table("bronze_part")

    return df.select(
        col("p_partkey").alias("part_id"),
        col("p_name").alias("part_name"),
        col("p_mfgr").alias("manufacturer"),
        col("p_brand").alias("brand"),
        col("p_type").alias("part_type"),
        col("p_size").alias("part_size"),
        col("p_container").alias("container"),
        col("p_retailprice").alias("retail_price"),
        col("_ingest_ts"),
        col("_ingest_file"),
    )

@dp.expect(
    "supplier_pk_not_null",
    "supplier_id IS NOT NULL",
)
@dp.materialized_view(
    name="dim_supplier",
    comment="Supplier dimension built from bronze_supplier.",
)
def dim_supplier() -> DataFrame:
    df = bronze_table("bronze_supplier")

    return df.select(
        col("s_suppkey").alias("supplier_id"),
        col("s_name").alias("supplier_name"),
        col("s_address").alias("supplier_address"),
        col("s_nationkey").alias("nation_id"),
        col("s_phone").alias("supplier_phone"),
        col("s_acctbal").alias("account_balance"),
        col("_ingest_ts"),
        col("_ingest_file"),
    )

@dp.expect(
    "nation_pk_not_null",
    "nation_id IS NOT NULL",
)
@dp.materialized_view(
    name="dim_nation",
    comment="Nation dimension built from bronze_nation.",
)
def dim_nation() -> DataFrame:
    df = bronze_table("bronze_nation")

    return df.select(
        col("n_nationkey").alias("nation_id"),
        col("n_name").alias("nation_name"),
        col("n_regionkey").alias("region_id"),
        col("n_comment").alias("nation_comment"),
        col("_ingest_ts"),
        col("_ingest_file"),
    )

@dp.expect(
    "region_pk_not_null",
    "region_id IS NOT NULL",
)
@dp.materialized_view(
    name="dim_region",
    comment="Region dimension built from bronze_region.",
)
def dim_region() -> DataFrame:
    df = bronze_table("bronze_region")

    return df.select(
        col("r_regionkey").alias("region_id"),
        col("r_name").alias("region_name"),
        col("r_comment").alias("region_comment"),
        col("_ingest_ts"),
        col("_ingest_file"),
    )

@dp.expect(
    "calendar_date_not_null",
    "date IS NOT NULL",
)
@dp.materialized_view(
    name="dim_calendar",
    comment="Calendar dimension derived from order dates in bronze_orders.",
)
def dim_calendar() -> DataFrame:
    df = bronze_table("bronze_orders")

    return (
        df.select(col("o_orderdate").alias("date"))
        .distinct()
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMM"))
    )

@dp.expect(
    "order_pk_not_null",
    "order_id IS NOT NULL",
)
@dp.materialized_view(
    name="dim_orders",
    comment="Order header dimension built from bronze_orders.",
)
def dim_orders() -> DataFrame:
    df = bronze_table("bronze_orders")

    return df.select(
        col("o_orderkey").alias("order_id"),
        col("o_custkey").alias("customer_id"),
        col("o_orderstatus").alias("order_status"),
        col("o_orderdate").alias("order_date"),
        col("o_orderpriority").alias("order_priority"),
        col("o_clerk").alias("order_clerk"),
        col("o_shippriority").alias("ship_priority"),
        col("o_comment").alias("comment"),
        "_ingest_ts",
        "_ingest_file",
    )

# Reusable condition for basic data quality
_FACT_VALID_CONDITION = (
    "quantity > 0 AND net_revenue >= 0 AND order_date IS NOT NULL"
)


@dp.expect(
    "quantity_positive",
    "quantity > 0",
)
@dp.expect(
    "net_revenue_non_negative",
    "net_revenue >= 0",
)
@dp.expect(
    "order_date_not_null",
    "order_date IS NOT NULL",
)
@dp.materialized_view(
    name="fact_lineitem",
    comment="Sales fact table (lineitem grain) built from bronze_lineitem + dim_orders.",
)
def fact_lineitem() -> DataFrame:
    """Main line-level fact table."""
    lineitem = bronze_table("bronze_lineitem").alias("l")

    spark = _get_spark()
    orders = spark.table("dim_orders").alias("o")

    joined = lineitem.join(
        orders,
        col("l.l_orderkey") == col("o.order_id"),
        "inner",
    )

    return joined.select(
        # Keys
        col("o.order_id").alias("order_id"),
        col("l.l_linenumber").alias("line_number"),
        col("o.customer_id").alias("customer_id"),
        col("l.l_partkey").alias("part_id"),
        col("l.l_suppkey").alias("supplier_id"),

        # Dates
        col("o.order_date").alias("order_date"),
        col("l.l_shipdate").alias("ship_date"),
        col("l.l_commitdate").alias("commit_date"),

        # Line attributes
        col("l.l_returnflag").alias("return_flag"),
        col("l.l_linestatus").alias("line_status"),

        # Measures
        col("l.l_quantity").cast("double").alias("quantity"),
        col("l.l_extendedprice").cast("double").alias("extended_price"),
        col("l.l_discount").cast("double").alias("discount"),
        col("l.l_tax").cast("double").alias("tax"),

        # Derived measures
        (col("l.l_extendedprice") * (1 - col("l.l_discount"))).alias("net_revenue"),
        (
            col("l.l_extendedprice")
            * (1 - col("l.l_discount"))
            * (1 + col("l.l_tax"))
        ).alias("gross_revenue"),

        # Metadata
        col("l._ingest_ts"),
        col("l._ingest_file"),
    )


@dp.materialized_view(
    name="fact_lineitem_quarantine",
    comment=(
        "Rows from fact_lineitem that do NOT pass basic data-quality rules: "
        f"({_FACT_VALID_CONDITION})."
    ),
)
def fact_lineitem_quarantine() -> DataFrame:
    """Rows that do not meet basic data quality rules."""
    spark = _get_spark()
    df = spark.table("fact_lineitem")
    return df.filter(f"NOT ({_FACT_VALID_CONDITION})")
