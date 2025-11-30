"""Gold layer (business-friendly aggregates) for the TPC-H Lakeflow demo."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as F_sum, when

from common import _get_spark


def _table(name: str) -> DataFrame:
    """Small helper to read tables created in this pipeline."""
    spark = _get_spark()
    return spark.table(name)


# Sales by year/month (calendar)

@dp.materialized_view(
    name="gold_sales_by_year_month",
    comment="Net and gross revenue aggregated by calendar year and month.",
)
def gold_sales_by_year_month() -> DataFrame:
    fact = _table("fact_lineitem").alias("f")
    cal = _table("dim_calendar").alias("cal")

    joined = fact.join(cal, col("f.order_date") == col("cal.date"), "inner")

    return (
        joined.groupBy("year", "month", "month_name")
        .agg(
            F_sum("net_revenue").alias("net_revenue"),
            F_sum("gross_revenue").alias("gross_revenue"),
        )
    )


# Top 10 customers by revenue

@dp.materialized_view(
    name="gold_top_customers",
    comment="Top 10 customers by net revenue.",
)
def gold_top_customers() -> DataFrame:
    fact = _table("fact_lineitem").alias("f")
    cust = _table("dim_customer").alias("cust")

    aggregated = (
        fact.join(cust, "customer_id", "inner")
        .groupBy("customer_id", "customer_name")
        .agg(F_sum("net_revenue").alias("net_revenue"))
        .orderBy(col("net_revenue").desc())
        .limit(10)
    )

    return aggregated


# Revenue by region and year

@dp.materialized_view(
    name="gold_sales_by_region_year",
    comment="Net revenue by region and year.",
)
def gold_sales_by_region_year() -> DataFrame:
    fact = _table("fact_lineitem").alias("f")
    cust = _table("dim_customer").alias("cust")
    nation = _table("dim_nation").alias("n")
    region = _table("dim_region").alias("r")
    cal = _table("dim_calendar").alias("cal")

    joined = (
        fact.join(cust, "customer_id")
        .join(nation, "nation_id")
        .join(region, "region_id")
        .join(cal, col("f.order_date") == col("cal.date"), "inner")
    )

    return (
        joined.groupBy("region_name", "year")
        .agg(F_sum("net_revenue").alias("net_revenue"))
    )


# Revenue distribution by discount bands

@dp.materialized_view(
    name="gold_discount_bands",
    comment="Net revenue distribution across discount bands.",
)
def gold_discount_bands() -> DataFrame:
    fact = _table("fact_lineitem")

    discount_band = (
        when(col("discount") < 0.02, "< 2%")
        .when(col("discount") < 0.05, "2% - 5%")
        .when(col("discount") < 0.10, "5% - 10%")
        .otherwise(">= 10%")
        .alias("discount_band")
    )

    return (
        fact.select(discount_band, col("net_revenue"))
        .groupBy("discount_band")
        .agg(F_sum("net_revenue").alias("net_revenue"))
    )