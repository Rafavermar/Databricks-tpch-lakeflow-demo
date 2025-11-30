"""
Bronze layer for the TPCH Lakeflow demo.

Copies the tables from `samples.tpch` adding ingestion metadata.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from common import source_table, with_ingest_metadata


@dp.materialized_view(
    name="bronze_customer",
    comment="Raw TPCH customer with ingestion metadata.",
)
def bronze_customer() -> DataFrame:
    base_df = source_table("customer")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_orders",
    comment="Raw TPCH orders with ingestion metadata.",
)
def bronze_orders() -> DataFrame:
    base_df = source_table("orders")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_lineitem",
    comment="Raw TPCH lineitem with ingestion metadata.",
)
def bronze_lineitem() -> DataFrame:
    base_df = source_table("lineitem")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_part",
    comment="Raw TPCH part with ingestion metadata.",
)
def bronze_part() -> DataFrame:
    base_df = source_table("part")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_supplier",
    comment="Raw TPCH supplier with ingestion metadata.",
)
def bronze_supplier() -> DataFrame:
    base_df = source_table("supplier")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_nation",
    comment="Raw TPCH nation with ingestion metadata.",
)
def bronze_nation() -> DataFrame:
    base_df = source_table("nation")
    return with_ingest_metadata(base_df)


@dp.materialized_view(
    name="bronze_region",
    comment="Raw TPCH region with ingestion metadata.",
)
def bronze_region() -> DataFrame:
    base_df = source_table("region")
    return with_ingest_metadata(base_df)