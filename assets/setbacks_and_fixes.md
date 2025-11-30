# Setbacks & fixes while building the Lakeflow demo (Free Edition)

This file documents the main issues I hit while reproducing the project on
**Databricks Free Edition** and how each one was fixed. The idea is that anyone
following the repo knows in advance what can go wrong and why.

---

## 1. Editable pip install fails in pipeline environment

**Symptom**

First pipeline run failed with:

- `ENVIRONMENT_PIP_INSTALL_ERROR`
- Message showed a command like:
  `pip install --editable /Workspace/.../rvmlakeflow`

**Root cause**

The auto-generated asset bundle template added an editable dependency:

```yaml
environment:
  dependencies:
    - --editable ${workspace.file_path}
```

On Free Edition Lakeflow clusters that `pip install --editable` is not supported.

**Fix**

Removed the entire `environment.dependencies` block from
`rvmlakeflow_etl.pipeline.yml`. The project does not need a separate package
install; the pipeline loads code directly from the `transformations/` folder.

---

## 2. Pipeline still shows old dependency after editing

**Symptom**

Even after editing the pipeline YAML, the UI pipeline still tried to run
`pip install --editable ...`.

**Root cause**

The pipeline had been **edited directly in the UI** using
“Unlock and edit anyway”, which broke the link with the asset bundle.

**Fix**

- Committed the YAML changes to Git.
- In the pipeline UI, clicked **“Redeploy to source”** (banner at the top).
- After redeploy, the UI pipeline reflected the bundle definition and the
  dependency disappeared.

---

## 3. Relative import error between `bronze.py` and `common.py`

**Symptom**

Bronze tables failed with:

- `ImportError: attempted relative import with no known parent package`

**Root cause**

The first version used relative imports like:

```python
from .common import source_table
```

In the Lakeflow runtime the standalone files were not treated as a regular
Python package in the way the import expected.

**Fix**

Switched to a simpler pattern:

- Keep helpers in `common.py`
- Use pure module import:

```python
from common import source_table, with_ingest_metadata
```

And implemented a tiny Spark accessor there:

```python
from pyspark.sql import SparkSession

def _get_spark() -> SparkSession:
    return SparkSession.getActiveSession()
```

All other modules call `_get_spark()` instead of relying on Spark globals.

---

## 4. `input_file_name` not supported in Unity Catalog

**Symptom**

Bronze layer produced an error:

> The command(s): input_file_name are not supported in Unity Catalog.  
> Please use _metadata.file_path instead.

**Root cause**

Original code used:

```python
F.input_file_name().alias("_ingest_file")
```

But when reading Unity Catalog tables, `input_file_name()` is not allowed.

**Fix**

Replaced the ingest helper in `common.py` with:

```python
from pyspark.sql import functions as F

def with_ingest_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_ingest_file", F.col("_metadata.file_path"))
    )
```

Now `_ingest_file` comes from the Unity Catalog `_metadata` column.

---

## 5. `spark` not defined inside helper functions

**Symptom**

After moving logic to `common.py`, Bronze failed with:

- `NameError: name 'spark' is not defined`

**Root cause**

Helpers referenced `spark.table(...)` without defining `spark` in that scope.

**Fix**

Centralised Spark access in `common._get_spark()`:

```python
from pyspark.sql import SparkSession

def _get_spark() -> SparkSession:
    return SparkSession.getActiveSession()
```

Then:

```python
spark = _get_spark()
df = spark.table(full_name)
```

This works both in notebooks and in Lakeflow.

---

## 6. Silver expectations refer to old column names

**Symptom**

Silver pipeline showed expectation errors such as:

- Column `c_custkey` not found in condition `customer_pk_not_null`.

**Root cause**

I  renamed columns while building dim tables (e.g. `c_custkey` → `customer_id`)
but the expectations still referenced the original TPC-H column names.

**Fix**

Updated the expectations to use the *renamed* columns, for example:

```python
@dp.expect("customer_pk_not_null", "customer_id IS NOT NULL")
@dp.materialized_view(name="dim_customer")
def dim_customer() -> DataFrame:
    ...
    col("c_custkey").alias("customer_id"),
    ...
```

Same pattern for part, supplier, nation, region, calendar and orders.

---

## 7. Calendar function returned `None`

**Symptom**

Silver run failed with:

> Query defined in function '_wrapped_query_fn' returned 'NoneType'

**Root cause**

While refactoring `dim_calendar`, the final `return` statement was accidentally
removed, so the function returned `None`.

**Fix**

Restored the proper return:

```python
@dp.materialized_view(name="dim_calendar", ...)
def dim_calendar() -> DataFrame:
    df = bronze_table("bronze_orders")
    return (
        df.select(col("o_orderdate").alias("date"))
          .distinct()
          .withColumn("year", year(col("date")))
          ...
    )
```

---

## 8. Fact table joins directly on Bronze instead of Silver

**Symptom**

The first version of `fact_lineitem` joined `bronze_lineitem` directly with
`bronze_orders`. It worked, but bypassed Silver logic and naming, making the
model harder to reason about.

**Fix**

Refactored to:

- Use `dim_orders` (Silver) as the header table.
- Keep `bronze_lineitem` as the measure source.
- Move join logic into `_fact_lineitem_base()` that both the main fact and
  quarantine reuse.

This keeps **Silver as the single source of truth** for cleaned dimensions,
while still leveraging the raw lineitem measures.

---

## 9. Quarantine vs expectations behavior

**Design choice**

By default, `@dp.expect` is **informational only**: it records metrics but does
not drop data. To avoid silently losing rows we explicitly separated:

- `fact_lineitem` - all rows
- `fact_lineitem_quarantine` - only rows that fail the condition:

```python
_FACT_VALID_CONDITION = (
    "quantity > 0 AND net_revenue >= 0 AND order_date IS NOT NULL"
)
```

This pattern keeps production facts intact while still giving a dedicated place
to analyse bad data.

---

## 10. Incremental vs full refresh behavior

**Observation**

After adding Silver and Gold, the pipeline UI showed:

- Bronze tables: `Full recompute`
- Most Silver dims + fact: `Incremental`
- Some tables (like `dim_orders`) initially showed as full recompute.

**Root cause**

Incrementality is inferred by Lakeflow based on:

- Lineage graph
- Source types
- Presence of stable keys

It is not manually configured in this project.

**Takeaway**

For this learning project I accepted the default behavior and kept the code
simple. If you need strict control over incremental strategy, you can extend
the design with surrogate keys, CDC or explicit incremental patterns but that
is beyond the scope of this demo.