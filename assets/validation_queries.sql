-- Validation queries for the Databricks TPC-H Lakeflow demo
-- Catalog/schema assumed: workspace.lf_demo

------------------------------------------------------------
-- 1. Check that all core tables exist
------------------------------------------------------------
SHOW TABLES IN workspace.lf_demo;

SHOW TABLES IN workspace.lf_demo LIKE 'bronze_%';
SHOW TABLES IN workspace.lf_demo LIKE 'dim_%';
SHOW TABLES IN workspace.lf_demo LIKE 'fact_lineitem%';
SHOW TABLES IN workspace.lf_demo LIKE 'gold_%';

------------------------------------------------------------
-- 2. Basic row count sanity checks
------------------------------------------------------------
SELECT 'bronze_customer' AS table_name, COUNT(*) AS cnt
FROM workspace.lf_demo.bronze_customer
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM workspace.lf_demo.dim_customer
UNION ALL
SELECT 'bronze_orders', COUNT(*) FROM workspace.lf_demo.bronze_orders
UNION ALL
SELECT 'dim_orders', COUNT(*) FROM workspace.lf_demo.dim_orders
UNION ALL
SELECT 'bronze_lineitem', COUNT(*) FROM workspace.lf_demo.bronze_lineitem
UNION ALL
SELECT 'fact_lineitem', COUNT(*) FROM workspace.lf_demo.fact_lineitem
UNION ALL
SELECT 'fact_lineitem_quarantine', COUNT(*) FROM workspace.lf_demo.fact_lineitem_quarantine;

------------------------------------------------------------
-- 3. Expectations-style checks in SQL
------------------------------------------------------------

-- Silver dimension PKs not null
SELECT
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id
FROM workspace.lf_demo.dim_customer;

SELECT
  SUM(CASE WHEN part_id IS NULL THEN 1 ELSE 0 END) AS null_part_id
FROM workspace.lf_demo.dim_part;

SELECT
  SUM(CASE WHEN supplier_id IS NULL THEN 1 ELSE 0 END) AS null_supplier_id
FROM workspace.lf_demo.dim_supplier;

SELECT
  SUM(CASE WHEN nation_id IS NULL THEN 1 ELSE 0 END) AS null_nation_id
FROM workspace.lf_demo.dim_nation;

SELECT
  SUM(CASE WHEN region_id IS NULL THEN 1 ELSE 0 END) AS null_region_id
FROM workspace.lf_demo.dim_region;

SELECT
  SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date
FROM workspace.lf_demo.dim_calendar;

SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id
FROM workspace.lf_demo.dim_orders;

-- Fact expectations
SELECT
  SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END)        AS bad_quantity,
  SUM(CASE WHEN net_revenue < 0 THEN 1 ELSE 0 END)      AS bad_net_revenue,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END)   AS missing_order_date
FROM workspace.lf_demo.fact_lineitem;

------------------------------------------------------------
-- 4. Inspect quarantine data
------------------------------------------------------------
SELECT COUNT(*) AS quarantined_rows
FROM workspace.lf_demo.fact_lineitem_quarantine;

SELECT *
FROM workspace.lf_demo.fact_lineitem_quarantine
LIMIT 100;

------------------------------------------------------------
-- 5. Revenue sanity checks
------------------------------------------------------------

-- Total revenue by year
SELECT
  c.year,
  SUM(f.net_revenue)      AS net_revenue,
  SUM(f.gross_revenue)    AS gross_revenue,
  COUNT(*)                AS rows
FROM workspace.lf_demo.fact_lineitem f
JOIN workspace.lf_demo.dim_calendar c
  ON f.order_date = c.date
GROUP BY c.year
ORDER BY c.year;

-- Revenue by region
SELECT
  r.region_name,
  SUM(f.net_revenue) AS net_revenue
FROM workspace.lf_demo.fact_lineitem f
JOIN workspace.lf_demo.dim_customer  c ON f.customer_id = c.customer_id
JOIN workspace.lf_demo.dim_nation    n ON c.nation_id   = n.nation_id
JOIN workspace.lf_demo.dim_region    r ON n.region_id   = r.region_id
GROUP BY r.region_name
ORDER BY net_revenue DESC;

------------------------------------------------------------
-- 6. Gold table inspections
------------------------------------------------------------

-- Daily sales
SELECT * FROM workspace.lf_demo.gold_daily_sales ORDER BY order_date LIMIT 100;

-- Sales by segment
SELECT * FROM workspace.lf_demo.gold_sales_by_segment ORDER BY net_revenue DESC LIMIT 100;

-- Sales by region
SELECT * FROM workspace.lf_demo.gold_sales_by_region ORDER BY net_revenue DESC LIMIT 100;

-- Top customers
SELECT * FROM workspace.lf_demo.gold_top_customers ORDER BY net_revenue DESC LIMIT 100;