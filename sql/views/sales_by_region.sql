-- Materialized at deploy time by executing this DDL (see template scope note).
CREATE OR REPLACE VIEW sales_by_region AS
SELECT region,
       date_trunc('month', sale_date) AS sales_month,
       SUM(revenue) AS total_revenue,
       SUM(units)   AS total_units
FROM sales_curated
GROUP BY 1, 2;
