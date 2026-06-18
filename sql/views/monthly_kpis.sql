-- Monthly KPI trend: revenue, cost, profit, margin %, orders, avg order value.
CREATE OR REPLACE VIEW monthly_kpis AS
SELECT load_year,
       load_month,
       COUNT(*)                                AS orders,
       SUM(units)                              AS units,
       ROUND(SUM(revenue), 2)                  AS revenue,
       ROUND(SUM(cost), 2)                     AS cost,
       ROUND(SUM(profit), 2)                   AS profit,
       ROUND(100.0 * SUM(profit) / SUM(revenue), 1) AS margin_pct,
       ROUND(SUM(revenue) / COUNT(*), 2)       AS avg_order_value
FROM sales_curated
GROUP BY load_year, load_month;
