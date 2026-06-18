-- Freshness: the most recent sale must be within 2 days.
SELECT date_diff('day', MAX(sale_date), current_date) <= 2          AS passed,
       CAST(date_diff('day', MAX(sale_date), current_date) AS varchar) AS detail
FROM sales_curated;
